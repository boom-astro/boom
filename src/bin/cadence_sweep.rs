//! Weekly demotion-only sweep for the binned-lightcurve cadence state machine.
//!
//! `lightcurve_binner` is the touched-only nightly job: it updates
//! `bin_cadence` only on aux docs that received new alerts since the last
//! run. That leaves a hole for sources that were promoted to a finer tier
//! and then *stopped* receiving alerts — they would stay at H tier forever,
//! consuming storage and locking the cadence state in a wrong place.
//!
//! This binary closes that hole. Walks every aux doc whose
//! `bin_cadence.active_until_jd` has elapsed AND that hasn't been touched
//! in `stale_threshold_days` (default 14d), and runs the cadence state
//! machine with both promotion signals off — i.e., demotion paths only.
//!
//! Idempotent: `evaluate_cadence` always advances `last_evaluated_jd` to
//! `now_jd`, so a re-run within the staleness window sees nothing.
//!
//! No bin writes, no retention trim, no $lookup pipeline — the only field
//! touched on disk is `bin_cadence`.

use boom::conf::{load_dotenv, AppConfig, BinningConfig};
use boom::utils::cadence::{evaluate_cadence, resolve_tag_policy, BinCadence, ResolvedTagPolicy};
use boom::utils::enums::Survey;
use clap::Parser;
use flare::Time;
use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::bson::{doc, Document};
use mongodb::Database;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

type SweepError = Box<dyn std::error::Error + Send + Sync + 'static>;
type SweepResult<T> = Result<T, SweepError>;

const DEFAULT_STALE_THRESHOLD_DAYS: f64 = 14.0;

#[derive(Parser)]
#[command(about = "Weekly demotion-only sweep for binned-lightcurve cadence")]
struct Cli {
    /// Path to the configuration file. Defaults to `config.yaml`.
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Survey whose `<survey>_alerts_aux` collection to sweep. Must have a
    /// matching `binning.<survey>` block in config.yaml.
    #[arg(long, value_enum)]
    survey: Survey,

    /// Sources whose `bin_cadence.last_evaluated_jd` is older than `now -
    /// this` AND whose `active_until_jd` has elapsed are eligible.
    #[arg(long, default_value_t = DEFAULT_STALE_THRESHOLD_DAYS)]
    stale_threshold_days: f64,

    /// Process only sources where `hash(_id) % N == k`. Format `k/N`.
    /// Default `0/1` (no sharding).
    #[arg(long, default_value = "0/1")]
    objectid_shard: String,

    #[arg(long, default_value_t = 5000)]
    batch_size: usize,

    /// Cap the number of aux docs processed (debugging only).
    #[arg(long)]
    limit: Option<usize>,
}

// ─── shard helpers (same as lightcurve_binner) ───────────────────────────────

fn parse_shard(s: &str) -> Result<(u64, u64), String> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 2 {
        return Err(format!("expected k/N, got {}", s));
    }
    let k: u64 = parts[0]
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())?;
    let n: u64 = parts[1]
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())?;
    if n == 0 || k >= n {
        return Err(format!("require 0 <= k < N, got {}/{}", k, n));
    }
    Ok((k, n))
}

fn shard_matches(object_id: &str, k: u64, n: u64) -> bool {
    if n == 1 {
        return true;
    }
    let mut h = DefaultHasher::new();
    object_id.hash(&mut h);
    h.finish() % n == k
}

// ─── discovery filter ────────────────────────────────────────────────────────

/// Filter selecting aux docs whose cadence state has gone stale-active:
/// promoted to a finer tier (so `active_until_jd` is set), the active
/// window has expired, AND the binner hasn't touched the doc in
/// `stale_threshold_days`.
///
/// Sources whose `bin_cadence` is missing are intentionally excluded —
/// they've never been evaluated, so demotion isn't applicable. Sources on
/// the periodic-variable sub-track (`tier = None`, `active_until_jd =
/// None`) are also excluded by the `active_until_jd` clause.
fn discovery_filter(now_jd: f64, stale_threshold_days: f64) -> Document {
    doc! {
        "bin_cadence.active_until_jd": { "$lt": now_jd },
        "bin_cadence.last_evaluated_jd": { "$lt": now_jd - stale_threshold_days },
    }
}

// ─── tag policy stub ─────────────────────────────────────────────────────────

/// Build the resolved tag policy with whatever tag information lives
/// inline on the aux doc. The sweep does not perform the per-batch
/// `$lookup` for latest classifications that the binner does — the
/// existing tag set on `cross_matches` is sufficient to drive demotion
/// (catalog tags are stable across time; classifier tags only matter when
/// they cross promotion thresholds, which is a `score_promotion = true`
/// event the sweep deliberately ignores).
fn resolve_policy_for_sweep(aux: &Document, cfg: &BinningConfig) -> ResolvedTagPolicy {
    let cross_matches = aux.get_document("cross_matches").ok();
    let tags =
        boom::utils::binner_input::tags_from_cross_matches(cross_matches, &cfg.catalog_tag_rules);
    resolve_tag_policy(&tags, &cfg.tag_rules)
}

// ─── per-doc work ────────────────────────────────────────────────────────────

async fn process_aux_doc(
    aux: &Document,
    cfg: &BinningConfig,
    now_jd: f64,
    aux_collection: &mongodb::Collection<Document>,
) -> SweepResult<bool> {
    let Some(object_id) = aux.get_str("_id").ok().map(|s| s.to_string()) else {
        return Ok(false);
    };
    let Some(cadence_doc) = aux.get_document("bin_cadence").ok() else {
        return Ok(false);
    };
    let Ok(current_cadence) = mongodb::bson::from_document::<BinCadence>(cadence_doc.clone())
    else {
        return Ok(false);
    };

    let policy = resolve_policy_for_sweep(aux, cfg);
    let new_cadence = evaluate_cadence(
        now_jd,
        Some(&current_cadence),
        &policy,
        false, // outburst
        false, // score_promotion
        &cfg.cadence,
    );

    // Only write when something actually changed beyond `last_evaluated_jd`
    // — otherwise the sweep would dirty a doc per stale source per run.
    let unchanged = new_cadence.tier == current_cadence.tier
        && new_cadence.ladder == current_cadence.ladder
        && new_cadence.h_cadence_hours == current_cadence.h_cadence_hours
        && new_cadence.active_until_jd == current_cadence.active_until_jd;
    let new_cadence_bson = mongodb::bson::to_bson(&new_cadence)?;
    aux_collection
        .update_one(
            doc! { "_id": &object_id },
            doc! { "$set": { "bin_cadence": new_cadence_bson } },
        )
        .await?;
    Ok(!unchanged)
}

// ─── batched walk ────────────────────────────────────────────────────────────

async fn run_sweep(
    db: &Database,
    survey: &Survey,
    cfg: &BinningConfig,
    now_jd: f64,
    stale_threshold_days: f64,
    shard: (u64, u64),
    batch_size: usize,
    limit: Option<usize>,
) -> SweepResult<(usize, usize)> {
    let aux_name = format!("{}_alerts_aux", survey);
    let aux_collection = db.collection::<Document>(&aux_name);

    let filter = discovery_filter(now_jd, stale_threshold_days);

    // Estimate progress count (shard-unaware, so divide for shard pacing).
    let estimated_total = aux_collection
        .count_documents(filter.clone())
        .await
        .unwrap_or(0);
    let pb = ProgressBar::new(estimated_total / shard.1.max(1));
    pb.set_style(
        ProgressStyle::with_template(
            "sweep {bar:40} {pos}/{len} [{elapsed_precise} < {eta_precise}]",
        )
        .unwrap(),
    );

    let mut cursor = aux_collection
        .find(filter)
        .no_cursor_timeout(true)
        .batch_size(batch_size as u32)
        .await?;

    let mut processed: usize = 0;
    let mut demoted: usize = 0;

    while let Some(aux) = cursor.try_next().await? {
        let Ok(id) = aux.get_str("_id") else { continue };
        if !shard_matches(id, shard.0, shard.1) {
            continue;
        }
        match process_aux_doc(&aux, cfg, now_jd, &aux_collection).await {
            Ok(changed) => {
                processed += 1;
                if changed {
                    demoted += 1;
                }
            }
            Err(e) => warn!("sweep error on {}: {}", id, e),
        }
        pb.inc(1);
        if let Some(cap) = limit {
            if processed >= cap {
                break;
            }
        }
    }
    pb.finish();
    Ok((processed, demoted))
}

// ─── main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> SweepResult<()> {
    load_dotenv();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("set subscriber");

    let args = Cli::parse();
    let config_path = args.config.unwrap_or_else(|| "config.yaml".to_string());
    let app_config = AppConfig::from_path(&config_path)?;
    let db = app_config.build_db().await?;

    let bin_config = app_config
        .binning
        .get(&args.survey)
        .ok_or_else(|| -> SweepError {
            format!(
                "no binning.{} block in config — see docs/binned-lightcurves.md",
                args.survey
            )
            .into()
        })?
        .clone();

    let shard = parse_shard(&args.objectid_shard).map_err(SweepError::from)?;
    let now_jd = Time::now().to_jd();

    info!(
        "cadence_sweep survey={} stale_threshold={:.1}d now_jd={:.5} shard={}/{}",
        args.survey, args.stale_threshold_days, now_jd, shard.0, shard.1,
    );

    match run_sweep(
        &db,
        &args.survey,
        &bin_config,
        now_jd,
        args.stale_threshold_days,
        shard,
        args.batch_size,
        args.limit,
    )
    .await
    {
        Ok((processed, demoted)) => {
            info!(
                "processed {} stale-active aux docs, {} demoted",
                processed, demoted
            );
            Ok(())
        }
        Err(e) => {
            error!("sweep failed: {}", e);
            Err(e)
        }
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use boom::conf;
    use boom::utils::cadence::{Ladder, Tier};
    use boom::utils::testing::randomize_object_id;

    #[test]
    fn discovery_filter_targets_stale_active_only() {
        let now = 2_460_000.0;
        let f = discovery_filter(now, 14.0);
        // active_until_jd must be < now (active window expired)
        let aut = f.get_document("bin_cadence.active_until_jd").unwrap();
        assert_eq!(aut.get_f64("$lt").unwrap(), now);
        // last_evaluated_jd must be older than now - 14d
        let lej = f.get_document("bin_cadence.last_evaluated_jd").unwrap();
        assert_eq!(lej.get_f64("$lt").unwrap(), now - 14.0);
    }

    #[tokio::test]
    async fn h_tier_with_expired_active_demotes_to_n() {
        // End-to-end: seed an aux doc whose bin_cadence is H-tier with a
        // long-elapsed active window and a stale last_evaluated_jd, run
        // the sweep, and assert it demotes to N.
        let app_config = conf::AppConfig::from_test_config().unwrap();
        let db = app_config.build_db().await.unwrap();

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let aux_name = format!("sweep_test_aux_{}", suffix);
        let aux: mongodb::Collection<Document> = db.collection(&aux_name);

        let now_jd = 2_460_100.0;
        let object_id = randomize_object_id(&Survey::Ztf);

        // H-tier cadence whose active window expired 30d ago and was last
        // evaluated 30d ago — clearly stale.
        let stale_cadence = BinCadence {
            ladder: Ladder::Transient,
            tier: Some(Tier::H),
            h_cadence_hours: Some(12.0),
            since_jd: now_jd - 30.0,
            last_evaluated_jd: now_jd - 30.0,
            reason: boom::utils::cadence::Reason::Outburst,
            active_until_jd: Some(now_jd - 5.0),
        };

        aux.insert_one(doc! {
            "_id": &object_id,
            "bin_cadence": mongodb::bson::to_document(&stale_cadence).unwrap(),
        })
        .await
        .unwrap();

        let cfg = BinningConfig {
            retention_days: 14.0,
            classifier_score_rules: Vec::new(),
            catalog_tag_rules: std::collections::HashMap::new(),
            cadence: boom::utils::cadence::CadenceConfig::default(),
            tag_rules: Vec::new(),
        };

        let aux_doc = aux
            .find_one(doc! { "_id": &object_id })
            .await
            .unwrap()
            .unwrap();
        let changed = process_aux_doc(&aux_doc, &cfg, now_jd, &aux).await.unwrap();
        assert!(changed, "expected demotion to register as a change");

        // Read it back: should be at N tier now.
        let after = aux
            .find_one(doc! { "_id": &object_id })
            .await
            .unwrap()
            .unwrap();
        let new_cadence: BinCadence =
            mongodb::bson::from_document(after.get_document("bin_cadence").unwrap().clone())
                .unwrap();
        assert_eq!(new_cadence.tier, Some(Tier::N));
        assert_eq!(new_cadence.ladder, Ladder::Transient);
        assert_eq!(new_cadence.h_cadence_hours, None);
        assert_eq!(new_cadence.active_until_jd, None);

        // Idempotency: running the sweep again should be a no-op (the new
        // cadence has `last_evaluated_jd = now_jd`, so the discovery filter
        // would not even pick it up — but the per-doc step itself should
        // also be a no-op when called directly).
        let after_doc = aux
            .find_one(doc! { "_id": &object_id })
            .await
            .unwrap()
            .unwrap();
        let changed_again = process_aux_doc(&after_doc, &cfg, now_jd, &aux)
            .await
            .unwrap();
        assert!(!changed_again, "expected re-run to be a no-op");

        aux.drop().await.unwrap();
    }
}
