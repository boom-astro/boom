//! Nightly aggregation binary for the binned-lightcurve infrastructure.
//!
//! Walks `<survey>_alerts_aux` documents touched since a watermark, computes
//! per-band median+error bins for the active window at the source's current
//! cadence tier, re-evaluates the cadence state machine, and atomically
//! replaces the bins / sets `bin_cadence` / trims raw photometry past the
//! retention window — all in one per-doc pipeline-update.
//!
//! See `docs/binned-lightcurves.md` for the schema, `src/utils/cadence.rs`
//! for the cadence state machine, and `src/utils/binner_input.rs` for the
//! derivation helpers.
//!
//! # Driving modes
//!
//! - **`--date YYYY-MM-DD`** — bin a specific UTC observing-night for the
//!   survey (local-noon-to-local-noon JD window). Deterministic; safe for
//!   replay/backfill. Re-runs replace bins via the `(band, window_start_jd)`
//!   idempotency key.
//!
//! - **`--since`** (default — pointer in `<survey>_binning_state`) — bin
//!   everything touched since the last successful run. Idempotent by
//!   exclusion. The pointer is advanced atomically at the end.

use boom::conf::{load_dotenv, AppConfig, BinningConfig};
use boom::utils::binner_input::{
    band_outburst_signal, tags_from_classifier_scores, tags_from_cross_matches,
};
use boom::utils::binning::{bin_band_windows, BinWindow, BinnedPoint, FluxPoint, SrcKind};
use boom::utils::cadence::{evaluate_cadence, resolve_tag_policy, BinCadence, Ladder, Tier};
use boom::utils::enums::Survey;
use boom::utils::lightcurves::Band;
use chrono::NaiveDate;
use clap::Parser;
use flare::Time;
use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::bson::{doc, Bson, Document};
use mongodb::Database;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

type BinnerError = Box<dyn std::error::Error + Send + Sync + 'static>;
type BinnerResult<T> = Result<T, BinnerError>;

const MJD_EPOCH_JD: f64 = 2400000.5;
const STATE_COLLECTION_SUFFIX: &str = "_binning_state";
const POINTER_DOC_ID: &str = "lightcurve_binner_pointer";

#[derive(Parser)]
#[command(about = "Nightly per-source binning of <survey>_alerts_aux photometry")]
struct Cli {
    /// Path to the configuration file. Defaults to `config.yaml`.
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Survey whose `<survey>_alerts_aux` collection to bin. Must have a
    /// matching `binning.<survey>` block in config.yaml.
    #[arg(long, value_enum)]
    survey: Survey,

    /// Bin a specific UTC observing night (YYYY-MM-DD). Local-noon-to-local-noon
    /// JD window. When set, overrides `--since`.
    #[arg(long)]
    date: Option<String>,

    /// Bin sources whose `updated_at` is greater than this JD. Default = pointer
    /// stored in `<survey>_binning_state`, or `now - 1` if absent.
    #[arg(long)]
    since: Option<f64>,

    /// Process only sources where `hash(_id) % N == k`. Format `k/N`. Default
    /// `0/1` (no sharding). Use for parallel multi-binner deployments.
    #[arg(long, default_value = "0/1")]
    objectid_shard: String,

    #[arg(long, default_value_t = 5000)]
    batch_size: usize,

    /// Cap the number of aux docs processed (debugging only).
    #[arg(long)]
    limit: Option<usize>,

    /// Emit a stress-test summary as JSON to this path on completion.
    /// Produces `{ wall_seconds, processed, docs_per_sec,
    /// expected_night_docs, wall_clock_margin }` — used by the PR #6
    /// stress-test harness in `comparison/stress_test.sh`.
    #[arg(long)]
    timing_json: Option<String>,

    /// Reference aux-docs-touched count for a representative ZTF night.
    /// Used only to compute the wall-clock margin in the timing JSON
    /// (`margin = (86400 × processed / wall_seconds) / expected`).
    /// Default 150_000 ≈ live ZTF nightly volume.
    #[arg(long, default_value_t = 150_000)]
    expected_night_docs: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct BinningStatePtr {
    #[serde(rename = "_id")]
    id: String,
    /// JD watermark — aux docs with `updated_at > since_jd` get processed.
    since_jd: f64,
    /// Wall-clock JD the pointer was last advanced.
    last_run_jd: f64,
}

// ─── window quantization ─────────────────────────────────────────────────────

/// Quantize `jd` to the start of the MJD-aligned window containing it.
/// All bin windows are integer multiples of `window_days` past `MJD_EPOCH_JD`,
/// so a given (jd, window_days) pair always produces the same `window_start_jd`
/// across runs — that's what makes the `(band, window_start_jd)` idempotency
/// key stable.
fn quantize_window_start(jd: f64, window_days: f64) -> f64 {
    MJD_EPOCH_JD + ((jd - MJD_EPOCH_JD) / window_days).floor() * window_days
}

/// Build the list of bin windows covering the active range at the given tier.
///
/// For H tier, `h_cadence_hours` sets the window width. For N/W/M tiers, the
/// width comes from `Tier::default_window_days`.
fn windows_for_tier(
    active_start_jd: f64,
    active_end_jd: f64,
    tier: Tier,
    h_cadence_hours: Option<f64>,
) -> Vec<BinWindow> {
    let window_days = match tier {
        Tier::H => h_cadence_hours.unwrap_or(12.0) / 24.0,
        Tier::N => 1.0,
        Tier::W => 7.0,
        Tier::M => 30.0,
    };
    let mut windows = Vec::new();
    let mut start = quantize_window_start(active_start_jd, window_days);
    while start < active_end_jd {
        windows.push(BinWindow {
            start_jd: start,
            end_jd: start + window_days,
        });
        start += window_days;
    }
    windows
}

// ─── shard filter ────────────────────────────────────────────────────────────

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

// ─── flux-point extraction (ZTF) ─────────────────────────────────────────────

/// Read a numeric BSON field as `f64`. Accepts Double, Int32, Int64.
fn bson_as_f64(b: Option<&Bson>) -> Option<f64> {
    match b? {
        Bson::Double(d) => Some(*d),
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        _ => None,
    }
}

/// Read the `band` field on a ZTF timeseries entry. The alert worker stores
/// it as a serialized `Band` enum (lowercase string `"g"`, `"r"`, etc.).
fn read_band(d: &Document) -> Option<Band> {
    let s = d.get_str("band").ok()?;
    match s {
        "g" => Some(Band::G),
        "r" => Some(Band::R),
        "i" => Some(Band::I),
        "z" => Some(Band::Z),
        "y" => Some(Band::Y),
        "u" => Some(Band::U),
        _ => None,
    }
}

/// Extract `FluxPoint`s per band from a ZTF aux doc's raw arrays.
///
/// Both `prv_candidates` and `fp_hists` carry `psfFlux` / `psfFluxErr`
/// directly (in units of `1e9 × ZTF_ZP-zero-pointed flux`); the alert worker
/// does the conversion from raw IPAC units. `prv_nondetections` carry only
/// `psfFluxErr` (representing the upper limit) with `psfFlux = None`.
///
/// Returns a per-band map of FluxPoints, sorted by JD ascending within each
/// band.
fn ztf_flux_points_by_band(
    prv_candidates: Option<&Vec<Bson>>,
    prv_nondetections: Option<&Vec<Bson>>,
    fp_hists: Option<&Vec<Bson>>,
) -> HashMap<Band, Vec<FluxPoint>> {
    let mut by_band: HashMap<Band, Vec<FluxPoint>> = HashMap::new();
    let mut push = |band: Band, point: FluxPoint| {
        by_band.entry(band).or_default().push(point);
    };

    for (arr, kind) in [(prv_candidates, SrcKind::Psf), (fp_hists, SrcKind::Fp)] {
        for entry in arr.into_iter().flatten() {
            let Some(d) = entry.as_document() else {
                continue;
            };
            let Some(jd) = bson_as_f64(d.get("jd")) else {
                continue;
            };
            let Some(band) = read_band(d) else { continue };
            let flux = bson_as_f64(d.get("psfFlux"));
            let Some(flux_err) = bson_as_f64(d.get("psfFluxErr")) else {
                continue;
            };
            push(
                band,
                FluxPoint {
                    jd,
                    flux,
                    flux_err,
                    kind: kind.clone(),
                },
            );
        }
    }

    for entry in prv_nondetections.into_iter().flatten() {
        let Some(d) = entry.as_document() else {
            continue;
        };
        let Some(jd) = bson_as_f64(d.get("jd")) else {
            continue;
        };
        let Some(band) = read_band(d) else { continue };
        let Some(flux_err) = bson_as_f64(d.get("psfFluxErr")) else {
            continue;
        };
        push(
            band,
            FluxPoint {
                jd,
                flux: None,
                flux_err,
                kind: SrcKind::Upper,
            },
        );
    }

    for points in by_band.values_mut() {
        points.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap_or(std::cmp::Ordering::Equal));
    }
    by_band
}

// ─── existing-bin extraction ─────────────────────────────────────────────────

/// Pull existing `binned_lightcurve.<band>` arrays off the aux doc and
/// deserialize each into `BinnedPoint`. Bins that fail to deserialize (e.g.,
/// from an older schema) are skipped silently — the binner re-emits them on
/// the next active-range run.
fn read_existing_bins(aux: &Document) -> HashMap<String, Vec<BinnedPoint>> {
    let mut out: HashMap<String, Vec<BinnedPoint>> = HashMap::new();
    let Ok(bins_doc) = aux.get_document("binned_lightcurve") else {
        return out;
    };
    for (band_str, arr) in bins_doc {
        let Some(arr) = arr.as_array() else { continue };
        let mut bins: Vec<BinnedPoint> = Vec::with_capacity(arr.len());
        for entry in arr {
            let Some(d) = entry.as_document() else {
                continue;
            };
            if let Ok(bp) = mongodb::bson::from_document::<BinnedPoint>(d.clone()) {
                bins.push(bp);
            }
        }
        out.insert(band_str.clone(), bins);
    }
    out
}

fn read_existing_cadence(aux: &Document) -> Option<BinCadence> {
    aux.get_document("bin_cadence")
        .ok()
        .and_then(|d| mongodb::bson::from_document::<BinCadence>(d.clone()).ok())
}

// ─── pipeline-update construction ────────────────────────────────────────────

/// Build the per-doc aggregation-pipeline `update` that atomically:
/// - replaces bins by `(band, window_start_jd)` (concat-with-filter)
/// - sets `bin_cadence`
/// - $filter-trims `prv_candidates`, `prv_nondetections`, `fp_hists` by
///   `jd > retention_floor_jd`
fn build_update_pipeline(
    new_bins_by_band: &HashMap<String, Vec<BinnedPoint>>,
    new_cadence: &BinCadence,
    retention_floor_jd: f64,
) -> BinnerResult<Vec<Document>> {
    let mut set = Document::new();

    for (band_str, bins) in new_bins_by_band {
        let new_starts: Vec<Bson> = bins
            .iter()
            .map(|b| Bson::Double(b.window_start_jd))
            .collect();
        let new_bins_bson: Vec<Bson> = bins
            .iter()
            .map(|b| mongodb::bson::to_bson(b))
            .collect::<Result<Vec<_>, _>>()?;
        let key = format!("binned_lightcurve.{}", band_str);
        let existing_ref = format!("$binned_lightcurve.{}", band_str);
        set.insert(
            key,
            doc! {
                "$concatArrays": [
                    {
                        "$filter": {
                            "input": { "$ifNull": [existing_ref, Vec::<Bson>::new()] },
                            "cond": { "$not": { "$in": ["$$this.window_start_jd", new_starts.clone()] } }
                        }
                    },
                    new_bins_bson,
                ]
            },
        );
    }

    set.insert("bin_cadence", mongodb::bson::to_bson(new_cadence)?);

    for raw in &["prv_candidates", "prv_nondetections", "fp_hists"] {
        let input_ref = format!("${}", raw);
        set.insert(
            raw.to_string(),
            doc! {
                "$filter": {
                    "input": { "$ifNull": [input_ref, Vec::<Bson>::new()] },
                    "cond": { "$gt": ["$$this.jd", retention_floor_jd] }
                }
            },
        );
    }

    Ok(vec![doc! { "$set": set }])
}

// ─── per-aux-doc work ────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn process_aux_doc(
    aux: Document,
    latest_classifications: Option<&Document>,
    cross_matches: Option<&Document>,
    config: &BinningConfig,
    active_start_jd: f64,
    active_end_jd: f64,
    now_jd: f64,
    aux_collection: &mongodb::Collection<Document>,
) -> BinnerResult<()> {
    let Some(object_id) = aux.get_str("_id").ok().map(|s| s.to_string()) else {
        return Ok(());
    };

    // ── Tag policy + score promotion ──
    let (mut tags, score_promotion) =
        tags_from_classifier_scores(latest_classifications, &config.classifier_score_rules);
    let xm_tags = tags_from_cross_matches(cross_matches, &config.catalog_tag_rules);
    for t in xm_tags {
        if !tags.contains(&t) {
            tags.push(t);
        }
    }
    let policy = resolve_tag_policy(&tags, &config.tag_rules);

    // ── Current cadence ──
    let current_cadence = read_existing_cadence(&aux);
    // The tier governing THIS run's bin windows is whatever the source is
    // already on — Q1 (append-only): tier changes apply to FUTURE windows.
    // Default for unseen sources: N tier (matches `BinCadence::initial`).
    let current_tier = current_cadence
        .as_ref()
        .and_then(|c| c.tier)
        .unwrap_or(Tier::N);
    let current_h_cadence = current_cadence.as_ref().and_then(|c| c.h_cadence_hours);
    let on_periodic_track = current_cadence
        .as_ref()
        .map(|c| c.ladder == Ladder::Variable && c.tier.is_none())
        .unwrap_or(false);

    // ── Compute new bins (skip entirely on periodic track — Q3) ──
    let mut new_bins_by_band: HashMap<String, Vec<BinnedPoint>> = HashMap::new();
    if !on_periodic_track {
        let windows = windows_for_tier(
            active_start_jd,
            active_end_jd,
            current_tier,
            current_h_cadence,
        );
        if !windows.is_empty() {
            let prv_candidates = aux.get_array("prv_candidates").ok();
            let prv_nondetections = aux.get_array("prv_nondetections").ok();
            let fp_hists = aux.get_array("fp_hists").ok();
            let by_band = ztf_flux_points_by_band(prv_candidates, prv_nondetections, fp_hists);

            for (band, points) in by_band {
                let bins = bin_band_windows(&points, &windows, band.clone());
                if bins.is_empty() {
                    continue;
                }
                new_bins_by_band.insert(band.to_string(), bins);
            }
        }
    }

    // ── Outburst signal: OR per-band ──
    let existing_bins = read_existing_bins(&aux);
    let outburst_detected = new_bins_by_band.iter().any(|(band_str, bins)| {
        let history = existing_bins
            .get(band_str)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        bins.iter().any(|b| {
            band_outburst_signal(
                b,
                history,
                config.cadence.rolling_window_k,
                config.cadence.outburst_sigma,
            )
        })
    });

    // ── Cadence state machine ──
    let new_cadence = evaluate_cadence(
        now_jd,
        current_cadence.as_ref(),
        &policy,
        outburst_detected,
        score_promotion,
        &config.cadence,
    );

    // ── Atomic per-doc update ──
    let retention_floor_jd = now_jd - config.retention_days;
    let pipeline = build_update_pipeline(&new_bins_by_band, &new_cadence, retention_floor_jd)?;
    aux_collection
        .update_one(doc! { "_id": &object_id }, pipeline)
        .await?;
    Ok(())
}

// ─── batched walk ────────────────────────────────────────────────────────────

async fn run_binner(
    db: &Database,
    survey: &Survey,
    config: &BinningConfig,
    active_start_jd: f64,
    active_end_jd: f64,
    now_jd: f64,
    shard: (u64, u64),
    batch_size: usize,
    limit: Option<usize>,
) -> BinnerResult<usize> {
    let aux_name = format!("{}_alerts_aux", survey);
    let alerts_name = format!("{}_alerts", survey);
    let aux_collection = db.collection::<Document>(&aux_name);

    // Discovery: stream `_id` cursor, app-side shard filter.
    let discovery_filter = doc! {
        "updated_at": { "$gt": active_start_jd, "$lte": active_end_jd }
    };
    let mut cursor = aux_collection
        .find(discovery_filter.clone())
        .projection(doc! { "_id": 1 })
        .no_cursor_timeout(true)
        .await?;

    // Estimate progress count (shard-unaware — close enough)
    let estimated_total = aux_collection
        .count_documents(discovery_filter)
        .await
        .unwrap_or(0);
    let pb = ProgressBar::new(estimated_total / shard.1.max(1));
    pb.set_style(
        ProgressStyle::with_template(
            "bin {bar:40} {pos}/{len} [{elapsed_precise} < {eta_precise}]",
        )
        .unwrap(),
    );

    let mut shard_ids: Vec<String> = Vec::with_capacity(batch_size);
    let mut processed: usize = 0;

    while let Some(d) = cursor.try_next().await? {
        let Ok(id) = d.get_str("_id") else { continue };
        if !shard_matches(id, shard.0, shard.1) {
            continue;
        }
        shard_ids.push(id.to_string());

        if shard_ids.len() >= batch_size {
            processed += process_batch(
                db,
                &aux_name,
                &alerts_name,
                &aux_collection,
                &shard_ids,
                config,
                active_start_jd,
                active_end_jd,
                now_jd,
            )
            .await?;
            pb.inc(shard_ids.len() as u64);
            shard_ids.clear();

            if let Some(cap) = limit {
                if processed >= cap {
                    break;
                }
            }
        }
    }

    if !shard_ids.is_empty() {
        let n = shard_ids.len() as u64;
        processed += process_batch(
            db,
            &aux_name,
            &alerts_name,
            &aux_collection,
            &shard_ids,
            config,
            active_start_jd,
            active_end_jd,
            now_jd,
        )
        .await?;
        pb.inc(n);
    }
    pb.finish();
    Ok(processed)
}

#[allow(clippy::too_many_arguments)]
async fn process_batch(
    db: &Database,
    aux_name: &str,
    alerts_name: &str,
    aux_collection: &mongodb::Collection<Document>,
    ids: &[String],
    config: &BinningConfig,
    active_start_jd: f64,
    active_end_jd: f64,
    now_jd: f64,
) -> BinnerResult<usize> {
    // One aggregate query per batch: $match by id, $lookup latest
    // classifications from <survey>_alerts.
    let pipeline = vec![
        doc! { "$match": { "_id": { "$in": ids.iter().map(|s| Bson::String(s.clone())).collect::<Vec<_>>() } } },
        doc! {
            "$lookup": {
                "from": alerts_name,
                "let": { "oid": "$_id" },
                "pipeline": [
                    { "$match": { "$expr": { "$eq": ["$objectId", "$$oid"] } } },
                    { "$sort": { "_id": -1 } },
                    { "$limit": 1 },
                    { "$project": { "classifications": 1, "_id": 0 } },
                ],
                "as": "latest_class",
            }
        },
    ];

    let mut cursor = db
        .collection::<Document>(aux_name)
        .aggregate(pipeline)
        .await?;
    let mut count = 0_usize;
    while let Some(aux_with_class) = cursor.try_next().await? {
        // Pull the looked-up classification off the joined doc, then strip
        // it so the rest of the work sees a clean aux document.
        let mut aux = aux_with_class;
        let latest_class_arr = aux.remove("latest_class");
        let latest_classifications: Option<Document> = latest_class_arr
            .and_then(|b| match b {
                Bson::Array(arr) => arr.into_iter().next(),
                _ => None,
            })
            .and_then(|b| match b {
                Bson::Document(d) => d.get_document("classifications").ok().cloned(),
                _ => None,
            });

        let cross_matches = aux.get_document("cross_matches").ok().cloned();

        if let Err(e) = process_aux_doc(
            aux,
            latest_classifications.as_ref(),
            cross_matches.as_ref(),
            config,
            active_start_jd,
            active_end_jd,
            now_jd,
            aux_collection,
        )
        .await
        {
            warn!("error processing aux doc: {}", e);
            continue;
        }
        count += 1;
    }
    Ok(count)
}

// ─── since-pointer state ─────────────────────────────────────────────────────

async fn read_since_pointer(db: &Database, survey: &Survey) -> Option<f64> {
    let coll: mongodb::Collection<BinningStatePtr> =
        db.collection(&format!("{}{}", survey, STATE_COLLECTION_SUFFIX));
    coll.find_one(doc! { "_id": POINTER_DOC_ID })
        .await
        .ok()
        .flatten()
        .map(|p| p.since_jd)
}

async fn write_since_pointer(
    db: &Database,
    survey: &Survey,
    since_jd: f64,
    now_jd: f64,
) -> BinnerResult<()> {
    let coll: mongodb::Collection<BinningStatePtr> =
        db.collection(&format!("{}{}", survey, STATE_COLLECTION_SUFFIX));
    coll.replace_one(
        doc! { "_id": POINTER_DOC_ID },
        BinningStatePtr {
            id: POINTER_DOC_ID.to_string(),
            since_jd,
            last_run_jd: now_jd,
        },
    )
    .upsert(true)
    .await?;
    Ok(())
}

// ─── main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> BinnerResult<()> {
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
        .ok_or_else(|| {
            format!(
                "no binning.{} block in config — see docs/binned-lightcurves.md",
                args.survey
            )
        })?
        .clone();

    let shard = parse_shard(&args.objectid_shard).map_err(BinnerError::from)?;
    let now_jd = Time::now().to_jd();

    let (active_start_jd, active_end_jd, mode_label) = if let Some(date_str) = &args.date {
        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
            .map_err(|e| format!("bad --date: {}", e))?;
        let start = args.survey.date_to_jd_local_noon(&date);
        let end = start + 1.0;
        (start, end, format!("--date {}", date_str))
    } else {
        let since = match args.since {
            Some(s) => s,
            None => {
                match read_since_pointer(&db, &args.survey).await {
                    Some(s) => s,
                    None => {
                        warn!("no since-pointer for {} and --since not provided; defaulting to now-1d", args.survey);
                        now_jd - 1.0
                    }
                }
            }
        };
        (since, now_jd, format!("--since {:.5}", since))
    };

    info!(
        "lightcurve_binner survey={} mode='{}' active_jd=[{:.5}, {:.5}) shard={}/{} batch_size={}",
        args.survey, mode_label, active_start_jd, active_end_jd, shard.0, shard.1, args.batch_size,
    );

    let t_run = std::time::Instant::now();
    let processed = run_binner(
        &db,
        &args.survey,
        &bin_config,
        active_start_jd,
        active_end_jd,
        now_jd,
        shard,
        args.batch_size,
        args.limit,
    )
    .await?;
    let wall_seconds = t_run.elapsed().as_secs_f64();
    let docs_per_sec = if wall_seconds > 0.0 {
        processed as f64 / wall_seconds
    } else {
        0.0
    };
    let wall_clock_margin = if args.expected_night_docs > 0 && wall_seconds > 0.0 {
        (86400.0 * docs_per_sec) / args.expected_night_docs as f64
    } else {
        0.0
    };

    info!(
        "processed {} aux docs in {:.2}s ({:.0} docs/s, {:.1}× nightly margin vs {} expected)",
        processed, wall_seconds, docs_per_sec, wall_clock_margin, args.expected_night_docs,
    );

    if let Some(path) = &args.timing_json {
        let report = serde_json::json!({
            "wall_seconds": wall_seconds,
            "processed": processed,
            "docs_per_sec": docs_per_sec,
            "expected_night_docs": args.expected_night_docs,
            "wall_clock_margin": wall_clock_margin,
            "shard": format!("{}/{}", shard.0, shard.1),
            "survey": args.survey.to_string(),
        });
        std::fs::write(path, serde_json::to_string_pretty(&report)?)?;
        info!("timing report written to {}", path);
    }

    // Advance the since-pointer only on `--since` driven runs and only when
    // we processed all shards (shard.1 == 1). Sharded runs leave the
    // pointer to the orchestrator.
    if args.date.is_none() && shard.1 == 1 {
        match write_since_pointer(&db, &args.survey, active_end_jd, now_jd).await {
            Ok(_) => info!("advanced since-pointer to {:.5}", active_end_jd),
            Err(e) => error!("failed to advance since-pointer: {}", e),
        }
    }

    Ok(())
}

// ─── unit tests for pure helpers ─────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantize_aligns_to_mjd_multiples() {
        // window=1d: floor((jd-MJD0)/1)*1 + MJD0
        let jd = MJD_EPOCH_JD + 60_000.7;
        assert!((quantize_window_start(jd, 1.0) - (MJD_EPOCH_JD + 60_000.0)).abs() < 1e-9);
        // window=7d
        let jd = MJD_EPOCH_JD + 60_005.5;
        // floor(60005.5/7)*7 = 60004 (since 60004/7 = 8572, exact)
        assert!((quantize_window_start(jd, 7.0) - (MJD_EPOCH_JD + 60004.0)).abs() < 1e-6);
    }

    #[test]
    fn quantize_is_stable_across_runs() {
        // Two calls with `jd` shifted within the same window must return the
        // same start. This is the guarantee that lets the bin's
        // `(band, window_start_jd)` key dedupe across re-runs.
        let day_a = MJD_EPOCH_JD + 60_000.1;
        let day_b = MJD_EPOCH_JD + 60_000.9;
        assert_eq!(
            quantize_window_start(day_a, 1.0),
            quantize_window_start(day_b, 1.0)
        );
    }

    #[test]
    fn windows_n_tier_returns_one_window_per_day() {
        let start = MJD_EPOCH_JD + 60_000.0;
        let end = start + 1.0;
        let ws = windows_for_tier(start, end, Tier::N, None);
        assert_eq!(ws.len(), 1);
        assert!((ws[0].width_days() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn windows_h_tier_returns_multiple_per_day() {
        let start = MJD_EPOCH_JD + 60_000.0;
        let end = start + 1.0;
        // h_cadence_hours=12 → window=0.5d → 2 windows per day
        let ws = windows_for_tier(start, end, Tier::H, Some(12.0));
        assert_eq!(ws.len(), 2);
        // h_cadence_hours=0.5 → window=0.5/24 d → 48 per day
        let ws = windows_for_tier(start, end, Tier::H, Some(0.5));
        assert_eq!(ws.len(), 48);
    }

    #[test]
    fn windows_w_tier_returns_one_window() {
        let start = MJD_EPOCH_JD + 60_000.0; // exact 7-day boundary
        let end = start + 1.0;
        let ws = windows_for_tier(start, end, Tier::W, None);
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].start_jd, MJD_EPOCH_JD + 59_997.0); // floor(60000/7)*7 = 59997
        assert!((ws[0].width_days() - 7.0).abs() < 1e-9);
    }

    #[test]
    fn shard_parse_round_trip() {
        assert_eq!(parse_shard("0/1").unwrap(), (0, 1));
        assert_eq!(parse_shard("3/4").unwrap(), (3, 4));
        assert!(parse_shard("4/4").is_err()); // k must be < N
        assert!(parse_shard("0/0").is_err()); // N must be > 0
        assert!(parse_shard("not/a/shard").is_err());
    }

    #[test]
    fn shard_matches_passes_everything_for_n_eq_1() {
        assert!(shard_matches("ZTF21abdpypc", 0, 1));
        assert!(shard_matches("anything", 0, 1));
    }

    #[test]
    fn shard_matches_partitions_evenly_in_aggregate() {
        // Sanity: across many ids, each shard gets a roughly equal slice.
        let n = 4;
        let mut counts = [0_usize; 4];
        for i in 0..1000 {
            let id = format!("ZTF24{:08x}", i);
            for k in 0..n {
                if shard_matches(&id, k, n as u64) {
                    counts[k as usize] += 1;
                }
            }
        }
        let total: usize = counts.iter().sum();
        assert_eq!(total, 1000);
        // Each bucket should be 250 ± a generous slack — `DefaultHasher` is
        // not designed for uniform sharding, so ~30% imbalance on a 1k
        // sample is normal. We're not asserting good hash quality here, just
        // that no shard gets nothing.
        for c in counts {
            assert!(c > 100, "shard got too few: counts={:?}", counts);
            assert!(c < 400, "shard got too many: counts={:?}", counts);
        }
    }

    #[tokio::test]
    async fn process_aux_doc_writes_bin_and_cadence_end_to_end() {
        // End-to-end integration: seed a synthetic aux doc with two
        // prv_candidates in r-band, run process_aux_doc against the live
        // test Mongo, and assert that
        //   - binned_lightcurve.r grows by exactly one bin with the
        //     expected window_start_jd and the inverse-variance-weighted
        //     central flux,
        //   - bin_cadence is written (initial state, transient ladder, N
        //     tier, since `BinCadence::initial`),
        //   - the raw prv_candidates entry that fell outside the
        //     retention window is trimmed away.
        use boom::conf;
        use boom::utils::testing::randomize_object_id;
        use mongodb::bson::Bson;

        let app_config = conf::AppConfig::from_test_config().unwrap();
        let db = app_config.build_db().await.unwrap();

        // Use a per-run-unique collection name so this test never clobbers
        // (or is clobbered by) the surrounding alert tests.
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let aux_name = format!("binner_test_aux_{}", suffix);
        let aux: mongodb::Collection<Document> = db.collection(&aux_name);

        // Active range: a single observing night centered around `now_jd`.
        let now_jd = 2_460_000.5;
        let active_start_jd = quantize_window_start(now_jd, 1.0);
        let active_end_jd = active_start_jd + 1.0;

        // Two in-window detections + one stale point that will be trimmed.
        // Window start = 2_460_000.0 (MJD-quantized day boundary).
        let object_id = randomize_object_id(&Survey::Ztf);
        aux.insert_one(doc! {
            "_id": &object_id,
            "updated_at": now_jd,
            "prv_candidates": [
                doc! {
                    "jd": active_start_jd + 0.2,
                    "band": "r",
                    "psfFlux": 100.0_f64,
                    "psfFluxErr": 5.0_f64,
                },
                doc! {
                    "jd": active_start_jd + 0.4,
                    "band": "r",
                    "psfFlux": 110.0_f64,
                    "psfFluxErr": 5.0_f64,
                },
                doc! {
                    // Old point — will be trimmed by retention floor.
                    "jd": now_jd - 100.0,
                    "band": "r",
                    "psfFlux": 50.0_f64,
                    "psfFluxErr": 5.0_f64,
                },
            ],
        })
        .await
        .unwrap();

        // Minimal binning config sufficient to drive the state machine.
        let cfg = BinningConfig {
            retention_days: 14.0,
            classifier_score_rules: Vec::new(),
            catalog_tag_rules: HashMap::new(),
            cadence: boom::utils::cadence::CadenceConfig::default(),
            tag_rules: Vec::new(),
        };

        process_aux_doc(
            aux.find_one(doc! { "_id": &object_id })
                .await
                .unwrap()
                .unwrap(),
            None,
            None,
            &cfg,
            active_start_jd,
            active_end_jd,
            now_jd,
            &aux,
        )
        .await
        .unwrap();

        let after = aux
            .find_one(doc! { "_id": &object_id })
            .await
            .unwrap()
            .unwrap();

        // ── Bin written ──
        let bins = after
            .get_document("binned_lightcurve")
            .unwrap()
            .get_array("r")
            .unwrap();
        assert_eq!(bins.len(), 1, "expected one r-band bin, got {:?}", bins);
        let bin_doc = bins[0].as_document().unwrap();
        let bp: BinnedPoint = mongodb::bson::from_document(bin_doc.clone()).unwrap();
        assert_eq!(bp.n, 2);
        // Equal-weight 2-point mean of {100, 110} = 105.
        assert!((bp.flux_med.unwrap() - 105.0).abs() < 1e-9);
        // Window start is MJD-quantized to the day.
        assert_eq!(bp.window_start_jd, active_start_jd);
        assert!((bp.window_days - 1.0).abs() < 1e-9);
        assert_eq!(bp.band, Band::R);

        // ── Cadence written (initial state for unseen source) ──
        let cadence_doc = after.get_document("bin_cadence").unwrap();
        let cadence: BinCadence = mongodb::bson::from_document(cadence_doc.clone()).unwrap();
        assert_eq!(cadence.ladder, Ladder::Transient);
        assert_eq!(cadence.tier, Some(Tier::N));

        // ── Raw trim: only the two in-window points remain ──
        let prv = after.get_array("prv_candidates").unwrap();
        assert_eq!(
            prv.len(),
            2,
            "expected 2 prv_candidates after trim, got {:?}",
            prv
        );
        for p in prv {
            let jd = p
                .as_document()
                .and_then(|d| d.get("jd"))
                .and_then(|b| match b {
                    Bson::Double(d) => Some(*d),
                    _ => None,
                });
            assert!(jd.unwrap() > now_jd - cfg.retention_days);
        }

        // ── Re-run idempotency: a second pass with the same inputs replaces
        //     the existing bin in place rather than appending. ──
        process_aux_doc(
            aux.find_one(doc! { "_id": &object_id })
                .await
                .unwrap()
                .unwrap(),
            None,
            None,
            &cfg,
            active_start_jd,
            active_end_jd,
            now_jd,
            &aux,
        )
        .await
        .unwrap();
        let after2 = aux
            .find_one(doc! { "_id": &object_id })
            .await
            .unwrap()
            .unwrap();
        let bins2 = after2
            .get_document("binned_lightcurve")
            .unwrap()
            .get_array("r")
            .unwrap();
        assert_eq!(
            bins2.len(),
            1,
            "expected still one r-band bin after re-run, got {:?}",
            bins2
        );

        aux.drop().await.unwrap();
    }

    #[test]
    fn build_update_pipeline_basic_shape() {
        // Sanity: the pipeline contains one $set stage with the expected
        // top-level keys (per-band binned_lightcurve.X, bin_cadence, and
        // the three trimmed raw arrays).
        let mut new_bins = HashMap::new();
        new_bins.insert(
            "r".to_string(),
            vec![BinnedPoint {
                jd: 60_000.5,
                n: 1,
                flux_med: Some(100.0),
                flux_err: 5.0,
                flux_mad: 0.0,
                has_nondet: false,
                src_kinds: vec![SrcKind::Psf],
                window_start_jd: 60_000.0,
                window_days: 1.0,
                band: Band::R,
            }],
        );
        let cadence = BinCadence::initial(60_000.0);
        let pipeline = build_update_pipeline(&new_bins, &cadence, 59_986.0).unwrap();
        assert_eq!(pipeline.len(), 1);
        let set_stage = pipeline[0].get_document("$set").unwrap();
        assert!(set_stage.contains_key("binned_lightcurve.r"));
        assert!(set_stage.contains_key("bin_cadence"));
        assert!(set_stage.contains_key("prv_candidates"));
        assert!(set_stage.contains_key("prv_nondetections"));
        assert!(set_stage.contains_key("fp_hists"));
    }
}
