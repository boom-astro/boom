//! Fill in the activity-span fields on existing alerts.
//!
//! `first_activity_jd`, `last_detection_jd` and `n_forced_detections` are
//! derived at enrichment, so alerts written before they existed do not carry
//! them. A counterpart search reaching into the archive would then behave
//! differently from the same search on the live stream: a missing field reads
//! as null, which is exactly the failure the fields were added to close.
//!
//! Bounded by `--days` so the counterpart filters can adopt the fields before
//! the whole history is done: run it at 31 days, which is what those searches
//! reach back by default, then widen it.
//!
//! The arrays are read straight from `<survey>_alerts_aux`: `snr_psf` is stored
//! on a forced epoch only when it cleared the detection threshold, so its
//! presence is the test, and no flux has to be reconverted here.

use boom::{
    conf::{load_dotenv, AppConfig},
    utils::{
        data::{make_progress_bar, spawn_progress_logger},
        db::{join_tasks, range_shards, shard_field, TaskError, CURSOR_BATCH_SIZE},
        enums::Survey,
        lightcurves::{summarise_detections, EPISODE_GAP_DAYS},
        parser::parse_positive_usize,
    },
};
use clap::Parser;
use futures::TryStreamExt;
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, Document},
    options::{UpdateOneModel, WriteModel},
    Collection, Namespace,
};
use std::collections::HashMap;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(about = "Backfill the activity-span fields on existing alerts")]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// How far back to reach, days. The counterpart searches use 31.
    #[arg(long, default_value_t = 31.0)]
    days: f64,

    /// Alerts held per worker before a bulk write is issued.
    #[arg(long, default_value_t = 2000, value_parser = parse_positive_usize)]
    batch_size: usize,

    #[arg(long, default_value_t = 8, value_parser = parse_positive_usize)]
    processes: usize,

    /// Count what would change without writing it.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

/// One alert to recompute: its id, its object, and the epoch to summarise at.
#[derive(serde::Deserialize)]
struct AlertRow {
    #[serde(rename = "_id")]
    id: mongodb::bson::Bson,
    #[serde(rename = "objectId")]
    object_id: String,
    candidate: CandidateJd,
}

#[derive(serde::Deserialize)]
struct CandidateJd {
    jd: f64,
}

/// The two arrays an object's span is computed from.
#[derive(Default)]
struct ObjectHistory {
    /// `(jd, is_negative)` for alert-level detections.
    detections: Vec<(f64, Option<bool>)>,
    /// JD of each forced epoch that cleared the threshold.
    forced: Vec<f64>,
}

fn f64_at(doc: &Document, key: &str) -> Option<f64> {
    match doc.get(key) {
        Some(mongodb::bson::Bson::Double(v)) => Some(*v),
        Some(mongodb::bson::Bson::Int32(v)) => Some(*v as f64),
        Some(mongodb::bson::Bson::Int64(v)) => Some(*v as f64),
        _ => None,
    }
}

/// Read one object's arrays, keeping only what the summary needs.
fn history_from_aux(aux: &Document) -> ObjectHistory {
    let mut out = ObjectHistory::default();
    if let Ok(points) = aux.get_array("prv_candidates") {
        for point in points.iter().filter_map(|p| p.as_document()) {
            let Some(jd) = f64_at(point, "jd") else {
                continue;
            };
            // Sign from psfFlux, as enrichment does; absent means undetected.
            let is_negative = f64_at(point, "psfFlux")
                .filter(|f| !f.is_nan())
                .map(|f| f < 0.0);
            out.detections.push((jd, is_negative));
        }
    }
    if let Ok(points) = aux.get_array("fp_hists") {
        for point in points.iter().filter_map(|p| p.as_document()) {
            // snr_psf is written only above the threshold, so it marks a detection.
            if point.get("snr_psf").is_none() {
                continue;
            }
            if let Some(jd) = f64_at(point, "jd") {
                out.forced.push(jd);
            }
        }
    }
    out
}

/// Recompute a batch of alerts against their objects' arrays.
async fn flush(
    batch: &mut Vec<AlertRow>,
    aux: &Collection<Document>,
    client: &mongodb::Client,
    alert_ns: &Namespace,
    dry_run: bool,
    pb: &ProgressBar,
) -> Result<u64, mongodb::error::Error> {
    if batch.is_empty() {
        return Ok(0);
    }
    // One query per batch rather than per alert: alerts of one object cluster.
    let object_ids: Vec<&str> = {
        let mut ids: Vec<&str> = batch.iter().map(|a| a.object_id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };
    let mut histories: HashMap<String, ObjectHistory> = HashMap::new();
    let mut cursor = aux
        .find(doc! { "_id": { "$in": &object_ids } })
        .projection(doc! {
            "prv_candidates.jd": 1, "prv_candidates.psfFlux": 1,
            "fp_hists.jd": 1, "fp_hists.snr_psf": 1,
        })
        .batch_size(CURSOR_BATCH_SIZE)
        .await?;
    while let Some(doc) = cursor.try_next().await? {
        if let Ok(id) = doc.get_str("_id") {
            histories.insert(id.to_string(), history_from_aux(&doc));
        }
    }

    let mut writes: Vec<WriteModel> = Vec::with_capacity(batch.len());
    for alert in batch.drain(..) {
        pb.inc(1);
        let Some(history) = histories.get(&alert.object_id) else {
            continue;
        };
        let (summary, _) = summarise_detections(
            history.detections.iter().copied(),
            history.forced.iter().copied(),
            alert.candidate.jd,
            EPISODE_GAP_DAYS,
        );
        writes.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(alert_ns.clone())
                .filter(doc! { "_id": alert.id })
                .update(doc! { "$set": {
                    "properties.detection_history.first_activity_jd": summary.first_activity_jd,
                    "properties.detection_history.last_detection_jd": summary.last_detection_jd,
                    "properties.detection_history.n_forced_detections": summary.n_forced_detections,
                }})
                .build(),
        ));
    }

    let n = writes.len() as u64;
    if !dry_run && !writes.is_empty() {
        client.bulk_write(writes).ordered(false).await?;
    }
    Ok(n)
}

#[allow(clippy::too_many_arguments)]
async fn run_shard(
    alerts: Collection<AlertRow>,
    aux: Collection<Document>,
    alert_ns: Namespace,
    filter: Document,
    cutoff_jd: f64,
    batch_size: usize,
    dry_run: bool,
    pb: ProgressBar,
) -> Result<u64, mongodb::error::Error> {
    let client = alerts.client().clone();
    let mut find_filter = filter;
    find_filter.insert("candidate.jd", doc! { "$gte": cutoff_jd });

    let mut cursor = alerts
        .find(find_filter)
        .projection(doc! { "_id": 1, "objectId": 1, "candidate.jd": 1 })
        // Unsorted: the shards are cut on an indexed insertion-order field, so
        // ordering by jd within one would mean an in-memory sort of the whole
        // shard. Recency comes from `--days`, widened run by run instead.
        .batch_size(CURSOR_BATCH_SIZE)
        .no_cursor_timeout(true)
        .await?;

    let mut batch: Vec<AlertRow> = Vec::with_capacity(batch_size);
    let mut written = 0u64;
    while let Some(row) = cursor.try_next().await? {
        batch.push(row);
        if batch.len() >= batch_size {
            written += flush(&mut batch, &aux, &client, &alert_ns, dry_run, &pb).await?;
        }
    }
    written += flush(&mut batch, &aux, &client, &alert_ns, dry_run, &pb).await?;
    Ok(written)
}

#[tokio::main]
async fn main() {
    load_dotenv();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
    let args = Cli::parse();

    let config = match AppConfig::from_path(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("failed to load config from {}: {}", args.config, e);
            std::process::exit(1);
        }
    };
    let db = match config.build_db().await {
        Ok(db) => db,
        Err(e) => {
            error!("failed to build mongo client: {}", e);
            std::process::exit(1);
        }
    };

    let alerts: Collection<AlertRow> = db.collection(&format!("{}_alerts", args.survey));
    let counter: Collection<Document> = db.collection(&format!("{}_alerts", args.survey));
    let aux: Collection<Document> = db.collection(&format!("{}_alerts_aux", args.survey));
    let alert_ns = alerts.namespace();

    let now_jd = flare::Time::now().to_jd();
    let cutoff_jd = now_jd - args.days;
    let base = doc! { "candidate.jd": { "$gte": cutoff_jd } };
    let total = counter.count_documents(base.clone()).await.unwrap_or(0);

    let field = shard_field(&counter).await;
    let shards = range_shards(&counter, args.processes, field, &base).await;
    info!(
        "backfilling {} alert(s) at or after jd {:.3} across {} shard(s) cut on '{}' (dry run: {})",
        total,
        cutoff_jd,
        shards.len(),
        field,
        args.dry_run
    );

    let label = format!("span→{}", args.survey);
    let pb = make_progress_bar(total, label.clone());
    pb.enable_steady_tick(std::time::Duration::from_millis(200));
    let logger = spawn_progress_logger(pb.clone(), label);

    let mut handles = Vec::with_capacity(shards.len());
    for filter in shards {
        let alerts = alerts.clone();
        let aux = aux.clone();
        let alert_ns = alert_ns.clone();
        let pb = pb.clone();
        let (batch_size, dry_run) = (args.batch_size, args.dry_run);
        handles.push(tokio::spawn(async move {
            run_shard(
                alerts, aux, alert_ns, filter, cutoff_jd, batch_size, dry_run, pb,
            )
            .await
        }));
    }

    let outcome: Result<Vec<u64>, TaskError> = join_tasks(handles, "shard").await;
    logger.abort();
    pb.finish();

    match outcome {
        Ok(counts) => {
            let n: u64 = counts.iter().sum();
            if args.dry_run {
                info!("dry run: {} alert(s) would be updated", n);
            } else {
                info!("updated the activity span on {} alert(s)", n);
            }
        }
        Err(e) => {
            error!("backfill failed: {}", e);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the stored field names the span is read from. `snr_psf` is written
    /// on a forced epoch only above threshold, and `psfFlux` carries the sign,
    /// so a rename upstream would otherwise leave this silently reading zeros.
    #[test]
    fn test_history_is_read_from_the_stored_names() {
        let aux = doc! {
            "_id": "ZTF26abcdefg",
            "prv_candidates": [
                doc! { "jd": 2461000.5, "psfFlux": 1200.0 },
                doc! { "jd": 2461002.5, "psfFlux": -300.0 },
                // No flux: detected at neither sign, so it has no known sign.
                doc! { "jd": 2461003.5 },
            ],
            "fp_hists": [
                // Above threshold: snr_psf is present.
                doc! { "jd": 2460990.5, "psfFlux": 800.0, "snr_psf": 4.4 },
                // Below: the converter leaves snr_psf off entirely.
                doc! { "jd": 2460995.5, "psfFlux": 50.0 },
            ],
        };
        let history = history_from_aux(&aux);
        assert_eq!(
            history.detections,
            vec![
                (2461000.5, Some(false)),
                (2461002.5, Some(true)),
                (2461003.5, None),
            ]
        );
        assert_eq!(history.forced, vec![2460990.5]);

        // And the summary a backfilled alert would be given.
        let (summary, _) = summarise_detections(
            history.detections.iter().copied(),
            history.forced.iter().copied(),
            2461002.5,
            EPISODE_GAP_DAYS,
        );
        // The forced epoch predates every alert-level detection.
        assert_eq!(summary.first_activity_jd, Some(2460990.5));
        assert_eq!(summary.last_detection_jd, Some(2461002.5));
        assert_eq!(summary.n_forced_detections, 1);
    }
}
