//! Fill in the activity-span fields on existing alerts.
//!
//! `first_activity_jd`, `last_detection_jd` and `n_forced_detections` are
//! derived at enrichment, so alerts written before they existed do not carry
//! them. A counterpart search reaching into the archive would then behave
//! differently from the same search on the live stream: a missing field reads
//! as null, which is exactly the failure the fields were added to close.
//!
//! Bounded by `days` so the counterpart filters can adopt the fields before the
//! whole history is done: run it at 31 days, which is what those searches reach
//! back by default, then widen it.
//!
//! The arrays are read straight from `<survey>_alerts_aux`: `snr_psf` is stored
//! on a forced epoch only when it cleared the detection threshold, so its
//! presence is the test, and no flux has to be reconverted here.
//!
//! **Idempotent.** The span is recomputed from the stored arrays rather than
//! amended, so a second run over the same alerts writes the same values.
//!
//! ZTF and LSST only, which is what those field names belong to. WINTER stores
//! no forced photometry at all, and DECam names the same quantities
//! differently and computes `snr` on every epoch rather than only the
//! significant ones, so presence would mark every point a detection. Reading
//! either with these names yields a wrong answer rather than an empty one,
//! which is why the survey is refused at submit rather than left to the caller.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::{
    db::{join_tasks, range_shards, shard_field, TaskError, CURSOR_BATCH_SIZE},
    enums::Survey,
    lightcurves::{summarise_detections, EPISODE_GAP_DAYS},
};
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    options::{UpdateOneModel, WriteModel},
    Collection, Namespace,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "backfill_detection_span";

const MAX_BATCH_SIZE: usize = 100_000;
const MAX_PROCESSES: usize = 64;
const MAX_DAYS: f64 = 10_000.0;
/// How often the progress ticker publishes.
const PROGRESS_TICK: std::time::Duration = std::time::Duration::from_secs(5);

fn default_days() -> f64 {
    31.0
}
fn default_batch_size() -> usize {
    2_000
}
fn default_processes() -> usize {
    8
}

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackfillDetectionSpanParams {
    /// ZTF or LSST. The others store these quantities under other names, or
    /// not at all.
    pub survey: Survey,
    /// How far back to reach, days. The counterpart searches use 31.
    #[serde(default = "default_days")]
    pub days: f64,
    /// Alerts held per worker before a bulk write.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Shards scanned at once.
    #[serde(default = "default_processes")]
    pub processes: usize,
    /// Count what would change without writing it.
    #[serde(default)]
    pub dry_run: bool,
}

impl BackfillDetectionSpanParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if !matches!(self.survey, Survey::Ztf | Survey::Lsst) {
            return Err(format!(
                "{} is not supported: this reads psfFlux and snr_psf, which WINTER and \
                 DECam do not store under those names",
                self.survey
            ));
        }
        if !(self.days > 0.0 && self.days <= MAX_DAYS) {
            return Err(format!("days must be between 0 and {MAX_DAYS}"));
        }
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        if self.processes == 0 || self.processes > MAX_PROCESSES {
            return Err(format!("processes must be between 1 and {MAX_PROCESSES}"));
        }
        Ok(())
    }
}

/// Stands in for the binary's terminal progress bar: the same `inc` calls,
/// counted for the run rather than drawn on a tty.
#[derive(Clone)]
pub struct Progress(Arc<AtomicU64>);

impl Progress {
    fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }
    fn inc(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    fn count(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn failed(e: impl std::fmt::Display) -> super::TaskError {
    super::TaskError::Failed(e.to_string())
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
    pb: &Progress,
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
    pb: Progress,
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

pub async fn run(
    ctx: &TaskContext,
    params: BackfillDetectionSpanParams,
) -> Result<serde_json::Value, super::TaskError> {
    params
        .validate_params()
        .map_err(super::TaskError::InvalidParams)?;

    let db = ctx.db().clone();
    let alerts: Collection<AlertRow> = db.collection(&format!("{}_alerts", params.survey));
    let counter: Collection<Document> = db.collection(&format!("{}_alerts", params.survey));
    let aux: Collection<Document> = db.collection(&format!("{}_alerts_aux", params.survey));
    let alert_ns = alerts.namespace();

    let cutoff_jd = flare::Time::now().to_jd() - params.days;
    let base = doc! { "candidate.jd": { "$gte": cutoff_jd } };
    let total = counter.count_documents(base.clone()).await.unwrap_or(0);

    let field = shard_field(&counter).await;
    let shards = range_shards(&counter, params.processes, field, &base).await;
    ctx.info(format!(
        "backfilling {} alert(s) at or after jd {:.3} across {} shard(s) cut on '{}'{}",
        total,
        cutoff_jd,
        shards.len(),
        field,
        if params.dry_run { " (dry run)" } else { "" }
    ));

    let pb = Progress::new();
    let ticker = {
        let ctx = ctx.clone();
        let pb = pb.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(PROGRESS_TICK).await;
                let done = pb.count();
                ctx.progress(done, total.max(done), format!("{done} alert(s) updated"))
                    .await;
            }
        })
    };

    let mut handles = Vec::with_capacity(shards.len());
    for filter in shards {
        let alerts = alerts.clone();
        let aux = aux.clone();
        let alert_ns = alert_ns.clone();
        let pb = pb.clone();
        let (batch_size, dry_run) = (params.batch_size, params.dry_run);
        handles.push(tokio::spawn(async move {
            run_shard(
                alerts, aux, alert_ns, filter, cutoff_jd, batch_size, dry_run, pb,
            )
            .await
        }));
    }

    let outcome: Result<Vec<u64>, TaskError> = join_tasks(handles, "shard").await;
    ticker.abort();
    let counts = outcome.map_err(failed)?;
    let updated: u64 = counts.iter().sum();

    if ctx.is_canceled() {
        return Err(super::TaskError::Canceled);
    }

    if !params.dry_run && updated > 0 {
        ctx.record_mutation(
            MutationTarget {
                database: db.name().to_string(),
                collection: format!("{}_alerts", params.survey),
                catalog: None,
                survey: Some(params.survey.to_string().to_lowercase()),
            },
            // Recompute: the span is derived from arrays already stored on the
            // aux record, not fetched from anywhere.
            Operation::Recompute,
            doc! {
                "fields": ["first_activity_jd", "last_detection_jd", "n_forced_detections"],
                "updated": updated as i64,
                "days": params.days,
                "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                    .unwrap_or(mongodb::bson::Bson::Null),
            },
        )
        .await;
    }

    ctx.info(if params.dry_run {
        format!("dry run: {updated} alert(s) would be updated")
    } else {
        format!("updated the activity span on {updated} alert(s)")
    });
    Ok(serde_json::json!({
        "survey": params.survey.to_string(),
        "scanned": total,
        "updated": updated,
        "days": params.days,
        "dry_run": params.dry_run,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the stored names: `snr_psf` marks a forced detection, `psfFlux`
    /// carries the sign. A rename would otherwise read as zeros.
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
