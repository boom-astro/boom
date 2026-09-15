//! Repair out-of-order photometry timeseries in `<survey>_alerts_aux`.
//!
//! Each aux document holds timeseries fields (`prv_candidates`,
//! `prv_nondetections`, `fp_hists`) that are expected to be strictly increasing
//! by `jd`. Arrays written before ingestion started sanitizing new points can be
//! out of order, hold duplicate `jd` values, or carry entries with a non-finite
//! or non-numeric `jd`.
//!
//! Ingestion self-heals only part of that: an out-of-order, duplicate or
//! non-finite `jd` makes `prepare_timeseries_update` reject the stored array and
//! the worker falls back to the in-database update path, which rewrites it. An
//! entry whose `jd` is missing or not a number never gets that far -- it fails
//! to deserialize in `get_existing_aux` and the alert errors out again on every
//! retry. This repairs both, plus the objects that will never receive another
//! alert.
//!
//! Ported from the `repair_photometry_ordering` binary. It deletes photometry
//! points, so it runs through the task system where the run is recorded, its
//! logs are kept, and it can be canceled -- not from a root shell. See
//! `docs/task-system.md`.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::{
    db::{
        check_shard_coverage, collection_exists, exact_count, range_shards, shard_field,
        update_timeseries_op, CURSOR_BATCH_SIZE,
    },
    enums::Survey,
};
use futures::{StreamExt, TryStreamExt};
use mongodb::{
    bson::{doc, Bson, Document},
    options::{UpdateModifications, UpdateOneModel, WriteModel},
    Collection,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "repair_photometry";

const MAX_BATCH_SIZE: usize = 100_000;
const MAX_PROCESSES: usize = 64;

/// How many documents to scan between progress publishes.
const PROGRESS_EVERY: u64 = 50_000;

fn default_batch_size() -> usize {
    5_000
}

fn default_processes() -> usize {
    1
}

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RepairPhotometryParams {
    pub survey: Survey,
    /// Updates accumulated per shard before a bulk write.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Parallel scan and repair shards, cut on an indexed insertion-order field.
    #[serde(default = "default_processes")]
    pub processes: usize,
    /// Scan and report what is broken without writing anything. Worth running
    /// first: the summary says how many points a real run would delete.
    #[serde(default)]
    pub dry_run: bool,
}

impl RepairPhotometryParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        if self.processes == 0 || self.processes > MAX_PROCESSES {
            return Err(format!("processes must be between 1 and {MAX_PROCESSES}"));
        }
        Ok(())
    }
}

fn timeseries_fields(survey: &Survey) -> &'static [&'static str] {
    match survey {
        Survey::Ztf => &["prv_candidates", "prv_nondetections", "fp_hists"],
        Survey::Lsst => &["prv_candidates", "fp_hists"],
        Survey::Decam => &["prv_candidates", "fp_hists"],
        Survey::Winter => &["prv_candidates"],
    }
}

#[derive(Default, Clone, Copy)]
struct FieldStats {
    broken: u64,
    dropped_invalid: u64,
    dropped_duplicate: u64,
}

impl FieldStats {
    fn dropped(&self) -> u64 {
        self.dropped_invalid + self.dropped_duplicate
    }

    fn merge(&mut self, other: &FieldStats) {
        self.broken += other.broken;
        self.dropped_invalid += other.dropped_invalid;
        self.dropped_duplicate += other.dropped_duplicate;
    }
}

/// Mirrors `update_timeseries_op`: drops non-finite jd points, keeps the first
/// of duplicated jds.
fn inspect_series(doc: &Document, field: &str) -> FieldStats {
    let mut stats = FieldStats::default();
    let arr = match doc.get_array(field) {
        Ok(a) => a,
        Err(_) => return stats,
    };
    let mut seen: HashSet<u64> = HashSet::with_capacity(arr.len());
    let mut prev: Option<f64> = None;
    let mut out_of_order = false;
    for item in arr {
        let jd = match item.as_document().and_then(|d| d.get("jd")) {
            Some(Bson::Double(v)) => *v,
            Some(Bson::Int32(v)) => *v as f64,
            Some(Bson::Int64(v)) => *v as f64,
            _ => {
                stats.dropped_invalid += 1;
                continue;
            }
        };
        if !jd.is_finite() {
            stats.dropped_invalid += 1;
            continue;
        }
        let key = if jd == 0.0 { 0.0f64 } else { jd };
        if !seen.insert(key.to_bits()) {
            stats.dropped_duplicate += 1;
            continue;
        }
        if prev.is_some_and(|p| jd < p) {
            out_of_order = true;
        }
        prev = Some(jd);
    }
    stats.broken = (out_of_order || stats.dropped() > 0) as u64;
    stats
}

fn jd_projection(fields: &[&str]) -> Document {
    let mut projection = doc! { "_id": 1 };
    for f in fields {
        projection.insert(format!("{}.jd", f), 1);
    }
    projection
}

struct ShardStats {
    scanned: u64,
    broken: u64,
    modified: u64,
    per_field: Vec<FieldStats>,
}

impl ShardStats {
    fn new(fields: usize) -> Self {
        ShardStats {
            scanned: 0,
            broken: 0,
            modified: 0,
            per_field: vec![FieldStats::default(); fields],
        }
    }

    fn merge(&mut self, other: &ShardStats) {
        self.scanned += other.scanned;
        self.broken += other.broken;
        self.modified += other.modified;
        for (acc, field) in self.per_field.iter_mut().zip(&other.per_field) {
            acc.merge(field);
        }
    }
}

/// Shared across shards so progress reflects the whole run, not one shard.
struct ScanProgress {
    scanned: AtomicU64,
    modified: AtomicU64,
    last_reported: AtomicU64,
    total: u64,
}

async fn flush_batch(
    client: &mongodb::Client,
    batch: &mut Vec<WriteModel>,
) -> Result<u64, mongodb::error::Error> {
    let result = client
        .bulk_write(std::mem::take(batch))
        .ordered(false)
        .await?;
    Ok(result.modified_count as u64)
}

#[allow(clippy::too_many_arguments)]
async fn scan_and_repair_shard(
    ctx: &TaskContext,
    aux_collection: Collection<Document>,
    fields: &'static [&'static str],
    filter: Document,
    batch_size: usize,
    dry_run: bool,
    progress: &ScanProgress,
) -> Result<ShardStats, super::TaskError> {
    let client = aux_collection.client().clone();
    let aux_ns = aux_collection.namespace();
    let mut cursor = aux_collection
        .find(filter)
        .projection(jd_projection(fields))
        .no_cursor_timeout(true)
        .batch_size(CURSOR_BATCH_SIZE)
        .await
        .map_err(|e| super::TaskError::Failed(e.to_string()))?;

    let mut scanned: u64 = 0;
    let mut broken_total: u64 = 0;
    let mut modified: u64 = 0;
    let mut per_field = vec![FieldStats::default(); fields.len()];
    let mut batch: Vec<WriteModel> = Vec::with_capacity(batch_size);

    while let Some(d) = cursor
        .try_next()
        .await
        .map_err(|e| super::TaskError::Failed(e.to_string()))?
    {
        scanned += 1;
        let seen = progress.scanned.fetch_add(1, Ordering::Relaxed) + 1;
        if seen - progress.last_reported.load(Ordering::Relaxed) >= PROGRESS_EVERY {
            progress.last_reported.store(seen, Ordering::Relaxed);
            let repaired = progress.modified.load(Ordering::Relaxed);
            ctx.progress(
                seen,
                progress.total.max(seen),
                format!("scanned {seen}, repaired {repaired}"),
            )
            .await;
        }

        let mut broken: Vec<&'static str> = Vec::new();
        for (i, f) in fields.iter().copied().enumerate() {
            let stats = inspect_series(&d, f);
            per_field[i].merge(&stats);
            if stats.broken > 0 {
                broken.push(f);
            }
        }
        if broken.is_empty() {
            continue;
        }
        broken_total += 1;

        if dry_run {
            continue;
        }

        let Some(id) = d.get("_id") else {
            continue;
        };
        let mut set_doc = Document::new();
        for f in &broken {
            set_doc.insert(*f, update_timeseries_op(f, "jd", &vec![]));
        }
        batch.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(aux_ns.clone())
                .filter(doc! { "_id": id.clone() })
                .update(UpdateModifications::Pipeline(vec![
                    doc! { "$set": set_doc },
                ]))
                .build(),
        ));
        if batch.len() >= batch_size {
            // Checked at the batch boundary: each repaired document is
            // independently correct, so stopping here leaves a state that is
            // easy to describe -- some documents repaired, the rest untouched.
            if ctx.is_canceled() {
                ctx.warn(format!(
                    "canceled after repairing {modified} document(s) in this shard; \
                     what is repaired stays repaired and re-running resumes, \
                     because a repaired array no longer looks broken"
                ));
                return Err(super::TaskError::Canceled);
            }
            let n = flush_batch(&client, &mut batch)
                .await
                .map_err(|e| super::TaskError::Failed(e.to_string()))?;
            modified += n;
            progress.modified.fetch_add(n, Ordering::Relaxed);
        }
    }
    if !batch.is_empty() {
        let n = flush_batch(&client, &mut batch)
            .await
            .map_err(|e| super::TaskError::Failed(e.to_string()))?;
        modified += n;
        progress.modified.fetch_add(n, Ordering::Relaxed);
    }
    Ok(ShardStats {
        scanned,
        broken: broken_total,
        modified,
        per_field,
    })
}

pub async fn run(
    ctx: &TaskContext,
    params: RepairPhotometryParams,
) -> Result<serde_json::Value, super::TaskError> {
    params
        .validate_params()
        .map_err(super::TaskError::InvalidParams)?;

    let db = ctx.db().clone();
    let aux_name = format!("{}_alerts_aux", params.survey);

    match collection_exists(&db, &aux_name).await {
        Ok(true) => {}
        Ok(false) => {
            return Err(super::TaskError::InvalidParams(format!(
                "collection {} does not exist in database {}",
                aux_name,
                db.name()
            )))
        }
        Err(e) => return Err(super::TaskError::Failed(e.to_string())),
    }

    let aux_collection: Collection<Document> = db.collection(&aux_name);
    let fields = timeseries_fields(&params.survey);

    let total = exact_count(&aux_collection)
        .await
        .map_err(|e| super::TaskError::Failed(e.to_string()))?;

    let shard_key = shard_field(&aux_collection).await;
    let shards = range_shards(
        &aux_collection,
        params.processes,
        shard_key,
        &Document::new(),
    )
    .await;
    let shard_count = shards.len();

    ctx.info(format!(
        "scanning {total} document(s) in {aux_name} across {shard_count} shard(s) cut on \
         '{shard_key}'{}",
        if params.dry_run {
            " (dry run, nothing will be written)"
        } else {
            ""
        }
    ));

    let progress = ScanProgress {
        scanned: AtomicU64::new(0),
        modified: AtomicU64::new(0),
        last_reported: AtomicU64::new(0),
        total,
    };

    // buffer_unordered rather than tokio::spawn: the shards borrow `ctx` to
    // report progress and log, so keeping them as futures avoids making
    // everything 'static just to run them concurrently.
    let results: Vec<ShardStats> = futures::stream::iter(shards)
        .map(|filter| {
            scan_and_repair_shard(
                ctx,
                aux_collection.clone(),
                fields,
                filter,
                params.batch_size,
                params.dry_run,
                &progress,
            )
        })
        .buffer_unordered(params.processes)
        .try_collect()
        .await?;

    let mut totals = ShardStats::new(fields.len());
    for shard in &results {
        totals.merge(shard);
    }

    for (field, f_stats) in fields.iter().copied().zip(&totals.per_field) {
        if f_stats.broken == 0 {
            continue;
        }
        ctx.info(format!(
            "{field}: {} document(s) need repair, {} point(s) with a non-finite or non-numeric \
             jd, {} duplicating an earlier jd",
            f_stats.broken, f_stats.dropped_invalid, f_stats.dropped_duplicate
        ));
    }

    let dropped_invalid: u64 = totals.per_field.iter().map(|f| f.dropped_invalid).sum();
    let dropped_duplicate: u64 = totals.per_field.iter().map(|f| f.dropped_duplicate).sum();
    let dropped = dropped_invalid + dropped_duplicate;
    if dropped > 0 {
        ctx.warn(format!(
            "{dropped} point(s) {} deleted: {dropped_invalid} with a non-finite or non-numeric \
             jd, {dropped_duplicate} duplicating an earlier jd. update_timeseries_op removes \
             them, the repaired document does not keep a copy",
            if params.dry_run { "would be" } else { "were" },
        ));
    }

    let covered = check_shard_coverage(totals.scanned, total, shard_count);
    if !covered {
        // Not a failure: what the shards did see was repaired. But the run
        // cannot claim the collection is clean, and saying so is the point.
        ctx.warn(format!(
            "shards covered {} of {total} document(s); what was scanned is repaired, but \
             re-run to cover the rest",
            totals.scanned
        ));
    }

    if !params.dry_run && totals.modified > 0 {
        ctx.record_mutation(
            MutationTarget {
                database: db.name().to_string(),
                collection: aux_name.clone(),
                catalog: None,
                survey: Some(params.survey.to_string().to_lowercase()),
            },
            // Recompute: the repaired array is rebuilt from the points already
            // stored on the document, not fetched from anywhere.
            Operation::Recompute,
            doc! {
                "scanned": totals.scanned as i64,
                "broken": totals.broken as i64,
                "modified": totals.modified as i64,
                "points_dropped": dropped as i64,
                "fields": fields.iter().map(|f| Bson::String(f.to_string())).collect::<Vec<_>>(),
            },
        )
        .await;
    }

    ctx.progress(
        totals.scanned,
        total.max(totals.scanned),
        format!("repaired {} document(s)", totals.modified),
    )
    .await;

    Ok(serde_json::json!({
        "survey": params.survey.to_string(),
        "scanned": totals.scanned,
        "broken": totals.broken,
        "modified": totals.modified,
        "points_dropped": dropped,
        "points_dropped_invalid": dropped_invalid,
        "points_dropped_duplicate": dropped_duplicate,
        "dry_run": params.dry_run,
        "full_coverage": covered,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(jds: &[f64]) -> Document {
        doc! { "fp_hists": jds.iter().map(|jd| doc! { "jd": jd }).collect::<Vec<_>>() }
    }

    fn broken(doc: &Document) -> u64 {
        inspect_series(doc, "fp_hists").broken
    }

    fn params(survey: Survey) -> RepairPhotometryParams {
        RepairPhotometryParams {
            survey,
            batch_size: default_batch_size(),
            processes: default_processes(),
            dry_run: false,
        }
    }

    #[test]
    fn accepts_strictly_increasing_and_empty_or_missing_series() {
        assert_eq!(broken(&series(&[1.0, 2.0, 3.0])), 0);
        assert_eq!(broken(&series(&[])), 0);
        assert_eq!(broken(&doc! {}), 0);
    }

    #[test]
    fn rejects_duplicate_decreasing_and_non_finite_jds() {
        assert_eq!(broken(&series(&[1.0, 1.0])), 1);
        assert_eq!(broken(&series(&[2.0, 1.0])), 1);
        assert_eq!(broken(&series(&[f64::NAN])), 1);
        assert_eq!(broken(&series(&[1.0, f64::INFINITY])), 1);
    }

    #[test]
    fn rejects_entries_without_a_numeric_jd() {
        let doc = doc! { "fp_hists": [doc! { "jd": 1.0 }, doc! { "flux": 1.0 }] };
        let stats = inspect_series(&doc, "fp_hists");
        assert_eq!(stats.broken, 1);
        assert_eq!(stats.dropped_invalid, 1);
        assert_eq!(stats.dropped_duplicate, 0);
    }

    #[test]
    fn accepts_integer_jds() {
        let doc = doc! { "fp_hists": [doc! { "jd": 1i32 }, doc! { "jd": 2i64 }] };
        assert_eq!(broken(&doc), 0);
    }

    #[test]
    fn counts_the_points_the_repair_deletes() {
        let stats = inspect_series(&series(&[3.0, 1.0, 1.0, f64::NAN, 2.0]), "fp_hists");
        assert_eq!(stats.dropped_invalid, 1);
        assert_eq!(stats.dropped_duplicate, 1);
        assert_eq!(stats.dropped(), 2);
    }

    #[test]
    fn reordering_alone_deletes_nothing() {
        let stats = inspect_series(&series(&[2.0, 1.0]), "fp_hists");
        assert_eq!(stats.broken, 1);
        assert_eq!(stats.dropped(), 0);
    }

    #[test]
    fn timeseries_fields_matches_the_alert_aux_for_update_structs() {
        for survey in [Survey::Ztf, Survey::Lsst, Survey::Decam, Survey::Winter] {
            let path = format!(
                "{}/src/alert/{}.rs",
                env!("CARGO_MANIFEST_DIR"),
                survey.to_string().to_lowercase()
            );
            let source = std::fs::read_to_string(&path).unwrap();
            let (_, block) = source.split_once("struct AlertAuxForUpdate {").unwrap();
            let fields: Vec<&str> = block[..block.find('}').unwrap()]
                .lines()
                .filter_map(|line| {
                    line.trim()
                        .strip_prefix("pub ")?
                        .split_once(": Vec<LightcurveJdOnly>")
                        .map(|(name, _)| name)
                })
                .collect();
            assert_eq!(timeseries_fields(&survey), fields.as_slice(), "{}", path);
        }
    }

    #[test]
    fn the_concurrency_knobs_are_bounded() {
        let mut p = params(Survey::Ztf);
        assert!(p.validate_params().is_ok());
        p.batch_size = 0;
        assert!(p.validate_params().is_err());
        p.batch_size = MAX_BATCH_SIZE + 1;
        assert!(p.validate_params().is_err());
        p.batch_size = default_batch_size();
        p.processes = 0;
        assert!(p.validate_params().is_err());
        p.processes = MAX_PROCESSES + 1;
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn dry_run_is_off_unless_asked_for() {
        // The default has to be the writing one, because a dry run that
        // silently did nothing while reporting success would be worse than
        // either behaviour chosen deliberately.
        let p: RepairPhotometryParams =
            serde_json::from_value(serde_json::json!({ "survey": "ztf" })).unwrap();
        assert!(!p.dry_run);
        assert_eq!(p.batch_size, default_batch_size());
        assert_eq!(p.processes, default_processes());
    }

    #[test]
    fn single_flight_is_keyed_by_survey() {
        let ztf =
            crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({ "survey": "ztf" }));
        let lsst =
            crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({ "survey": "lsst" }));
        assert!(ztf.is_some());
        assert_ne!(ztf, lsst, "two surveys must not block each other");
    }
}
