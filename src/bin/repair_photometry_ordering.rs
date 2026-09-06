use boom::{
    conf::{load_dotenv, AppConfig},
    utils::{
        data::{make_progress_bar, spawn_progress_logger},
        db::{
            join_tasks, range_shards, shard_field, update_timeseries_op, TaskError,
            CURSOR_BATCH_SIZE,
        },
        enums::Survey,
        parser::parse_positive_usize,
    },
};
use clap::Parser;
use futures::TryStreamExt;
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, Bson, Document},
    options::{UpdateModifications, UpdateOneModel, WriteModel},
    Collection,
};
use std::collections::HashSet;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Repair the photometry timeseries arrays in `<survey>_alerts_aux`.
///
/// Each aux document holds timeseries fields (e.g. `prv_candidates`,
/// `prv_nondetections`, `fp_hists`) that are expected to be strictly increasing
/// by `jd`. Arrays written before ingestion started sanitizing new points can be
/// out of order, hold duplicate `jd` values, or carry entries with a non-finite
/// or non-numeric `jd`.
///
/// Ingestion self-heals only part of that: an out-of-order, duplicate or
/// non-finite `jd` makes `prepare_timeseries_update` reject the stored array and
/// the worker falls back to the in-database update path, which rewrites it. An
/// entry whose `jd` is missing or not a number never gets that far, it fails to
/// deserialize in `get_existing_aux` and the alert errors out again on every
/// retry. This tool fixes both, plus the objects that are never going to receive
/// another alert.
///
/// Pipeline:
/// 1. Resolve the survey-specific set of timeseries fields and project only
///    `_id` and each field's `jd` so the scan stays cheap.
/// 2. Split the collection into `--processes` shards cut on an indexed
///    insertion-order field, scanned concurrently.
/// 3. For each document, flag fields that violate the strictly-increasing
///    invariant and count the points the repair deletes (`inspect_series`).
/// 4. For broken fields, issue a `$set` update whose value is the same
///    aggregation expression ingestion uses (`update_timeseries_op` with no new
///    points), which filters, dedups and sorts the array in place. Updates are
///    batched into bulk writes.
///
/// `--dry-run` performs steps 1-3 and reports the counts, including how many
/// points a real run would delete, without writing anything.
#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    #[arg(long, default_value_t = 5000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of parallel scan+repair shards.
    #[arg(long, default_value_t = 1, value_parser = parse_positive_usize)]
    processes: usize,

    /// Scan and report broken records without writing anything.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

/// Timeseries fields stored in `<survey>_alerts_aux` that must be strictly
/// increasing by `jd`. Source of truth: the `AlertAuxForUpdate` structs in
/// `src/alert/<survey>.rs`.
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

/// Mirrors what `update_timeseries_op` does server-side: drop points whose `jd`
/// is not a finite number, keep only the first occurrence of a duplicated `jd`,
/// sort the rest. `broken` is 0 or 1, so summing it counts documents.
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
        if !seen.insert(if jd == 0.0 { 0.0f64 } else { jd }.to_bits()) {
            stats.dropped_duplicate += 1;
            continue;
        }
        if prev.is_some_and(|p| jd < p) {
            out_of_order = true;
        }
        prev = Some(jd);
    }
    if out_of_order || stats.dropped() > 0 {
        stats.broken = 1;
    }
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

async fn scan_and_repair_shard(
    aux_collection: Collection<Document>,
    fields: &'static [&'static str],
    filter: Document,
    batch_size: usize,
    dry_run: bool,
    pb: ProgressBar,
) -> Result<ShardStats, mongodb::error::Error> {
    let client = aux_collection.client().clone();
    let aux_ns = aux_collection.namespace();
    let mut cursor = aux_collection
        .find(filter)
        .projection(jd_projection(fields))
        .no_cursor_timeout(true)
        .batch_size(CURSOR_BATCH_SIZE)
        .await?;

    let mut scanned: u64 = 0;
    let mut broken_total: u64 = 0;
    let mut modified: u64 = 0;
    let mut per_field = vec![FieldStats::default(); fields.len()];
    let mut batch: Vec<WriteModel> = Vec::with_capacity(batch_size);

    while let Some(d) = cursor.try_next().await? {
        scanned += 1;
        pb.inc(1);

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

        let id = match d.get("_id") {
            Some(v) => v.clone(),
            None => continue,
        };
        let mut set_doc = Document::new();
        for f in &broken {
            set_doc.insert(*f, update_timeseries_op(f, "jd", &vec![]));
        }
        batch.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(aux_ns.clone())
                .filter(doc! { "_id": id })
                .update(UpdateModifications::Pipeline(vec![
                    doc! { "$set": set_doc },
                ]))
                .build(),
        ));
        if batch.len() >= batch_size {
            modified += flush_batch(&client, &mut batch).await?;
        }
    }
    if !batch.is_empty() {
        modified += flush_batch(&client, &mut batch).await?;
    }
    Ok(ShardStats {
        scanned,
        broken: broken_total,
        modified,
        per_field,
    })
}

async fn flush_batch(
    client: &mongodb::Client,
    batch: &mut Vec<WriteModel>,
) -> Result<u64, mongodb::error::Error> {
    // Keep the capacity: a bare mem::take makes every following batch regrow from zero.
    let capacity = batch.capacity();
    let drained = std::mem::replace(batch, Vec::with_capacity(capacity));
    let result = client.bulk_write(drained).ordered(false).await?;
    Ok(result.modified_count as u64)
}

/// `Ok(false)` means the shards did not cover the whole collection: what they saw was
/// repaired, but the run is not a clean pass over it.
async fn run_repair(
    survey: &Survey,
    aux_collection: Collection<Document>,
    batch_size: usize,
    processes: usize,
    dry_run: bool,
) -> Result<bool, TaskError> {
    let aux_ns = aux_collection.namespace();
    let fields = timeseries_fields(survey);

    let shard_field = shard_field(&aux_collection).await;
    let shards = range_shards(&aux_collection, processes, shard_field).await;
    info!(
        "scanning {} in {} shard(s) cut on '{}'",
        aux_ns,
        shards.len(),
        shard_field
    );

    let total = aux_collection.count_documents(doc! {}).await?;
    let label = format!("scan→{}", survey);
    let pb = make_progress_bar(total, label.clone());
    pb.enable_steady_tick(std::time::Duration::from_millis(200));
    let logger = spawn_progress_logger(pb.clone(), label);

    let mut handles = Vec::with_capacity(shards.len());
    for filter in shards {
        let aux = aux_collection.clone();
        let pb = pb.clone();
        handles.push(tokio::spawn(async move {
            scan_and_repair_shard(aux, fields, filter, batch_size, dry_run, pb).await
        }));
    }

    let outcome = join_tasks(handles, "shard").await;
    logger.abort();
    pb.finish();
    let stats = outcome?;

    let scanned = stats.iter().map(|s| s.scanned).sum::<u64>();
    let mut per_field = vec![FieldStats::default(); fields.len()];
    for shard in &stats {
        for (acc, shard_total) in per_field.iter_mut().zip(&shard.per_field) {
            acc.merge(shard_total);
        }
    }
    for (field, totals) in fields.iter().copied().zip(&per_field) {
        if totals.broken == 0 {
            continue;
        }
        info!(
            field,
            documents = totals.broken,
            points_dropped_invalid = totals.dropped_invalid,
            points_dropped_duplicate = totals.dropped_duplicate,
            "field needs repair"
        );
    }

    let dropped_invalid = per_field.iter().map(|f| f.dropped_invalid).sum::<u64>();
    let dropped_duplicate = per_field.iter().map(|f| f.dropped_duplicate).sum::<u64>();
    let dropped = dropped_invalid + dropped_duplicate;
    if dropped > 0 {
        warn!(
            "{} point(s) {} deleted: {} with a non-finite or non-numeric jd, {} duplicating an \
             earlier jd. update_timeseries_op removes them, the repaired document does not keep \
             a copy",
            dropped,
            if dry_run { "would be" } else { "were" },
            dropped_invalid,
            dropped_duplicate
        );
    }

    info!(
        survey = %survey,
        scanned,
        broken = stats.iter().map(|s| s.broken).sum::<u64>(),
        modified = stats.iter().map(|s| s.modified).sum::<u64>(),
        points_dropped = dropped,
        dry_run,
        "repair_photometry_ordering done"
    );

    if scanned < total {
        if shard_count > 1 {
            error!(
                "scanned {} of the {} document(s) in {}: the shards did not cover it all, re-run \
                 with --processes 1",
                scanned, total, aux_ns
            );
            return Ok(false);
        }
        warn!(
            "scanned {} of the {} document(s) counted in {} before the pass: documents were \
             deleted while it ran",
            scanned, total, aux_ns
        );
    }
    Ok(true)
}

#[tokio::main]
async fn main() {
    load_dotenv();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting subscriber failed");

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

    let aux_name = format!("{}_alerts_aux", args.survey);
    match db.list_collection_names().await {
        Ok(names) => {
            if !names.iter().any(|n| n == &aux_name) {
                error!(
                    "collection {} does not exist in database {}, check that --config points at \
                     the right database",
                    aux_name,
                    db.name()
                );
                std::process::exit(1);
            }
        }
        Err(e) => {
            error!("error listing collections: {}", e);
            std::process::exit(1);
        }
    }

    info!(
        "starting repair_photometry_ordering: survey={} processes={} batch_size={} dry_run={}",
        args.survey, args.processes, args.batch_size, args.dry_run,
    );

    match run_repair(
        &args.survey,
        db.collection(&aux_name),
        args.batch_size,
        args.processes,
        args.dry_run,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(e) => {
            error!("repair run failed: {}", e);
            std::process::exit(1);
        }
    }
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
}
