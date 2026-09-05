use boom::{
    conf::{load_dotenv, AppConfig},
    utils::{
        data::{make_progress_bar, spawn_progress_logger},
        db::{join_tasks, range_shards, shard_field, update_timeseries_op, TaskError},
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
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

/// Repair the photometry timeseries arrays in `<survey>_alerts_aux`.
///
/// Each aux document holds timeseries fields (e.g. `prv_candidates`,
/// `prv_nondetections`, `fp_hists`) that are expected to be strictly increasing
/// by `jd`. Bugs in the alert ingestion path could leave these arrays out of
/// order, containing duplicate `jd` values, or carrying entries with a
/// non-finite/non-numeric `jd`. Ingestion repairs such a document the next time
/// the object is updated (`prepare_timeseries_update` rejects the stored array
/// and the worker falls back to the in-database update path), so this tool
/// exists to fix the objects that are not going to receive another alert, and
/// to clear the error path for the ones that are.
///
/// Pipeline:
/// 1. Resolve the survey-specific set of timeseries fields and project only
///    `_id` and each field's `jd` so the scan stays cheap.
/// 2. Split the collection into `--processes` shards cut on an indexed
///    insertion-order field, scanned concurrently.
/// 3. For each document, flag fields that violate the strictly-increasing
///    invariant (`is_strictly_increasing`).
/// 4. For broken fields, issue a `$set` update whose value is the same
///    aggregation expression ingestion uses (`update_timeseries_op` with no new
///    points), which filters, dedups and sorts the array in place. Updates are
///    batched into bulk writes.
///
/// `--dry-run` performs steps 1-3 and reports counts without writing anything.
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

const CURSOR_BATCH_SIZE: u32 = 10_000;

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

/// Mirrors `TimeSeries::validate_monotonic_increasing`:
/// any non-finite `jd` or `jd <= prev_jd` makes the series invalid. A missing
/// or non-array field is treated as valid (nothing to repair).
fn is_strictly_increasing(doc: &Document, field: &str) -> bool {
    let arr = match doc.get_array(field) {
        Ok(a) => a,
        Err(_) => return true,
    };
    let mut prev: Option<f64> = None;
    for item in arr {
        let jd = match item.as_document().and_then(|d| d.get("jd")) {
            Some(Bson::Double(v)) => *v,
            Some(Bson::Int32(v)) => *v as f64,
            Some(Bson::Int64(v)) => *v as f64,
            _ => return false,
        };
        if !jd.is_finite() {
            return false;
        }
        if let Some(p) = prev {
            if jd <= p {
                return false;
            }
        }
        prev = Some(jd);
    }
    true
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
    let mut batch: Vec<WriteModel> = Vec::with_capacity(batch_size);

    while let Some(d) = cursor.try_next().await? {
        scanned += 1;
        pb.inc(1);

        let broken: Vec<&'static str> = fields
            .iter()
            .copied()
            .filter(|f| !is_strictly_increasing(&d, f))
            .collect();
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
    })
}

async fn flush_batch(
    client: &mongodb::Client,
    batch: &mut Vec<WriteModel>,
) -> Result<u64, mongodb::error::Error> {
    if batch.is_empty() {
        return Ok(0);
    }
    let drained: Vec<WriteModel> = std::mem::take(batch);
    let result = client.bulk_write(drained).ordered(false).await?;
    Ok(result.modified_count as u64)
}

async fn run_repair(
    survey: &Survey,
    db: mongodb::Database,
    batch_size: usize,
    processes: usize,
    dry_run: bool,
) -> Result<(), TaskError> {
    let aux_collection: Collection<Document> = db.collection(&format!("{}_alerts_aux", survey));
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

    let estimated = aux_collection.estimated_document_count().await.unwrap_or(0);
    let label = format!("scan→{}", survey);
    let pb = make_progress_bar(estimated, label.clone());
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

    info!(
        survey = %survey,
        scanned = stats.iter().map(|s| s.scanned).sum::<u64>(),
        broken = stats.iter().map(|s| s.broken).sum::<u64>(),
        modified = stats.iter().map(|s| s.modified).sum::<u64>(),
        dry_run,
        "repair_photometry_ordering done"
    );
    Ok(())
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

    info!(
        "starting repair_photometry_ordering: survey={} processes={} batch_size={} dry_run={}",
        args.survey, args.processes, args.batch_size, args.dry_run,
    );

    if let Err(e) = run_repair(
        &args.survey,
        db,
        args.batch_size,
        args.processes,
        args.dry_run,
    )
    .await
    {
        error!("repair run failed: {}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(jds: &[f64]) -> Document {
        doc! { "fp_hists": jds.iter().map(|jd| doc! { "jd": jd }).collect::<Vec<_>>() }
    }

    #[test]
    fn accepts_strictly_increasing_and_empty_or_missing_series() {
        assert!(is_strictly_increasing(
            &series(&[1.0, 2.0, 3.0]),
            "fp_hists"
        ));
        assert!(is_strictly_increasing(&series(&[]), "fp_hists"));
        assert!(is_strictly_increasing(&doc! {}, "fp_hists"));
    }

    #[test]
    fn rejects_duplicate_decreasing_and_non_finite_jds() {
        assert!(!is_strictly_increasing(&series(&[1.0, 1.0]), "fp_hists"));
        assert!(!is_strictly_increasing(&series(&[2.0, 1.0]), "fp_hists"));
        assert!(!is_strictly_increasing(&series(&[f64::NAN]), "fp_hists"));
        assert!(!is_strictly_increasing(
            &series(&[1.0, f64::INFINITY]),
            "fp_hists"
        ));
    }

    #[test]
    fn rejects_entries_without_a_numeric_jd() {
        let doc = doc! { "fp_hists": [doc! { "jd": 1.0 }, doc! { "flux": 1.0 }] };
        assert!(!is_strictly_increasing(&doc, "fp_hists"));
    }

    #[test]
    fn accepts_integer_jds() {
        let doc = doc! { "fp_hists": [doc! { "jd": 1i32 }, doc! { "jd": 2i64 }] };
        assert!(is_strictly_increasing(&doc, "fp_hists"));
    }
}
