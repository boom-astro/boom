use boom::{
    conf::{load_dotenv, AppConfig},
    utils::{data::make_progress_bar, enums::Survey, parser::parse_positive_usize},
};
use clap::Parser;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    options::{UpdateModifications, UpdateOneModel, WriteModel},
    Namespace,
};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

const QUEUE_MULTIPLIER: usize = 2;

/// Binary for repairing the ordering of timeseries fields
/// (`fp_hists`, `prv_candidates`, and `prv_nondetections` for ZTF) in the
/// `<survey>_alerts_aux` collection.
///
/// The boom pipeline require each series to be strictly increasing by
/// `jd`. Records inserted before that invariant was enforced can contain
/// out-of-order points, which causes `prepare_timeseries_update`
/// to log the error "is not strictly increasing".
///
/// This binary streams `<survey>_alerts_aux` projecting only each series'
/// `jd` arrays, detects which records violate the invariant, and dispatches a
/// server-side aggregation update that filters out non-finite `jd`s, sorts by
/// `jd` ascending, and drops duplicate-`jd` neighbours - the exact same
/// sanitization `TimeSeries::sanitize_timeseries` performs in Rust.
#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    #[arg(long, default_value_t = 5000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of parallel worker tasks.
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
    }
}

/// Mirrors `TimeSeries::validate_monotonic_increasing` in `src/alert/base.rs`:
/// any non-finite `jd` or `jd <= prev_jd` makes the series invalid. A missing
/// or non-array field is treated as valid (nothing to repair).
fn is_strictly_increasing(doc: &Document, field: &str) -> bool {
    let arr = match doc.get_array(field) {
        Ok(a) => a,
        Err(_) => return true,
    };
    let mut prev: Option<f64> = None;
    for item in arr {
        let jd = match item.as_document().and_then(|d| d.get_f64("jd").ok()) {
            Some(v) => v,
            None => return false,
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

/// Aggregation expression that returns a strictly-increasing-by-`jd` version
/// of `$<field>`:
///   1. `$filter` out elements whose `jd` is missing, non-numeric, NaN, or
///      infinite (matches `sanitize_timeseries`'s `is_finite()` retain).
///   2. `$sortArray` by `jd` ascending.
///   3. `$reduce` to drop any element whose `jd` equals its left neighbour
///      (keeping the first occurrence, matching `dedup_by`).
/// A non-array field is returned unchanged.
fn sort_dedup_expr(field: &str) -> Document {
    let field_ref = format!("${}", field);

    let filter_finite = doc! {
        "$filter": {
            "input": &field_ref,
            "as": "x",
            "cond": {
                "$and": [
                    { "$isNumber": "$$x.jd" },
                    { "$eq": ["$$x.jd", "$$x.jd"] },
                    { "$lt": [{ "$abs": "$$x.jd" }, 1e308_f64] }
                ]
            }
        }
    };

    let sorted = doc! {
        "$sortArray": {
            "input": filter_finite,
            "sortBy": { "jd": 1 }
        }
    };

    let keep_this = doc! {
        "$or": [
            { "$eq": [{ "$size": "$$value" }, 0] },
            {
                "$let": {
                    "vars": { "last": { "$arrayElemAt": ["$$value", -1] } },
                    "in": { "$ne": ["$$last.jd", "$$this.jd"] }
                }
            }
        ]
    };

    let dedup = doc! {
        "$reduce": {
            "input": sorted,
            "initialValue": [],
            "in": {
                "$cond": [
                    keep_this,
                    { "$concatArrays": ["$$value", ["$$this"]] },
                    "$$value"
                ]
            }
        }
    };

    doc! {
        "$cond": [
            { "$isArray": &field_ref },
            dedup,
            &field_ref
        ]
    }
}

struct RepairWork {
    id: mongodb::bson::Bson,
    broken_fields: Vec<&'static str>,
}

async fn run_repair(
    survey: &Survey,
    db: mongodb::Database,
    batch_size: usize,
    processes: usize,
    dry_run: bool,
) -> Result<(), mongodb::error::Error> {
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let aux_ns = aux_collection.namespace();
    let fields = timeseries_fields(survey);

    // Only fetch what we need to validate ordering: `_id` and each series' `jd`s.
    let mut projection = doc! { "_id": 1 };
    for f in fields {
        projection.insert(format!("{}.jd", f), 1);
    }

    let estimated = aux_collection.estimated_document_count().await.unwrap_or(0);
    let pb = make_progress_bar(estimated, format!("scan→{}", survey));

    let queue_capacity = processes * batch_size * QUEUE_MULTIPLIER;
    let (tx, rx) = async_channel::bounded::<RepairWork>(queue_capacity);

    let mut workers = Vec::with_capacity(processes);
    for _ in 0..processes {
        let rx = rx.clone();
        let db = db.clone();
        let aux_ns = aux_ns.clone();
        workers.push(tokio::spawn(async move {
            repair_worker(db, aux_ns, rx, batch_size, dry_run).await
        }));
    }
    drop(rx);

    let mut cursor = aux_collection
        .find(doc! {})
        .projection(projection)
        .no_cursor_timeout(true)
        .await?;

    let mut scanned: u64 = 0;
    let mut broken_total: u64 = 0;
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
        let id = match d.get("_id") {
            Some(v) => v.clone(),
            None => continue,
        };
        if tx
            .send(RepairWork {
                id,
                broken_fields: broken,
            })
            .await
            .is_err()
        {
            break;
        }
    }
    drop(tx);

    let mut first_err: Option<mongodb::error::Error> = None;
    let mut modified_total: u64 = 0;
    for h in workers {
        match h.await {
            Ok(Ok(n)) => modified_total += n,
            Ok(Err(e)) => {
                error!("worker failed: {}", e);
                first_err.get_or_insert(e);
            }
            Err(e) => {
                error!("worker join failed: {}", e);
            }
        }
    }
    pb.finish();

    info!(
        survey = %survey,
        scanned,
        broken = broken_total,
        modified = modified_total,
        dry_run,
        "repair_photometry_ordering done"
    );

    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(())
}

async fn repair_worker(
    db: mongodb::Database,
    aux_ns: Namespace,
    rx: async_channel::Receiver<RepairWork>,
    batch_size: usize,
    dry_run: bool,
) -> Result<u64, mongodb::error::Error> {
    let client = db.client().clone();
    let mut batch: Vec<WriteModel> = Vec::with_capacity(batch_size);
    let mut modified: u64 = 0;

    while let Ok(work) = rx.recv().await {
        if dry_run {
            warn!(
                id = ?work.id,
                fields = ?work.broken_fields,
                "would repair"
            );
            continue;
        }
        let mut set_doc = Document::new();
        for f in &work.broken_fields {
            set_doc.insert(*f, sort_dedup_expr(f));
        }
        let pipeline = vec![doc! { "$set": set_doc }];
        batch.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(aux_ns.clone())
                .filter(doc! { "_id": work.id })
                .update(UpdateModifications::Pipeline(pipeline))
                .build(),
        ));
        if batch.len() >= batch_size {
            modified += flush_batch(&client, &mut batch).await?;
        }
    }
    if !batch.is_empty() {
        modified += flush_batch(&client, &mut batch).await?;
    }
    Ok(modified)
}

async fn flush_batch(
    client: &mongodb::Client,
    batch: &mut Vec<WriteModel>,
) -> Result<u64, mongodb::error::Error> {
    if batch.is_empty() {
        return Ok(0);
    }
    let drained: Vec<WriteModel> = std::mem::take(batch);
    let result = client.bulk_write(drained).await?;
    Ok(result.modified_count as u64)
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
