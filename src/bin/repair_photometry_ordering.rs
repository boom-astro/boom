use boom::{
    conf::{load_dotenv, AppConfig},
    utils::{data::make_progress_bar, enums::Survey, parser::parse_positive_usize},
};
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, Bson, Document},
    options::{Hint, UpdateModifications, UpdateOneModel, WriteModel},
    Collection, Namespace,
};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Repair the photometry timeseries arrays in `<survey>_alerts_aux`.
///
/// Each aux document holds timeseries fields (e.g. `prv_candidates`,
/// `prv_nondetections`, `fp_hists`) that are expected to be strictly increasing
/// by `jd`. Bugs in the alert ingestion path could leave these arrays out of
/// order, containing duplicate `jd` values, or carrying entries with a
/// non-finite/non-numeric `jd`. This is a one-shot maintenance tool that finds
/// and fixes those documents.
///
/// Pipeline:
/// 1. Resolve the survey-specific set of timeseries fields and project only
///    `_id` and each field's `jd` so the scan stays cheap.
/// 2. If an index on `created_at` exists, split the collection into
///    `--processes` half-open partitions (via `$sample`) scanned concurrently;
///    otherwise fall back to a single unhinted full scan.
/// 3. For each document, flag fields that violate the strictly-increasing
///    invariant (`is_strictly_increasing`).
/// 4. For broken fields, issue a `$set` update whose value is an aggregation
///    expression (`sort_dedup_expr`) that filters, sorts, and dedups the array
///    in place. Updates are batched into bulk writes.
///
/// `--dry-run` performs steps 1-3 and reports counts without writing anything.
/// The repair logic mirrors the validation/normalization rules used at
/// ingestion time so a repaired document matches what a fresh write would
/// produce.
#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    #[arg(long, default_value_t = 5000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of parallel scan+repair partitions.
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

/// Builds the MongoDB aggregation expression used in a `$set` pipeline stage to
/// repair `field` in place. If the field is not an array it is left untouched;
/// otherwise it is rebuilt by: filtering out elements with a non-numeric or
/// non-finite `jd`, sorting the survivors ascending by `jd`, then deduplicating
/// so that for any group of equal `jd` only the last one (after sorting) is
/// kept. The result is a strictly-increasing-by-`jd` array, matching the
/// invariant checked by `is_strictly_increasing`.
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

/// Sample `processes - 1` `_id` values and return `processes` half-open
/// partitions covering the full collection. `None` on a side means
/// open-ended (no lower / no upper bound).
async fn compute_partitions(
    aux_collection: &Collection<Document>,
    processes: usize,
) -> Result<Vec<(Option<Bson>, Option<Bson>)>, mongodb::error::Error> {
    if processes <= 1 {
        return Ok(vec![(None, None)]);
    }
    let pipeline = vec![
        doc! { "$sample": { "size": (processes - 1) as i64 } },
        doc! { "$project": { "created_at": 1 } },
        doc! { "$sort": { "created_at": 1 } },
    ];
    let mut cursor = aux_collection.aggregate(pipeline).await?;
    let mut boundaries: Vec<Bson> = Vec::with_capacity(processes - 1);
    while let Some(d) = cursor.try_next().await? {
        if let Some(v) = d.get("created_at") {
            boundaries.push(v.clone());
        }
    }
    boundaries.dedup();

    let mut parts = Vec::with_capacity(boundaries.len() + 1);
    let mut prev: Option<Bson> = None;
    for b in &boundaries {
        parts.push((prev.clone(), Some(b.clone())));
        prev = Some(b.clone());
    }
    parts.push((prev, None));
    Ok(parts)
}

/// Returns the direction (`1` or `-1`) of an index whose first key is
/// `created_at`, or `None` if no such index exists. The direction is needed so
/// the scan hint matches the actual index spec (a hint must correspond exactly
/// to an existing index).
async fn created_at_index_direction(
    aux_collection: &Collection<Document>,
) -> Result<Option<i32>, mongodb::error::Error> {
    let mut cursor = aux_collection.list_indexes().await?;
    while let Some(idx) = cursor.next().await {
        let idx = idx?;
        if let Some((k, v)) = idx.keys.iter().next() {
            if k == "created_at" {
                let direction = v.as_i32().or_else(|| v.as_i64().map(|n| n as i32));
                return Ok(direction);
            }
        }
    }
    Ok(None)
}

fn partition_filter(lower: &Option<Bson>, upper: &Option<Bson>) -> Document {
    let mut cond = Document::new();
    if let Some(l) = lower {
        cond.insert("$gte", l.clone());
    }
    if let Some(u) = upper {
        cond.insert("$lt", u.clone());
    }
    let mut f = Document::new();
    if !cond.is_empty() {
        f.insert("created_at", cond);
    }
    f
}

struct PartitionStats {
    scanned: u64,
    broken: u64,
    modified: u64,
}

async fn scan_and_repair_partition(
    aux_collection: Collection<Document>,
    aux_ns: Namespace,
    fields: &'static [&'static str],
    filter: Document,
    projection: Document,
    batch_size: usize,
    dry_run: bool,
    created_at_direction: Option<i32>,
    pb: ProgressBar,
) -> Result<PartitionStats, mongodb::error::Error> {
    let client = aux_collection.client().clone();
    let mut find = aux_collection
        .find(filter)
        .projection(projection)
        .no_cursor_timeout(true)
        .batch_size(batch_size as u32);
    if let Some(dir) = created_at_direction {
        find = find.hint(Hint::Keys(doc! { "created_at": dir }));
    }
    let mut cursor = find.await?;

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
        let id = match d.get("_id") {
            Some(v) => v.clone(),
            None => continue,
        };

        if dry_run {
            continue;
        }

        let mut set_doc = Document::new();
        for f in &broken {
            set_doc.insert(*f, sort_dedup_expr(f));
        }
        let pipeline = vec![doc! { "$set": set_doc }];
        batch.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(aux_ns.clone())
                .filter(doc! { "_id": id })
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
    Ok(PartitionStats {
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
    let result = client.bulk_write(drained).await?;
    Ok(result.modified_count as u64)
}

async fn run_repair(
    survey: &Survey,
    db: mongodb::Database,
    batch_size: usize,
    processes: usize,
    dry_run: bool,
) -> Result<(), mongodb::error::Error> {
    let aux_collection: Collection<Document> = db.collection(&format!("{}_alerts_aux", survey));
    let aux_ns = aux_collection.namespace();
    let fields = timeseries_fields(survey);

    let mut projection = doc! { "_id": 1 };
    for f in fields {
        projection.insert(format!("{}.jd", f), 1);
    }

    let created_at_direction = created_at_index_direction(&aux_collection).await?;
    let partitions = if created_at_direction.is_some() {
        info!(
            "computing {} partitions via $sample on created_at",
            processes
        );
        let parts = compute_partitions(&aux_collection, processes).await?;
        info!(
            "computed {} partition(s) (requested {})",
            parts.len(),
            processes
        );
        parts
    } else {
        warn!(
            "no index on `created_at` for {}; falling back to a single unhinted full scan",
            aux_ns
        );
        vec![(None, None)]
    };

    let estimated = aux_collection.estimated_document_count().await.unwrap_or(0);
    let pb = make_progress_bar(estimated, format!("scan→{}", survey));
    pb.enable_steady_tick(std::time::Duration::from_millis(200));

    let mut handles = Vec::with_capacity(partitions.len());
    for (lower, upper) in partitions {
        let aux = aux_collection.clone();
        let aux_ns = aux_ns.clone();
        let proj = projection.clone();
        let pb = pb.clone();
        let filter = partition_filter(&lower, &upper);
        handles.push(tokio::spawn(async move {
            scan_and_repair_partition(
                aux,
                aux_ns,
                fields,
                filter,
                proj,
                batch_size,
                dry_run,
                created_at_direction,
                pb,
            )
            .await
        }));
    }

    let mut first_err: Option<mongodb::error::Error> = None;
    let mut scanned_total: u64 = 0;
    let mut broken_total: u64 = 0;
    let mut modified_total: u64 = 0;
    for h in handles {
        match h.await {
            Ok(Ok(s)) => {
                scanned_total += s.scanned;
                broken_total += s.broken;
                modified_total += s.modified;
            }
            Ok(Err(e)) => {
                error!("partition failed: {}", e);
                first_err.get_or_insert(e);
            }
            Err(e) => {
                error!("partition join failed: {}", e);
            }
        }
    }
    pb.finish();

    info!(
        survey = %survey,
        scanned = scanned_total,
        broken = broken_total,
        modified = modified_total,
        dry_run,
        "repair_photometry_ordering_optimized done"
    );

    if let Some(e) = first_err {
        return Err(e);
    }
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
        "starting repair_photometry_ordering_optimized: survey={} processes={} batch_size={} dry_run={}",
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
