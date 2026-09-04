use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use boom::{
    api::catalogs::WATCHLIST_PREFIX,
    conf::{load_dotenv, AppConfig, CatalogXmatchConfig},
    utils::{
        data::{make_progress_bar, spawn_progress_logger},
        db::{join_tasks, range_shards, shard_field, TaskError},
        enums::Survey,
        parser::parse_positive_usize,
        spatial::{
            cm_radius_arcsec, distance_kpc_from_arcsec, get_f64_from_doc, watchlist_match_field,
            xmatch, Coordinates,
        },
    },
};
use clap::{Parser, ValueEnum};
use flare::{spatial::great_circle_distance, Time};
use futures::{StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, Document},
    options::{UpdateModifications, UpdateOneModel, WriteModel},
    Namespace,
};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

const QUEUE_MULTIPLIER: usize = 2;
const CURSOR_BATCH_SIZE: u32 = 10_000;
const ARCSEC_TO_RAD: f64 = std::f64::consts::PI / 180.0 / 3600.0;
const STATE_COLLECTION: &str = "reprocess_crossmatch_state";
const STATUS_MATCHING: &str = "matching";
const STATUS_CLEAN: &str = "clean";
const TEMP_PROBE_SAMPLE: i64 = 10_000;

/// Catalog-driven costs extra full passes over alerts_aux, so it only wins when the
/// catalog is substantially smaller, not merely smaller.
const CATALOG_DRIVEN_MARGIN: u64 = 4;

/// Binary for reprocessing crossmatches between a survey's alerts_aux collection and one or more catalogs.
/// The scheduler pipeline only crossmatches at first insert, so adding a catalog to
/// `crossmatch.<survey>` in config.yaml leaves pre-existing alerts_aux records with
/// no entry for it, so this binary fills in those gaps. It can also be used to reprocess existing
/// crossmatches if the matching parameters (e.g. radius) for a catalog are changed.
///
/// Watchlist catalogs (name prefixed with `watchlist_`) are handled differently: instead of
/// writing onto `alerts_aux.cross_matches`, ingestion records the matching alert object_ids on
/// the watchlist document itself under `matching_<survey>_objects`. For those, this binary loops
/// over the (small) watchlist entries and `$addToSet`s the object_ids of every alerts_aux record
/// within radius. `$addToSet` is idempotent, so re-running is safe and concurrent with live ingest.
#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    /// Each catalog must already be declared under `crossmatch.<survey>` in
    /// config.yaml (radius / projection / etc. are read from there).
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    catalogs: Vec<String>,

    #[arg(long, value_enum, default_value_t = Direction::Auto)]
    direction: Direction,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Number of records accumulated per worker before a bulk write is issued.
    #[arg(long, default_value_t = 5000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of parallel worker tasks, and of shards the cleanup passes are split into.
    #[arg(long, default_value_t = 1, value_parser = parse_positive_usize)]
    processes: usize,

    /// Queries kept in flight per worker. Workers are database-bound, so
    /// `processes` × `concurrency` is what sets throughput, not the core count.
    /// Keep that product under `database.max_pool_size`.
    #[arg(long, default_value_t = 8, value_parser = parse_positive_usize)]
    concurrency: usize,

    /// Objects-driven only: skip alerts_aux records that already carry every
    /// selected catalog. Makes an interrupted run resumable.
    #[arg(long, default_value_t = false)]
    skip_existing: bool,

    /// Catalog-driven only: leave records with no match untouched instead of writing an
    /// empty array. Safe when filling in a new catalog, but it will not clear stale matches.
    #[arg(long, default_value_t = false)]
    skip_empty: bool,

    /// Catalog-driven only: force the scan that clears the temp buffer, which is
    /// otherwise skipped when no interrupted run is detected.
    #[arg(long, default_value_t = false)]
    reset_temp: bool,
}

/// Reprocessing can be done in two directions:
/// - Checking the crossmatch catalogs for each alerts_aux record,
/// - Checking the alerts_aux collection for each catalog record.
///
/// To optimize the reprocessing, the binary can loop over either
/// the alerts_aux collection or the catalog collection, depending on which is smaller.
/// If `--direction` is not provided, it checks the estimated document counts
/// of each collection and loops over the smaller one.
#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Direction {
    /// Pick `objects` or `catalog` per catalog based on which side has fewer rows.
    Auto,
    /// Loop over alerts_aux records, query catalog. Best when alerts_aux is smaller.
    Objects,
    /// Loop over catalog rows, query aux. Best when catalog is smaller.
    Catalog,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct AuxIdAndCoords {
    #[serde(rename = "_id")]
    object_id: String,
    coordinates: Coordinates,
}

fn aux_match_projection() -> Document {
    doc! { "_id": 1, "coordinates.radec_geojson.coordinates": 1 }
}

async fn set_reprocess_state(
    db: &mongodb::Database,
    state_id: &str,
    status: &str,
) -> Result<(), mongodb::error::Error> {
    db.collection::<Document>(STATE_COLLECTION)
        .update_one(
            doc! { "_id": state_id },
            doc! { "$set": { "status": status, "updated_at": Time::now().to_jd() } },
        )
        .upsert(true)
        .await?;
    Ok(())
}

/// A run is bracketed by a state marker, so an interrupted one is known without touching
/// alerts_aux. Versions before the marker wrote temp on *every* existing record, which is
/// why a leftover from those is still caught by a small random sample.
async fn temp_needs_reset(
    db: &mongodb::Database,
    aux_collection: &mongodb::Collection<Document>,
    state_id: &str,
    temp_field: &str,
) -> Result<bool, mongodb::error::Error> {
    let previous = db
        .collection::<Document>(STATE_COLLECTION)
        .find_one(doc! { "_id": state_id })
        .await?;
    if let Some(previous) = previous {
        if previous.get_str("status").unwrap_or(STATUS_CLEAN) == STATUS_MATCHING {
            warn!(
                "previous run for '{}' was interrupted while matching",
                state_id
            );
            return Ok(true);
        }
    }
    let mut probe = aux_collection
        .aggregate(vec![
            doc! { "$sample": { "size": TEMP_PROBE_SAMPLE } },
            doc! { "$match": { temp_field: { "$exists": true } } },
            doc! { "$limit": 1 },
        ])
        .await?;
    Ok(probe.try_next().await?.is_some())
}

// -----------------------------------------------------------------------------
// Sharded full-collection updates: a single `update_many` runs as one server-side
// operation, so splitting it into ranges is the only way to use more than one
// thread for a pass over a billion documents. Ranges are cut on an indexed field
// that tracks insertion order, so that each shard walks a roughly contiguous
// region on disk rather than jumping around it.
// -----------------------------------------------------------------------------

async fn sharded_update_many(
    collection: &mongodb::Collection<Document>,
    shards: &[Document],
    base_filter: &Document,
    update: UpdateModifications,
    label: &str,
) -> Result<u64, mongodb::error::Error> {
    let done = Arc::new(AtomicUsize::new(0));
    let total = shards.len();
    let results = futures::future::join_all(shards.iter().enumerate().map(|(index, shard)| {
        let filter = if shard.is_empty() {
            base_filter.clone()
        } else if base_filter.is_empty() {
            shard.clone()
        } else {
            doc! { "$and": [base_filter.clone(), shard.clone()] }
        };
        let collection = collection.clone();
        let update = update.clone();
        let done = Arc::clone(&done);
        async move {
            let result = collection.update_many(filter, update).await;
            let completed = done.fetch_add(1, Ordering::Relaxed) + 1;
            match &result {
                Ok(outcome) => info!(
                    "[{}] shard {}/{} done, {} modified ({} shards complete)",
                    label,
                    index + 1,
                    total,
                    outcome.modified_count,
                    completed
                ),
                Err(e) => warn!(
                    error = %e,
                    "[{}] shard {}/{} failed ({} shards complete)",
                    label,
                    index + 1,
                    total,
                    completed
                ),
            }
            result
        }
    }))
    .await;

    let mut modified = 0;
    for result in results {
        modified += result?.modified_count;
    }
    Ok(modified)
}

// -----------------------------------------------------------------------------
// objects-driven: stream alerts_aux records, fan out to N workers running xmatch().
// One pass updates all selected catalogs at once via the existing 1×N xmatch.
// -----------------------------------------------------------------------------
async fn run_objects_driven(
    survey: &Survey,
    catalogs: Vec<CatalogXmatchConfig>,
    db: mongodb::Database,
    batch_size: usize,
    processes: usize,
    concurrency: usize,
    skip_existing: bool,
) -> Result<(), TaskError> {
    let aux_collection: mongodb::Collection<AuxIdAndCoords> =
        db.collection(&format!("{}_alerts_aux", survey));
    let estimated = aux_collection.estimated_document_count().await.unwrap_or(0);
    let label = catalogs
        .iter()
        .map(|c| c.catalog.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let label = format!("objects→{}", label);
    let pb = make_progress_bar(estimated, label.clone());
    let logger = spawn_progress_logger(pb.clone(), label);

    let queue_capacity = processes * batch_size * QUEUE_MULTIPLIER;
    let (tx, rx) = async_channel::bounded::<AuxIdAndCoords>(queue_capacity);

    let mut workers = Vec::with_capacity(processes);
    for _ in 0..processes {
        let rx = rx.clone();
        let pb = pb.clone();
        let survey = survey.clone();
        let db = db.clone();
        let catalogs = catalogs.clone();
        workers.push(tokio::spawn(async move {
            objects_worker(survey, db, catalogs, rx, batch_size, concurrency, pb).await
        }));
    }
    drop(rx);

    let find_filter = if skip_existing {
        let missing: Vec<Document> = catalogs
            .iter()
            .map(|c| doc! { format!("cross_matches.{}", c.catalog): { "$exists": false } })
            .collect();
        doc! { "$or": missing }
    } else {
        doc! {}
    };

    let mut cursor = aux_collection
        .find(find_filter)
        .projection(doc! { "_id": 1, "coordinates": 1 })
        .batch_size(CURSOR_BATCH_SIZE)
        .no_cursor_timeout(true)
        .await?;
    while let Some(d) = cursor.try_next().await? {
        if tx.send(d).await.is_err() {
            break;
        }
    }
    drop(tx);

    let outcome = join_tasks(workers, "worker").await;
    logger.abort();
    pb.finish();
    outcome?;
    Ok(())
}

async fn objects_worker(
    survey: Survey,
    db: mongodb::Database,
    catalogs: Vec<CatalogXmatchConfig>,
    rx: async_channel::Receiver<AuxIdAndCoords>,
    batch_size: usize,
    concurrency: usize,
    pb: ProgressBar,
) -> Result<(), mongodb::error::Error> {
    let client = db.client().clone();
    let aux_collection: mongodb::Collection<AuxIdAndCoords> =
        db.collection(&format!("{}_alerts_aux", survey));
    let aux_ns = aux_collection.namespace();

    let mut batch = Vec::with_capacity(batch_size);
    while let Ok(item) = rx.recv().await {
        batch.push(item);
        if batch.len() >= batch_size {
            flush_objects_batch(
                &db,
                &client,
                &aux_ns,
                &survey,
                &catalogs,
                &mut batch,
                concurrency,
                &pb,
            )
            .await?;
        }
    }
    if !batch.is_empty() {
        flush_objects_batch(
            &db,
            &client,
            &aux_ns,
            &survey,
            &catalogs,
            &mut batch,
            concurrency,
            &pb,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn flush_objects_batch(
    db: &mongodb::Database,
    client: &mongodb::Client,
    aux_ns: &Namespace,
    survey: &Survey,
    catalogs: &[CatalogXmatchConfig],
    batch: &mut Vec<AuxIdAndCoords>,
    concurrency: usize,
    pb: &ProgressBar,
) -> Result<(), mongodb::error::Error> {
    let writes: Vec<WriteModel> = futures::stream::iter(batch.drain(..))
        .map(|obj| async move {
            let (ra, dec) = obj.coordinates.get_radec();
            let result = xmatch(ra, dec, &obj.object_id, survey, catalogs, db).await;
            pb.inc(1);
            let mut xmatches = match result {
                Ok(r) => r,
                Err(e) => {
                    warn!(object_id = %obj.object_id, error = %e, "xmatch failed, skipping");
                    return None;
                }
            };
            let mut set_doc = Document::new();
            for cat in catalogs {
                let matches = xmatches.remove(&cat.catalog).unwrap_or_default();
                set_doc.insert(format!("cross_matches.{}", cat.catalog), matches);
            }
            Some(WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(aux_ns.clone())
                    .filter(doc! { "_id": obj.object_id })
                    .update(doc! { "$set": set_doc })
                    .build(),
            ))
        })
        .buffer_unordered(concurrency)
        .filter_map(|w| async move { w })
        .collect()
        .await;

    if !writes.is_empty() {
        client.bulk_write(writes).ordered(false).await?;
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// watchlist-driven: stream the (small) watchlist entries, fan out to N workers
// that geo-lookup every alerts_aux record within radius and `$addToSet` their
// object_ids onto the watchlist document under `matching_<survey>_objects`.
// This mirrors the side effect ingestion's xmatch() performs, but for records
// that already existed in the DB when the watchlist was added. `$addToSet` is
// idempotent so re-running is safe and never races with live ingest.
// -----------------------------------------------------------------------------
async fn run_watchlist_driven(
    survey: &Survey,
    watchlist_config: CatalogXmatchConfig,
    db: mongodb::Database,
    batch_size: usize,
    processes: usize,
) -> Result<(), TaskError> {
    let wl_collection: mongodb::Collection<Document> = db.collection(&watchlist_config.catalog);
    let estimated = wl_collection.estimated_document_count().await.unwrap_or(0);
    let label = format!("watchlist→{}", watchlist_config.catalog);
    let pb = make_progress_bar(estimated, label.clone());
    let logger = spawn_progress_logger(pb.clone(), label);

    let queue_capacity = processes * batch_size * QUEUE_MULTIPLIER;
    let (tx, rx) = async_channel::bounded::<Document>(queue_capacity);

    let mut workers = Vec::with_capacity(processes);
    for _ in 0..processes {
        let rx = rx.clone();
        let pb = pb.clone();
        let survey = survey.clone();
        let db = db.clone();
        let watchlist_config = watchlist_config.clone();
        workers.push(tokio::spawn(async move {
            watchlist_worker(survey, db, watchlist_config, rx, pb).await
        }));
    }
    drop(rx);

    // Only `_id` and `coordinates` are needed: coordinates.radec_geojson is
    // guaranteed present (ingestion's geo match relies on it).
    let mut cursor = wl_collection
        .find(doc! {})
        .projection(doc! { "_id": 1, "coordinates": 1 })
        .batch_size(CURSOR_BATCH_SIZE)
        .no_cursor_timeout(true)
        .await?;
    while let Some(d) = cursor.try_next().await? {
        if tx.send(d).await.is_err() {
            break;
        }
    }
    drop(tx);

    let outcome = join_tasks(workers, "worker").await;
    logger.abort();
    pb.finish();
    outcome?;
    Ok(())
}

async fn watchlist_worker(
    survey: Survey,
    db: mongodb::Database,
    watchlist_config: CatalogXmatchConfig,
    rx: async_channel::Receiver<Document>,
    pb: ProgressBar,
) -> Result<(), mongodb::error::Error> {
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let wl_collection: mongodb::Collection<Document> = db.collection(&watchlist_config.catalog);
    let field = watchlist_match_field(&survey);

    while let Ok(wl_doc) = rx.recv().await {
        pb.inc(1);
        if let Err(e) = process_watchlist_doc(
            &aux_collection,
            &wl_collection,
            &watchlist_config,
            &field,
            &wl_doc,
        )
        .await
        {
            warn!(error = %e, "watchlist row processing failed, skipping");
        }
    }
    Ok(())
}

async fn process_watchlist_doc(
    aux_collection: &mongodb::Collection<Document>,
    wl_collection: &mongodb::Collection<Document>,
    watchlist_config: &CatalogXmatchConfig,
    field: &str,
    wl_doc: &Document,
) -> Result<(), mongodb::error::Error> {
    let wl_id = match wl_doc.get("_id") {
        Some(v) => v.clone(),
        None => return Ok(()),
    };
    let (wl_ra, wl_dec) = match extract_radec(wl_doc) {
        Some(v) => v,
        None => return Ok(()),
    };

    let wl_ra_geojson = wl_ra - 180.0;
    let aux_filter = doc! {
        "coordinates.radec_geojson": {
            "$geoWithin": {
                "$centerSphere": [[wl_ra_geojson, wl_dec], watchlist_config.radius]
            }
        },
    };
    let mut aux_cursor = aux_collection
        .find(aux_filter)
        .projection(doc! { "_id": 1 })
        .batch_size(CURSOR_BATCH_SIZE)
        .await?;

    let mut object_ids: Vec<mongodb::bson::Bson> = Vec::new();
    while let Some(aux_doc) = aux_cursor.try_next().await? {
        if let Ok(id) = aux_doc.get_str("_id") {
            object_ids.push(mongodb::bson::Bson::String(id.to_string()));
        }
    }
    if object_ids.is_empty() {
        return Ok(());
    }

    wl_collection
        .update_one(
            doc! { "_id": wl_id },
            doc! { "$addToSet": { field: { "$each": object_ids } } },
        )
        .await?;
    Ok(())
}

// -----------------------------------------------------------------------------
// catalog-driven: stream catalog rows, fan out to N workers that geo-lookup
// matching alerts_aux records and accumulate $push updates. Uses a temp field
// (`cross_matches.<catalog>_temp`) as a buffer so the `cross_matches.<catalog>` field is
// never empty mid-run.
//
// Concurrency with the live ingest pipeline: every phase is gated on
// `created_at < run_start_jd` so records inserted during the run are left
// completely untouched (the scheduler pipeline already filled their cross_matches).
// Without this guard, the final `$set live = $temp` would overwrite a new record's
// field with a missing/partial temp and silently delete it.
// -----------------------------------------------------------------------------
#[allow(clippy::too_many_arguments)]
async fn run_catalog_driven(
    survey: &Survey,
    catalog_config: CatalogXmatchConfig,
    db: mongodb::Database,
    batch_size: usize,
    processes: usize,
    concurrency: usize,
    skip_empty: bool,
    reset_temp: bool,
) -> Result<(), TaskError> {
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let cat_collection: mongodb::Collection<Document> = db.collection(&catalog_config.catalog);
    let live_field = format!("cross_matches.{}", catalog_config.catalog);
    let temp_field = format!("cross_matches.{}_temp", catalog_config.catalog);
    let run_start_jd = Time::now().to_jd();
    let shard_field = shard_field(&aux_collection).await;
    let shards = range_shards(&aux_collection, processes, shard_field).await;
    info!(
        "[catalog\u{2192}{}] cleanup passes sharded on '{}' into {} ranges",
        catalog_config.catalog,
        shard_field,
        shards.len()
    );

    // Phase 1: drop temp left behind by an interrupted run. Nothing can be left behind
    // after a run that reached phase 3, so the scan is skipped on the normal path.
    let state_id = format!("{}_alerts_aux:{}", survey, catalog_config.catalog);
    if reset_temp || temp_needs_reset(&db, &aux_collection, &state_id, &temp_field).await? {
        info!(
            "[catalog→{}] phase 1/3: clearing leftover temp field ({} shards)",
            catalog_config.catalog,
            shards.len()
        );
        let cleared = sharded_update_many(
            &aux_collection,
            &shards,
            &doc! { &temp_field: { "$exists": true } },
            UpdateModifications::Document(doc! { "$unset": { &temp_field: "" } }),
            &format!("catalog→{} phase 1", catalog_config.catalog),
        )
        .await?;
        info!(
            "[catalog→{}] phase 1/3: cleared {} leftover temp fields",
            catalog_config.catalog, cleared
        );
    } else {
        info!(
            "[catalog→{}] phase 1/3: no interrupted run to clean up, skipping the reset scan",
            catalog_config.catalog
        );
    }
    set_reprocess_state(&db, &state_id, STATUS_MATCHING).await?;

    // Phase 2: stream catalog rows through a worker pool, $push matches to temp.
    info!(
        "[catalog→{}] phase 2/3: streaming catalog rows",
        catalog_config.catalog
    );
    let mut cat_projection = catalog_config.projection.clone();
    cat_projection.insert("_id", 1);
    cat_projection.insert("ra", 1);
    cat_projection.insert("dec", 1);
    if let Some(dk) = &catalog_config.distance_key {
        cat_projection.insert(dk.as_str(), 1);
    }

    let cat_estimated = cat_collection.estimated_document_count().await.unwrap_or(0);
    let label = format!("catalog→{}", catalog_config.catalog);
    let pb = make_progress_bar(cat_estimated, label.clone());
    let logger = spawn_progress_logger(pb.clone(), label);
    let queue_capacity = processes * batch_size * QUEUE_MULTIPLIER;
    let (tx, rx) = async_channel::bounded::<Document>(queue_capacity);

    let mut workers = Vec::with_capacity(processes);
    for _ in 0..processes {
        let rx = rx.clone();
        let pb = pb.clone();
        let survey = survey.clone();
        let db = db.clone();
        let catalog_config = catalog_config.clone();
        let temp_field = temp_field.clone();
        workers.push(tokio::spawn(async move {
            catalog_worker(
                survey,
                db,
                catalog_config,
                temp_field,
                run_start_jd,
                rx,
                batch_size,
                concurrency,
                pb,
            )
            .await
        }));
    }
    drop(rx);

    let mut cursor = cat_collection
        .find(doc! {})
        .projection(cat_projection)
        .batch_size(CURSOR_BATCH_SIZE)
        .no_cursor_timeout(true)
        .await?;
    while let Some(d) = cursor.try_next().await? {
        if tx.send(d).await.is_err() {
            break;
        }
    }
    drop(tx);
    let outcome = join_tasks(workers, "worker").await;
    logger.abort();
    pb.finish();
    // CRITICAL: temp holds a partial result; committing it would overwrite valid
    // live cross_matches with an incomplete list. The next run's phase 1 clears it.
    outcome?;

    // Phase 3: sort, trim and commit in a single pass. $ifNull gives records with no
    // match the empty array that phase 1 used to pre-write on every record.
    info!(
        "[catalog→{}] phase 3/3: sorting, trimming and swapping temp into live ({} shards)",
        catalog_config.catalog,
        shards.len()
    );
    let mut commit_filter = doc! { "created_at": { "$lt": run_start_jd } };
    if skip_empty {
        commit_filter.insert(&temp_field, doc! { "$exists": true });
    }
    sharded_update_many(
        &aux_collection,
        &shards,
        &commit_filter,
        UpdateModifications::Pipeline(make_commit_pipeline(
            &catalog_config,
            &temp_field,
            &live_field,
        )),
        &format!("catalog→{} phase 3", catalog_config.catalog),
    )
    .await?;
    set_reprocess_state(&db, &state_id, STATUS_CLEAN).await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn catalog_worker(
    survey: Survey,
    db: mongodb::Database,
    catalog_config: CatalogXmatchConfig,
    temp_field: String,
    run_start_jd: f64,
    rx: async_channel::Receiver<Document>,
    batch_size: usize,
    concurrency: usize,
    pb: ProgressBar,
) -> Result<(), mongodb::error::Error> {
    let client = db.client().clone();
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let aux_ns = aux_collection.namespace();

    let mut rows = Vec::with_capacity(batch_size);
    while let Ok(cat_doc) = rx.recv().await {
        rows.push(cat_doc);
        if rows.len() >= batch_size {
            flush_catalog_batch(
                &aux_collection,
                &client,
                &aux_ns,
                &catalog_config,
                &temp_field,
                run_start_jd,
                &mut rows,
                concurrency,
                &pb,
            )
            .await?;
        }
    }
    if !rows.is_empty() {
        flush_catalog_batch(
            &aux_collection,
            &client,
            &aux_ns,
            &catalog_config,
            &temp_field,
            run_start_jd,
            &mut rows,
            concurrency,
            &pb,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn flush_catalog_batch(
    aux_collection: &mongodb::Collection<Document>,
    client: &mongodb::Client,
    aux_ns: &Namespace,
    catalog_config: &CatalogXmatchConfig,
    temp_field: &str,
    run_start_jd: f64,
    rows: &mut Vec<Document>,
    concurrency: usize,
    pb: &ProgressBar,
) -> Result<(), mongodb::error::Error> {
    let mut stream = futures::stream::iter(rows.drain(..))
        .map(|cat_doc| async move {
            let result =
                process_cat_doc(aux_collection, catalog_config, run_start_jd, &cat_doc).await;
            pb.inc(1);
            match result {
                Ok(matches) => matches,
                Err(e) => {
                    warn!(error = %e, "catalog row processing failed, skipping");
                    Vec::new()
                }
            }
        })
        .buffer_unordered(concurrency);

    let mut pending: HashMap<String, Vec<Document>> = HashMap::new();
    while let Some(matches) = stream.next().await {
        for (aux_id, match_doc) in matches {
            pending.entry(aux_id).or_default().push(match_doc);
        }
    }
    drop(stream);

    if !pending.is_empty() {
        flush_pending(client, aux_ns, temp_field, &mut pending).await?;
    }
    Ok(())
}

async fn process_cat_doc(
    aux_collection: &mongodb::Collection<Document>,
    catalog_config: &CatalogXmatchConfig,
    run_start_jd: f64,
    cat_doc: &Document,
) -> Result<Vec<(String, Document)>, mongodb::error::Error> {
    let cat_ra = match get_f64_from_doc(cat_doc, "ra") {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    let cat_dec = match get_f64_from_doc(cat_doc, "dec") {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };

    // A `use_distance` row's effective radius depends only on its own redshift, so query
    // that instead of the configured maximum and discarding most of what comes back.
    let mut search_radius = catalog_config.radius;
    let use_distance_data: Option<(f64, f64)> = if catalog_config.use_distance {
        let dk = catalog_config
            .distance_key
            .as_ref()
            .expect("validated in config");
        let z = match get_f64_from_doc(cat_doc, dk) {
            Some(v) => v,
            None => return Ok(Vec::new()),
        };
        let dmax = catalog_config.distance_max.expect("validated in config");
        let dmax_near = catalog_config
            .distance_max_near
            .expect("validated in config");
        let cm_radius = cm_radius_arcsec(z, dmax, dmax_near);
        search_radius = search_radius.min(cm_radius * ARCSEC_TO_RAD);
        Some((z, cm_radius))
    } else {
        None
    };
    if search_radius <= 0.0 {
        return Ok(Vec::new());
    }

    let cat_ra_geojson = cat_ra - 180.0;
    let aux_filter = doc! {
        "coordinates.radec_geojson": {
            "$geoWithin": {
                "$centerSphere": [[cat_ra_geojson, cat_dec], search_radius]
            }
        },
        "created_at": { "$lt": run_start_jd },
    };
    let mut aux_cursor = aux_collection
        .find(aux_filter)
        .projection(aux_match_projection())
        .batch_size(CURSOR_BATCH_SIZE)
        .await?;

    let mut matches = Vec::new();
    while let Some(aux_doc) = aux_cursor.try_next().await? {
        let aux_id = match aux_doc.get_str("_id") {
            Ok(s) => s.to_string(),
            Err(_) => continue,
        };
        let (aux_ra, aux_dec) = match extract_radec(&aux_doc) {
            Some(v) => v,
            None => continue,
        };
        let distance_arcsec = great_circle_distance(aux_ra, aux_dec, cat_ra, cat_dec) * 3600.0;

        let mut match_doc = cat_doc.clone();
        match_doc.insert("distance_arcsec", distance_arcsec);

        if let Some((z, cm_radius)) = use_distance_data {
            if distance_arcsec >= cm_radius {
                continue;
            }
            match_doc.insert("distance_kpc", distance_kpc_from_arcsec(distance_arcsec, z));
        }

        matches.push((aux_id, match_doc));
    }
    Ok(matches)
}

async fn flush_pending(
    client: &mongodb::Client,
    aux_ns: &Namespace,
    field: &str,
    pending: &mut HashMap<String, Vec<Document>>,
) -> Result<(), mongodb::error::Error> {
    let drained: Vec<(String, Vec<Document>)> = pending.drain().collect();
    let models: Vec<WriteModel> = drained
        .into_iter()
        .map(|(aux_id, docs)| {
            WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(aux_ns.clone())
                    .filter(doc! { "_id": aux_id })
                    .update(doc! { "$push": { field: { "$each": docs } } })
                    .build(),
            )
        })
        .collect();
    if !models.is_empty() {
        client.bulk_write(models).ordered(false).await?;
    }
    Ok(())
}

/// `coordinates.radec_geojson.coordinates` is `[ra - 180, dec]`.
fn extract_radec(doc: &Document) -> Option<(f64, f64)> {
    let arr = doc
        .get_document("coordinates")
        .ok()?
        .get_document("radec_geojson")
        .ok()?
        .get_array("coordinates")
        .ok()?;
    if arr.len() != 2 {
        return None;
    }
    let ra_geojson = arr[0].as_f64()?;
    let dec = arr[1].as_f64()?;
    if !ra_geojson.is_finite() || !dec.is_finite() {
        return None;
    }
    Some((ra_geojson + 180.0, dec))
}

/// Mongo-aggregation mirror of the in-Rust sort/trim performed by
/// `utils::spatial::xmatch` (see that function for the source of truth on
/// ordering semantics), followed by the swap of the temp buffer into the live
/// field. `use_distance` and `max_results` are mutually exclusive at config load.
fn make_commit_pipeline(
    catalog_config: &CatalogXmatchConfig,
    temp_field: &str,
    live_field: &str,
) -> Vec<Document> {
    let sort_by = if catalog_config.use_distance {
        doc! { "distance_kpc": 1, "distance_arcsec": 1 }
    } else {
        doc! { "distance_arcsec": 1 }
    };
    let input = doc! { "$ifNull": [format!("${}", temp_field), []] };
    let sorted = doc! { "$sortArray": { "input": input, "sortBy": sort_by } };
    let final_value: Document = if let Some(max) = catalog_config.max_results {
        doc! { "$slice": [sorted, max as i64] }
    } else {
        sorted
    };
    vec![
        doc! { "$set": { live_field: final_value } },
        doc! { "$unset": temp_field },
    ]
}

async fn pick_direction(
    survey: &Survey,
    catalog_config: &CatalogXmatchConfig,
    db: &mongodb::Database,
) -> Direction {
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let cat_collection: mongodb::Collection<Document> = db.collection(&catalog_config.catalog);
    let aux_count = aux_collection.estimated_document_count().await.unwrap_or(0);
    let cat_count = cat_collection.estimated_document_count().await.unwrap_or(0);
    info!(
        "auto: catalog '{}' ~{} rows, '{}_alerts_aux' ~{} rows",
        catalog_config.catalog, cat_count, survey, aux_count
    );
    if cat_count.saturating_mul(CATALOG_DRIVEN_MARGIN) < aux_count {
        Direction::Catalog
    } else {
        Direction::Objects
    }
}

#[tokio::main]
async fn main() {
    load_dotenv();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting subscriber failed");

    let args = Cli::parse();

    if args.catalogs.is_empty() {
        error!("--catalogs requires at least one catalog name");
        std::process::exit(1);
    }

    let config = match AppConfig::from_path(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("failed to load config from {}: {}", args.config, e);
            std::process::exit(1);
        }
    };

    let in_flight = args.processes * args.concurrency;
    if in_flight > config.database.max_pool_size as usize {
        warn!(
            "processes × concurrency = {} exceeds database.max_pool_size = {}; \
             workers will queue on the connection pool",
            in_flight, config.database.max_pool_size
        );
    }

    let db = match config.build_db().await {
        Ok(db) => db,
        Err(e) => {
            error!("failed to build mongo client: {}", e);
            std::process::exit(1);
        }
    };

    let survey_configs: &Vec<CatalogXmatchConfig> = match config.crossmatch.get(&args.survey) {
        Some(v) => v,
        None => {
            error!(
                "survey '{}' has no `crossmatch.{}` section in {}",
                args.survey,
                args.survey.to_string().to_lowercase(),
                args.config,
            );
            std::process::exit(1);
        }
    };
    let mut resolved: Vec<CatalogXmatchConfig> = Vec::with_capacity(args.catalogs.len());
    for name in &args.catalogs {
        if resolved.iter().any(|c| &c.catalog == name) {
            warn!(
                "catalog '{}' listed more than once, ignoring the copy",
                name
            );
            continue;
        }
        match survey_configs.iter().find(|c| &c.catalog == name) {
            Some(c) => resolved.push(c.clone()),
            None => {
                error!(
                    "catalog '{}' not declared under crossmatch.{} in {}",
                    name,
                    args.survey.to_string().to_lowercase(),
                    args.config,
                );
                std::process::exit(1);
            }
        }
    }

    // Watchlist catalogs use a dedicated path (loop over watchlist entries, $addToSet
    // object_ids onto the watchlist doc) — the `--direction` flag does not apply to them.
    let mut watchlist_catalogs: Vec<CatalogXmatchConfig> = Vec::new();
    let mut non_watchlist: Vec<CatalogXmatchConfig> = Vec::new();
    for cat in resolved {
        if cat.catalog.starts_with(WATCHLIST_PREFIX) {
            watchlist_catalogs.push(cat);
        } else {
            non_watchlist.push(cat);
        }
    }

    // If direction is Auto, split catalogs into two groups based on which collection is smaller.
    let mut objects_catalogs: Vec<CatalogXmatchConfig> = Vec::new();
    let mut catalog_catalogs: Vec<CatalogXmatchConfig> = Vec::new();
    for cat in non_watchlist {
        let direction = match args.direction {
            Direction::Auto => pick_direction(&args.survey, &cat, &db).await,
            d => d,
        };
        match direction {
            Direction::Objects => objects_catalogs.push(cat),
            Direction::Catalog => catalog_catalogs.push(cat),
            Direction::Auto => unreachable!(),
        }
    }

    info!(
        "starting reprocess: survey={} processes={} concurrency={} in_flight={} batch_size={} objects_driven={:?} catalogs_driven={:?} watchlist_driven={:?}",
        args.survey,
        args.processes,
        args.concurrency,
        in_flight,
        args.batch_size,
        objects_catalogs
            .iter()
            .map(|c| &c.catalog)
            .collect::<Vec<_>>(),
        catalog_catalogs
            .iter()
            .map(|c| &c.catalog)
            .collect::<Vec<_>>(),
        watchlist_catalogs
            .iter()
            .map(|c| &c.catalog)
            .collect::<Vec<_>>(),
    );

    for cat in watchlist_catalogs {
        let name = cat.catalog.clone();
        if let Err(e) = run_watchlist_driven(
            &args.survey,
            cat,
            db.clone(),
            args.batch_size,
            args.processes,
        )
        .await
        {
            error!("watchlist-driven run for '{}' failed: {}", name, e);
            std::process::exit(1);
        }
    }

    if !objects_catalogs.is_empty() {
        if let Err(e) = run_objects_driven(
            &args.survey,
            objects_catalogs,
            db.clone(),
            args.batch_size,
            args.processes,
            args.concurrency,
            args.skip_existing,
        )
        .await
        {
            error!("objects-driven run failed: {}", e);
            std::process::exit(1);
        }
    }

    for cat in catalog_catalogs {
        let name = cat.catalog.clone();
        if let Err(e) = run_catalog_driven(
            &args.survey,
            cat,
            db.clone(),
            args.batch_size,
            args.processes,
            args.concurrency,
            args.skip_empty,
            args.reset_temp,
        )
        .await
        {
            error!("catalog-driven run for '{}' failed: {}", name, e);
            std::process::exit(1);
        }
    }

    info!("reprocess_crossmatch complete.");
}
