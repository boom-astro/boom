use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use boom::{
    api::catalogs::WATCHLIST_PREFIX,
    conf::{load_dotenv, AppConfig, CatalogXmatchConfig},
    utils::{
        data::{make_progress_bar, spawn_progress_logger},
        db::{join_tasks, merge_filters, range_shards, shard_field, TaskError, CURSOR_BATCH_SIZE},
        enums::Survey,
        parser::parse_positive_usize,
        spatial::{
            distance_kpc_from_arcsec, get_f64_from_doc, row_match_radius_arcsec, row_redshift,
            watchlist_match_field, xmatch, Coordinates, COINCIDENT_ARCSEC, NO_PROJECTED_DISTANCE,
        },
    },
};
use clap::{Parser, ValueEnum};
use flare::{spatial::great_circle_distance, Time};
use futures::{StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, Bson, Document},
    error::{ErrorKind, InsertManyError},
    options::{UpdateOneModel, WriteModel},
    Namespace,
};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

const QUEUE_MULTIPLIER: usize = 2;
const ARCSEC_TO_RAD: f64 = std::f64::consts::PI / 180.0 / 3600.0;
const STATE_COLLECTION: &str = "reprocess_crossmatch_state";
const STATUS_MATCHING: &str = "matching";
const STATUS_COMMITTING: &str = "committing";
const STATUS_CLEAN: &str = "clean";
const SHARDS_PER_PROCESS: usize = 8;

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

    /// Number of parallel worker tasks, and of cleanup shards run at once.
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

    /// Catalog-driven only: discard the progress of an interrupted run and start over,
    /// e.g. after changing the catalog's matching parameters.
    #[arg(long, default_value_t = false)]
    restart: bool,
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
    doc! { "_id": 1, "coordinates.radec_geojson.coordinates": 1, "created_at": 1 }
}

async fn set_reprocess_state(
    db: &mongodb::Database,
    state_id: &str,
    mut fields: Document,
) -> Result<(), mongodb::error::Error> {
    fields.insert("updated_at", Time::now().to_jd());
    db.collection::<Document>(STATE_COLLECTION)
        .update_one(doc! { "_id": state_id }, doc! { "$set": fields })
        .upsert(true)
        .await?;
    Ok(())
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
// catalog-driven: matches go to a buffer collection, alerts_aux is written once per
// record at commit. Only records with `created_at < run_start_jd` are touched, and the
// start is persisted so a resumed run keeps it.
// -----------------------------------------------------------------------------
struct CatalogRun {
    run_start_jd: f64,
    checkpoint: Option<Bson>,
    committing: bool,
}

struct Page {
    seq: u64,
    rows: Vec<Document>,
}

type PageTracker = tokio::sync::Mutex<VecDeque<(u64, Bson, bool)>>;

async fn load_catalog_run(
    db: &mongodb::Database,
    state_id: &str,
) -> Result<Option<CatalogRun>, mongodb::error::Error> {
    let Some(state) = db
        .collection::<Document>(STATE_COLLECTION)
        .find_one(doc! { "_id": state_id })
        .await?
    else {
        return Ok(None);
    };
    let committing = match state.get_str("status") {
        Ok(STATUS_MATCHING) => false,
        Ok(STATUS_COMMITTING) => true,
        _ => return Ok(None),
    };
    let Ok(run_start_jd) = state.get_f64("run_start_jd") else {
        return Ok(None);
    };
    let checkpoint = state
        .get("checkpoint")
        .filter(|id| !matches!(id, Bson::Null))
        .cloned();
    Ok(Some(CatalogRun {
        run_start_jd,
        checkpoint,
        committing,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn run_catalog_driven(
    survey: &Survey,
    catalog_config: CatalogXmatchConfig,
    db: mongodb::Database,
    batch_size: usize,
    processes: usize,
    concurrency: usize,
    skip_empty: bool,
    restart: bool,
) -> Result<(), TaskError> {
    let label = format!("catalog\u{2192}{}", catalog_config.catalog);
    let state_id = format!("{}_alerts_aux:{}", survey, catalog_config.catalog);
    let buffer_name = format!(
        "reprocess_crossmatch_buffer_{}_{}",
        survey, catalog_config.catalog
    );
    let buffer: mongodb::Collection<Document> = db.collection(&buffer_name);
    let grouped: mongodb::Collection<Document> = db.collection(&format!("{}_grouped", buffer_name));

    let previous = if restart {
        None
    } else {
        load_catalog_run(&db, &state_id).await?
    };
    let run = match previous {
        Some(run) => {
            info!(
                "[{}] resuming the run started at JD {} ({})",
                label,
                run.run_start_jd,
                if run.committing { "commit" } else { "matching" }
            );
            run
        }
        None => {
            buffer.drop().await?;
            grouped.drop().await?;
            let run = CatalogRun {
                run_start_jd: Time::now().to_jd(),
                checkpoint: None,
                committing: false,
            };
            set_reprocess_state(
                &db,
                &state_id,
                doc! {
                    "status": STATUS_MATCHING,
                    "run_start_jd": run.run_start_jd,
                    "checkpoint": Bson::Null,
                },
            )
            .await?;
            run
        }
    };

    if !run.committing {
        info!(
            "[{}] phase 1/2: matching catalog rows into '{}'",
            label, buffer_name
        );
        match_catalog(
            survey,
            &catalog_config,
            &db,
            &buffer,
            &state_id,
            &run,
            batch_size,
            processes,
            concurrency,
            &label,
        )
        .await?;
        set_reprocess_state(&db, &state_id, doc! { "status": STATUS_COMMITTING }).await?;
    }

    info!("[{}] phase 2/2: committing matches to alerts_aux", label);
    commit_catalog(
        survey,
        &catalog_config,
        &db,
        &buffer,
        &grouped,
        run.run_start_jd,
        processes,
        skip_empty,
        &label,
    )
    .await?;
    set_reprocess_state(&db, &state_id, doc! { "status": STATUS_CLEAN }).await?;
    buffer.drop().await?;
    grouped.drop().await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn match_catalog(
    survey: &Survey,
    catalog_config: &CatalogXmatchConfig,
    db: &mongodb::Database,
    buffer: &mongodb::Collection<Document>,
    state_id: &str,
    run: &CatalogRun,
    batch_size: usize,
    processes: usize,
    concurrency: usize,
    label: &str,
) -> Result<(), TaskError> {
    let cat_collection: mongodb::Collection<Document> =
        db.collection(catalog_config.collection_name());
    let mut cat_projection = catalog_config.projection.clone();
    cat_projection.insert("_id", 1);
    cat_projection.insert("ra", 1);
    cat_projection.insert("dec", 1);
    if let Some(dk) = &catalog_config.distance_key {
        cat_projection.insert(dk.as_str(), 1);
    }

    let cat_estimated = cat_collection.estimated_document_count().await.unwrap_or(0);
    let pb = make_progress_bar(cat_estimated, label.to_string());
    if let Some(checkpoint) = &run.checkpoint {
        let done = cat_collection
            .count_documents(doc! { "_id": { "$lte": checkpoint.clone() } })
            .await?;
        info!(
            "[{}] {} rows already matched, resuming after them",
            label, done
        );
        pb.set_position(done);
    }
    let logger = spawn_progress_logger(pb.clone(), label.to_string());

    let tracker: Arc<PageTracker> = Arc::default();
    let (tx, rx) = async_channel::bounded::<Page>(processes * QUEUE_MULTIPLIER);
    let mut workers = Vec::with_capacity(processes);
    for _ in 0..processes {
        let rx = rx.clone();
        let pb = pb.clone();
        let db = db.clone();
        let aux_collection: mongodb::Collection<Document> =
            db.collection(&format!("{}_alerts_aux", survey));
        let buffer = buffer.clone();
        let catalog_config = catalog_config.clone();
        let tracker = Arc::clone(&tracker);
        let state_id = state_id.to_string();
        let run_start_jd = run.run_start_jd;
        workers.push(tokio::spawn(async move {
            while let Ok(page) = rx.recv().await {
                let docs = match_page(
                    &aux_collection,
                    &catalog_config,
                    run_start_jd,
                    page.rows,
                    concurrency,
                    &pb,
                )
                .await?;
                if !docs.is_empty() {
                    insert_ignoring_duplicates(&buffer, docs).await?;
                }
                complete_page(&tracker, page.seq, &db, &state_id).await?;
            }
            Ok(())
        }));
    }
    drop(rx);

    let produced: Result<(), mongodb::error::Error> = async {
        let mut last_id = run.checkpoint.clone();
        for seq in 0.. {
            let filter = match &last_id {
                Some(id) => doc! { "_id": { "$gt": id.clone() } },
                None => doc! {},
            };
            let rows: Vec<Document> = cat_collection
                .find(filter)
                .sort(doc! { "_id": 1 })
                .limit(batch_size as i64)
                .projection(cat_projection.clone())
                .await?
                .try_collect()
                .await?;
            let Some(id) = rows.last().and_then(|row| row.get("_id")).cloned() else {
                break;
            };
            tracker.lock().await.push_back((seq, id.clone(), false));
            if tx.send(Page { seq, rows }).await.is_err() {
                break;
            }
            last_id = Some(id);
        }
        Ok(())
    }
    .await;
    drop(tx);
    let outcome = join_tasks(workers, "worker").await;
    logger.abort();
    pb.finish();
    produced?;
    outcome?;
    Ok(())
}

async fn match_page(
    aux_collection: &mongodb::Collection<Document>,
    catalog_config: &CatalogXmatchConfig,
    run_start_jd: f64,
    mut rows: Vec<Document>,
    concurrency: usize,
    pb: &ProgressBar,
) -> Result<Vec<Document>, mongodb::error::Error> {
    let mut stream = futures::stream::iter(rows.drain(..))
        .map(|cat_doc| async move {
            let result =
                process_cat_doc(aux_collection, catalog_config, run_start_jd, &cat_doc).await;
            pb.inc(1);
            result
        })
        .buffer_unordered(concurrency);
    let mut docs = Vec::new();
    while let Some(matches) = stream.next().await {
        docs.extend(matches?);
    }
    Ok(docs)
}

/// The checkpoint only moves past a page once every page before it is done.
async fn complete_page(
    tracker: &PageTracker,
    seq: u64,
    db: &mongodb::Database,
    state_id: &str,
) -> Result<(), mongodb::error::Error> {
    let mut pending = tracker.lock().await;
    if let Some(entry) = pending.iter_mut().find(|entry| entry.0 == seq) {
        entry.2 = true;
    }
    let mut checkpoint = None;
    while pending.front().is_some_and(|entry| entry.2) {
        checkpoint = pending.pop_front().map(|entry| entry.1);
    }
    if let Some(checkpoint) = checkpoint {
        set_reprocess_state(db, state_id, doc! { "checkpoint": checkpoint }).await?;
    }
    Ok(())
}

async fn insert_ignoring_duplicates(
    collection: &mongodb::Collection<Document>,
    docs: Vec<Document>,
) -> Result<(), mongodb::error::Error> {
    match collection.insert_many(docs).ordered(false).await {
        Ok(_) => Ok(()),
        Err(e) => match e.kind.as_ref() {
            ErrorKind::InsertMany(InsertManyError {
                write_errors,
                write_concern_error: None,
                ..
            }) if write_errors
                .as_ref()
                .is_some_and(|errors| errors.iter().all(|we| we.code == 11000)) =>
            {
                Ok(())
            }
            _ => Err(e),
        },
    }
}

async fn process_cat_doc(
    aux_collection: &mongodb::Collection<Document>,
    catalog_config: &CatalogXmatchConfig,
    run_start_jd: f64,
    cat_doc: &Document,
) -> Result<Vec<Document>, mongodb::error::Error> {
    let Some(cat_id) = cat_doc.get("_id") else {
        return Ok(Vec::new());
    };
    let cat_ra = match get_f64_from_doc(cat_doc, "ra") {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    let cat_dec = match get_f64_from_doc(cat_doc, "dec") {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };

    // A row's effective radius depends only on the row itself, so query that
    // instead of the configured maximum and discarding most of what comes back.
    let search_radius = row_match_radius_arcsec(catalog_config, cat_doc) * ARCSEC_TO_RAD;
    let row_z = row_redshift(catalog_config, cat_doc);
    if search_radius <= 0.0 {
        return Ok(Vec::new());
    }

    // No `created_at` in the filter, or the planner can pick its index over the 2dsphere one.
    let cat_ra_geojson = cat_ra - 180.0;
    let aux_filter = doc! {
        "coordinates.radec_geojson": {
            "$geoWithin": {
                "$centerSphere": [[cat_ra_geojson, cat_dec], search_radius]
            }
        },
    };
    let mut aux_cursor = aux_collection
        .find(aux_filter)
        .projection(aux_match_projection())
        .batch_size(CURSOR_BATCH_SIZE)
        .await?;

    let mut matches = Vec::new();
    while let Some(aux_doc) = aux_cursor.try_next().await? {
        if !get_f64_from_doc(&aux_doc, "created_at").is_some_and(|t| t < run_start_jd) {
            continue;
        }
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

        if let Some(z) = row_z {
            match_doc.insert("distance_kpc", distance_kpc_from_arcsec(distance_arcsec, z));
        }

        matches.push(doc! { "_id": { "a": aux_id, "c": cat_id.clone() }, "m": match_doc });
    }
    Ok(matches)
}

#[allow(clippy::too_many_arguments)]
async fn commit_catalog(
    survey: &Survey,
    catalog_config: &CatalogXmatchConfig,
    db: &mongodb::Database,
    buffer: &mongodb::Collection<Document>,
    grouped: &mongodb::Collection<Document>,
    run_start_jd: f64,
    processes: usize,
    skip_empty: bool,
    label: &str,
) -> Result<(), TaskError> {
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let live_field = format!("cross_matches.{}", catalog_config.catalog);
    // Buffer of older versions, still on some alerts_aux records.
    let legacy_temp_field = format!("cross_matches.{}_temp", catalog_config.catalog);

    info!(
        "[{}] grouping, sorting and trimming the matches per record",
        label
    );
    buffer
        .aggregate(vec![
            doc! { "$group": { "_id": "$_id.a", "m": { "$push": "$m" } } },
            doc! { "$set": { "m": sorted_matches(catalog_config, "$m") } },
            doc! { "$out": grouped.name() },
        ])
        .allow_disk_use(true)
        .await?;

    let grouped_shards = range_shards(
        grouped,
        processes * SHARDS_PER_PROCESS,
        "_id",
        &Document::new(),
    )
    .await;
    sharded_aggregate(
        grouped,
        &grouped_shards,
        processes,
        &Document::new(),
        vec![doc! { "$merge": {
            "into": aux_collection.name(),
            "on": "_id",
            "whenMatched": [
                { "$set": { &live_field: "$$new.m" } },
                { "$unset": &legacy_temp_field },
            ],
            "whenNotMatched": "discard",
        }}],
        &format!("{} matched", label),
    )
    .await?;

    if skip_empty {
        return Ok(());
    }
    let aux_shards = range_shards(
        &aux_collection,
        processes * SHARDS_PER_PROCESS,
        shard_field(&aux_collection).await,
        &Document::new(),
    )
    .await;
    sharded_aggregate(
        &aux_collection,
        &aux_shards,
        processes,
        &doc! {
            "created_at": { "$lt": run_start_jd },
            "$or": [
                { &live_field: { "$exists": false } },
                { format!("{}.0", live_field): { "$exists": true } },
                { &legacy_temp_field: { "$exists": true } },
            ],
        },
        vec![
            doc! { "$project": { "_id": 1 } },
            doc! { "$lookup": {
                "from": grouped.name(),
                "localField": "_id",
                "foreignField": "_id",
                "pipeline": [{ "$project": { "_id": 1 } }],
                "as": "hit",
            }},
            doc! { "$match": { "hit": { "$size": 0 } } },
            doc! { "$project": { "_id": 1 } },
            doc! { "$merge": {
                "into": aux_collection.name(),
                "on": "_id",
                "whenMatched": [
                    { "$set": { &live_field: [] } },
                    { "$unset": &legacy_temp_field },
                ],
                "whenNotMatched": "discard",
            }},
        ],
        &format!("{} unmatched", label),
    )
    .await?;
    Ok(())
}

async fn sharded_aggregate(
    collection: &mongodb::Collection<Document>,
    shards: &[Document],
    processes: usize,
    base_filter: &Document,
    stages: Vec<Document>,
    label: &str,
) -> Result<(), mongodb::error::Error> {
    info!("[{}] running over {} shards", label, shards.len());
    let done = Arc::new(AtomicUsize::new(0));
    let total = shards.len();
    let results: Vec<_> = futures::stream::iter(shards.iter().enumerate().map(|(index, shard)| {
        let filter = merge_filters(base_filter, shard);
        let mut pipeline = Vec::with_capacity(stages.len() + 1);
        if !filter.is_empty() {
            pipeline.push(doc! { "$match": filter });
        }
        pipeline.extend(stages.iter().cloned());
        let collection = collection.clone();
        let done = Arc::clone(&done);
        async move {
            let result = collection.aggregate(pipeline).allow_disk_use(true).await;
            let completed = done.fetch_add(1, Ordering::Relaxed) + 1;
            match &result {
                Ok(_) => info!(
                    "[{}] shard {}/{} done ({} shards complete)",
                    label,
                    index + 1,
                    total,
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
            result.map(|_| ())
        }
    }))
    .buffer_unordered(processes)
    .collect()
    .await;
    results.into_iter().collect()
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

fn stellar_expr(catalog_config: &CatalogXmatchConfig) -> Document {
    let (Some(type_key), false) = (
        catalog_config.type_key.as_ref(),
        catalog_config.stellar_types.is_empty(),
    ) else {
        return doc! { "$literal": false };
    };
    let values: Vec<String> = catalog_config
        .stellar_types
        .iter()
        .map(|s| s.to_lowercase())
        .collect();
    let value = doc! { "$convert": {
        "input": format!("$$this.{type_key}"),
        "to": "string",
        "onError": "",
        "onNull": "",
    }};
    doc! { "$in": [{ "$toLower": { "$trim": { "input": value } } }, values] }
}

/// Mongo-aggregation mirror of the in-Rust sort/trim performed by
/// `utils::spatial::xmatch` (see that function for the source of truth on
/// ordering semantics).
fn sorted_matches(catalog_config: &CatalogXmatchConfig, input: &str) -> Document {
    let rank = doc! { "$switch": {
        "branches": [
            { "case": { "$lt": ["$$a", COINCIDENT_ARCSEC] }, "then": 0 },
            { "case": stellar_expr(catalog_config), "then": 3 },
            { "case": { "$eq": ["$$k", NO_PROJECTED_DISTANCE] }, "then": 1 },
        ],
        "default": 2,
    }};
    let keyed = doc! { "$map": {
        "input": { "$ifNull": [input, []] },
        "in": { "$let": {
            "vars": {
                "a": { "$ifNull": ["$$this.distance_arcsec", f64::MAX] },
                "k": { "$ifNull": ["$$this.distance_kpc", f64::MAX] },
            },
            "in": { "$let": {
                "vars": { "r": rank },
                "in": {
                    "r": "$$r",
                    "k": { "$cond": [{ "$eq": ["$$r", 2] }, "$$k", 0.0] },
                    "a": "$$a",
                    "doc": "$$this",
                },
            }},
        }},
    }};
    let sorted = doc! { "$map": {
        "input": { "$sortArray": { "input": keyed, "sortBy": { "r": 1, "k": 1, "a": 1 } } },
        "in": "$$this.doc",
    }};
    if let Some(max) = catalog_config.max_results {
        doc! { "$slice": [sorted, max as i64] }
    } else {
        sorted
    }
}

async fn pick_direction(
    survey: &Survey,
    catalog_config: &CatalogXmatchConfig,
    db: &mongodb::Database,
) -> Direction {
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let cat_collection: mongodb::Collection<Document> =
        db.collection(catalog_config.collection_name());
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
            args.restart,
        )
        .await
        {
            error!("catalog-driven run for '{}' failed: {}", name, e);
            std::process::exit(1);
        }
    }

    info!("reprocess_crossmatch complete.");
}

#[cfg(test)]
mod commit_pipeline_tests {
    use super::*;

    fn config(type_key: Option<&str>) -> CatalogXmatchConfig {
        CatalogXmatchConfig {
            catalog: "NED".to_string(),
            max_results: Some(50),
            type_key: type_key.map(str::to_string),
            stellar_types: type_key
                .map(|_| vec!["STAR".to_string()])
                .unwrap_or_default(),
            ..Default::default()
        }
    }

    #[test]
    fn test_a_catalog_without_a_type_column_ranks_nothing_as_stellar() {
        let expr = stellar_expr(&config(None));
        assert!(!expr.get_bool("$literal").unwrap());
    }

    #[test]
    fn test_stellar_values_are_compared_case_insensitively() {
        let expr = stellar_expr(&config(Some("spectype")));
        let args = expr.get_array("$in").unwrap();
        assert!(format!("{:?}", args[0]).contains("toLower"));
        assert_eq!(args[1].as_array().unwrap()[0].as_str().unwrap(), "star");
    }

    /// The keys are the ones `host_sort_key` returns, in that order, and the
    /// wrapper they live on is dropped before the array is stored.
    #[test]
    fn test_rows_are_sorted_on_rank_then_distance() {
        let rendered = format!("{:?}", sorted_matches(&config(None), "$m"));
        assert!(rendered
            .contains(r#""sortBy": Document({"r": Int32(1), "k": Int32(1), "a": Int32(1)})"#));
        assert!(rendered.contains(r#""in": String("$$this.doc")"#));
        assert!(!rendered.contains("distance_kpc\": Int32(1)"));
    }

    #[test]
    fn test_the_array_is_trimmed() {
        let expr = sorted_matches(&config(None), "$m");
        assert!(expr.contains_key("$slice"));
    }
}
