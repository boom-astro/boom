use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use boom::{
    api::catalogs::WATCHLIST_PREFIX,
    conf::{load_dotenv, AppConfig, CatalogXmatchConfig},
    utils::{
        data::{
            format_duration, format_eta, make_progress_bar, spawn_elapsed_logger,
            spawn_progress_logger,
        },
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
///
/// With `--remove`, the binary does the reverse: it unsets `cross_matches.<catalog>` from every
/// alerts_aux record, for a catalog that has been dropped from `crossmatch.<survey>`.
#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    /// Each catalog must already be declared under `crossmatch.<survey>` in
    /// config.yaml (radius / projection / etc. are read from there). With
    /// `--remove`, the opposite: nothing in the config may still reference it.
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    catalogs: Vec<String>,

    #[arg(long, value_enum, default_value_t = Direction::Auto)]
    direction: Direction,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Number of records accumulated per worker before a bulk write is issued.
    #[arg(long, default_value_t = 5000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of parallel worker tasks, and of commit shards run at once.
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

    /// Catalog-driven only: discard the progress of an interrupted run and start over.
    #[arg(long, default_value_t = false)]
    restart: bool,

    /// Unset `cross_matches.<catalog>` from every alerts_aux record instead of
    /// computing matches, along with any state an earlier reprocess run left behind.
    /// Point `--config` at the deployment's own config, which is what the safety
    /// check reads. Idempotent, so an interrupted run can simply be rerun. Every
    /// shard is one update over many records, so on a production replica set keep
    /// `--processes` low to bound replication lag.
    #[arg(
        long,
        default_value_t = false,
        conflicts_with_all = ["direction", "batch_size", "concurrency", "skip_existing", "skip_empty", "restart"]
    )]
    remove: bool,

    /// With `--remove`: proceed even if a catalog is still declared under
    /// `crossmatch.<survey>` or used by `host_galaxy`. Live ingest will keep
    /// writing it on new records.
    #[arg(long, default_value_t = false, requires = "remove")]
    force: bool,
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
            .map(|c| doc! { xmatch_field(&c.catalog): { "$exists": false } })
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
                set_doc.insert(xmatch_field(&cat.catalog), matches);
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

    let mut object_ids: Vec<Bson> = Vec::new();
    while let Some(aux_doc) = aux_cursor.try_next().await? {
        if let Ok(id) = aux_doc.get_str("_id") {
            object_ids.push(Bson::String(id.to_string()));
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
// catalog-driven: skips records created after run_start_jd; a resume reuses the stored value.
// -----------------------------------------------------------------------------
struct CatalogRun {
    run_start_jd: f64,
    checkpoint: Option<Bson>,
    committing: bool,
}

struct Page {
    index: u64,
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
    let label = format!("catalog→{}", catalog_config.catalog);
    let state_id = catalog_state_id(survey, &catalog_config.catalog);
    let buffer_name = catalog_buffer_name(survey, &catalog_config.catalog);
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
                if run.committing {
                    STATUS_COMMITTING
                } else {
                    STATUS_MATCHING
                }
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
    let catalog_collection: mongodb::Collection<Document> =
        db.collection(catalog_config.collection_name());
    let mut catalog_projection = catalog_config.projection.clone();
    catalog_projection.insert("_id", 1);
    catalog_projection.insert("ra", 1);
    catalog_projection.insert("dec", 1);
    if let Some(distance_key) = &catalog_config.distance_key {
        catalog_projection.insert(distance_key.as_str(), 1);
    }

    let catalog_estimated = catalog_collection
        .estimated_document_count()
        .await
        .unwrap_or(0);
    let pb = make_progress_bar(catalog_estimated, label.to_string());
    if let Some(checkpoint) = &run.checkpoint {
        let done = catalog_collection
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
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let mut workers = Vec::with_capacity(processes);
    for _ in 0..processes {
        let rx = rx.clone();
        let pb = pb.clone();
        let db = db.clone();
        let aux_collection = aux_collection.clone();
        let buffer = buffer.clone();
        let catalog_config = catalog_config.clone();
        let tracker = Arc::clone(&tracker);
        let state_id = state_id.to_string();
        let run_start_jd = run.run_start_jd;
        workers.push(tokio::spawn(async move {
            while let Ok(page) = rx.recv().await {
                let matches = match_page(
                    &aux_collection,
                    &catalog_config,
                    run_start_jd,
                    page.rows,
                    concurrency,
                    &pb,
                )
                .await?;
                if !matches.is_empty() {
                    insert_ignoring_duplicates(&buffer, matches).await?;
                }
                complete_page(&tracker, page.index, &db, &state_id).await?;
            }
            Ok(())
        }));
    }
    drop(rx);

    let produced: Result<(), mongodb::error::Error> = async {
        let mut last_id = run.checkpoint.clone();
        for index in 0.. {
            let filter = match &last_id {
                Some(id) => doc! { "_id": { "$gt": id.clone() } },
                None => doc! {},
            };
            let rows: Vec<Document> = catalog_collection
                .find(filter)
                .sort(doc! { "_id": 1 })
                .limit(batch_size as i64)
                .projection(catalog_projection.clone())
                .await?
                .try_collect()
                .await?;
            let Some(id) = rows.last().and_then(|row| row.get("_id")).cloned() else {
                break;
            };
            tracker.lock().await.push_back((index, id.clone(), false));
            if tx.send(Page { index, rows }).await.is_err() {
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
    rows: Vec<Document>,
    concurrency: usize,
    pb: &ProgressBar,
) -> Result<Vec<Document>, mongodb::error::Error> {
    futures::stream::iter(rows)
        .map(|cat_doc| async move {
            let result =
                process_cat_doc(aux_collection, catalog_config, run_start_jd, &cat_doc).await;
            pb.inc(1);
            result
        })
        .buffer_unordered(concurrency)
        .try_concat()
        .await
}

/// The checkpoint only moves past a page once every page before it is done.
async fn complete_page(
    tracker: &PageTracker,
    index: u64,
    db: &mongodb::Database,
    state_id: &str,
) -> Result<(), mongodb::error::Error> {
    let mut pending = tracker.lock().await;
    if let Some((_, _, done)) = pending
        .iter_mut()
        .find(|(page_index, _, _)| *page_index == index)
    {
        *done = true;
    }
    let mut checkpoint = None;
    while pending.front().is_some_and(|(_, _, done)| *done) {
        checkpoint = pending.pop_front().map(|(_, last_id, _)| last_id);
    }
    if let Some(checkpoint) = checkpoint {
        set_reprocess_state(db, state_id, doc! { "checkpoint": checkpoint }).await?;
    }
    Ok(())
}

async fn insert_ignoring_duplicates(
    collection: &mongodb::Collection<Document>,
    documents: Vec<Document>,
) -> Result<(), mongodb::error::Error> {
    let Err(error) = collection.insert_many(documents).ordered(false).await else {
        return Ok(());
    };
    match error.kind.as_ref() {
        ErrorKind::InsertMany(InsertManyError {
            write_errors: Some(write_errors),
            write_concern_error: None,
            ..
        }) if write_errors
            .iter()
            .all(|write_error| write_error.code == 11000) =>
        {
            Ok(())
        }
        _ => Err(error),
    }
}

async fn process_cat_doc(
    aux_collection: &mongodb::Collection<Document>,
    catalog_config: &CatalogXmatchConfig,
    run_start_jd: f64,
    cat_doc: &Document,
) -> Result<Vec<Document>, mongodb::error::Error> {
    let Some(catalog_id) = cat_doc.get("_id") else {
        return Ok(Vec::new());
    };
    let Some(cat_ra) = get_f64_from_doc(cat_doc, "ra") else {
        return Ok(Vec::new());
    };
    let Some(cat_dec) = get_f64_from_doc(cat_doc, "dec") else {
        return Ok(Vec::new());
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
        .projection(doc! { "_id": 1, "coordinates.radec_geojson.coordinates": 1, "created_at": 1 })
        .batch_size(CURSOR_BATCH_SIZE)
        .await?;

    let mut matches = Vec::new();
    while let Some(aux_doc) = aux_cursor.try_next().await? {
        if !get_f64_from_doc(&aux_doc, "created_at")
            .is_some_and(|created_at| created_at < run_start_jd)
        {
            continue;
        }
        let Ok(aux_id) = aux_doc.get_str("_id") else {
            continue;
        };
        let Some((aux_ra, aux_dec)) = extract_radec(&aux_doc) else {
            continue;
        };
        let distance_arcsec = great_circle_distance(aux_ra, aux_dec, cat_ra, cat_dec) * 3600.0;

        let mut match_doc = cat_doc.clone();
        match_doc.insert("distance_arcsec", distance_arcsec);

        if let Some(z) = row_z {
            match_doc.insert("distance_kpc", distance_kpc_from_arcsec(distance_arcsec, z));
        }

        matches.push(doc! { "_id": { "a": aux_id, "c": catalog_id.clone() }, "m": match_doc });
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
    let live_field = xmatch_field(&catalog_config.catalog);
    let legacy_temp_field = legacy_temp_field(&catalog_config.catalog);
    let merge_into_aux = |value: Bson| {
        doc! { "$merge": {
            "into": aux_collection.name(),
            "on": "_id",
            "whenMatched": [
                { "$set": { &live_field: value } },
                { "$unset": &legacy_temp_field },
            ],
            "whenNotMatched": "discard",
        }}
    };

    let buffered = buffer.estimated_document_count().await?;
    info!(
        "[{}] grouping, sorting and trimming {} matches per record",
        label, buffered
    );
    let logger = spawn_elapsed_logger(label.to_string(), "still grouping");
    let grouping = buffer
        .aggregate(vec![
            doc! { "$group": { "_id": "$_id.a", "m": { "$push": "$m" } } },
            doc! { "$set": { "m": sorted_matches(catalog_config, "$m") } },
            doc! { "$out": grouped.name() },
        ])
        .allow_disk_use(true)
        .await;
    logger.abort();
    grouping?;

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
        vec![merge_into_aux(Bson::from("$$new.m"))],
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
            merge_into_aux(Bson::Array(Vec::new())),
        ],
        &format!("{} unmatched", label),
    )
    .await?;
    Ok(())
}

fn catalog_state_id(survey: &Survey, catalog: &str) -> String {
    format!("{}_alerts_aux:{}", survey, catalog)
}

fn catalog_buffer_name(survey: &Survey, catalog: &str) -> String {
    format!("reprocess_crossmatch_buffer_{}_{}", survey, catalog)
}

fn xmatch_field(catalog: &str) -> String {
    format!("cross_matches.{}", catalog)
}

/// Left by older versions of this binary, still present on some alerts_aux records.
fn legacy_temp_field(catalog: &str) -> String {
    format!("cross_matches.{}_temp", catalog)
}

/// Every field a catalog's matches can occupy on an alerts_aux record.
fn catalog_fields(catalogs: &[&str]) -> Vec<String> {
    catalogs
        .iter()
        .flat_map(|c| [xmatch_field(c), legacy_temp_field(c)])
        .collect()
}

/// Matches records carrying any of the catalogs' fields.
fn has_any_field(catalogs: &[&str]) -> Document {
    let clauses: Vec<Document> = catalog_fields(catalogs)
        .into_iter()
        .map(|field| doc! { field: { "$exists": true } })
        .collect();
    doc! { "$or": clauses }
}

fn shard_progress(completed: usize, total: usize, started: Instant) -> String {
    let elapsed = started.elapsed();
    format!(
        "{} shards complete, {} elapsed, eta {}",
        completed,
        format_duration(elapsed.as_secs()),
        format_eta(
            (total - completed) as u64,
            completed as f64 / elapsed.as_secs_f64()
        )
    )
}

// -----------------------------------------------------------------------------
// remove: unset dropped catalogs' matches from alerts_aux in one sharded pass.
// -----------------------------------------------------------------------------
/// Unsets the catalogs from records created before `run_start_jd`, leaving the
/// newer ones as evidence of anything still writing them. Returns the number of
/// records modified.
async fn remove_fields(
    aux_collection: &mongodb::Collection<Document>,
    catalogs: &[&str],
    run_start_jd: f64,
    processes: usize,
) -> Result<u64, mongodb::error::Error> {
    let filter = merge_filters(
        &has_any_field(catalogs),
        &doc! { "$or": [
            { "created_at": { "$lt": run_start_jd } },
            { "created_at": { "$exists": false } },
        ]},
    );
    let unset: Document = catalog_fields(catalogs)
        .into_iter()
        .map(|field| (field, Bson::String(String::new())))
        .collect();
    let update = doc! { "$unset": unset };

    let shards = range_shards(
        aux_collection,
        processes * SHARDS_PER_PROCESS,
        shard_field(aux_collection).await,
        &Document::new(),
    )
    .await;
    let modified = run_sharded(
        &shards,
        processes,
        &format!("remove→{}", catalogs.join(",")),
        |shard| {
            let filter = merge_filters(&filter, shard);
            let update = update.clone();
            async move {
                let result = aux_collection.update_many(filter, update).await?;
                Ok(result.modified_count)
            }
        },
    )
    .await?;
    Ok(modified.into_iter().sum())
}

/// Deletes what a catalog-driven run left behind for the catalog: its state
/// document and its buffer collections.
async fn clear_reprocess_state(
    db: &mongodb::Database,
    survey: &Survey,
    catalog: &str,
) -> Result<(), mongodb::error::Error> {
    db.collection::<Document>(STATE_COLLECTION)
        .delete_one(doc! { "_id": catalog_state_id(survey, catalog) })
        .await?;
    let buffer_name = catalog_buffer_name(survey, catalog);
    db.collection::<Document>(&buffer_name).drop().await?;
    db.collection::<Document>(&format!("{}_grouped", buffer_name))
        .drop()
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
    run_sharded(shards, processes, label, |shard| {
        let filter = merge_filters(base_filter, shard);
        let mut pipeline = Vec::with_capacity(stages.len() + 1);
        if !filter.is_empty() {
            pipeline.push(doc! { "$match": filter });
        }
        pipeline.extend(stages.iter().cloned());
        async move {
            collection.aggregate(pipeline).allow_disk_use(true).await?;
            Ok(())
        }
    })
    .await?;
    Ok(())
}

/// Runs `op` on each shard filter, `processes` at a time, logging progress as
/// shards complete. Fails with the first shard error once every shard has run.
async fn run_sharded<T, F, Fut>(
    shards: &[Document],
    processes: usize,
    label: &str,
    op: F,
) -> Result<Vec<T>, mongodb::error::Error>
where
    F: Fn(&Document) -> Fut,
    Fut: Future<Output = Result<T, mongodb::error::Error>>,
{
    let total = shards.len();
    info!("[{}] running over {} shards", label, total);
    let done = &AtomicUsize::new(0);
    let started = Instant::now();
    let results: Vec<_> = futures::stream::iter(shards.iter().enumerate().map(|(index, shard)| {
        let shard_op = op(shard);
        async move {
            let result = shard_op.await;
            let completed = done.fetch_add(1, Ordering::Relaxed) + 1;
            let progress = shard_progress(completed, total, started);
            match &result {
                Ok(_) => info!(
                    "[{}] shard {}/{} done ({})",
                    label,
                    index + 1,
                    total,
                    progress
                ),
                Err(error) => warn!(
                    %error,
                    "[{}] shard {}/{} failed ({})",
                    label,
                    index + 1,
                    total,
                    progress
                ),
            }
            result
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

/// Mirrors the sort and trim of `utils::spatial::xmatch`; keep the two in sync.
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

    // --remove runs one update per shard, `processes` at a time.
    let in_flight = if args.remove {
        args.processes
    } else {
        args.processes * args.concurrency
    };
    if in_flight > config.database.max_pool_size as usize {
        warn!(
            "{} concurrent queries exceed database.max_pool_size = {}; \
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

    if args.remove {
        run_remove_mode(&args, &config, db).await;
        return;
    }

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
    for name in dedup_catalogs(&args.catalogs) {
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

fn dedup_catalogs(names: &[String]) -> Vec<&String> {
    let mut unique: Vec<&String> = Vec::with_capacity(names.len());
    for name in names {
        if unique.contains(&name) {
            warn!(
                "catalog '{}' listed more than once, ignoring the copy",
                name
            );
            continue;
        }
        unique.push(name);
    }
    unique
}

/// The config settings that still read or write the catalog's matches.
fn catalog_references(config: &AppConfig, survey: &Survey, catalog: &str) -> Vec<String> {
    let mut references = Vec::new();
    if let Some(declared) = config.crossmatch.get(survey) {
        if declared.iter().any(|c| c.catalog == catalog) {
            references.push(format!("crossmatch.{}", survey.to_string().to_lowercase()));
        }
    }
    let host = &config.host_galaxy;
    if host.enabled && host.ned_catalog == catalog {
        references.push("host_galaxy.ned_catalog".to_string());
    }
    if host.enabled && host.ls_dr10_catalog == catalog {
        references.push("host_galaxy.ls_dr10_catalog".to_string());
    }
    references
}

async fn run_remove_mode(args: &Cli, config: &AppConfig, db: mongodb::Database) {
    // The safety check is only as good as the config it reads, so say which one.
    info!(
        "checking against {}: crossmatch.{} declares {:?}",
        args.config,
        args.survey.to_string().to_lowercase(),
        config
            .crossmatch
            .get(&args.survey)
            .map(|c| c.iter().map(|c| c.catalog.as_str()).collect::<Vec<_>>())
            .unwrap_or_default()
    );

    let mut catalogs: Vec<&str> = Vec::with_capacity(args.catalogs.len());
    let mut still_referenced: Vec<&str> = Vec::new();
    for name in dedup_catalogs(&args.catalogs) {
        // Watchlist matches live on the watchlist documents, not on alerts_aux.
        if name.starts_with(WATCHLIST_PREFIX) {
            error!(
                "catalog '{}' is a watchlist; --remove only clears alerts_aux crossmatches",
                name
            );
            std::process::exit(1);
        }
        let references = catalog_references(config, &args.survey, name);
        if !references.is_empty() {
            if !args.force {
                error!(
                    "catalog '{}' is still referenced by {} in {}; remove it from the \
                     config first, or pass --force",
                    name,
                    references.join(", "),
                    args.config,
                );
                std::process::exit(1);
            }
            warn!(
                "--force: removing '{}' although {} still references it",
                name,
                references.join(", ")
            );
            still_referenced.push(name);
        }
        catalogs.push(name);
    }

    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", args.survey));
    // Each check stops at the first hit, so it only scans the whole collection
    // for a name that matches nothing, which is the case worth reporting.
    let checks = futures::future::join_all(catalogs.iter().map(|&name| {
        let aux_collection = &aux_collection;
        async move {
            let found = aux_collection
                .find_one(has_any_field(&[name]))
                .projection(doc! { "_id": 1 })
                .await;
            (name, found)
        }
    }))
    .await;
    let mut present: Vec<&str> = Vec::with_capacity(catalogs.len());
    for (name, found) in checks {
        match found {
            Ok(Some(_)) => present.push(name),
            Ok(None) => warn!(
                "no {} record carries cross_matches.{}, nothing to remove (already removed, or \
                 a misspelled catalog name)",
                aux_collection.name(),
                name
            ),
            Err(e) => {
                error!("could not check for '{}' matches: {}", name, e);
                std::process::exit(1);
            }
        }
    }

    if !present.is_empty() {
        info!(
            "starting remove: survey={} processes={} catalogs={:?}",
            args.survey, args.processes, present
        );
        let run_start_jd = Time::now().to_jd();
        match remove_fields(&aux_collection, &present, run_start_jd, args.processes).await {
            Ok(modified) => info!("unset {:?} from {} records", present, modified),
            Err(e) => {
                error!("remove failed: {}", e);
                std::process::exit(1);
            }
        }
        // The run leaves records created after it started untouched, so any of
        // them carrying the field was written by something still crossmatching it.
        let written_since = merge_filters(
            &doc! { "created_at": { "$gte": run_start_jd } },
            &has_any_field(&present),
        );
        match aux_collection.count_documents(written_since).await {
            Ok(0) => {}
            Ok(count) => warn!(
                "{} records created during the run carry {:?} matches, so ingest is still \
                 running with a config that crossmatches them; redeploy it, then rerun",
                count, present
            ),
            Err(e) => warn!("could not check for records written during the run: {}", e),
        }
    }

    // A catalog still in the config may have a catalog-driven run in flight,
    // so its state is left alone.
    for name in catalogs.iter().filter(|c| !still_referenced.contains(c)) {
        if let Err(e) = clear_reprocess_state(&db, &args.survey, name).await {
            error!("could not clear reprocess state for '{}': {}", name, e);
            std::process::exit(1);
        }
    }
    info!("reprocess_crossmatch remove complete.");
}

#[cfg(test)]
mod sorted_matches_tests {
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
        let expression = stellar_expr(&config(None));
        assert!(!expression.get_bool("$literal").unwrap());
    }

    #[test]
    fn test_stellar_values_are_compared_case_insensitively() {
        let expression = stellar_expr(&config(Some("spectype")));
        let args = expression.get_array("$in").unwrap();
        assert!(format!("{:?}", args[0]).contains("toLower"));
        assert_eq!(args[1].as_array().unwrap()[0].as_str().unwrap(), "star");
    }

    /// Keys follow `host_sort_key`, and their wrapper is dropped before the array is stored.
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
        let expression = sorted_matches(&config(None), "$m");
        assert!(expression.contains_key("$slice"));
    }
}

#[cfg(test)]
mod remove_tests {
    use super::*;
    use boom::conf::get_test_db;

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn test_dedup_catalogs_keeps_first_occurrence_order() {
        let input = names(&["A", "B", "A", "C", "B"]);
        assert_eq!(dedup_catalogs(&input), vec!["A", "B", "C"]);
    }

    #[test]
    fn test_catalog_references_lists_every_setting_using_the_catalog() {
        let mut config = AppConfig::from_test_config().unwrap();
        config.crossmatch.insert(
            Survey::Ztf,
            vec![CatalogXmatchConfig {
                catalog: "NED".to_string(),
                ..Default::default()
            }],
        );
        config.host_galaxy.ned_catalog = "NED".to_string();
        config.host_galaxy.ls_dr10_catalog = "LSDR10".to_string();

        config.host_galaxy.enabled = false;
        assert_eq!(
            catalog_references(&config, &Survey::Ztf, "NED"),
            vec!["crossmatch.ztf"]
        );
        assert!(catalog_references(&config, &Survey::Ztf, "LSDR10").is_empty());

        config.host_galaxy.enabled = true;
        assert_eq!(
            catalog_references(&config, &Survey::Ztf, "NED"),
            vec!["crossmatch.ztf", "host_galaxy.ned_catalog"]
        );
        assert_eq!(
            catalog_references(&config, &Survey::Ztf, "LSDR10"),
            vec!["host_galaxy.ls_dr10_catalog"]
        );
        assert!(catalog_references(&config, &Survey::Lsst, "OTHER").is_empty());
    }

    #[tokio::test]
    async fn test_remove_unsets_only_the_given_catalogs_across_shards() {
        let db = get_test_db().await;
        let aux: mongodb::Collection<Document> =
            db.collection(&format!("remove_test_{}", uuid::Uuid::new_v4().simple()));
        // Enough records for range_shards to cut real shards rather than fall
        // back to one unfiltered pass.
        let records: Vec<Document> = (0..1000)
            .map(|i| {
                let mut matches = doc! { "KEEP": [] };
                if i % 2 == 0 {
                    matches.insert("OLD", vec![doc! { "id": i }]);
                }
                if i % 5 == 0 {
                    matches.insert("OLD_temp", Vec::<Document>::new());
                }
                doc! { "_id": format!("obj{:04}", i), "cross_matches": matches, "coordinates": {} }
            })
            .collect();
        aux.insert_many(records).await.unwrap();
        // Written by ingest after the run started, so it is left in place.
        let run_start_jd = 2_460_000.0;
        aux.insert_one(doc! {
            "_id": "new",
            "created_at": run_start_jd + 1.0,
            "cross_matches": { "OLD": [] },
        })
        .await
        .unwrap();

        let first = remove_fields(&aux, &["OLD"], run_start_jd, 4).await;
        let shards = range_shards(&aux, 4 * SHARDS_PER_PROCESS, "_id", &Document::new()).await;
        let left = aux.count_documents(has_any_field(&["OLD"])).await.unwrap();
        let kept = aux
            .count_documents(doc! { "cross_matches.KEEP": { "$exists": true }, "coordinates": { "$exists": true } })
            .await
            .unwrap();
        let rerun = remove_fields(&aux, &["OLD"], run_start_jd, 4).await;
        aux.drop().await.unwrap();

        assert!(shards.len() > 1, "the test should exercise several shards");
        // Every even record, plus the odd multiples of 5 that carry only OLD_temp.
        assert_eq!(first.unwrap(), 600);
        assert_eq!(left, 1, "only the record created after the run start");
        assert_eq!(kept, 1000);
        assert_eq!(rerun.unwrap(), 0);
    }
}
