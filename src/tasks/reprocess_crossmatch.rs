//! The `reprocess_crossmatch` task: fill in or refresh crossmatches.
//!
//! The scheduler only crossmatches at first insert, so adding a catalog to
//! `crossmatch.<survey>` leaves every pre-existing `alerts_aux` record without
//! an entry for it. This fills those gaps, and can also refresh existing
//! crossmatches when a catalog's radius or projection changes.
//!
//! **Idempotent.** Every write recomputes a record's matches from the catalog as
//! it stands rather than amending what was there before; watchlists use
//! `$addToSet`, which is idempotent by construction and safe alongside live
//! ingest. That is what lets the queue resume a run whose lease lapsed.
//!
//! **Resumable without touching the records.** The catalog-driven path pages
//! through the catalog by `_id` and records how far it got in
//! `reprocess_crossmatch_state`, so an interrupted run continues from its
//! checkpoint. It used to mark progress in a temporary field on `alerts_aux`
//! itself, which meant a run wrote to the records twice and left a field
//! behind if it died between the two passes (#694). `restart` discards the
//! checkpoint and starts over.
//!
//! Ported from `src/bin/reprocess_crossmatch.rs`: the drivers already returned
//! `Result`, so the move was about giving the feeders a cancellation check and
//! pointing progress at the run rather than a terminal.

use super::batch::PROGRESS_EVERY;
use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::{
    api::catalogs::WATCHLIST_PREFIX,
    conf::CatalogXmatchConfig,
    utils::{
        data::{format_duration, format_eta, spawn_elapsed_logger},
        db::{join_tasks, merge_filters, range_shards, shard_field, TaskError, CURSOR_BATCH_SIZE},
        enums::Survey,
        spatial::{
            distance_kpc_from_arcsec, get_f64_from_doc, row_match_radius_arcsec, row_redshift,
            watchlist_match_field, xmatch, Coordinates, COINCIDENT_ARCSEC, NO_PROJECTED_DISTANCE,
        },
    },
};
use flare::{spatial::great_circle_distance, Time};
use futures::{StreamExt, TryStreamExt};
use mongodb::{
    bson::{doc, Bson, Document},
    error::{ErrorKind, InsertManyError},
    options::{UpdateOneModel, WriteModel},
    Namespace,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "reprocess_crossmatch";

/// How often the progress ticker publishes while a driver runs.
const PROGRESS_TICK: std::time::Duration = std::time::Duration::from_secs(5);

/// Stands in for the binary's terminal progress bar.
///
/// The drivers thread one through their workers and `inc` it as records land.
/// A task has no terminal, so this keeps the same call shape and sends the
/// count to the run instead. It is separate from the publishing because `inc`
/// is called from code that cannot await.
#[derive(Clone)]
struct Progress(Arc<std::sync::atomic::AtomicU64>);

impl Progress {
    fn new() -> Self {
        Self(Arc::new(std::sync::atomic::AtomicU64::new(0)))
    }

    fn inc(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    fn set_position(&self, n: u64) {
        self.0.store(n, Ordering::Relaxed);
    }

    fn count(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// No-op, kept so the call sites read as they do in the binary this came
    /// from: there is nothing to tidy up on a counter.
    fn finish(&self) {}
}

/// Publish a [`Progress`] to the run until the handle is aborted.
fn spawn_progress_ticker(
    ctx: TaskContext,
    progress: Progress,
    total: u64,
    label: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(PROGRESS_TICK).await;
            let done = progress.count();
            ctx.progress(done, total.max(done), format!("{label}: {done}"))
                .await;
        }
    })
}

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReprocessCrossmatchParams {
    pub survey: Survey,
    /// Each must already be declared under `crossmatch.<survey>` in the config,
    /// which is where the radius and projection come from.
    pub catalogs: Vec<String>,
    #[serde(default)]
    pub direction: Direction,
    /// Records accumulated per worker before a bulk write.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Parallel worker tasks, and the number of shards the cleanup passes split
    /// into.
    #[serde(default = "default_processes")]
    pub processes: usize,
    /// Queries in flight per worker. Workers are database-bound, so
    /// `processes × concurrency` sets throughput -- keep it under
    /// `database.max_pool_size`.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Objects-driven only: skip records that already carry every selected
    /// catalog.
    #[serde(default)]
    pub skip_existing: bool,
    /// Catalog-driven only: leave unmatched records untouched rather than
    /// writing an empty array. Safe when filling in a new catalog, but it will
    /// not clear stale matches.
    #[serde(default)]
    pub skip_empty: bool,
    /// Catalog-driven only: discard an interrupted run's progress and start
    /// over. A resumed run otherwise picks up from the checkpoint it recorded.
    #[serde(default)]
    pub restart: bool,
}

fn default_batch_size() -> usize {
    5_000
}
fn default_processes() -> usize {
    1
}
fn default_concurrency() -> usize {
    8
}
const QUEUE_MULTIPLIER: usize = 2;
const ARCSEC_TO_RAD: f64 = std::f64::consts::PI / 180.0 / 3600.0;
const STATE_COLLECTION: &str = "reprocess_crossmatch_state";
const STATUS_MATCHING: &str = "matching";
const STATUS_COMMITTING: &str = "committing";
const STATUS_CLEAN: &str = "clean";
const SHARDS_PER_PROCESS: usize = 8;

/// Catalog-driven costs extra full passes over alerts_aux, so it only wins when
/// the catalog is substantially smaller, not merely smaller.
const CATALOG_DRIVEN_MARGIN: u64 = 4;

/// Guards against a submission that would exhaust the connection pool or build
/// a write far larger than Mongo will accept.
const MAX_BATCH_SIZE: usize = 100_000;
const MAX_PROCESSES: usize = 64;
const MAX_CONCURRENCY: usize = 64;

impl ReprocessCrossmatchParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.catalogs.is_empty() {
            return Err("at least one catalog is required".to_string());
        }
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        if self.processes == 0 || self.processes > MAX_PROCESSES {
            return Err(format!("processes must be between 1 and {MAX_PROCESSES}"));
        }
        if self.concurrency == 0 || self.concurrency > MAX_CONCURRENCY {
            return Err(format!(
                "concurrency must be between 1 and {MAX_CONCURRENCY}"
            ));
        }
        Ok(())
    }
}

/// Resolve the requested catalog names against the survey's crossmatch config.
///
/// Done up front so an unknown or undeclared catalog is a rejected submission
/// rather than a run that fails partway with some collections already rewritten.
fn resolve_catalogs(
    config: &crate::conf::AppConfig,
    survey: &Survey,
    names: &[String],
) -> Result<Vec<CatalogXmatchConfig>, String> {
    let survey_configs = config.crossmatch.get(survey).ok_or_else(|| {
        format!(
            "survey {survey} has no crossmatch.{} section in the config",
            survey.to_string().to_lowercase()
        )
    })?;

    let mut resolved: Vec<CatalogXmatchConfig> = Vec::with_capacity(names.len());
    for name in names {
        if resolved.iter().any(|c| &c.catalog == name) {
            continue; // listed twice; the second mention adds nothing
        }
        match survey_configs.iter().find(|c| &c.catalog == name) {
            Some(c) => resolved.push(c.clone()),
            None => {
                return Err(format!(
                    "catalog {name:?} is not declared under crossmatch.{} in the config",
                    survey.to_string().to_lowercase()
                ))
            }
        }
    }
    Ok(resolved)
}

/// Run the reprocess.
pub async fn run(
    ctx: &TaskContext,
    params: ReprocessCrossmatchParams,
) -> Result<serde_json::Value, super::TaskError> {
    let db = ctx.db().clone();
    let config = ctx.config();

    let resolved = resolve_catalogs(config, &params.survey, &params.catalogs)
        .map_err(super::TaskError::InvalidParams)?;

    let in_flight = params.processes * params.concurrency;
    if in_flight > config.database.max_pool_size as usize {
        ctx.warn(format!(
            "processes x concurrency = {in_flight} exceeds database.max_pool_size = {}; \
             workers will queue on the connection pool",
            config.database.max_pool_size
        ));
    }

    // Watchlists take a different path entirely -- object ids are recorded on
    // the watchlist document rather than on alerts_aux -- so `direction` does
    // not apply to them.
    let (watchlist, non_watchlist): (Vec<_>, Vec<_>) = resolved
        .into_iter()
        .partition(|c| c.catalog.starts_with(WATCHLIST_PREFIX));

    let mut objects_catalogs = Vec::new();
    let mut catalog_catalogs = Vec::new();
    for cat in non_watchlist {
        let direction = match params.direction {
            Direction::Auto => pick_direction(&params.survey, &cat, &db).await,
            other => other,
        };
        match direction {
            Direction::Objects => objects_catalogs.push(cat),
            Direction::Catalog => catalog_catalogs.push(cat),
            Direction::Auto => unreachable!("pick_direction never returns Auto"),
        }
    }

    ctx.info(format!(
        "reprocessing {} crossmatches: objects-driven {:?}, catalog-driven {:?}, watchlist {:?}",
        params.survey,
        objects_catalogs
            .iter()
            .map(|c| &c.catalog)
            .collect::<Vec<_>>(),
        catalog_catalogs
            .iter()
            .map(|c| &c.catalog)
            .collect::<Vec<_>>(),
        watchlist.iter().map(|c| &c.catalog).collect::<Vec<_>>(),
    ));

    // Untyped on purpose: the drivers return `utils::db::TaskError`, a
    // different type from this module's `super::TaskError` despite the name.
    let failed = |e: TaskError| super::TaskError::Failed(e.to_string());

    let mut done: Vec<String> = Vec::new();

    for cat in watchlist {
        let name = cat.catalog.clone();
        run_watchlist_driven(
            ctx,
            &params.survey,
            cat,
            db.clone(),
            params.batch_size,
            params.processes,
        )
        .await
        .map_err(failed)?;
        done.push(name);
    }

    if !objects_catalogs.is_empty() {
        let names: Vec<String> = objects_catalogs.iter().map(|c| c.catalog.clone()).collect();
        run_objects_driven(
            ctx,
            &params.survey,
            objects_catalogs,
            db.clone(),
            params.batch_size,
            params.processes,
            params.concurrency,
            params.skip_existing,
        )
        .await
        .map_err(failed)?;
        done.extend(names);
    }

    for cat in catalog_catalogs {
        let name = cat.catalog.clone();
        run_catalog_driven(
            ctx,
            &params.survey,
            cat,
            db.clone(),
            params.batch_size,
            params.processes,
            params.concurrency,
            params.skip_empty,
            params.restart,
        )
        .await
        .map_err(failed)?;
        done.push(name);
    }

    if ctx.is_canceled() {
        return Err(super::TaskError::Canceled);
    }

    let aux = format!("{}_alerts_aux", params.survey);
    ctx.record_mutation(
        MutationTarget {
            database: db.name().to_string(),
            collection: aux.clone(),
            catalog: None,
            survey: Some(params.survey.to_string().to_lowercase()),
        },
        // Backfill rather than Recompute: the values come from a separate
        // catalog collection, not from fields already on the record.
        Operation::Backfill,
        doc! {
            "catalogs": done.clone(),
            "direction": format!("{:?}", params.direction),
            "processes": params.processes as i64,
            "concurrency": params.concurrency as i64,
            "batch_size": params.batch_size as i64,
            "skip_existing": params.skip_existing,
            "skip_empty": params.skip_empty,
            "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                .unwrap_or(mongodb::bson::Bson::Null),
        },
    )
    .await;

    ctx.info(format!("reprocess complete for {done:?}"));
    Ok(serde_json::json!({
        "survey": params.survey.to_string(),
        "collection": aux,
        "catalogs": done,
    }))
}

/// Reprocessing can be done in two directions:
/// - Checking the crossmatch catalogs for each alerts_aux record,
/// - Checking the alerts_aux collection for each catalog record.
///
/// To optimize the reprocessing, the binary can loop over either
/// the alerts_aux collection or the catalog collection, depending on which is smaller.
/// If `--direction` is not provided, it checks the estimated document counts
/// of each collection and loops over the smaller one.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// Pick `objects` or `catalog` per catalog based on which side has fewer rows.
    #[default]
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
// Eight arguments, all of them independent knobs the caller genuinely varies.
// Bundling them into a struct would just move the same list somewhere else.
#[allow(clippy::too_many_arguments)]
async fn run_objects_driven(
    ctx: &TaskContext,
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
    let pb = Progress::new();
    let logger = spawn_progress_ticker(ctx.clone(), pb.clone(), estimated, label);

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
    let mut fed: u64 = 0;
    let mut last_reported: u64 = 0;
    while let Some(d) = cursor.try_next().await? {
        // Cancelling here rather than inside the workers: they drain whatever is
        // already queued and flush their batches, so a cancelled run leaves
        // whole records written rather than a half-applied bulk write.
        if ctx.is_canceled() {
            ctx.warn("cancellation requested; no longer feeding workers");
            break;
        }
        if tx.send(d).await.is_err() {
            break;
        }
        fed += 1;
        if fed - last_reported >= PROGRESS_EVERY {
            last_reported = fed;
            ctx.progress(fed, estimated.max(fed), format!("{fed} records queued"))
                .await;
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
    pb: Progress,
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
    pb: &Progress,
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
    ctx: &TaskContext,
    survey: &Survey,
    watchlist_config: CatalogXmatchConfig,
    db: mongodb::Database,
    batch_size: usize,
    processes: usize,
) -> Result<(), TaskError> {
    let wl_collection: mongodb::Collection<Document> = db.collection(&watchlist_config.catalog);
    let estimated = wl_collection.estimated_document_count().await.unwrap_or(0);
    let label = format!("watchlist→{}", watchlist_config.catalog);
    let pb = Progress::new();
    let logger = spawn_progress_ticker(ctx.clone(), pb.clone(), estimated, label);

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
    let mut fed: u64 = 0;
    let mut last_reported: u64 = 0;
    while let Some(d) = cursor.try_next().await? {
        // Cancelling here rather than inside the workers: they drain whatever is
        // already queued and flush their batches, so a cancelled run leaves
        // whole records written rather than a half-applied bulk write.
        if ctx.is_canceled() {
            ctx.warn("cancellation requested; no longer feeding workers");
            break;
        }
        if tx.send(d).await.is_err() {
            break;
        }
        fed += 1;
        if fed - last_reported >= PROGRESS_EVERY {
            last_reported = fed;
            ctx.progress(fed, estimated.max(fed), format!("{fed} records queued"))
                .await;
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
    pb: Progress,
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
    ctx: &TaskContext,
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
            ctx,
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
    ctx: &TaskContext,
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
    let pb = Progress::new();
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
    let logger = spawn_progress_ticker(
        ctx.clone(),
        pb.clone(),
        catalog_estimated,
        label.to_string(),
    );

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
            // At the page boundary: a page's matches are buffered and its
            // checkpoint recorded before the next is fed, so stopping here
            // leaves a state the next run resumes from rather than repeats.
            if ctx.is_canceled() {
                ctx.warn(format!(
                    "{label}: canceled while feeding catalog pages; the checkpoint records \
                     what has been matched, so re-running continues from there"
                ));
                break;
            }
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
    pb: &Progress,
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
    let live_field = format!("cross_matches.{}", catalog_config.catalog);
    // Left by the temp-field approach this replaced, still present on some
    // alerts_aux records, so the merge clears it as it goes.
    let legacy_temp_field = format!("cross_matches.{}_temp", catalog_config.catalog);
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

async fn sharded_aggregate(
    collection: &mongodb::Collection<Document>,
    shards: &[Document],
    processes: usize,
    base_filter: &Document,
    stages: Vec<Document>,
    label: &str,
) -> Result<(), mongodb::error::Error> {
    let total = shards.len();
    info!("[{}] running over {} shards", label, total);
    let done = &AtomicUsize::new(0);
    let started = Instant::now();
    let results: Vec<_> = futures::stream::iter(shards.iter().enumerate().map(|(index, shard)| {
        let filter = merge_filters(base_filter, shard);
        let mut pipeline = Vec::with_capacity(stages.len() + 1);
        if !filter.is_empty() {
            pipeline.push(doc! { "$match": filter });
        }
        pipeline.extend(stages.iter().cloned());
        async move {
            let result = collection.aggregate(pipeline).allow_disk_use(true).await;
            let completed = done.fetch_add(1, Ordering::Relaxed) + 1;
            let elapsed = started.elapsed();
            let progress = format!(
                "{} shards complete, {} elapsed, eta {}",
                completed,
                format_duration(elapsed.as_secs()),
                format_eta(
                    (total - completed) as u64,
                    completed as f64 / elapsed.as_secs_f64()
                )
            );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::AppConfig;

    fn params(catalogs: &[&str]) -> ReprocessCrossmatchParams {
        ReprocessCrossmatchParams {
            survey: Survey::Ztf,
            catalogs: catalogs.iter().map(|c| c.to_string()).collect(),
            direction: Direction::Auto,
            batch_size: default_batch_size(),
            processes: default_processes(),
            concurrency: default_concurrency(),
            skip_existing: false,
            skip_empty: false,
            restart: false,
        }
    }

    #[test]
    fn at_least_one_catalog_is_required() {
        // An empty list would run to completion having done nothing, which
        // reads as success.
        assert!(params(&[]).validate_params().is_err());
        assert!(params(&["NED"]).validate_params().is_ok());
    }

    #[test]
    fn the_concurrency_knobs_are_bounded() {
        // processes x concurrency is what can exhaust the connection pool.
        let mut p = params(&["NED"]);
        p.processes = MAX_PROCESSES + 1;
        assert!(p.validate_params().is_err());
        let mut p = params(&["NED"]);
        p.concurrency = 0;
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn direction_defaults_to_auto() {
        // Auto picks per catalog based on which side has fewer rows, which is
        // the right default for someone who has not measured.
        let parsed: ReprocessCrossmatchParams = serde_json::from_value(serde_json::json!({
            "survey": "ztf",
            "catalogs": ["NED"],
        }))
        .expect("defaults");
        assert_eq!(parsed.direction, Direction::Auto);
        assert_eq!(parsed.processes, default_processes());
    }

    #[test]
    fn an_undeclared_catalog_is_rejected_before_anything_is_written() {
        // Resolved up front: a bad name partway through would leave earlier
        // catalogs already rewritten.
        let config = AppConfig::from_test_config().expect("test config");
        let err = resolve_catalogs(&config, &Survey::Ztf, &["NotACatalog".into()])
            .expect_err("should reject");
        assert!(err.contains("NotACatalog"), "{err}");
        assert!(err.contains("crossmatch.ztf"), "{err}");
    }

    #[test]
    fn a_declared_catalog_resolves_to_its_crossmatch_config() {
        // The radius and projection come from config, not from the request --
        // a client cannot widen a search radius by asking.
        let config = AppConfig::from_test_config().expect("test config");
        let declared = config
            .crossmatch
            .get(&Survey::Ztf)
            .and_then(|c| c.first())
            .map(|c| c.catalog.clone())
            .expect("the test config crossmatches ztf against something");
        let resolved = resolve_catalogs(&config, &Survey::Ztf, std::slice::from_ref(&declared))
            .expect("resolves");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].catalog, declared);
    }

    #[test]
    fn a_catalog_listed_twice_is_only_processed_once() {
        let config = AppConfig::from_test_config().expect("test config");
        let declared = config
            .crossmatch
            .get(&Survey::Ztf)
            .and_then(|c| c.first())
            .map(|c| c.catalog.clone())
            .expect("a declared catalog");
        let resolved = resolve_catalogs(&config, &Survey::Ztf, &[declared.clone(), declared])
            .expect("resolves");
        assert_eq!(resolved.len(), 1);
    }

    #[test]
    fn the_task_is_registered_and_retryable() {
        assert!(crate::tasks::is_retryable(TASK_TYPE));
    }

    #[test]
    fn single_flight_is_keyed_by_survey() {
        assert_eq!(
            crate::tasks::single_flight_key(
                TASK_TYPE,
                &serde_json::json!({ "survey": "ztf", "catalogs": ["NED"] })
            ),
            Some(mongodb::bson::doc! { "survey": "ztf" })
        );
    }
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
