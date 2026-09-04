//! Archival catalogs: what BOOM crossmatches incoming alerts against, and how
//! each one gets into MongoDB.
//!
//! See [`docs/catalogs.md`](../../docs/catalogs.md). A catalog is declared once
//! in [`CATALOGS`] -- where its source lives is declared on the Python side, in
//! `boompy` -- and ingested by [`add_catalog`], which is a plain async function
//! so that the task system can call it directly rather than shelling out to a
//! binary.
//!
//! Ingest is **chunked**: the catalog is fetched, ingested and deleted one
//! chunk at a time, and each completed chunk is recorded. That bounds peak disk
//! at one chunk rather than one catalog -- NED is a gigabyte and AllWISE is
//! hundreds -- and it makes an interrupted run resumable, which matters when
//! the run takes a day and the host reboots.

pub mod ascii;
pub mod csv;
pub mod download;
#[cfg(feature = "catalogs")]
pub mod fits;
pub mod ingest;
#[cfg(feature = "catalogs")]
pub mod parquet;
pub mod types;

use crate::tasks::TaskContext;
use download::{Boompy, Chunk, DownloadError};
use ingest::{IngestError, IngestReport, Inserter};
use mongodb::bson::{doc, Document};
use mongodb::Database;
use std::path::{Path, PathBuf};
use tracing::instrument;

/// Per-catalog ingest state: which chunks are in, and how many records.
///
/// Operational bookkeeping, not science data -- it is in
/// `api::db::PROTECTED_COLLECTION_NAMES` so it does not show up as a catalog in
/// its own right.
pub const STATE_COLLECTION: &str = "catalog_state";

/// Which reader turns this catalog's source files into documents.
///
/// One variant per catalog rather than one per format: the format only says how
/// to get columns out of a file, and the record type is what says which columns
/// there are and what they mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reader {
    /// 2MASS PSC, pipe-delimited text.
    TwoMass,
    /// NED-LVS, a FITS binary table.
    Ned,
    /// AllWISE, parquet partitions.
    AllWise,
}

impl Reader {
    /// Whether ingesting this catalog needs a build with the `catalogs`
    /// feature, i.e. whether its format engine is one of the gated ones.
    pub const fn needs_feature(&self) -> bool {
        match self {
            Reader::TwoMass => false,
            Reader::Ned | Reader::AllWise => true,
        }
    }
}

/// A catalog BOOM knows how to ingest.
#[derive(Debug, Clone, Copy)]
pub struct CatalogDef {
    /// Kebab-case slug, as written in the `catalogs` list in `config.yaml`.
    pub id: &'static str,
    /// MongoDB collection name, as written in `crossmatch.<survey>[].catalog`.
    pub collection: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub reader: Reader,
}

/// Every catalog this release knows how to build.
///
/// In code rather than in config because how to ingest a catalog is the same on
/// every BOOM deployment and is worth reviewing as a PR; which catalogs a given
/// deployment holds is the config's business.
pub const CATALOGS: &[CatalogDef] = &[
    CatalogDef {
        id: "2mass",
        collection: "2MASS",
        title: "2MASS Point Source Catalog",
        description: "Near-infrared JHKs photometry for 471 million point sources, \
                      published as ~92 pipe-delimited files.",
        reader: Reader::TwoMass,
    },
    CatalogDef {
        id: "ned-lvs",
        collection: "NED_LVS",
        title: "NED Local Volume Sample",
        description: "Redshifts, distances, stellar masses and angular diameters for \
                      nearby galaxies. One FITS table, always the current release.",
        reader: Reader::Ned,
    },
    CatalogDef {
        id: "allwise",
        collection: "AllWISE",
        title: "AllWISE Source Catalog",
        description: "Mid-infrared W1-W4 photometry and proper motions for 748 million \
                      sources, read from the LSDB HATS mirror one HEALPix partition at a time.",
        reader: Reader::AllWise,
    },
];

/// Look up a catalog by its slug.
pub fn find(id: &str) -> Option<&'static CatalogDef> {
    CATALOGS.iter().find(|c| c.id == id)
}

#[derive(thiserror::Error, Debug)]
pub enum CatalogError {
    #[error("unknown catalog {id:?}; known catalogs are {known}")]
    Unknown { id: String, known: String },
    #[error(
        "ingesting {id} needs a build with the `catalogs` feature \
         (cargo build --features catalogs)"
    )]
    FeatureRequired { id: String },
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
    #[error("failed to prepare {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

/// What [`add_catalog`] should do.
#[derive(Debug, Clone)]
pub struct AddCatalogParams {
    /// Catalog slug, e.g. `2mass`.
    pub catalog: String,
    /// Drop the collection and start over, rather than resuming.
    pub drop_existing: bool,
    /// Where chunks are downloaded to. Each is deleted once ingested, so this
    /// needs room for the largest single chunk, not for the catalog.
    pub download_dir: PathBuf,
    /// Directory holding boompy's `pyproject.toml`.
    pub boompy_dir: PathBuf,
    pub num_workers: usize,
    pub batch_size: usize,
    pub channel_capacity: usize,
    /// Stop after this many chunks. For smoke-testing a catalog end to end
    /// without ingesting all of it.
    pub max_chunks: Option<usize>,
    /// Keep downloaded files instead of deleting each ingested chunk. Only for
    /// debugging a parse -- the default exists so a catalog cannot fill the
    /// disk.
    pub keep_downloads: bool,
}

impl AddCatalogParams {
    pub fn new(catalog: impl Into<String>, download_dir: impl Into<PathBuf>) -> Self {
        Self {
            catalog: catalog.into(),
            drop_existing: false,
            download_dir: download_dir.into(),
            boompy_dir: PathBuf::from("boompy"),
            num_workers: 4,
            batch_size: 10_000,
            channel_capacity: 100_000,
            max_chunks: None,
            keep_downloads: false,
        }
    }
}

/// What one [`add_catalog`] run did.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AddCatalogReport {
    pub catalog: String,
    pub collection: String,
    /// Chunks ingested by this run, excluding ones already done.
    pub chunks_ingested: usize,
    /// Chunks skipped because a previous run had already done them.
    pub chunks_resumed: usize,
    pub chunks_total: usize,
    pub records: IngestReport,
    /// Whether every chunk is now in. False when `max_chunks` or a cancellation
    /// cut the run short.
    pub complete: bool,
    /// Whether the run stopped because cancellation was requested.
    pub canceled: bool,
}

/// Download and ingest a catalog, chunk by chunk, resuming where a previous run
/// left off.
///
/// Safe to re-run, which is what makes it usable as a task: completed chunks are
/// skipped, and because every catalog derives `_id` from a stable source
/// identifier, re-ingesting a chunk that was interrupted mid-write upserts
/// rather than duplicates. A run cut short by a cancellation, a deploy or a
/// crash therefore costs one chunk, not the whole catalog.
///
/// Cancellation is checked at chunk boundaries. Stopping mid-chunk would leave
/// a partially written chunk unrecorded, which the next run would redo anyway --
/// so the wait is bounded by one chunk and buys a clean resume point.
#[instrument(
    skip(ctx, params),
    fields(catalog = %params.catalog, collection = tracing::field::Empty),
    err
)]
pub async fn add_catalog(
    ctx: &TaskContext,
    params: &AddCatalogParams,
) -> Result<AddCatalogReport, CatalogError> {
    let db = ctx.db();
    let def = find(&params.catalog).ok_or_else(|| CatalogError::Unknown {
        id: params.catalog.clone(),
        known: CATALOGS.iter().map(|c| c.id).collect::<Vec<_>>().join(", "),
    })?;
    tracing::Span::current().record("collection", def.collection);

    // Fail before downloading a gigabyte, not after.
    if def.reader.needs_feature() && !cfg!(feature = "catalogs") {
        return Err(CatalogError::FeatureRequired {
            id: def.id.to_string(),
        });
    }

    let state = db.collection::<Document>(STATE_COLLECTION);
    if params.drop_existing {
        tracing::warn!("dropping {} and its ingest state", def.collection);
        db.collection::<Document>(def.collection).drop().await?;
        state.delete_one(doc! { "_id": def.collection }).await?;
    }

    let download_dir = params.download_dir.join(def.id);
    std::fs::create_dir_all(&download_dir).map_err(|e| CatalogError::Io {
        path: download_dir.clone(),
        source: e,
    })?;

    let boompy = Boompy::new(&params.boompy_dir);
    let chunks = boompy.list_chunks(def.id).await?;
    let done = chunks_done(&state, def.collection).await?;
    ctx.info(format!(
        "ingesting {} into {}: {} chunks, {} already done",
        def.id,
        def.collection,
        chunks.len(),
        done.len()
    ));

    let inserter = Inserter::new(
        db.clone(),
        def.collection,
        params.num_workers,
        params.batch_size,
        params.channel_capacity,
    );
    let mut report = AddCatalogReport {
        catalog: def.id.to_string(),
        collection: def.collection.to_string(),
        chunks_ingested: 0,
        chunks_resumed: 0,
        chunks_total: chunks.len(),
        records: IngestReport::default(),
        complete: false,
        canceled: false,
    };
    start_state(&state, def, chunks.len()).await?;

    for chunk in &chunks {
        if done.contains(&chunk.id) {
            report.chunks_resumed += 1;
            continue;
        }
        if ctx.is_canceled() {
            report.canceled = true;
            ctx.warn(format!(
                "canceled after {} of {} chunks; the chunks already recorded are kept, \
                 so a later run resumes from here",
                report.chunks_ingested + report.chunks_resumed,
                report.chunks_total
            ));
            break;
        }
        if params
            .max_chunks
            .is_some_and(|max| report.chunks_ingested >= max)
        {
            ctx.info(format!(
                "stopping after {} chunks as requested",
                report.chunks_ingested
            ));
            break;
        }
        let ingested = ingest_chunk(&boompy, &inserter, def, chunk, &download_dir, params).await?;
        report.records.merge(ingested);
        report.chunks_ingested += 1;
        record_chunk(&state, def.collection, &chunk.id, ingested.inserted).await?;

        let done_count = (report.chunks_ingested + report.chunks_resumed) as u64;
        ctx.info(format!(
            "chunk {} done ({}/{}): {} read, {} inserted",
            chunk.id, done_count, report.chunks_total, ingested.read, ingested.inserted
        ));
        ctx.progress(
            done_count,
            report.chunks_total as u64,
            format!("chunk {} of {}", done_count, report.chunks_total),
        )
        .await;
    }

    report.complete = report.chunks_ingested + report.chunks_resumed == report.chunks_total;
    if report.complete {
        // Indexed only at the end: an index maintained during the load roughly
        // doubles the time to ingest a large catalog, and a partially ingested
        // catalog should not be servable anyway.
        ctx.info(format!("building indexes on {}", def.collection));
        inserter.create_indexes(true).await?;
        finish_state(&state, def.collection).await?;
        ctx.info(format!(
            "{} complete: {} records in {}",
            def.id, report.records.inserted, def.collection
        ));
    } else {
        ctx.warn(format!(
            "{} incomplete: {}/{} chunks; run it again to resume",
            def.id,
            report.chunks_ingested + report.chunks_resumed,
            report.chunks_total
        ));
    }
    ctx.flush_logs().await;
    Ok(report)
}

/// Fetch one chunk, ingest every file it produced, then delete them.
///
/// The delete is the whole point of chunking, so it happens even when the
/// ingest fails -- otherwise a run that fails repeatedly on one chunk fills the
/// disk with retries.
async fn ingest_chunk(
    boompy: &Boompy,
    inserter: &Inserter,
    def: &CatalogDef,
    chunk: &Chunk,
    download_dir: &Path,
    params: &AddCatalogParams,
) -> Result<IngestReport, CatalogError> {
    tracing::info!(
        chunk = %chunk.id,
        label = chunk.label.as_deref().unwrap_or(""),
        "fetching"
    );
    let files = boompy.fetch_chunk(def.id, &chunk.id, download_dir).await?;

    let mut report = IngestReport::default();
    let mut result = Ok(());
    for file in &files {
        match ingest_file(def.reader, inserter, file).await {
            Ok(one) => report.merge(one),
            Err(e) => {
                result = Err(e);
                break;
            }
        }
    }

    if !params.keep_downloads {
        for file in &files {
            if let Err(e) = std::fs::remove_file(file) {
                // Not fatal on its own, but it is how the disk fills up.
                tracing::warn!("failed to delete {}: {}", file.display(), e);
            }
        }
    }
    result.map(|()| report)
}

/// Dispatch one source file to the engine its catalog is read by.
async fn ingest_file(
    reader: Reader,
    inserter: &Inserter,
    path: &Path,
) -> Result<IngestReport, CatalogError> {
    match reader {
        Reader::TwoMass => Ok(ascii::ingest_ascii::<types::TwoMass>(inserter, path).await?),
        #[cfg(feature = "catalogs")]
        Reader::Ned => Ok(fits::ingest_fits::<types::Ned>(inserter, path).await?),
        #[cfg(feature = "catalogs")]
        Reader::AllWise => Ok(parquet::ingest_parquet::<types::AllWise>(inserter, path).await?),
        // Unreachable via add_catalog, which checks the feature up front, but
        // ingest_file is reachable on its own.
        #[cfg(not(feature = "catalogs"))]
        Reader::Ned | Reader::AllWise => Err(CatalogError::FeatureRequired {
            id: format!("{:?}", reader),
        }),
    }
}

/// Chunk ids a previous run finished.
async fn chunks_done(
    state: &mongodb::Collection<Document>,
    collection: &str,
) -> Result<std::collections::HashSet<String>, CatalogError> {
    let Some(doc) = state.find_one(doc! { "_id": collection }).await? else {
        return Ok(Default::default());
    };
    Ok(doc
        .get_array("chunks_done")
        .map(|ids| {
            ids.iter()
                .filter_map(|id| id.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default())
}

fn now() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

async fn start_state(
    state: &mongodb::Collection<Document>,
    def: &CatalogDef,
    chunks_total: usize,
) -> Result<(), CatalogError> {
    state
        .update_one(
            doc! { "_id": def.collection },
            doc! {
                "$set": {
                    "catalog": def.id,
                    "status": "ingesting",
                    "chunks_total": chunks_total as i64,
                    "updated_at": now(),
                },
                "$setOnInsert": { "started_at": now(), "n_records": 0i64 },
            },
        )
        .upsert(true)
        .await?;
    Ok(())
}

/// Record a chunk as done, atomically with its record count.
///
/// `$addToSet` rather than `$push` so a chunk re-ingested after an interrupted
/// write is not listed twice.
async fn record_chunk(
    state: &mongodb::Collection<Document>,
    collection: &str,
    chunk_id: &str,
    inserted: u64,
) -> Result<(), CatalogError> {
    state
        .update_one(
            doc! { "_id": collection },
            doc! {
                "$addToSet": { "chunks_done": chunk_id },
                "$inc": { "n_records": inserted as i64 },
                "$set": { "updated_at": now() },
            },
        )
        .upsert(true)
        .await?;
    Ok(())
}

async fn finish_state(
    state: &mongodb::Collection<Document>,
    collection: &str,
) -> Result<(), CatalogError> {
    state
        .update_one(
            doc! { "_id": collection },
            doc! { "$set": { "status": "complete", "completed_at": now(), "updated_at": now() } },
        )
        .await?;
    Ok(())
}

/// How a declared catalog compares to what is actually in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogHealth {
    /// Declared, ingested, every chunk in.
    Present,
    /// Declared, but the collection has never been ingested.
    Missing,
    /// Declared and started, but not every chunk is in. The collection exists
    /// and is partly populated, so a crossmatch against it silently returns
    /// fewer matches than it should -- worse than absent, which at least fails
    /// loudly.
    Partial,
    /// Declared in config, but this release has no definition for it. Almost
    /// always a typo in the slug.
    Undeclared,
}

/// The state of one declared catalog.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CatalogStatus {
    pub id: String,
    pub collection: Option<String>,
    pub title: Option<String>,
    pub health: CatalogHealth,
    pub chunks_done: usize,
    pub chunks_total: usize,
    pub n_records: i64,
}

/// Compare the catalogs config declares against what is in the database.
///
/// Reports; never acts. Ingesting a catalog is hours to days of work and has to
/// stay an explicit, attributed decision -- a typo in `catalogs:` must not be
/// able to rebuild anything. See `docs/catalogs.md`.
#[instrument(skip(db, declared))]
pub async fn status(
    db: &Database,
    declared: &[String],
) -> Result<Vec<CatalogStatus>, CatalogError> {
    let state = db.collection::<Document>(STATE_COLLECTION);
    let mut statuses = Vec::with_capacity(declared.len());

    for id in declared {
        let Some(def) = find(id) else {
            statuses.push(CatalogStatus {
                id: id.clone(),
                collection: None,
                title: None,
                health: CatalogHealth::Undeclared,
                chunks_done: 0,
                chunks_total: 0,
                n_records: 0,
            });
            continue;
        };
        let doc = state.find_one(doc! { "_id": def.collection }).await?;
        let (health, chunks_done, chunks_total, n_records) = match &doc {
            None => (CatalogHealth::Missing, 0, 0, 0),
            Some(doc) => {
                let done = doc
                    .get_array("chunks_done")
                    .map(|c| c.len())
                    .unwrap_or_default();
                let total = doc.get_i64("chunks_total").unwrap_or_default() as usize;
                let records = doc.get_i64("n_records").unwrap_or_default();
                let complete = doc.get_str("status").is_ok_and(|s| s == "complete");
                let health = if complete {
                    CatalogHealth::Present
                } else {
                    CatalogHealth::Partial
                };
                (health, done, total, records)
            }
        };
        statuses.push(CatalogStatus {
            id: def.id.to_string(),
            collection: Some(def.collection.to_string()),
            title: Some(def.title.to_string()),
            health,
            chunks_done,
            chunks_total,
            n_records,
        });
    }
    Ok(statuses)
}
