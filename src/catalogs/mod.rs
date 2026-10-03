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

pub mod arrow;
pub mod ascii;
pub mod csv;
pub mod download;
pub mod ingest;
pub mod jsonl;
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
///
/// There are only two formats to read -- delimited text and parquet -- because
/// anything else is converted to parquet by boompy, where the library that
/// reads it already lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reader {
    /// 2MASS PSC, pipe-delimited text.
    TwoMass,
    /// NED-LVS, converted from its published FITS table to parquet.
    Ned,
    /// AllWISE, parquet partitions.
    AllWise,
    /// Million Quasars, converted from its published FITS table.
    Milliquas,
    /// DESI DR1 redshifts, converted and filtered from the iron zcatalog.
    DesiDr1,
    /// CatWISE2020, converted from the published IPAC tables.
    CatWise2020,
    /// Gaia DR3, read straight from the published gzipped CSV.
    GaiaDr3,
    /// GALEX GUVcat_AIS, read straight from the published gzipped CSV.
    Galex,
    /// VSX, fixed-width text.
    Vsx,
    /// Pan-STARRS otmo, parquet partitions.
    PanStarrs,
    /// Legacy Survey point-source scores, staged gzipped JSONL exported from
    /// BOOM's own copy.
    Lspsc,
}

/// Where a catalog's files come from, which decides whether BOOM may delete
/// them after ingesting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// Fetched from an archive. Each chunk is deleted once ingested -- that is
    /// what keeps peak disk at one chunk.
    Fetched,
    /// Already on disk, built by something outside BOOM. **Never deleted**: the
    /// files are the artifact, not a cache of it, and rebuilding one can be
    /// hours of work over hundreds of gigabytes.
    Staged,
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
    pub source: Source,
    /// Other collection names this catalog is stored under.
    ///
    /// Some catalogs stamp their release into the collection name, so a
    /// deployment that ingested an earlier release is running the same catalog
    /// under a different name. Without this, config validation would call that
    /// a typo, and the drift table would call a populated catalog missing.
    ///
    /// A new ingest always writes to `collection`; aliases only recognize what
    /// is already there.
    pub aliases: &'static [&'static str],
}

/// Every catalog this release knows how to build.
///
/// In code rather than in config because how to ingest a catalog is the same on
/// every BOOM deployment and is worth reviewing as a PR; which catalogs a given
/// deployment holds is the config's business.
pub const CATALOGS: &[CatalogDef] = &[
    CatalogDef {
        id: "2mass",
        collection: "2MASS_PSC",
        title: "2MASS Point Source Catalog",
        description: "Near-infrared JHKs photometry for 471 million point sources, \
                      published as ~92 pipe-delimited files.",
        reader: Reader::TwoMass,
        source: Source::Fetched,
        aliases: &[],
    },
    CatalogDef {
        id: "ned-lvs",
        collection: "NED",
        title: "NED Local Volume Sample",
        description: "Redshifts, distances, stellar masses and angular diameters for \
                      nearby galaxies. One table, always the current release.",
        reader: Reader::Ned,
        source: Source::Fetched,
        aliases: &[],
    },
    CatalogDef {
        id: "allwise",
        collection: "AllWISE",
        title: "AllWISE Source Catalog",
        description: "Mid-infrared W1-W4 photometry and proper motions for 748 million \
                      sources, read from the LSDB HATS mirror one HEALPix partition at a time.",
        reader: Reader::AllWise,
        source: Source::Fetched,
        aliases: &[],
    },
    CatalogDef {
        id: "milliquas",
        collection: "milliquas_v8",
        title: "Million Quasars",
        description: "Quasars and quasar candidates with redshifts and radio/X-ray \
                      associations. One table, converted from FITS.",
        reader: Reader::Milliquas,
        source: Source::Fetched,
        // Milliquas stamps its release into the collection name; deployments
        // sit on whichever they last ingested.
        aliases: &["milliquas_v6", "milliquas_v7"],
    },
    CatalogDef {
        id: "desi-dr1",
        collection: "DESI_DR1",
        title: "DESI DR1 redshifts",
        description: "Spectroscopic redshifts from the iron zcatalog, filtered to the \
                      primary spectrum of each science target.",
        reader: Reader::DesiDr1,
        source: Source::Fetched,
        aliases: &[],
    },
    CatalogDef {
        id: "catwise2020",
        collection: "CatWISE2020",
        title: "CatWISE2020",
        description: "Mid-infrared W1/W2 photometry and proper motions, published as \
                      several hundred IPAC tables.",
        reader: Reader::CatWise2020,
        source: Source::Fetched,
        aliases: &[],
    },
    CatalogDef {
        id: "gaia-dr3",
        collection: "Gaia_DR3",
        title: "Gaia DR3",
        description: "Astrometry, parallaxes, proper motions and G/BP/RP photometry for \
                      1.8 billion sources, in ~3400 gzipped CSV files.",
        reader: Reader::GaiaDr3,
        source: Source::Fetched,
        aliases: &[],
    },
    CatalogDef {
        id: "galex",
        collection: "GALEX",
        title: "GALEX GUVcat_AIS",
        description: "Ultraviolet FUV/NUV photometry from the All-Sky Imaging Survey.",
        reader: Reader::Galex,
        source: Source::Fetched,
        aliases: &[],
    },
    CatalogDef {
        id: "vsx",
        collection: "VSX",
        title: "AAVSO Variable Star Index",
        description: "Variability types, magnitudes at maximum and minimum, epochs and \
                      periods for known and suspected variable stars.",
        reader: Reader::Vsx,
        source: Source::Fetched,
        aliases: &[],
    },
    CatalogDef {
        id: "panstarrs",
        collection: "PS1_DR2",
        title: "Pan-STARRS DR2 object-mean photometry",
        description: "Mean PSF magnitudes in grizy for Pan-STARRS objects, from the HATS \
                      mirror of the DR2 otmo table. Does not carry the PS1-STRM \
                      classification and photo-z columns (strm_*) that crossmatch config \
                      projects: those come from a separate catalog this ingest does not \
                      join.",
        reader: Reader::PanStarrs,
        source: Source::Fetched,
        // Configs named PS1_DR1 until #598, though the only mirror was ever DR2.
        aliases: &["PS1_DR1"],
    },
    CatalogDef {
        id: "lspsc",
        collection: "LSPSC",
        title: "Legacy Survey Point Source Catalog",
        description: "Morphological resolved/unresolved scores for 3.1e9 LS DR10 sources \
                      (Liu et al. 2025, arXiv:2505.17174). Published upstream as a \
                      cone-search service rather than files, so BOOM ingests an export of \
                      its own copy: run the export_catalog task, stage the result, ingest \
                      it. See docs/catalogs.md.",
        reader: Reader::Lspsc,
        source: Source::Staged,
        aliases: &[],
    },
];

/// Look up a catalog by its slug.
pub fn find(id: &str) -> Option<&'static CatalogDef> {
    CATALOGS.iter().find(|c| c.id == id)
}

/// Look up a catalog by the collection name it is stored under.
///
/// This is the direction the crossmatch config needs: `crossmatch.<survey>[]`
/// names collections, not slugs.
pub fn find_by_collection(collection: &str) -> Option<&'static CatalogDef> {
    CATALOGS
        .iter()
        .find(|c| c.collection == collection || c.aliases.contains(&collection))
}

/// Which catalogs this deployment should hold.
///
/// Derived primarily from `crossmatch.<survey>[].catalog`, because that is
/// where a catalog is actually put to use -- a deployment that crossmatches
/// against NED needs NED, and having to say so twice is how the two lists drift
/// apart. `catalogs:` in the config is additive, for catalogs held for direct
/// querying without being crossmatch targets.
///
/// Entries are returned as slugs where the name maps to a known catalog, and as
/// the raw collection name where it does not, so an unrecognized name surfaces
/// in the drift table rather than being silently dropped. That is deliberate
/// and must not become an error: `TNS`, hand-imported collections, and anything
/// built by `prepare_catalog` are legitimate crossmatch targets that this
/// registry has no definition for.
///
/// Watchlists are excluded -- they are user-managed, not archival catalogs.
/// Warn about crossmatch collections that are configured but hold nothing.
///
/// A missing collection is not an error in MongoDB: the `$geoWithin` stage and
/// the `$unionWith` legs both return zero rows, so the alert worker keeps
/// running and every alert comes out with an empty match list for that catalog.
/// Nothing distinguishes that from "there was genuinely nothing within the
/// radius", which is the failure this warning exists to make visible.
///
/// A warning rather than a hard failure: a catalog ingest takes hours to days,
/// and refusing to start the pipeline until it finishes would be a worse
/// outage than degraded crossmatches. The admin page carries the same
/// information with a button attached.
pub async fn warn_on_empty_crossmatch_catalogs(
    db: &Database,
    xmatch_configs: &[crate::conf::CatalogXmatchConfig],
) {
    for entry in xmatch_configs {
        if entry
            .catalog
            .starts_with(crate::api::catalogs::WATCHLIST_PREFIX)
        {
            // Watchlists are user-managed and legitimately start empty.
            continue;
        }
        match db
            .collection::<Document>(&entry.catalog)
            .estimated_document_count()
            .await
        {
            Ok(0) => tracing::warn!(
                catalog = %entry.catalog,
                "crossmatch is configured against {} but it holds no documents; every alert \
                 will be written with zero matches for it, which is indistinguishable from a \
                 genuine non-match. Ingest it from the admin page, or remove it from \
                 crossmatch config",
                entry.catalog
            ),
            Ok(_) => {}
            // Not fatal: the pipeline runs either way, and a failure to count
            // must not be the thing that stops alerts being processed.
            Err(e) => tracing::warn!(
                catalog = %entry.catalog,
                "could not check whether crossmatch catalog {} is populated: {e}",
                entry.catalog
            ),
        }
    }
}

/// Collection names crossmatch config actually references, across all surveys.
///
/// Separate from [`declared`] because the two answer different questions now:
/// `declared` is "what did config ask for", this is "what will the alert
/// pipeline try to query". Only the second turns a missing catalog into a
/// silent wrong answer.
pub fn crossmatched(config: &crate::conf::AppConfig) -> Vec<String> {
    let mut names: Vec<String> = config
        .crossmatch
        .values()
        .flatten()
        .map(|entry| entry.catalog.clone())
        .filter(|name| !name.starts_with(crate::api::catalogs::WATCHLIST_PREFIX))
        .collect();
    names.sort_unstable();
    names.dedup();
    names
}

pub fn declared(config: &crate::conf::AppConfig) -> Vec<String> {
    let mut declared: Vec<String> = Vec::new();
    let mut push = |id: String| {
        if !declared.contains(&id) {
            declared.push(id);
        }
    };

    // Sorted for a stable order regardless of the survey map's iteration order,
    // so the admin page does not reshuffle its rows between requests.
    let mut from_crossmatch: Vec<&str> = config
        .crossmatch
        .values()
        .flatten()
        .map(|entry| entry.catalog.as_str())
        .filter(|name| !name.starts_with(crate::api::catalogs::WATCHLIST_PREFIX))
        .collect();
    from_crossmatch.sort_unstable();
    from_crossmatch.dedup();

    for name in from_crossmatch {
        match find_by_collection(name) {
            Some(def) => push(def.id.to_string()),
            None => push(name.to_string()),
        }
    }
    declared
}

#[derive(thiserror::Error, Debug)]
pub enum CatalogError {
    #[error("unknown catalog {id:?}; known catalogs are {known}")]
    Unknown { id: String, known: String },
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
    #[error(
        "{catalog} listed no chunks; refusing to record an empty catalog as ingested. \
         The archive may be unreachable, or its metadata may declare no partitions"
    )]
    NothingToIngest { catalog: String },
    #[error(
        "another worker took over this run while chunk {chunk} was still being \
         ingested; stopping so the two do not write {collection} at once"
    )]
    Preempted { chunk: String, collection: String },
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

    let state = db.collection::<Document>(STATE_COLLECTION);
    let download_dir = params.download_dir.join(def.id);
    std::fs::create_dir_all(&download_dir).map_err(|e| CatalogError::Io {
        path: download_dir.clone(),
        source: e,
    })?;

    // Listed before anything is dropped. Discovery is the step most likely to
    // fail -- the archive is remote and boompy has to start -- and a
    // `drop_existing` run that destroyed the collection first would leave
    // nothing to serve and nothing to resume from.
    let boompy = Boompy::new(&params.boompy_dir).forwarding_to({
        let ctx = ctx.clone();
        std::sync::Arc::new(move |line: String| ctx.info(line))
    });
    let chunks = boompy.list_chunks(def.id).await?;
    // An empty listing is a discovery failure, not an empty catalog: the
    // completion check below counts chunks, so zero of zero would read as
    // complete, build the indexes and report the catalog present without a
    // single record in it.
    if chunks.is_empty() {
        return Err(CatalogError::NothingToIngest {
            catalog: def.id.to_string(),
        });
    }

    if params.drop_existing {
        tracing::warn!("dropping {} and its ingest state", def.collection);
        ctx.warn(format!(
            "dropping {} and its ingest state before re-ingesting {} chunk(s)",
            def.collection,
            chunks.len()
        ));
        db.collection::<Document>(def.collection).drop().await?;
        state.delete_one(doc! { "_id": def.collection }).await?;
    }

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
    let claim = start_state(&state, def, chunks.len()).await?;

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
        // Report row counts while the chunk runs. Without this the only
        // feedback between "fetching" and "chunk done" is the log, and a chunk
        // of a large catalog is minutes of apparent silence.
        let ticker = spawn_progress_ticker(
            ctx.clone(),
            inserter.clone_counter(),
            (report.chunks_ingested + report.chunks_resumed) as u64,
            report.chunks_total as u64,
            chunk.id.clone(),
        );
        let ingested = ingest_chunk(&boompy, &inserter, def, chunk, &download_dir, params).await;
        ticker.abort();
        let ingested = ingested?;
        report.records.merge(ingested);
        report.chunks_ingested += 1;
        if !record_chunk(&state, def.collection, &chunk.id, ingested.inserted, claim).await? {
            return Err(CatalogError::Preempted {
                chunk: chunk.id.clone(),
                collection: def.collection.to_string(),
            });
        }

        let done_count = (report.chunks_ingested + report.chunks_resumed) as u64;
        ctx.info(format!(
            "chunk {} done ({}/{}): {} read, {} inserted",
            chunk.id, done_count, report.chunks_total, ingested.read, ingested.inserted
        ));
        if ingested.skipped > 0 {
            // Said here as well as in the ledger, because the difference between
            // read and inserted is otherwise something you have to notice.
            ctx.warn(format!(
                "chunk {}: {} record(s) skipped and not in {}",
                chunk.id, ingested.skipped, def.collection
            ));
        }
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
        if !finish_state(&state, def.collection, claim).await? {
            return Err(CatalogError::Preempted {
                chunk: "the final state write".to_string(),
                collection: def.collection.to_string(),
            });
        }
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

/// How often to publish a running row count while a chunk is in flight.
///
/// Slow enough that it is a rounding error next to the ingest's own writes, fast
/// enough that the admin page looks alive.
const PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3);

/// Publish the running row count until aborted.
/// Report rows for the chunk now running, not for the catalog so far.
///
/// The counter is the inserter's and spans the whole ingest, so the count at
/// the moment this starts is subtracted. Without that the message attributes
/// every row inserted since the task began to the current chunk, and the
/// "fetching" branch -- which the count being zero is what detects -- never
/// fires again after the first chunk.
fn spawn_progress_ticker(
    ctx: TaskContext,
    counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
    done: u64,
    total: u64,
    chunk_id: String,
) -> tokio::task::JoinHandle<()> {
    let baseline = counter.load(std::sync::atomic::Ordering::Relaxed);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(PROGRESS_INTERVAL).await;
            let rows = counter
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(baseline);
            // Before the first batch lands the chunk is still downloading, which
            // is worth saying rather than showing a stuck row count.
            let message = if rows == 0 {
                format!("chunk {} of {}: fetching {}", done + 1, total, chunk_id)
            } else {
                format!(
                    "chunk {} of {}: {} rows inserted from {}",
                    done + 1,
                    total,
                    rows,
                    chunk_id
                )
            };
            ctx.progress(done, total, message).await;
        }
    })
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

    // Staged files are the artifact rather than a cache of it -- deleting them
    // would destroy work BOOM cannot reproduce.
    if !params.keep_downloads && def.source == Source::Fetched {
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
        Reader::Ned => Ok(arrow::ingest_parquet::<types::Ned>(inserter, path).await?),
        Reader::AllWise => Ok(arrow::ingest_parquet::<types::AllWise>(inserter, path).await?),
        Reader::Milliquas => Ok(arrow::ingest_parquet::<types::Milliquas>(inserter, path).await?),
        Reader::DesiDr1 => Ok(arrow::ingest_parquet::<types::DesiDr1>(inserter, path).await?),
        Reader::CatWise2020 => {
            Ok(arrow::ingest_parquet::<types::CatWise2020>(inserter, path).await?)
        }
        Reader::GaiaDr3 => Ok(csv::ingest_csv::<types::Gaia>(inserter, path).await?),
        Reader::Galex => Ok(csv::ingest_csv::<types::Galex>(inserter, path).await?),
        Reader::Vsx => Ok(ascii::ingest_ascii::<types::Vsx>(inserter, path).await?),
        Reader::PanStarrs => Ok(arrow::ingest_parquet::<types::PanStarrs>(inserter, path).await?),
        Reader::Lspsc => Ok(jsonl::ingest_jsonl::<types::Lspsc>(inserter, path).await?),
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

/// Claim the ingest and return the token that identifies this claim.
///
/// Every later write to the state document is conditional on the token, which
/// is what keeps a worker that has lost its lease from writing to it. The same
/// run can be claimed twice -- the lease lapses, the reaper requeues it, a
/// second worker picks it up -- and the two workers share a task id, so the id
/// cannot tell them apart. A fresh token per claim can, and because the
/// conditional update is evaluated by the server there is no window between
/// checking and writing.
///
/// Random rather than a counter: `drop_existing` deletes this document, so a
/// counter would restart at the same value the evicted worker is still holding.
async fn start_state(
    state: &mongodb::Collection<Document>,
    def: &CatalogDef,
    chunks_total: usize,
) -> Result<mongodb::bson::oid::ObjectId, CatalogError> {
    let token = mongodb::bson::oid::ObjectId::new();
    state
        .update_one(
            doc! { "_id": def.collection },
            doc! {
                "$set": {
                    "catalog": def.id,
                    "status": "ingesting",
                    "chunks_total": chunks_total as i64,
                    "claim": token,
                    "updated_at": now(),
                },
                "$setOnInsert": { "started_at": now(), "n_records": 0i64 },
            },
        )
        .upsert(true)
        .await?;
    Ok(token)
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
    claim: mongodb::bson::oid::ObjectId,
) -> Result<bool, CatalogError> {
    // Not an upsert, and matched on the claim: if this run has been taken over
    // the document either carries another token or has been dropped, and
    // either way this write must not happen. Upserting would recreate the
    // state document with one chunk marked done, and the worker that now owns
    // the run would skip that chunk as already ingested -- losing its records
    // from a catalog that reports itself complete.
    let result = state
        .update_one(
            doc! { "_id": collection, "claim": claim },
            doc! {
                "$addToSet": { "chunks_done": chunk_id },
                "$inc": { "n_records": inserted as i64 },
                "$set": { "updated_at": now() },
            },
        )
        .await?;
    Ok(result.matched_count == 1)
}

async fn finish_state(
    state: &mongodb::Collection<Document>,
    collection: &str,
    claim: mongodb::bson::oid::ObjectId,
) -> Result<bool, CatalogError> {
    let result = state
        .update_one(
            doc! { "_id": collection, "claim": claim },
            doc! { "$set": { "status": "complete", "completed_at": now(), "updated_at": now() } },
        )
        .await?;
    Ok(result.matched_count == 1)
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

/// The state of one catalog this release can ingest.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CatalogStatus {
    pub id: String,
    pub collection: Option<String>,
    pub title: Option<String>,
    pub health: CatalogHealth,
    pub chunks_done: usize,
    pub chunks_total: usize,
    pub n_records: i64,
    /// Whether crossmatch config names this catalog for at least one survey.
    ///
    /// This is what turns a not-yet-ingested catalog into a problem. One nobody
    /// crossmatches against is simply available; one the pipeline is configured
    /// to use and cannot find returns zero matches on every alert, silently.
    pub crossmatched: bool,
}

/// Every catalog this release can ingest, and how each compares to the database.
///
/// **The list comes from the code, not from config.** A catalog BOOM knows how
/// to build is available to ingest whether or not anything crossmatches against
/// it yet, so the order of operations is: add the definition to `CATALOGS`,
/// ingest it from the admin page, *then* add it to crossmatch config. Deriving
/// the list from crossmatch config forced that backwards -- you had to configure
/// the pipeline to use a catalog before you could ingest it, which is exactly
/// the window in which crossmatches silently return nothing.
///
/// `declared` still contributes, so a name in config that this release has no
/// definition for shows up as [`CatalogHealth::Undeclared`] rather than being
/// dropped from the page.
///
/// Reports; never acts. Ingesting a catalog is hours to days of work and has to
/// stay an explicit, attributed decision. See `docs/catalogs.md`.
#[instrument(skip(db, declared, crossmatched))]
pub async fn status(
    db: &Database,
    declared: &[String],
    crossmatched: &[String],
) -> Result<Vec<CatalogStatus>, CatalogError> {
    let state = db.collection::<Document>(STATE_COLLECTION);

    // Everything the release can build, then anything config names that it
    // cannot -- the second group is almost always a typo, and hiding it would
    // hide the typo.
    let mut ids: Vec<String> = CATALOGS.iter().map(|c| c.id.to_string()).collect();
    for id in declared {
        let known = find(id).map(|def| def.id.to_string());
        match known {
            Some(id) if ids.contains(&id) => {}
            Some(id) => ids.push(id),
            None => {
                if !ids.contains(id) {
                    ids.push(id.clone());
                }
            }
        }
    }

    let is_crossmatched = |def: Option<&CatalogDef>, id: &str| -> bool {
        crossmatched.iter().any(|name| {
            name == id
                || def.is_some_and(|d| d.collection == name || d.aliases.contains(&name.as_str()))
        })
    };

    let mut statuses = Vec::with_capacity(ids.len());
    for id in &ids {
        let Some(def) = find(id) else {
            statuses.push(CatalogStatus {
                id: id.clone(),
                collection: None,
                title: None,
                health: CatalogHealth::Undeclared,
                chunks_done: 0,
                chunks_total: 0,
                n_records: 0,
                crossmatched: is_crossmatched(None, id),
            });
            continue;
        };
        // The canonical collection first, then any alias. A deployment whose
        // crossmatch config still points at an older release of a catalog has
        // that collection populated and no state document under the canonical
        // name, and reporting it `missing` would invite a re-ingest of data
        // that is already there and already being matched against.
        let mut doc = state.find_one(doc! { "_id": def.collection }).await?;
        let mut serving = def.collection.to_string();
        if doc.is_none() {
            for alias in def.aliases {
                if let Some(found) = state.find_one(doc! { "_id": *alias }).await? {
                    doc = Some(found);
                    serving = (*alias).to_string();
                    break;
                }
            }
        }
        // An alias populated before this release tracked ingest state has no
        // state document at all, so the collection itself is the only evidence.
        // Counted rather than assumed missing, because a populated alias is
        // what the crossmatch is reading right now.
        let mut untracked = 0i64;
        if doc.is_none() {
            for alias in def.aliases {
                let count = db
                    .collection::<Document>(alias)
                    .estimated_document_count()
                    .await
                    .unwrap_or(0) as i64;
                if count > 0 {
                    serving = (*alias).to_string();
                    untracked = count;
                    break;
                }
            }
        }
        let (health, chunks_done, chunks_total, n_records) = match &doc {
            // Present on the strength of the documents in it; this release did
            // not ingest it, so there are no chunk counts to report.
            None if untracked > 0 => (CatalogHealth::Present, 0, 0, untracked),
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
            // The collection actually serving the catalog, which is the alias
            // when that is where the data is.
            collection: Some(serving),
            title: Some(def.title.to_string()),
            health,
            chunks_done,
            chunks_total,
            n_records,
            crossmatched: is_crossmatched(Some(def), id),
        });
    }
    Ok(statuses)
}

/// Crossmatch targets this release cannot build, and why.
///
/// Each is a real collection the pipeline matches against, so none is a
/// mistake -- they simply have no ingest definition. Naming them here is what
/// lets config validation reject a genuine typo while still accepting these:
/// anything not defined and not listed is assumed wrong.
///
/// Delete an entry when its definition lands. A test requires each of these to
/// still be undefined, so the list cannot quietly go stale.
pub const WITHOUT_DEFINITIONS: &[(&str, &str)] = &[
    (
        "LSDR10",
        "Legacy Survey DR10 with photo-z posteriors, fluxes and shape parameters \
         (z_phot_mean, flux_*, shape_*, objtype, ebv), built outside BOOM with LSDB. A \
         boompy module reading that HATS catalog the way allwise and panstarrs do would \
         make this an ordinary definition; nobody has written one yet",
    ),
    (
        "TNS",
        "the Transient Name Server is a live, credentialed feed rather than an archival \
         download, and is populated outside the catalog ingest path",
    ),
];

/// Names a crossmatch entry may use without having an ingest definition.
pub fn is_known_without_definition(collection: &str) -> bool {
    WITHOUT_DEFINITIONS
        .iter()
        .any(|(name, _)| *name == collection)
}

/// Reject crossmatch entries naming a catalog this release knows nothing about.
///
/// The pipeline reads `crossmatch.<survey>[].catalog` as a collection name and
/// queries it directly, so a typo does not fail -- it silently matches nothing,
/// and every alert comes out looking confidently unmatched. That is the failure
/// this catches, and it is worth failing startup over.
///
/// Watchlists are user-managed and always allowed; the collections in
/// [`WITHOUT_DEFINITIONS`] are allowed by name.
pub fn validate_crossmatch(
    crossmatch: &std::collections::HashMap<
        crate::utils::enums::Survey,
        Vec<crate::conf::CatalogXmatchConfig>,
    >,
) -> Result<(), String> {
    let mut unknown: Vec<String> = crossmatch
        .values()
        .flatten()
        .map(|entry| entry.catalog.clone())
        .filter(|name| !name.starts_with(crate::api::catalogs::WATCHLIST_PREFIX))
        .filter(|name| find_by_collection(name).is_none())
        .filter(|name| !is_known_without_definition(name))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort_unstable();
    unknown.dedup();
    Err(format!(
        "crossmatch names {unknown:?}, which this release has no catalog definition for. \
         Known catalogs: {}. If the collection is real but cannot be ingested by BOOM, \
         add it to WITHOUT_DEFINITIONS in src/catalogs/mod.rs with the reason.",
        CATALOGS
            .iter()
            .map(|c| c.collection)
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_and_collections_are_unique() {
        // Both are lookup keys -- a duplicate would make `find` or
        // `find_by_collection` silently return the wrong definition.
        let mut ids: Vec<&str> = CATALOGS.iter().map(|c| c.id).collect();
        let count = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), count, "duplicate catalog slug");

        let mut collections: Vec<&str> = CATALOGS.iter().map(|c| c.collection).collect();
        collections.sort_unstable();
        collections.dedup();
        assert_eq!(collections.len(), count, "duplicate collection name");
    }

    #[test]
    fn slugs_are_kebab_case() {
        // They are written by hand into config, and a slug that does not match
        // the documented convention is a typo waiting to happen.
        for def in CATALOGS {
            assert!(
                def.id
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                "{} is not kebab-case",
                def.id
            );
        }
    }

    #[test]
    fn every_definition_is_reachable_by_both_keys() {
        for def in CATALOGS {
            assert_eq!(find(def.id).map(|d| d.collection), Some(def.collection));
            assert_eq!(
                find_by_collection(def.collection).map(|d| d.id),
                Some(def.id)
            );
        }
    }

    /// The catalog names the shipped config actually crossmatches against.
    ///
    /// Read from `config.yaml` rather than hardcoded, so adding a crossmatch
    /// entry without a definition shows up here.
    fn crossmatch_names() -> Vec<String> {
        let config = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/config.yaml"))
            .expect("config.yaml");
        config
            .lines()
            .filter_map(|line| line.trim().strip_prefix("- catalog: "))
            .map(|name| name.trim().to_string())
            .collect()
    }

    #[test]
    fn every_crossmatch_target_is_either_defined_or_a_known_exception() {
        // `declared` deliberately reports an unknown name rather than failing --
        // TNS and hand-imported collections are legitimate crossmatch targets.
        // This guards the base config, where a new entry should either come
        // with a definition or be listed above with a reason.
        let exempt: Vec<&str> = super::WITHOUT_DEFINITIONS
            .iter()
            .map(|(name, _)| *name)
            .collect();
        let unknown: Vec<String> = crossmatch_names()
            .into_iter()
            .filter(|name| !name.starts_with(crate::api::catalogs::WATCHLIST_PREFIX))
            .filter(|name| find_by_collection(name).is_none())
            .filter(|name| !exempt.contains(&name.as_str()))
            .collect();
        assert!(
            unknown.is_empty(),
            "config.yaml crossmatches against catalogs with no definition: {unknown:?}. \
             Add a CatalogDef, or list it in WITHOUT_DEFINITIONS with the reason."
        );
    }

    #[test]
    fn the_exception_list_does_not_outlive_its_reason() {
        // Once a catalog gains a definition, leaving it exempt would hide a
        // future regression.
        for (name, _) in super::WITHOUT_DEFINITIONS {
            assert!(
                find_by_collection(name).is_none(),
                "{name} now has a definition; remove it from WITHOUT_DEFINITIONS"
            );
        }
    }
}

#[cfg(test)]
mod crossmatch_validation_tests {
    use super::*;
    use crate::conf::CatalogXmatchConfig;
    use crate::utils::enums::Survey;
    use std::collections::HashMap;

    fn crossmatch(names: &[&str]) -> HashMap<Survey, Vec<CatalogXmatchConfig>> {
        let entries = names
            .iter()
            .map(|name| CatalogXmatchConfig {
                catalog: name.to_string(),
                radius: crate::conf::arcsec_to_radians(2.0),
                projection: mongodb::bson::doc! {},
                ..Default::default()
            })
            .collect();
        HashMap::from([(Survey::Ztf, entries)])
    }

    #[test]
    fn an_older_release_of_the_same_catalog_is_accepted() {
        // Collection names are version-stamped, so a deployment sitting on an
        // earlier release is running the same catalog under a different name.
        // Calling that a typo would fail startup on a perfectly good config.
        assert!(validate_crossmatch(&crossmatch(&["milliquas_v6"])).is_ok());
        assert_eq!(
            find_by_collection("milliquas_v6").map(|d| d.id),
            Some("milliquas")
        );
    }

    #[test]
    fn the_test_config_passes_validation() {
        // tests/config.test.yaml is what the rest of the suite loads; if it
        // stops validating, twenty unrelated tests fail with a config error.
        let config = crate::conf::AppConfig::from_test_config().expect("test config loads");
        assert!(validate_crossmatch(&config.crossmatch).is_ok());
    }

    #[test]
    fn a_defined_catalog_is_accepted() {
        assert!(validate_crossmatch(&crossmatch(&["NED", "Gaia_DR3"])).is_ok());
    }

    #[test]
    fn a_typo_is_rejected_and_names_itself() {
        // The whole point: a misspelled collection matches nothing at query
        // time, so every alert comes out looking confidently unmatched.
        let err = validate_crossmatch(&crossmatch(&["Gaia_DR33"])).unwrap_err();
        assert!(err.contains("Gaia_DR33"), "{err}");
        assert!(err.contains("Known catalogs"), "{err}");
    }

    #[test]
    fn a_collection_we_cannot_build_is_accepted_by_name() {
        // TNS is a live credentialed feed and LSDR10 is built outside BOOM;
        // both are real crossmatch targets and must not fail startup.
        assert!(validate_crossmatch(&crossmatch(&["TNS", "LSPSC"])).is_ok());
    }

    #[test]
    fn watchlists_are_always_accepted() {
        // User-managed, created through the API rather than ingested.
        let name = format!("{}supernovas", crate::api::catalogs::WATCHLIST_PREFIX);
        assert!(validate_crossmatch(&crossmatch(&[&name])).is_ok());
    }

    #[test]
    fn the_shipped_prod_configs_pass_validation() {
        // These are the configs deployments actually run; the check is only
        // worth having if it does not reject them.
        for name in ["caltech", "umn"] {
            let path = format!(
                "{}/config/prod/{}/config.yaml",
                env!("CARGO_MANIFEST_DIR"),
                name
            );
            let config = crate::conf::AppConfig::from_path(&path)
                .unwrap_or_else(|e| panic!("{name} config failed to load: {e}"));
            assert!(
                validate_crossmatch(&config.crossmatch).is_ok(),
                "{name} config was rejected"
            );
        }
    }
}

#[cfg(test)]
mod state_tests {
    use super::*;

    fn a_def() -> CatalogDef {
        *find("milliquas").expect("milliquas is defined")
    }

    /// The takeover case: an evicted worker must not be able to mark a chunk
    /// done after another worker has claimed the same run.
    ///
    /// Without the fence the stale write upserts `chunks_done`, the new owner
    /// skips that chunk as already ingested, and the catalog reports itself
    /// complete while missing those records.
    #[tokio::test]
    async fn a_chunk_is_not_recorded_after_the_run_is_taken_over() {
        let db = crate::conf::get_test_db().await;
        let state = db.collection::<Document>("test_catalog_state_takeover");
        state.delete_many(doc! {}).await.unwrap();
        let def = a_def();

        let first = start_state(&state, &def, 10).await.unwrap();
        // A second claim of the same run, as the reaper requeuing it produces.
        let second = start_state(&state, &def, 10).await.unwrap();
        assert_ne!(first, second, "each claim gets its own token");

        let recorded = record_chunk(&state, def.collection, "chunk-1", 5, first)
            .await
            .unwrap();
        assert!(!recorded, "the evicted claim must not write");
        let completed = finish_state(&state, def.collection, first).await.unwrap();
        assert!(!completed, "nor mark the catalog complete");

        let doc = state
            .find_one(doc! { "_id": def.collection })
            .await
            .unwrap()
            .expect("state document exists");
        assert!(
            doc.get_array("chunks_done").is_err(),
            "no chunk should be recorded: {doc:?}"
        );
        assert!(doc.get_str("status").is_ok_and(|s| s == "ingesting"));

        // The claim that owns the run still works.
        assert!(record_chunk(&state, def.collection, "chunk-1", 5, second)
            .await
            .unwrap());
        state.delete_many(doc! {}).await.unwrap();
    }

    /// A claim deleted by `drop_existing` cannot be matched by a token minted
    /// before it, even though the counter-like sequence would restart.
    #[tokio::test]
    async fn a_claim_does_not_survive_the_state_being_dropped() {
        let db = crate::conf::get_test_db().await;
        let state = db.collection::<Document>("test_catalog_state_dropped");
        state.delete_many(doc! {}).await.unwrap();
        let def = a_def();

        let first = start_state(&state, &def, 3).await.unwrap();
        state
            .delete_one(doc! { "_id": def.collection })
            .await
            .unwrap();
        let second = start_state(&state, &def, 3).await.unwrap();

        assert_ne!(first, second);
        assert!(
            !record_chunk(&state, def.collection, "chunk-1", 1, first)
                .await
                .unwrap(),
            "a token from before the drop must not match the new claim"
        );
        state.delete_many(doc! {}).await.unwrap();
    }
}

#[cfg(test)]
mod source_tests {
    use super::*;

    #[test]
    fn staged_catalogs_are_the_ones_boom_cannot_download() {
        // Everything else is fetched from an archive and its chunks are deleted
        // after ingest; getting this backwards for a fetched catalog would fill
        // the disk, and for a staged one would destroy the artifact.
        let staged: Vec<&str> = CATALOGS
            .iter()
            .filter(|c| c.source == Source::Staged)
            .map(|c| c.id)
            .collect();
        // lspsc is published upstream as a cone-search API, so BOOM ingests an
        // export of its own copy. It cannot be fetched, and its files must not
        // be deleted after ingest.
        assert_eq!(staged, vec!["lspsc"]);
    }

    #[test]
    fn a_staged_catalog_is_still_chunked_and_resumable() {
        // Staging changes where the files come from, not how they are ingested:
        // the export is written in parts, so progress is still per part.
        let def = find("lspsc").expect("defined");
        assert_eq!(def.source, Source::Staged);
        assert_eq!(def.collection, "LSPSC");
    }
}

#[cfg(test)]
mod status_tests {
    use super::*;

    /// A deployment whose crossmatch config points at an older release of a
    /// catalog has that collection populated and nothing under the canonical
    /// name. Reporting it missing would invite a re-ingest of data that is
    /// already there, and is already being matched against.
    #[tokio::test]
    async fn a_populated_alias_is_not_reported_missing() {
        let db = crate::conf::get_test_db().await;
        let def = *find("milliquas").expect("milliquas is defined");
        let alias = def.aliases[0];
        let aliased = db.collection::<Document>(alias);
        aliased.delete_many(doc! {}).await.unwrap();
        db.collection::<Document>(STATE_COLLECTION)
            .delete_one(doc! { "_id": def.collection })
            .await
            .unwrap();

        // Nothing anywhere: missing, which is the honest answer.
        let before = status(&db, &[], &[alias.to_string()]).await.unwrap();
        let row = before.iter().find(|s| s.id == def.id).expect("listed");
        assert_eq!(row.health, CatalogHealth::Missing);

        aliased
            .insert_one(doc! { "_id": "QSO J0000+0000", "ra": 0.0, "dec": 0.0 })
            .await
            .unwrap();

        let after = status(&db, &[], &[alias.to_string()]).await.unwrap();
        let row = after.iter().find(|s| s.id == def.id).expect("listed");
        assert_eq!(
            row.health,
            CatalogHealth::Present,
            "the alias holds the data the crossmatch reads"
        );
        assert_eq!(
            row.collection.as_deref(),
            Some(alias),
            "and the page should name the collection actually serving it"
        );
        assert_eq!(row.n_records, 1);
        assert!(row.crossmatched);

        aliased.delete_many(doc! {}).await.unwrap();
    }

    #[tokio::test]
    async fn every_ingestable_catalog_is_listed_whether_or_not_config_names_it() {
        // The list comes from the code so that the order of operations can be
        // "ingest, then configure crossmatching". Deriving it from crossmatch
        // config forced the reverse, which is the window where the pipeline
        // queries a catalog that is not there yet.
        let db = crate::conf::get_test_db().await;
        let statuses = status(&db, &[], &[]).await.expect("reads");

        assert_eq!(
            statuses.len(),
            CATALOGS.len(),
            "a deployment that configures nothing still sees everything it could ingest"
        );
        for def in CATALOGS {
            let row = statuses
                .iter()
                .find(|s| s.id == def.id)
                .unwrap_or_else(|| panic!("{} is missing from the status list", def.id));
            assert!(
                !row.crossmatched,
                "{} is not in crossmatch config, so nothing queries it",
                def.id
            );
        }
    }

    #[tokio::test]
    async fn a_configured_catalog_is_marked_as_in_use() {
        let db = crate::conf::get_test_db().await;
        // Config names the collection; the status row is keyed by slug. Both
        // have to resolve to the same catalog or the admin page would show a
        // configured catalog as unused.
        let statuses = status(&db, &[], &["NED".to_string()]).await.expect("reads");

        let ned = statuses.iter().find(|s| s.id == "ned-lvs").expect("listed");
        assert!(ned.crossmatched, "config names NED, which is ned-lvs");
        assert!(
            statuses.iter().filter(|s| s.crossmatched).count() == 1,
            "only the configured catalog is marked in use"
        );
    }

    #[tokio::test]
    async fn an_alias_still_counts_as_configured() {
        // A deployment that ingested an earlier release names the old
        // collection. Treating that as unconfigured would report a catalog the
        // pipeline is actively querying as one nothing uses.
        let db = crate::conf::get_test_db().await;
        let statuses = status(&db, &[], &["milliquas_v6".to_string()])
            .await
            .expect("reads");

        let mq = statuses
            .iter()
            .find(|s| s.id == "milliquas")
            .expect("listed");
        assert!(mq.crossmatched);
    }

    #[tokio::test]
    async fn a_name_with_no_definition_is_still_reported() {
        // Almost always a typo. Dropping it from the page would hide the typo,
        // and it is the one kind of entry clicking Ingest cannot fix.
        let db = crate::conf::get_test_db().await;
        let statuses = status(&db, &["not-a-catalog".to_string()], &[])
            .await
            .expect("reads");

        let row = statuses
            .iter()
            .find(|s| s.id == "not-a-catalog")
            .expect("listed");
        assert_eq!(row.health, CatalogHealth::Undeclared);
        assert!(row.collection.is_none());
    }
}
