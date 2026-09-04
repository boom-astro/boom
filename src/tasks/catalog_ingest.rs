//! The `catalog_ingest` task: download an archival catalog and insert it.
//!
//! A thin body over [`crate::catalogs::add_catalog`], which holds the actual
//! ingest. What lives here is the split between the parameters a client may
//! choose and the ones the deployment decides.

use super::context::TaskContext;
use crate::catalogs::{self, AddCatalogParams};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use utoipa::ToSchema;

/// Stable identifier for this task type. Historical runs are read back by it,
/// so it never changes.
pub const TASK_TYPE: &str = "catalog_ingest";

/// Where chunks are staged. A deployment setting, not a task parameter --
/// letting a client choose a path would make this an arbitrary-write primitive.
const DOWNLOAD_DIR_ENV: &str = "BOOM_CATALOG_DATA_PATH";
const BOOMPY_DIR_ENV: &str = "BOOM_BOOMPY_PATH";

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CatalogIngestParams {
    /// Catalog slug, e.g. `2mass`.
    pub catalog: String,
    /// Drop the collection and start over instead of resuming.
    #[serde(default)]
    pub drop_existing: bool,
    /// Stop after this many chunks, for smoke-testing a catalog end to end.
    #[serde(default)]
    pub max_chunks: Option<usize>,
    /// Concurrent insert workers. Lower this to leave more of the database to
    /// the alert pipeline: an ingest at full tilt competes with live alert
    /// writes for WiredTiger cache and write tickets, and it is the ingest --
    /// not the task queue -- that is heavy enough to notice.
    #[serde(default = "default_num_workers")]
    pub num_workers: usize,
    /// Documents per insert batch. Same trade as `num_workers`.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_num_workers() -> usize {
    4
}

fn default_batch_size() -> usize {
    10_000
}

#[derive(thiserror::Error, Debug)]
pub enum ParamsError {
    #[error("unknown catalog {id:?}; known catalogs are {known}")]
    UnknownCatalog { id: String, known: String },
    #[error("max_chunks must be greater than zero")]
    ZeroChunks,
    #[error("num_workers must be between 1 and {max}")]
    WorkerCount { max: usize },
    #[error("batch_size must be between 1 and {max}")]
    BatchSize { max: usize },
}

/// Caps on how much of the database one ingest may take.
///
/// Not a correctness limit -- higher values work -- but a client should not be
/// able to submit a run that starves the alert pipeline.
const MAX_WORKERS: usize = 16;
const MAX_BATCH_SIZE: usize = 100_000;

impl CatalogIngestParams {
    /// Reject what the worker would only fail on later.
    ///
    /// Validated at submit time so a bad catalog name comes back as a 400 the
    /// client can act on, rather than as a failed run someone has to go read
    /// the logs of.
    pub fn validate(&self) -> Result<&'static catalogs::CatalogDef, ParamsError> {
        if self.max_chunks == Some(0) {
            return Err(ParamsError::ZeroChunks);
        }
        if self.num_workers == 0 || self.num_workers > MAX_WORKERS {
            return Err(ParamsError::WorkerCount { max: MAX_WORKERS });
        }
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(ParamsError::BatchSize {
                max: MAX_BATCH_SIZE,
            });
        }
        catalogs::find(&self.catalog).ok_or_else(|| ParamsError::UnknownCatalog {
            id: self.catalog.clone(),
            known: catalogs::CATALOGS
                .iter()
                .map(|c| c.id)
                .collect::<Vec<_>>()
                .join(", "),
        })
    }
}

/// Run one catalog ingest.
pub async fn run(
    ctx: &TaskContext,
    params: CatalogIngestParams,
) -> Result<serde_json::Value, super::TaskError> {
    let download_dir = std::env::var(DOWNLOAD_DIR_ENV).unwrap_or_else(|_| "data/catalogs".into());
    let boompy_dir = std::env::var(BOOMPY_DIR_ENV).unwrap_or_else(|_| "boompy".into());

    let ingest = AddCatalogParams {
        catalog: params.catalog,
        drop_existing: params.drop_existing,
        download_dir: PathBuf::from(download_dir),
        boompy_dir: PathBuf::from(boompy_dir),
        num_workers: params.num_workers,
        batch_size: params.batch_size,
        channel_capacity: params.batch_size * 10,
        max_chunks: params.max_chunks,
        keep_downloads: false,
    };

    let report = catalogs::add_catalog(ctx, &ingest)
        .await
        .map_err(|e| super::TaskError::Failed(e.to_string()))?;

    // A canceled run is reported as canceled rather than as a success with a
    // partial result -- the difference matters when reading back what was done
    // to a collection.
    if report.canceled {
        return Err(super::TaskError::Canceled);
    }
    serde_json::to_value(&report).map_err(|e| super::TaskError::Failed(e.to_string()))
}
