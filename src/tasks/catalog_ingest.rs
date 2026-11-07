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
    #[error(
        "num_workers x batch_size is {requested} records in flight, more than the {max} \
         this worker will hold; lower either one"
    )]
    TooMuchInFlight { requested: usize, max: usize },
}

/// Caps on how much of the database one ingest may take.
///
/// Not a correctness limit -- higher values work -- but a client should not be
/// able to submit a run that starves the alert pipeline.
const MAX_WORKERS: usize = 16;
const MAX_BATCH_SIZE: usize = 100_000;
/// Records that may be in flight at once, across the channel and the batch
/// each worker is accumulating.
///
/// Both limits above are fine on their own and ruinous together: 16 workers at
/// a batch of 100,000, with a channel sized from the batch, is millions of
/// records resident. NED and CatWISE rows are wide, so that is gigabytes in a
/// worker that is also running the alert pipeline -- a request that passes
/// validation should not be able to OOM it.
const MAX_RECORDS_IN_FLIGHT: usize = 400_000;
/// The channel only has to keep the workers fed while the reader parses, so it
/// is capped outright rather than scaled with the batch.
const MAX_CHANNEL_RECORDS: usize = 50_000;

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
        let in_flight = self.num_workers.saturating_mul(self.batch_size);
        if in_flight > MAX_RECORDS_IN_FLIGHT {
            return Err(ParamsError::TooMuchInFlight {
                requested: in_flight,
                max: MAX_RECORDS_IN_FLIGHT,
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

    // Captured before `params` is moved into the ingest parameters; whether a
    // run replaced a catalog or resumed one is exactly the kind of thing the
    // ledger exists to answer later.
    let drop_existing = params.drop_existing;

    let ingest = AddCatalogParams {
        catalog: params.catalog,
        drop_existing: params.drop_existing,
        download_dir: PathBuf::from(download_dir),
        boompy_dir: PathBuf::from(boompy_dir),
        num_workers: params.num_workers,
        batch_size: params.batch_size,
        channel_capacity: (params.batch_size * 10).min(MAX_CHANNEL_RECORDS),
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

    // Recorded only for a run that finished the catalog. A partial ingest has
    // changed data too, but "what has been done to this collection" is only
    // answerable once the answer is stable -- a resumed run appends the entry
    // that covers the whole catalog.
    if report.complete {
        ctx.record_mutation(
            super::ledger::MutationTarget {
                database: ctx.db().name().to_string(),
                collection: report.collection.clone(),
                catalog: Some(report.catalog.clone()),
                survey: None,
            },
            super::ledger::Operation::Ingest,
            mongodb::bson::doc! {
                "chunks_total": report.chunks_total as i64,
                "chunks_ingested": report.chunks_ingested as i64,
                "chunks_resumed": report.chunks_resumed as i64,
                "records_read": report.records.read as i64,
                "records_inserted": report.records.inserted as i64,
                "records_skipped": report.records.skipped as i64,
                "drop_existing": drop_existing,
                "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                    .unwrap_or(mongodb::bson::Bson::Null),
            },
        )
        .await;
    }

    serde_json::to_value(&report).map_err(|e| super::TaskError::Failed(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> CatalogIngestParams {
        CatalogIngestParams {
            catalog: "2mass".to_string(),
            drop_existing: false,
            max_chunks: None,
            num_workers: default_num_workers(),
            batch_size: default_batch_size(),
        }
    }

    #[test]
    fn the_defaults_are_accepted() {
        // The form submits these, so a default that fails validation would make
        // the button unusable.
        assert!(params().validate().is_ok());
        assert!(
            default_num_workers() * default_batch_size() <= MAX_RECORDS_IN_FLIGHT,
            "the defaults have to sit inside the in-flight bound"
        );
    }

    /// The two caps are each fine alone and ruinous together, which is the
    /// combination a client can ask for while passing both.
    #[test]
    fn the_worst_legal_combination_is_rejected() {
        let mut p = params();
        p.num_workers = MAX_WORKERS;
        p.batch_size = MAX_BATCH_SIZE;
        // Each passes its own cap.
        assert!(p.num_workers <= MAX_WORKERS && p.batch_size <= MAX_BATCH_SIZE);
        assert!(matches!(
            p.validate(),
            Err(ParamsError::TooMuchInFlight { .. })
        ));
    }

    #[test]
    fn the_channel_does_not_grow_with_the_batch() {
        // Sized from the batch it would be 10x a batch of 100,000, which is
        // the allocation the bound above exists to prevent.
        assert_eq!(
            (MAX_BATCH_SIZE * 10).min(MAX_CHANNEL_RECORDS),
            MAX_CHANNEL_RECORDS
        );
    }

    #[test]
    fn degenerate_and_unknown_requests_are_refused() {
        let cases: Vec<(&str, Box<dyn Fn(&mut CatalogIngestParams)>)> = vec![
            (
                "no workers",
                Box::new(|p: &mut CatalogIngestParams| p.num_workers = 0),
            ),
            (
                "too many workers",
                Box::new(|p: &mut CatalogIngestParams| p.num_workers = MAX_WORKERS + 1),
            ),
            (
                "no batch",
                Box::new(|p: &mut CatalogIngestParams| p.batch_size = 0),
            ),
            (
                "batch too large",
                Box::new(|p: &mut CatalogIngestParams| p.batch_size = MAX_BATCH_SIZE + 1),
            ),
            (
                "zero chunks",
                Box::new(|p: &mut CatalogIngestParams| p.max_chunks = Some(0)),
            ),
            (
                "unknown catalog",
                Box::new(|p: &mut CatalogIngestParams| p.catalog = "no-such-catalog".into()),
            ),
        ];
        for (name, apply) in cases {
            let mut p = params();
            apply(&mut p);
            assert!(p.validate().is_err(), "{name} should be refused at submit");
        }
    }
}
