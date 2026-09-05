//! The async (typically long-running) task system used to do things like
//! reprocess alerts, download and insert new archival catalogs, etc.
//!
//! See [`docs/task-system.md`](../../docs/task-system.md).
//!
//! Anything that mutates BOOM's data outside the live alert pipeline runs here
//! rather than as a binary someone starts over SSH. That is not only about
//! access: these jobs run for hours to days, so they have to survive a deploy,
//! report their logs while running, and be cancellable, and they have to leave
//! a record of who ran what, when, and with which parameters.
//!
//! A run is submitted through the API, written to `task_runs` with
//! `status: queued`, and claimed by the task worker, which holds a lease on it
//! and renews it with a heartbeat. A run whose lease lapses -- because the
//! worker was deployed over or killed -- is requeued and picked up again. Task
//! bodies are therefore written to be **resumable**: re-running one continues
//! rather than repeating.

pub mod catalog_ingest;
pub mod context;
pub mod ledger;
pub mod logs;
pub mod models;
pub mod queue;

pub use context::TaskContext;
pub use models::{Actor, TaskRun, TaskStatus, Trigger};

use mongodb::bson::doc;
use serde::Deserialize;

#[derive(thiserror::Error, Debug)]
pub enum TaskError {
    #[error("{0}")]
    Failed(String),
    #[error("canceled")]
    Canceled,
    #[error("unknown task type {id:?}; known types are {known}")]
    UnknownType { id: String, known: String },
    #[error("invalid parameters: {0}")]
    InvalidParams(String),
}

/// A declared kind of work.
///
/// In code rather than in the database: a task type is a piece of the release,
/// and pinning the code version pins what the task does. See the "Concepts"
/// section of the design doc.
#[derive(Debug, Clone, Copy)]
pub struct TaskSpec {
    /// Stable identifier. Never changes -- historical runs are read back by it.
    pub id: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    /// Whether running it twice with the same parameters leaves the same state.
    /// Only an idempotent task can be safely requeued after a lost lease.
    pub idempotent: bool,
    /// Whether it can destroy data, and so needs the client to confirm.
    pub destructive: bool,
}

// TODO: port the remaining data-mutating binaries -- `enrich_reprocess`,
// `migrate_fp_flux`, `migrate_snr`, `reprocess_crossmatch`, `copy_cutouts`,
// `prepare_catalog` -- so that operators stop running them over SSH too. Each becomes a body plus an
// arm in `dispatch`; their existing Valkey work queues already give them the
// resumability a task needs, so what they mainly want is the params struct and
// a cancellation check in their batch loop.
//
// TODO: recurring runs. A scheduled task needs an enqueue loop that submits
// with `Trigger::Schedule` and `Actor::system()`, plus a cron expression on
// TaskSpec; `single_flight_key` already prevents a schedule from stacking runs
// up when one is still going. Wanted for periodic maintenance work such as
// trimming old LSST cutouts (#518) -- though for that specific case a TTL index
// on the cutout documents does the job without a task at all, and only the
// one-off backfill of the existing rows needs to run here.

/// Every task type this release knows how to run.
pub const TASKS: &[TaskSpec] = &[TaskSpec {
    id: catalog_ingest::TASK_TYPE,
    title: "Ingest an archival catalog",
    description: "Download an archival catalog and insert it into MongoDB, one chunk at a \
                  time. Resumable: re-running continues from the last completed chunk.",
    idempotent: true,
    // Only with drop_existing, which the client has to ask for explicitly.
    destructive: true,
}];

pub fn find(id: &str) -> Option<&'static TaskSpec> {
    TASKS.iter().find(|t| t.id == id)
}

fn known_types() -> String {
    TASKS.iter().map(|t| t.id).collect::<Vec<_>>().join(", ")
}

/// Check parameters against the task type, without running anything.
///
/// Called by the API at submit time so a malformed request is a 400 rather than
/// a run that fails minutes later on a worker.
pub fn validate_params(task_type: &str, params: &serde_json::Value) -> Result<(), TaskError> {
    match task_type {
        catalog_ingest::TASK_TYPE => {
            let parsed: catalog_ingest::CatalogIngestParams =
                serde_json::from_value(params.clone())
                    .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed
                .validate()
                .map(|_| ())
                .map_err(|e| TaskError::InvalidParams(e.to_string()))
        }
        other => Err(TaskError::UnknownType {
            id: other.to_string(),
            known: known_types(),
        }),
    }
}

/// Params that must not be concurrently active for a new run of this type.
///
/// Two ingests of the same catalog would race on the same collection and the
/// same chunk state, so submission is single-flight per catalog rather than per
/// task type -- ingesting 2MASS should not block ingesting NED.
pub fn single_flight_key(
    task_type: &str,
    params: &serde_json::Value,
) -> Option<mongodb::bson::Document> {
    match task_type {
        catalog_ingest::TASK_TYPE => params
            .get("catalog")
            .and_then(|c| c.as_str())
            .map(|catalog| doc! { "catalog": catalog }),
        _ => None,
    }
}

/// Run a task body by type.
///
/// The one place a task type turns into work. Adding a task means a body, an
/// arm here, an arm in [`validate_params`], and an entry in [`TASKS`].
pub async fn dispatch(
    ctx: &TaskContext,
    task_type: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, TaskError> {
    match task_type {
        catalog_ingest::TASK_TYPE => {
            let params = catalog_ingest::CatalogIngestParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            catalog_ingest::run(ctx, params).await
        }
        other => Err(TaskError::UnknownType {
            id: other.to_string(),
            known: known_types(),
        }),
    }
}
