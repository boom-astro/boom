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

pub mod context;
pub mod ledger;
pub mod logs;
pub mod models;
pub mod queue;
pub mod redact;

pub use context::TaskContext;
pub use models::{Actor, TaskRun, TaskStatus, Trigger};

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
    /// JSON Schema for this task's parameters, for a client to render a form
    /// from.
    ///
    /// Derived from the params struct's `ToSchema`, so it cannot drift from
    /// what the API will actually accept, and the field descriptions are the
    /// doc comments already written on each field.
    pub params_schema: fn() -> serde_json::Value,
}

// TODO: recurring runs, for periodic maintenance such as the LSST cutout
// retention policy (#518). The run document is already ready for them --
// `Trigger::Schedule` and `Actor::system()` exist so a scheduled run is
// distinguishable from one a person asked for, and lease, heartbeat, cancel,
// logs and the ledger are all keyed off the run rather than off what triggered
// it. What is missing is where a schedule is declared and the loop that fires
// it.
//
// Two things to get right, neither of which the current code handles:
//
// 1. **Firing exactly once per tick across a fleet.** Every task-worker wakes
//    at the same cron instant, and `single_flight_key` will not save us: it is
//    a check-then-insert in the API handler (`api::routes::tasks::submit`), not
//    an invariant of `queue::submit`, so a scheduler enqueuing directly bypasses
//    it and two schedulers racing would both pass the check anyway. The fix
//    needs no leader election -- give a scheduled run a deterministic id such
//    as `sched:{schedule}:{unix_fire_time}` and let the `_id` uniqueness Mongo
//    already enforces settle it. One worker inserts, the rest get a duplicate
//    key and move on.
//
// 2. **Missed ticks.** If the fleet was down over a fire time, maintenance work
//    wants skip-to-next rather than a backfilled run per missed tick: the work
//    is cumulative, so one run catches up on all of it.
//
// Note that #518 is an *offload* to S3, not a delete -- Babamul is meant to
// read the archived cutouts back. A TTL index would destroy exactly the data
// the issue wants kept, so this does need a task body: a chunked, resumable
// copy-then-delete of the same shape as `catalog_ingest`, with `CutoutStorage`
// and `copy_cutouts` already covering most of the moving part.

/// Every task type this release knows how to run.
pub const TASKS: &[TaskSpec] = &[];

pub fn find(id: &str) -> Option<&'static TaskSpec> {
    TASKS.iter().find(|t| t.id == id)
}

/// Whether a run of this type may be retried automatically.
///
/// An unknown type is treated as **not** idempotent. A run can outlive the
/// release that created it -- a task type removed or renamed in a later version
/// still has rows in `task_runs` -- and re-running something this build cannot
/// even describe is exactly the case to be conservative about.
pub fn is_retryable(task_type: &str) -> bool {
    find(task_type).is_some_and(|spec| spec.idempotent)
}

/// Task types safe to requeue after a lost lease.
pub fn retryable_task_types() -> Vec<&'static str> {
    TASKS
        .iter()
        .filter(|spec| spec.idempotent)
        .map(|spec| spec.id)
        .collect()
}

fn known_types() -> String {
    TASKS.iter().map(|t| t.id).collect::<Vec<_>>().join(", ")
}

/// Check parameters against the task type, without running anything.
///
/// Called by the API at submit time so a malformed request is a 400 rather than
/// a run that fails minutes later on a worker.
pub fn validate_params(task_type: &str, _params: &serde_json::Value) -> Result<(), TaskError> {
    Err(TaskError::UnknownType {
        id: task_type.to_string(),
        known: known_types(),
    })
}

/// Params that must not be concurrently active for a new run of this type.
///
/// Two ingests of the same catalog would race on the same collection and the
/// same chunk state, so submission is single-flight per catalog rather than per
/// task type -- ingesting 2MASS should not block ingesting NED.
pub fn single_flight_key(
    _task_type: &str,
    _params: &serde_json::Value,
) -> Option<mongodb::bson::Document> {
    None
}

/// Run a task body by type.
///
/// The one place a task type turns into work. Adding a task means a body, an
/// arm here, an arm in [`validate_params`], and an entry in [`TASKS`].
pub async fn dispatch(
    _ctx: &TaskContext,
    task_type: &str,
    _params: serde_json::Value,
) -> Result<serde_json::Value, TaskError> {
    Err(TaskError::UnknownType {
        id: task_type.to_string(),
        known: known_types(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_task_publishes_a_schema_a_form_can_be_built_from() {
        // The admin page renders its submission form from this. A task whose
        // schema has no properties would appear in the list and then offer no
        // way to fill it in.
        for spec in TASKS {
            let schema = (spec.params_schema)();
            let properties = schema
                .get("properties")
                .and_then(|p| p.as_object())
                .unwrap_or_else(|| panic!("{} has no properties", spec.id));
            assert!(!properties.is_empty(), "{} has an empty schema", spec.id);
        }
    }

    #[test]
    fn every_registered_task_is_idempotent() {
        // Not a rule of the system -- the queue handles a non-idempotent task
        // by failing it rather than retrying -- but it is the property that
        // makes a task survive a deploy, which is most of the point. Adding one
        // without it is a deliberate choice, so make it a deliberate edit here.
        let not: Vec<&str> = TASKS
            .iter()
            .filter(|spec| !spec.idempotent)
            .map(|spec| spec.id)
            .collect();
        assert!(
            not.is_empty(),
            "these task types would be failed rather than resumed when a worker \
             goes away: {not:?}. If that is intended, update this test and say why."
        );
    }

    #[test]
    fn an_unknown_task_type_is_not_retryable() {
        // A run can outlive the release that registered its type.
        assert!(!is_retryable("a_type_from_some_future_release"));
    }

    #[test]
    fn task_ids_are_unique_and_stable_looking() {
        let mut ids: Vec<&str> = TASKS.iter().map(|t| t.id).collect();
        let count = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), count, "duplicate task id");
        for id in &ids {
            // Historical runs are read back by this string, so it wants to look
            // like an identifier rather than a sentence.
            assert!(
                id.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{id} is not a snake_case identifier"
            );
        }
    }

    #[test]
    fn every_registered_task_validates_and_dispatches() {
        // A type in TASKS with no arm in validate_params is a task the admin
        // page offers and the API rejects.
        for spec in TASKS {
            let err = validate_params(spec.id, &serde_json::json!({})).err();
            assert!(
                !matches!(err, Some(TaskError::UnknownType { .. })),
                "{} is registered but validate_params does not know it",
                spec.id
            );
        }
    }
}
