//! Claiming, leasing and finishing runs.
//!
//! MongoDB is the queue as well as the record: an atomic `find_one_and_update`
//! moves a run from `queued` to `running` and stamps a lease in one operation,
//! so there is no separate broker to keep consistent with the run document. At
//! a few runs a week that is the right trade -- see docs/task-system.md.

use super::models::{now, TaskRun, TaskStatus, RUNS_COLLECTION};
use mongodb::bson::{doc, to_bson, Document};
use mongodb::options::ReturnDocument;
use mongodb::Database;
use tracing::instrument;

/// How long a claim is good for without a heartbeat.
///
/// Long enough that a briefly stalled worker is not robbed of its run, short
/// enough that a killed one is picked up promptly. The heartbeat renews at a
/// third of this.
pub const LEASE_SECONDS: f64 = 60.0;

#[derive(thiserror::Error, Debug)]
pub enum QueueError {
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
    #[error("failed to serialize the run: {0}")]
    Serialize(#[from] mongodb::bson::ser::Error),
    #[error("failed to deserialize the run: {0}")]
    Deserialize(#[from] mongodb::bson::de::Error),
}

fn runs(db: &Database) -> mongodb::Collection<TaskRun> {
    db.collection::<TaskRun>(RUNS_COLLECTION)
}

/// Put a run on the queue.
pub async fn submit(db: &Database, run: &TaskRun) -> Result<(), QueueError> {
    runs(db).insert_one(run).await?;
    Ok(())
}

/// Claim the oldest queued run, if there is one.
///
/// The status guard in the filter is what makes this safe with more than one
/// worker: two workers racing on the same document both match, but only the
/// first update sees `status: "queued"`.
#[instrument(skip(db), err)]
pub async fn claim_next(db: &Database, worker: &str) -> Result<Option<TaskRun>, QueueError> {
    let claimed = runs(db)
        .find_one_and_update(
            doc! { "status": TaskStatus::Queued.as_str() },
            doc! {
                "$set": {
                    "status": TaskStatus::Running.as_str(),
                    "started_at": now(),
                    "worker": worker,
                    "lease_expires_at": now() + LEASE_SECONDS,
                },
                "$inc": { "attempts": 1 },
            },
        )
        // Oldest first: a queue that runs newest-first can starve a run
        // indefinitely, and these runs are hours long.
        .sort(doc! { "requested_at": 1 })
        .return_document(ReturnDocument::After)
        .await?;
    Ok(claimed)
}

/// Renew the lease, and report whether cancellation has been requested.
///
/// One round trip for both because the heartbeat is the only thing that has to
/// stay live while a task runs; making cancellation a second poll would double
/// the traffic for no benefit.
///
/// Returns `None` if the run is no longer ours -- the lease expired and another
/// worker took it -- which the caller treats as a cancellation, since two
/// workers ingesting the same catalog is exactly what the lease prevents.
pub async fn heartbeat(
    db: &Database,
    run_id: &str,
    worker: &str,
) -> Result<Option<bool>, QueueError> {
    let updated = runs(db)
        .find_one_and_update(
            doc! { "_id": run_id, "worker": worker, "status": TaskStatus::Running.as_str() },
            doc! { "$set": { "lease_expires_at": now() + LEASE_SECONDS } },
        )
        .return_document(ReturnDocument::After)
        .await?;
    Ok(updated.map(|run| run.cancel_requested))
}

/// Record progress. Best-effort: a failed progress write must not fail the run.
pub async fn report_progress(
    db: &Database,
    run_id: &str,
    done: u64,
    total: u64,
    message: &str,
) -> Result<(), QueueError> {
    runs(db)
        .update_one(
            doc! { "_id": run_id },
            doc! { "$set": { "progress": { "done": done as i64, "total": total as i64, "message": message } } },
        )
        .await?;
    Ok(())
}

/// Mark a run finished, clearing its lease so the reaper leaves it alone.
#[instrument(skip(db, error), fields(status = status.as_str()), err)]
pub async fn finish(
    db: &Database,
    run_id: &str,
    status: TaskStatus,
    error: Option<String>,
) -> Result<(), QueueError> {
    runs(db)
        .update_one(
            doc! { "_id": run_id },
            doc! {
                "$set": {
                    "status": status.as_str(),
                    "finished_at": now(),
                    "error": to_bson(&error)?,
                    "lease_expires_at": mongodb::bson::Bson::Null,
                },
            },
        )
        .await?;
    Ok(())
}

/// Ask a running task to stop.
///
/// Sets a flag rather than killing anything: the task notices it at the next
/// chunk boundary and stops cleanly, leaving the chunks it finished recorded
/// so a later run resumes rather than starting over. A queued run is canceled
/// outright, since nothing has started.
pub async fn request_cancel(db: &Database, run_id: &str) -> Result<Option<TaskStatus>, QueueError> {
    let Some(run) = runs(db).find_one(doc! { "_id": run_id }).await? else {
        return Ok(None);
    };
    match run.status {
        TaskStatus::Queued => {
            finish(db, run_id, TaskStatus::Canceled, None).await?;
            Ok(Some(TaskStatus::Canceled))
        }
        TaskStatus::Running => {
            runs(db)
                .update_one(
                    doc! { "_id": run_id },
                    doc! { "$set": { "cancel_requested": true } },
                )
                .await?;
            Ok(Some(TaskStatus::Running))
        }
        terminal => Ok(Some(terminal)),
    }
}

/// Requeue runs whose lease has expired.
///
/// This is what makes a task survive a deploy: the worker goes away mid-run,
/// its lease lapses, and the next worker to start picks the run back up. Tasks
/// are written to be resumable, so re-running one continues rather than
/// repeating -- a catalog ingest skips the chunks it already recorded.
#[instrument(skip(db))]
pub async fn requeue_expired(db: &Database) -> Result<u64, QueueError> {
    let result = runs(db)
        .update_many(
            doc! {
                "status": TaskStatus::Running.as_str(),
                "lease_expires_at": { "$lt": now() },
            },
            doc! {
                "$set": {
                    "status": TaskStatus::Queued.as_str(),
                    "worker": mongodb::bson::Bson::Null,
                    "lease_expires_at": mongodb::bson::Bson::Null,
                },
            },
        )
        .await?;
    if result.modified_count > 0 {
        tracing::warn!(
            "requeued {} run(s) whose worker stopped renewing its lease",
            result.modified_count
        );
    }
    Ok(result.modified_count)
}

/// Hand a run back without failing it, for a worker shutting down cleanly.
///
/// Distinct from letting the lease lapse only in that it is immediate: on a
/// deploy the replacement worker can pick the run up right away instead of
/// waiting out the lease.
pub async fn release(db: &Database, run_id: &str, worker: &str) -> Result<(), QueueError> {
    runs(db)
        .update_one(
            doc! { "_id": run_id, "worker": worker },
            doc! {
                "$set": {
                    "status": TaskStatus::Queued.as_str(),
                    "worker": mongodb::bson::Bson::Null,
                    "lease_expires_at": mongodb::bson::Bson::Null,
                },
            },
        )
        .await?;
    Ok(())
}

pub async fn get(db: &Database, run_id: &str) -> Result<Option<TaskRun>, QueueError> {
    Ok(runs(db).find_one(doc! { "_id": run_id }).await?)
}

/// Most recent runs first, optionally filtered by task type.
pub async fn list(
    db: &Database,
    task_type: Option<&str>,
    limit: i64,
) -> Result<Vec<TaskRun>, QueueError> {
    use futures::TryStreamExt;
    let filter: Document = match task_type {
        Some(t) => doc! { "task_type": t },
        None => doc! {},
    };
    let cursor = runs(db)
        .find(filter)
        .sort(doc! { "requested_at": -1 })
        .limit(limit)
        .await?;
    Ok(cursor.try_collect().await?)
}

/// Whether a run of this type is already queued or running.
///
/// Single-flight per task type and target: two concurrent ingests of the same
/// catalog would race on the same collection and on the same chunk state.
pub async fn find_active(
    db: &Database,
    task_type: &str,
    params_match: Document,
) -> Result<Option<TaskRun>, QueueError> {
    let mut filter = doc! {
        "task_type": task_type,
        "status": { "$in": [TaskStatus::Queued.as_str(), TaskStatus::Running.as_str()] },
    };
    for (key, value) in params_match {
        filter.insert(format!("params.{key}"), value);
    }
    Ok(runs(db).find_one(filter).await?)
}
