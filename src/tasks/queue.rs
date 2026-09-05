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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tasks::models::{Actor, Progress, Trigger};

    /// A queued run, with a unique id so concurrent tests cannot collide.
    fn queued(task_type: &str, params: serde_json::Value) -> TaskRun {
        TaskRun {
            id: uuid::Uuid::new_v4().to_string(),
            task_type: task_type.to_string(),
            params,
            status: TaskStatus::Queued,
            actor: Actor {
                user_id: "test".into(),
                username: "test".into(),
            },
            trigger: Trigger::Api,
            requested_at: now(),
            started_at: None,
            finished_at: None,
            progress: Progress::default(),
            worker: None,
            lease_expires_at: None,
            cancel_requested: false,
            error: None,
            attempts: 0,
        }
    }

    /// Each test uses its own task type, so a test only ever claims its own
    /// runs however many run in parallel against the shared test database.
    fn unique_type() -> String {
        format!("test_{}", uuid::Uuid::new_v4().simple())
    }

    /// Serializes the tests that call `claim_next`.
    ///
    /// `claim_next` takes the oldest queued run in the database, whatever it is
    /// -- that is the behavior under test, not an accident. Two such tests
    /// running at once therefore claim each other's runs and both fail. The
    /// lock is only held by tests that claim; the rest still run in parallel.
    static CLAIM_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    /// Claim until we get a run of our own task type.
    ///
    /// Even holding [`CLAIM_LOCK`], the database can hold queued runs left by
    /// tests that never claim, so this still filters for its own. Foreign runs
    /// are parked (left claimed) and handed back afterwards, so each iteration
    /// makes progress instead of re-claiming the same run forever.
    async fn claim_ours(db: &Database, worker: &str, task_type: &str) -> Option<TaskRun> {
        let mut parked: Vec<String> = Vec::new();
        let mut ours = None;
        while let Some(run) = claim_next(db, worker).await.unwrap() {
            if run.task_type == task_type {
                ours = Some(run);
                break;
            }
            parked.push(run.id);
        }
        for id in parked {
            release(db, &id, worker).await.unwrap();
        }
        ours
    }

    async fn cleanup(db: &Database, task_type: &str) {
        let _ = db
            .collection::<TaskRun>(RUNS_COLLECTION)
            .delete_many(doc! { "task_type": task_type })
            .await;
    }

    #[tokio::test]
    async fn claiming_moves_a_run_to_running_and_takes_a_lease() {
        let _claiming = CLAIM_LOCK.lock().await;
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        let run = queued(&task_type, serde_json::json!({}));
        submit(&db, &run).await.unwrap();

        let claimed = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");
        assert_eq!(claimed.status, TaskStatus::Running);
        assert_eq!(claimed.worker.as_deref(), Some("worker-a"));
        assert!(claimed.lease_expires_at.unwrap() > now());
        // The attempt counter is what tells an operator a run was resumed.
        assert_eq!(claimed.attempts, 1);
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn only_one_worker_can_claim_the_same_run() {
        let _claiming = CLAIM_LOCK.lock().await;
        // The guard that keeps two workers from ingesting one catalog at once.
        // Both updates match the document; only the first sees `queued`.
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        submit(&db, &queued(&task_type, serde_json::json!({})))
            .await
            .unwrap();

        let ours = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");
        // A second worker cannot take the same run: both updates match the
        // document, but only the first sees `status: queued`.
        let stolen = claim_ours(&db, "worker-b", &task_type).await;
        assert!(stolen.is_none(), "a second worker claimed the same run");
        assert_eq!(
            get(&db, &ours.id).await.unwrap().unwrap().worker.as_deref(),
            Some("worker-a")
        );
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn the_oldest_run_is_claimed_first() {
        let _claiming = CLAIM_LOCK.lock().await;
        // Newest-first would let a long queue starve a run indefinitely, and
        // these runs are hours long.
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        let mut older = queued(&task_type, serde_json::json!({ "n": 1 }));
        older.requested_at = now() - 600.0;
        let mut newer = queued(&task_type, serde_json::json!({ "n": 2 }));
        newer.requested_at = now() - 60.0;
        submit(&db, &newer).await.unwrap();
        submit(&db, &older).await.unwrap();

        let claimed = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");
        assert_eq!(claimed.id, older.id);
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn a_heartbeat_renews_the_lease_and_reports_cancellation() {
        let _claiming = CLAIM_LOCK.lock().await;
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        submit(&db, &queued(&task_type, serde_json::json!({})))
            .await
            .unwrap();
        let run = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        assert_eq!(
            heartbeat(&db, &run.id, "worker-a").await.unwrap(),
            Some(false)
        );
        request_cancel(&db, &run.id).await.unwrap();
        // This is how the flag reaches the running task.
        assert_eq!(
            heartbeat(&db, &run.id, "worker-a").await.unwrap(),
            Some(true)
        );
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn a_worker_that_lost_its_lease_is_told_to_stand_down() {
        let _claiming = CLAIM_LOCK.lock().await;
        // If another worker took the run, continuing would mean two workers
        // writing the same collection -- exactly what the lease prevents.
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        submit(&db, &queued(&task_type, serde_json::json!({})))
            .await
            .unwrap();
        let run = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        assert_eq!(heartbeat(&db, &run.id, "worker-b").await.unwrap(), None);
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn an_expired_lease_is_requeued_but_a_live_one_is_left_alone() {
        let _claiming = CLAIM_LOCK.lock().await;
        // This is what makes a run survive a worker being deployed over.
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        submit(&db, &queued(&task_type, serde_json::json!({})))
            .await
            .unwrap();
        let run = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        requeue_expired(&db).await.unwrap();
        assert_eq!(
            get(&db, &run.id).await.unwrap().unwrap().status,
            TaskStatus::Running,
            "a live lease must not be stolen"
        );

        db.collection::<TaskRun>(RUNS_COLLECTION)
            .update_one(
                doc! { "_id": &run.id },
                doc! { "$set": { "lease_expires_at": now() - 1.0 } },
            )
            .await
            .unwrap();
        requeue_expired(&db).await.unwrap();

        let reaped = get(&db, &run.id).await.unwrap().unwrap();
        assert_eq!(reaped.status, TaskStatus::Queued);
        assert!(reaped.worker.is_none());
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn a_queued_run_is_canceled_outright() {
        // Nothing has started, so there is no safe point to wait for.
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        let run = queued(&task_type, serde_json::json!({}));
        submit(&db, &run).await.unwrap();

        assert_eq!(
            request_cancel(&db, &run.id).await.unwrap(),
            Some(TaskStatus::Canceled)
        );
        assert_eq!(
            get(&db, &run.id).await.unwrap().unwrap().status,
            TaskStatus::Canceled
        );
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn canceling_an_unknown_run_is_reported_rather_than_invented() {
        let db = crate::conf::get_test_db().await;
        assert_eq!(request_cancel(&db, "no-such-run").await.unwrap(), None);
    }

    #[tokio::test]
    async fn releasing_hands_a_run_back_without_failing_it() {
        let _claiming = CLAIM_LOCK.lock().await;
        // A deploy is not an outcome: the replacement worker should resume it
        // rather than someone having to resubmit by hand.
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        submit(&db, &queued(&task_type, serde_json::json!({})))
            .await
            .unwrap();
        let run = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        release(&db, &run.id, "worker-a").await.unwrap();
        let back = get(&db, &run.id).await.unwrap().unwrap();
        assert_eq!(back.status, TaskStatus::Queued);
        assert!(back.lease_expires_at.is_none());
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn finishing_clears_the_lease_so_the_reaper_ignores_it() {
        let _claiming = CLAIM_LOCK.lock().await;
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        submit(&db, &queued(&task_type, serde_json::json!({})))
            .await
            .unwrap();
        let run = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        finish(&db, &run.id, TaskStatus::Failed, Some("boom".into()))
            .await
            .unwrap();
        let done = get(&db, &run.id).await.unwrap().unwrap();
        assert_eq!(done.status, TaskStatus::Failed);
        assert_eq!(done.error.as_deref(), Some("boom"));
        assert!(done.lease_expires_at.is_none());
        assert!(done.finished_at.is_some());

        // A terminal run is never claimed again.
        requeue_expired(&db).await.unwrap();
        assert_eq!(
            get(&db, &run.id).await.unwrap().unwrap().status,
            TaskStatus::Failed
        );
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn single_flight_matches_on_params_not_just_task_type() {
        // Two ingests of the same catalog would race on one collection, but
        // ingesting 2MASS must not block ingesting NED.
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        submit(
            &db,
            &queued(&task_type, serde_json::json!({ "catalog": "2mass" })),
        )
        .await
        .unwrap();

        let same = find_active(&db, &task_type, doc! { "catalog": "2mass" })
            .await
            .unwrap();
        let other = find_active(&db, &task_type, doc! { "catalog": "ned-lvs" })
            .await
            .unwrap();
        assert!(same.is_some());
        assert!(other.is_none());
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn a_finished_run_no_longer_blocks_a_new_one() {
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        let run = queued(&task_type, serde_json::json!({ "catalog": "2mass" }));
        submit(&db, &run).await.unwrap();
        finish(&db, &run.id, TaskStatus::Succeeded, None)
            .await
            .unwrap();

        assert!(find_active(&db, &task_type, doc! { "catalog": "2mass" })
            .await
            .unwrap()
            .is_none());
        cleanup(&db, &task_type).await;
    }
}
