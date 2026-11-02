//! Claiming, leasing and finishing runs.
//!
//! MongoDB is the queue as well as the record: an atomic `find_one_and_update`
//! moves a run from `queued` to `running` and stamps a lease in one operation,
//! so there is no separate broker to keep consistent with the task document. At
//! a few runs a week that is the right trade -- see docs/task-system.md.

use super::models::{now, Task, TaskStatus, FAILED_LOG_RETENTION_DAYS, TASKS_COLLECTION};
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

fn collection(db: &Database) -> mongodb::Collection<Task> {
    db.collection::<Task>(TASKS_COLLECTION)
}

/// Put a run on the queue.
pub async fn submit(db: &Database, task: &Task) -> Result<(), QueueError> {
    collection(db).insert_one(task).await?;
    Ok(())
}

/// Claim the oldest queued run, if there is one.
///
/// The status guard in the filter is what makes this safe with more than one
/// worker: two workers racing on the same document both match, but only the
/// first update sees `status: "queued"`.
#[instrument(skip(db), err)]
pub async fn claim_next(db: &Database, worker: &str) -> Result<Option<Task>, QueueError> {
    let claimed = collection(db)
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
    task_id: &str,
    worker: &str,
) -> Result<Option<bool>, QueueError> {
    let updated = collection(db)
        .find_one_and_update(
            doc! { "_id": task_id, "worker": worker, "status": TaskStatus::Running.as_str() },
            doc! { "$set": { "lease_expires_at": now() + LEASE_SECONDS } },
        )
        .return_document(ReturnDocument::After)
        .await?;
    Ok(updated.map(|task| task.cancel_requested))
}

/// Record progress. Best-effort: a failed progress write must not fail the run.
pub async fn report_progress(
    db: &Database,
    task_id: &str,
    done: u64,
    total: u64,
    message: &str,
) -> Result<(), QueueError> {
    collection(db)
        .update_one(
            doc! { "_id": task_id },
            doc! { "$set": { "progress": { "done": done as i64, "total": total as i64, "message": message } } },
        )
        .await?;
    Ok(())
}

/// Mark a run finished without an ownership guard.
///
/// Only test setup may manufacture terminal runs this way. Production workers
/// must use [`finish_claimed`] so a stale worker cannot overwrite a reclaimed
/// run.
#[cfg(test)]
#[instrument(skip(db, error), fields(status = status.as_str()), err)]
pub async fn finish(
    db: &Database,
    task_id: &str,
    status: TaskStatus,
    error: Option<String>,
) -> Result<(), QueueError> {
    collection(db)
        .update_one(
            doc! { "_id": task_id },
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

/// Mark a run finished, but only if this worker still owns its live lease.
///
/// A task can notice a lost lease only at a safe point, which may be after a
/// replacement worker has claimed the run. The ownership guard prevents that
/// stale task from overwriting the replacement worker's status or clearing its
/// lease as it winds down.
pub async fn finish_claimed(
    db: &Database,
    task_id: &str,
    worker: &str,
    status: TaskStatus,
    error: Option<String>,
) -> Result<bool, QueueError> {
    let result = collection(db)
        .update_one(
            doc! {
                "_id": task_id,
                "worker": worker,
                "status": TaskStatus::Running.as_str(),
            },
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
    if matches!(status, TaskStatus::Failed | TaskStatus::Canceled) {
        // The outcome decides how long the logs are worth keeping, and this is
        // the first point that knows it.
        super::logs::extend_retention(db, task_id, FAILED_LOG_RETENTION_DAYS).await;
    }
    Ok(result.matched_count == 1)
}

/// Ask a running task to stop.
///
/// Sets a flag rather than killing anything: the task notices it at the next
/// chunk boundary and stops cleanly, leaving the chunks it finished recorded
/// so a later run resumes rather than starting over. A queued run is canceled
/// outright, since nothing has started.
pub async fn request_cancel(
    db: &Database,
    task_id: &str,
) -> Result<Option<TaskStatus>, QueueError> {
    // Each transition includes the observed status in its filter. A worker
    // can claim a queued run while this request is in flight, so a read then
    // unguarded write could otherwise mark an actively running task canceled.
    if collection(db)
        .update_one(
            doc! { "_id": task_id, "status": TaskStatus::Queued.as_str() },
            doc! {
                "$set": {
                    "status": TaskStatus::Canceled.as_str(),
                    "finished_at": now(),
                    "error": mongodb::bson::Bson::Null,
                    "lease_expires_at": mongodb::bson::Bson::Null,
                },
            },
        )
        .await?
        .matched_count
        == 1
    {
        return Ok(Some(TaskStatus::Canceled));
    }

    if collection(db)
        .update_one(
            doc! { "_id": task_id, "status": TaskStatus::Running.as_str() },
            doc! { "$set": { "cancel_requested": true } },
        )
        .await?
        .matched_count
        == 1
    {
        return Ok(Some(TaskStatus::Running));
    }

    Ok(collection(db)
        .find_one(doc! { "_id": task_id })
        .await?
        .map(|task| task.status))
}

/// What a sweep of expired leases did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReapReport {
    /// Put back on the queue, to be picked up and resumed.
    pub requeued: u64,
    /// Failed instead of retried, because the task does not declare itself
    /// idempotent and re-running it could apply the change twice.
    pub failed: u64,
}

/// Deal with runs whose lease has expired.
///
/// This is what makes a task survive a deploy: the worker goes away mid-run,
/// its lease lapses, and the next worker picks the run back up.
///
/// Only for task types that declare themselves **idempotent**, though. Retrying
/// is safe exactly when re-running produces the same state -- a catalog ingest
/// skips the chunks it recorded, a recompute derives from untouched inputs. A
/// task without that property could be applied twice by a retry, so its run is
/// failed and left for a person to look at. Silently doing the work again is
/// the one outcome nobody could detect afterwards.
#[instrument(skip(db))]
pub async fn requeue_expired(db: &Database) -> Result<ReapReport, QueueError> {
    // Owned Strings: a `Vec<&str>` does not land in the filter as a BSON array
    // of strings, and the `$in` then matches nothing -- which would fail every
    // orphaned run instead of resuming the retryable ones.
    let retryable: Vec<String> = super::retryable_task_types()
        .into_iter()
        .map(str::to_string)
        .collect();
    let expired = |extra: Document| {
        let mut filter = doc! {
            "status": TaskStatus::Running.as_str(),
            "lease_expires_at": { "$lt": now() },
        };
        for (key, value) in extra {
            filter.insert(key, value);
        }
        filter
    };

    let requeued = collection(db)
        .update_many(
            expired(doc! { "task_type": { "$in": retryable.clone() } }),
            doc! {
                "$set": {
                    "status": TaskStatus::Queued.as_str(),
                    "worker": mongodb::bson::Bson::Null,
                    "lease_expires_at": mongodb::bson::Bson::Null,
                },
            },
        )
        .await?;

    // Read the ids before the update, because `update_many` does not return
    // them and these are the runs whose logs are worth keeping longest. A run
    // that slips out of the filter in between just keeps its logs longer than
    // it needed to, which is the harmless direction.
    let failing = expired(doc! { "task_type": { "$nin": retryable.clone() } });
    let failed_ids: Vec<String> = {
        use futures::TryStreamExt;
        // As `Document`, not `Task`: a projection of `_id` alone cannot
        // deserialize into the full type, and that failure would only show up
        // at runtime.
        collection(db)
            .clone_with_type::<Document>()
            .find(failing.clone())
            .projection(doc! { "_id": 1 })
            .await?
            .try_collect::<Vec<Document>>()
            .await?
            .iter()
            .filter_map(|d| d.get_str("_id").ok().map(str::to_string))
            .collect()
    };

    let failed = collection(db)
        .update_many(
            failing,
            doc! {
                "$set": {
                    "status": TaskStatus::Failed.as_str(),
                    "finished_at": now(),
                    "error": "the worker running this stopped renewing its lease, and this \
                              task type is not declared idempotent, so it was not retried \
                              automatically. Check what it had already done before running \
                              it again.",
                    "worker": mongodb::bson::Bson::Null,
                    "lease_expires_at": mongodb::bson::Bson::Null,
                },
            },
        )
        .await?;

    if failed.modified_count > 0 {
        // Same reasoning as finish_claimed: these runs died without recording
        // their own outcome, which makes their logs the most worth keeping.
        for id in &failed_ids {
            super::logs::extend_retention(db, id, FAILED_LOG_RETENTION_DAYS).await;
        }
    }

    if requeued.modified_count > 0 {
        tracing::warn!(
            "requeued {} run(s) whose worker stopped renewing its lease",
            requeued.modified_count
        );
    }
    if failed.modified_count > 0 {
        tracing::error!(
            "failed {} orphaned run(s) that are not safe to retry automatically",
            failed.modified_count
        );
    }
    Ok(ReapReport {
        requeued: requeued.modified_count,
        failed: failed.modified_count,
    })
}

/// Hand a run back without failing it, for a worker shutting down cleanly.
///
/// Distinct from letting the lease lapse only in that it is immediate: on a
/// deploy the replacement worker can pick the run up right away instead of
/// waiting out the lease.
///
/// The caller must only use this for a task that is safe to retry; see
/// [`super::is_retryable`].
pub async fn release(db: &Database, task_id: &str, worker: &str) -> Result<(), QueueError> {
    collection(db)
        .update_one(
            doc! { "_id": task_id, "worker": worker, "status": TaskStatus::Running.as_str() },
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

pub async fn get(db: &Database, task_id: &str) -> Result<Option<Task>, QueueError> {
    Ok(collection(db).find_one(doc! { "_id": task_id }).await?)
}

/// Most recent runs first, optionally filtered by task type.
pub async fn list(
    db: &Database,
    task_type: Option<&str>,
    limit: i64,
) -> Result<Vec<Task>, QueueError> {
    use futures::TryStreamExt;
    let filter: Document = match task_type {
        Some(t) => doc! { "task_type": t },
        None => doc! {},
    };
    let cursor = collection(db)
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
) -> Result<Option<Task>, QueueError> {
    let mut filter = doc! {
        "task_type": task_type,
        "status": { "$in": [TaskStatus::Queued.as_str(), TaskStatus::Running.as_str()] },
    };
    for (key, value) in params_match {
        filter.insert(format!("params.{key}"), value);
    }
    Ok(collection(db).find_one(filter).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tasks::models::{Actor, Progress, Trigger};

    /// A queued run, with a unique id so concurrent tests cannot collide.
    fn queued(task_type: &str, params: serde_json::Value) -> Task {
        Task {
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

    /// Serializes every test whose run has to stay queued.
    ///
    /// `claim_next` takes the oldest queued run in the database, whatever it is
    /// -- that is the behavior under test, not an accident. So the shared thing
    /// this guards is the pool of queued runs, not the claim call: a test that
    /// submits a run and then asserts anything about it being queued needs the
    /// lock too, or a concurrent claim elsewhere moves it to running first.
    /// Tests that only read, or that assert on queued-or-running, do not.
    static CLAIM_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    /// Claim until we get a run of our own task type.
    ///
    /// Even holding [`CLAIM_LOCK`], the database can hold queued runs left by
    /// tests that never claim, so this still filters for its own. Foreign runs
    /// are parked (left claimed) and handed back afterwards, so each iteration
    /// makes progress instead of re-claiming the same run forever.
    async fn claim_ours(db: &Database, worker: &str, task_type: &str) -> Option<Task> {
        let mut parked: Vec<String> = Vec::new();
        let mut ours = None;
        while let Some(task) = claim_next(db, worker).await.unwrap() {
            if task.task_type == task_type {
                ours = Some(task);
                break;
            }
            parked.push(task.id);
        }
        for id in parked {
            release(db, &id, worker).await.unwrap();
        }
        ours
    }

    async fn cleanup(db: &Database, task_type: &str) {
        let _ = db
            .collection::<Task>(TASKS_COLLECTION)
            .delete_many(doc! { "task_type": task_type })
            .await;
    }

    #[tokio::test]
    async fn claiming_moves_a_run_to_running_and_takes_a_lease() {
        let _claiming = CLAIM_LOCK.lock().await;
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        let task = queued(&task_type, serde_json::json!({}));
        submit(&db, &task).await.unwrap();

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
        let task = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        assert_eq!(
            heartbeat(&db, &task.id, "worker-a").await.unwrap(),
            Some(false)
        );
        request_cancel(&db, &task.id).await.unwrap();
        // This is how the flag reaches the running task.
        assert_eq!(
            heartbeat(&db, &task.id, "worker-a").await.unwrap(),
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
        let task = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        assert_eq!(heartbeat(&db, &task.id, "worker-b").await.unwrap(), None);
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn an_expired_lease_is_requeued_but_a_live_one_is_left_alone() {
        let _claiming = CLAIM_LOCK.lock().await;
        // This is what makes a run survive a worker being deployed over. It uses
        // a registered idempotent type, because only those are retried -- an
        // unknown one is failed instead, which
        // `an_orphaned_run_of_an_unknown_type_is_failed_rather_than_retried`
        // covers.
        let db = crate::conf::get_test_db().await;
        let task_type = crate::tasks::catalog_ingest::TASK_TYPE.to_string();
        submit(
            &db,
            &queued(&task_type, serde_json::json!({ "catalog": "test-only" })),
        )
        .await
        .unwrap();
        let task = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        requeue_expired(&db).await.unwrap();
        assert_eq!(
            get(&db, &task.id).await.unwrap().unwrap().status,
            TaskStatus::Running,
            "a live lease must not be stolen"
        );

        db.collection::<Task>(TASKS_COLLECTION)
            .update_one(
                doc! { "_id": &task.id },
                doc! { "$set": { "lease_expires_at": now() - 1.0 } },
            )
            .await
            .unwrap();
        let report = requeue_expired(&db).await.unwrap();
        assert!(report.requeued >= 1);

        let reaped = get(&db, &task.id).await.unwrap().unwrap();
        assert_eq!(reaped.status, TaskStatus::Queued);
        assert!(reaped.worker.is_none());
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn an_orphaned_run_of_an_unknown_type_is_failed_rather_than_retried() {
        // A run can outlive the release that created it. Re-running something
        // this build cannot even describe is the case to be conservative about,
        // so an unknown type is treated as not idempotent.
        let _claiming = CLAIM_LOCK.lock().await;
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type(); // never registered in TASKS
        submit(&db, &queued(&task_type, serde_json::json!({})))
            .await
            .unwrap();
        let task = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        db.collection::<Task>(TASKS_COLLECTION)
            .update_one(
                doc! { "_id": &task.id },
                doc! { "$set": { "lease_expires_at": now() - 1.0 } },
            )
            .await
            .unwrap();
        let report = requeue_expired(&db).await.unwrap();
        assert!(report.failed >= 1);

        let reaped = get(&db, &task.id).await.unwrap().unwrap();
        assert_eq!(reaped.status, TaskStatus::Failed);
        // The message has to say why it was not retried, or the next person
        // just resubmits it and applies the change twice by hand.
        assert!(
            reaped.error.unwrap().contains("not declared idempotent"),
            "the failure must explain itself"
        );
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn an_orphaned_idempotent_run_is_requeued() {
        // The other half of the rule: a task that declares itself safe to
        // re-run is picked back up, which is what survives a deploy.
        let _claiming = CLAIM_LOCK.lock().await;
        let db = crate::conf::get_test_db().await;
        assert!(
            crate::tasks::is_retryable(crate::tasks::catalog_ingest::TASK_TYPE),
            "catalog_ingest is the worked example of a resumable task"
        );

        let task = queued(
            crate::tasks::catalog_ingest::TASK_TYPE,
            serde_json::json!({ "catalog": "test-only" }),
        );
        submit(&db, &task).await.unwrap();
        let claimed = claim_ours(&db, "worker-a", crate::tasks::catalog_ingest::TASK_TYPE)
            .await
            .expect("claimed");

        db.collection::<Task>(TASKS_COLLECTION)
            .update_one(
                doc! { "_id": &claimed.id },
                doc! { "$set": { "lease_expires_at": now() - 1.0 } },
            )
            .await
            .unwrap();
        requeue_expired(&db).await.unwrap();

        assert_eq!(
            get(&db, &claimed.id).await.unwrap().unwrap().status,
            TaskStatus::Queued
        );
        let _ = db
            .collection::<Task>(TASKS_COLLECTION)
            .delete_one(doc! { "_id": &claimed.id })
            .await;
    }

    #[tokio::test]
    async fn a_queued_run_is_canceled_outright() {
        // Holds the lock because the run has to still be queued when
        // `request_cancel` sees it: claimed first, it takes the running path
        // and asks for cancellation instead of cancelling outright.
        let _claiming = CLAIM_LOCK.lock().await;
        // Nothing has started, so there is no safe point to wait for.
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        let task = queued(&task_type, serde_json::json!({}));
        submit(&db, &task).await.unwrap();

        assert_eq!(
            request_cancel(&db, &task.id).await.unwrap(),
            Some(TaskStatus::Canceled)
        );
        assert_eq!(
            get(&db, &task.id).await.unwrap().unwrap().status,
            TaskStatus::Canceled
        );
        cleanup(&db, &task_type).await;
    }

    #[tokio::test]
    async fn a_run_claimed_before_the_request_is_asked_to_stop() {
        let _claiming = CLAIM_LOCK.lock().await;
        // The window the queued path is guarded against: a worker can take a
        // run between someone asking to cancel it and the request being
        // served. Cancelling outright would mark a task canceled while it was
        // still writing, so it is asked to stop and notices at its next
        // boundary instead.
        let db = crate::conf::get_test_db().await;
        let task_type = unique_type();
        let task = queued(&task_type, serde_json::json!({}));
        submit(&db, &task).await.unwrap();
        claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        assert_eq!(
            request_cancel(&db, &task.id).await.unwrap(),
            Some(TaskStatus::Running)
        );
        let after = get(&db, &task.id).await.unwrap().unwrap();
        assert!(after.cancel_requested);
        assert_eq!(after.status, TaskStatus::Running);
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
        let task = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        release(&db, &task.id, "worker-a").await.unwrap();
        let back = get(&db, &task.id).await.unwrap().unwrap();
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
        let task = claim_ours(&db, "worker-a", &task_type)
            .await
            .expect("claimed");

        finish(&db, &task.id, TaskStatus::Failed, Some("boom".into()))
            .await
            .unwrap();
        let done = get(&db, &task.id).await.unwrap().unwrap();
        assert_eq!(done.status, TaskStatus::Failed);
        assert_eq!(done.error.as_deref(), Some("boom"));
        assert!(done.lease_expires_at.is_none());
        assert!(done.finished_at.is_some());

        // A terminal run is never claimed again.
        requeue_expired(&db).await.unwrap();
        assert_eq!(
            get(&db, &task.id).await.unwrap().unwrap().status,
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
        let task = queued(&task_type, serde_json::json!({ "catalog": "2mass" }));
        submit(&db, &task).await.unwrap();
        finish(&db, &task.id, TaskStatus::Succeeded, None)
            .await
            .unwrap();

        assert!(find_active(&db, &task_type, doc! { "catalog": "2mass" })
            .await
            .unwrap()
            .is_none());
        cleanup(&db, &task_type).await;
    }
}
