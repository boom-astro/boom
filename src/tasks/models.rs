//! The stored shape of a task run.

use mongodb::bson::{doc, Document};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Runs, and the queue they are claimed from. Mongo is both, so there is no
/// second store to keep consistent with the run record.
pub const RUNS_COLLECTION: &str = "task_runs";
/// Log lines, chunked -- one document per flush rather than one per line.
pub const LOGS_COLLECTION: &str = "task_logs";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Submitted, waiting for a worker.
    Queued,
    /// Claimed by a worker holding a lease.
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Queued => "queued",
            TaskStatus::Running => "running",
            TaskStatus::Succeeded => "succeeded",
            TaskStatus::Failed => "failed",
            TaskStatus::Canceled => "canceled",
        }
    }

    /// Whether the run is over. A finished run is never claimed again.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled
        )
    }
}

/// Who asked for the run.
///
/// Recorded on every run because "what has been done to this database, by whom"
/// is the question the task system exists to answer -- see docs/task-system.md.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Actor {
    pub user_id: String,
    pub username: String,
}

impl Actor {
    /// The actor for a run nothing human asked for.
    ///
    /// Exists so that when recurring tasks land, a scheduled run is
    /// distinguishable from one a person submitted rather than being attributed
    /// to whoever happened to configure the schedule.
    pub fn system() -> Self {
        Self {
            user_id: "system".to_string(),
            username: "system".to_string(),
        }
    }
}

/// What caused the run to be submitted.
///
/// On the run document from the start: adding it later would leave every
/// historical run unable to say where it came from, and the whole point of
/// keeping these records is being able to read them back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// A person, through the API.
    #[default]
    Api,
    /// A recurring schedule. Not yet implemented -- see docs/task-system.md.
    Schedule,
}

/// How far along a running task is.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct Progress {
    pub done: u64,
    pub total: u64,
    pub message: String,
}

/// One execution of a task type with concrete parameters.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TaskRun {
    #[serde(rename = "_id")]
    pub id: String,
    pub task_type: String,
    /// Validated against the task's params type at submit time, so a worker
    /// never deserializes something the API did not accept.
    pub params: serde_json::Value,
    pub status: TaskStatus,
    pub actor: Actor,
    #[serde(default)]
    pub trigger: Trigger,
    pub requested_at: f64,
    #[serde(default)]
    pub started_at: Option<f64>,
    #[serde(default)]
    pub finished_at: Option<f64>,
    #[serde(default)]
    pub progress: Progress,
    /// Which worker holds it, for debugging a stuck run.
    #[serde(default)]
    pub worker: Option<String>,
    /// Renewed by the worker's heartbeat. A run whose lease has expired was
    /// orphaned -- the worker was killed, deployed over, or lost the database
    /// -- and is requeued.
    #[serde(default)]
    pub lease_expires_at: Option<f64>,
    #[serde(default)]
    pub cancel_requested: bool,
    #[serde(default)]
    pub error: Option<String>,
    /// How many times a worker has claimed this run. A run resumed after a
    /// deploy is on its second attempt, which is normal and worth seeing.
    #[serde(default)]
    pub attempts: u32,
}

/// One flush of log lines from a run.
///
/// Chunked rather than one document per line to keep write volume sane: a
/// catalog ingest logs steadily for hours.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TaskLogChunk {
    pub run_id: String,
    /// Monotonic per run. The client tails by asking for `seq` greater than the
    /// last one it saw, which is stable under concurrent writes in a way that
    /// a timestamp cursor is not.
    pub seq: u64,
    pub ts: f64,
    /// When MongoDB may delete this chunk.
    ///
    /// A BSON date, not a number: the TTL monitor ignores a document whose
    /// indexed field is anything else, and it does so silently, so an index
    /// hung off `ts` above would look installed and delete nothing. Same
    /// reason `PendingAuthorization` carries `expires_at_date` beside its
    /// numeric `expires_at`.
    ///
    /// `value_type` because this type is also an API response shape and
    /// `bson::DateTime` has no schema of its own; the wire form is the ISO
    /// string serde gives it.
    #[schema(value_type = String)]
    pub expires_at: mongodb::bson::DateTime,
    pub lines: Vec<TaskLogLine>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TaskLogLine {
    pub ts: f64,
    pub level: String,
    pub message: String,
}

pub fn now() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// How long a run's logs are kept.
///
/// Loki holds the firehose for seven days; this copy is the per-run record the
/// admin page reads, so it outlasts that by a margin wide enough for any audit
/// anyone actually performs. The ledger has no expiry at all -- it is the
/// permanent answer to "what changed", and logs are the evidence of how, which
/// is worth keeping for a while rather than forever.
pub const LOG_RETENTION_DAYS: i64 = 90;

/// How long the logs of a run that failed or was canceled are kept.
///
/// Those are the ones somebody comes back to months later, and they are a small
/// fraction of the volume: a successful ingest logs steadily for hours, a failed
/// one usually stops early.
pub const FAILED_LOG_RETENTION_DAYS: i64 = 365;

/// `now` plus `days`, as the BSON date the TTL monitor reads.
pub fn expires_in_days(days: i64) -> mongodb::bson::DateTime {
    mongodb::bson::DateTime::from_millis(
        chrono::Utc::now().timestamp_millis() + days * 24 * 60 * 60 * 1000,
    )
}

/// Indexes the queue depends on.
///
/// The claim query sorts queued runs by submission time, and the reaper scans
/// running runs by lease -- both are hot enough to matter once there is any
/// history in the collection.
pub async fn initialize_indexes(db: &mongodb::Database) -> Result<(), mongodb::error::Error> {
    let runs = db.collection::<Document>(RUNS_COLLECTION);
    runs.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "status": 1, "requested_at": 1 })
            .build(),
    )
    .await?;
    runs.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "status": 1, "lease_expires_at": 1 })
            .build(),
    )
    .await?;
    runs.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "requested_at": -1 })
            .build(),
    )
    .await?;
    let logs = db.collection::<Document>(LOGS_COLLECTION);
    logs.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "run_id": 1, "seq": 1 })
            .build(),
    )
    .await?;
    // Expiry is enforced by mongod's own TTL monitor rather than by anything
    // here: no cron, no loop in the worker. `expire_after(0)` means each chunk
    // goes at the instant its own `expires_at` names, which is what lets a
    // failed run's logs outlive a successful one's -- a uniform window could
    // not express that. The cost of that shape is that a wrong `expires_at`
    // deletes immediately rather than late, so it is always computed.
    logs.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "expires_at": 1 })
            .options(
                mongodb::options::IndexOptions::builder()
                    .expire_after(std::time::Duration::from_secs(0))
                    .build(),
            )
            .build(),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_expiry_field_serializes_as_a_bson_date() {
        // The whole point of the field. MongoDB's TTL monitor ignores a
        // document whose indexed field is not a date, silently, so if this ever
        // becomes a number the index stays installed and deletes nothing.
        let chunk = TaskLogChunk {
            run_id: "r1".to_string(),
            seq: 0,
            ts: now(),
            expires_at: expires_in_days(LOG_RETENTION_DAYS),
            lines: vec![],
        };
        let doc = mongodb::bson::to_document(&chunk).expect("serializes");
        assert!(
            matches!(
                doc.get("expires_at"),
                Some(mongodb::bson::Bson::DateTime(_))
            ),
            "expires_at must be a BSON date, got {:?}",
            doc.get("expires_at")
        );
        // And `ts` is deliberately not the TTL field: it is a double.
        assert!(matches!(
            doc.get("ts"),
            Some(mongodb::bson::Bson::Double(_))
        ));
    }

    #[test]
    fn a_bad_outcome_keeps_its_logs_longer() {
        assert!(FAILED_LOG_RETENTION_DAYS > LOG_RETENTION_DAYS);
        let ordinary = expires_in_days(LOG_RETENTION_DAYS).timestamp_millis();
        let failed = expires_in_days(FAILED_LOG_RETENTION_DAYS).timestamp_millis();
        assert!(failed > ordinary);
    }

    #[test]
    fn retention_outlasts_loki() {
        // Loki keeps the firehose for seven days (config/loki/loki-config.yaml).
        // This copy is the per-run record, so it has to outlast that or the
        // division of labor between the two is pointless.
        assert!(LOG_RETENTION_DAYS > 7);
    }
}
