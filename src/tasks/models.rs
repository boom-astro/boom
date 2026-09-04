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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// A person, through the API.
    Api,
    /// A recurring schedule. Not yet implemented -- see docs/task-system.md.
    Schedule,
}

impl Default for Trigger {
    fn default() -> Self {
        Trigger::Api
    }
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
    db.collection::<Document>(LOGS_COLLECTION)
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "run_id": 1, "seq": 1 })
                .build(),
        )
        .await?;
    Ok(())
}
