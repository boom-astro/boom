//! What a running task is handed.
//!
//! Carries the database, the run id, a cancellation flag the worker's heartbeat
//! keeps current, a progress sink and a log sink. A task body takes this plus
//! its typed params and returns a report -- see `docs/task-system.md`.

use super::logs::LogSink;
use super::queue;
use mongodb::Database;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Handed to a task body for the duration of one run.
#[derive(Clone)]
pub struct TaskContext {
    db: Database,
    run_id: String,
    /// Set by the worker's heartbeat when cancellation is requested, or when
    /// the worker is shutting down. Checked by tasks at their own safe points.
    canceled: Arc<AtomicBool>,
    logs: LogSink,
}

impl TaskContext {
    pub fn new(db: Database, run_id: impl Into<String>, canceled: Arc<AtomicBool>) -> Self {
        let run_id = run_id.into();
        Self {
            logs: LogSink::new(db.clone(), &run_id),
            db,
            run_id,
            canceled,
        }
    }

    /// A context not attached to a run: logs go only to `tracing`, progress is
    /// dropped, and nothing ever cancels. For tests and for calling a task body
    /// directly.
    pub fn detached(db: Database) -> Self {
        Self {
            db,
            run_id: String::new(),
            canceled: Arc::new(AtomicBool::new(false)),
            logs: LogSink::detached(),
        }
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn logs(&self) -> &LogSink {
        &self.logs
    }

    /// Whether the task should stop at its next safe point.
    ///
    /// Cheap enough to check in a loop -- it reads a flag the heartbeat
    /// maintains, rather than querying Mongo.
    pub fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Relaxed)
    }

    /// Log a line to both the run's log and the process log.
    ///
    /// Both, deliberately: the run log is what the admin page tails, and the
    /// process log is what survives log retention and reaches Loki.
    pub fn info(&self, message: impl Into<String>) {
        let message = message.into();
        tracing::info!(run_id = %self.run_id, "{}", message);
        self.logs.info(message);
    }

    pub fn warn(&self, message: impl Into<String>) {
        let message = message.into();
        tracing::warn!(run_id = %self.run_id, "{}", message);
        self.logs.warn(message);
    }

    pub fn error(&self, message: impl Into<String>) {
        let message = message.into();
        tracing::error!(run_id = %self.run_id, "{}", message);
        self.logs.error(message);
    }

    /// Record how far along the run is. Best-effort.
    pub async fn progress(&self, done: u64, total: u64, message: impl Into<String>) {
        if self.run_id.is_empty() {
            return;
        }
        let message = message.into();
        if let Err(e) = queue::report_progress(&self.db, &self.run_id, done, total, &message).await
        {
            tracing::warn!("failed to record progress: {}", e);
        }
    }

    pub async fn flush_logs(&self) {
        self.logs.flush().await
    }
}
