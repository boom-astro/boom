//! The log copy the admin page reads.
//!
//! The firehose still goes to the container log and on to Loki through the
//! normal `tracing` path; this is the convenience copy, scoped to one run, that
//! the UI can tail without a Loki query. Lines are buffered and flushed in
//! chunks -- a catalog ingest logs steadily for hours, and one document per
//! line would be a lot of writes for something nobody reads most of the time.

use super::models::{now, TaskLogChunk, TaskLogLine, LOGS_COLLECTION};
use mongodb::bson::doc;
use mongodb::Database;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Lines buffered before a flush is forced.
const FLUSH_AT_LINES: usize = 50;

/// Total lines kept per run.
///
/// A task that logs in a loop must not be able to fill the disk. Past this the
/// sink drops lines and says so once, rather than truncating silently.
const MAX_LINES_PER_RUN: u64 = 100_000;

/// Collects a run's log lines and writes them to `task_logs` in chunks.
#[derive(Clone)]
pub struct LogSink {
    inner: Arc<Inner>,
}

struct Inner {
    db: Option<Database>,
    run_id: String,
    buffer: Mutex<Vec<TaskLogLine>>,
    seq: AtomicU64,
    written: AtomicU64,
    truncation_reported: std::sync::atomic::AtomicBool,
}

impl LogSink {
    pub fn new(db: Database, run_id: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                db: Some(db),
                run_id: run_id.into(),
                buffer: Mutex::new(Vec::new()),
                seq: AtomicU64::new(0),
                written: AtomicU64::new(0),
                truncation_reported: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// A sink that keeps nothing, for running a task outside the task system.
    pub fn detached() -> Self {
        Self {
            inner: Arc::new(Inner {
                db: None,
                run_id: String::new(),
                buffer: Mutex::new(Vec::new()),
                seq: AtomicU64::new(0),
                written: AtomicU64::new(0),
                truncation_reported: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// Buffer one line, flushing if the buffer is full.
    ///
    /// Not async: tasks log from inside loops, and making every line a
    /// suspension point would be both noisy to write and slower than the batch
    /// write it replaces.
    pub fn line(&self, level: &str, message: impl Into<String>) {
        let message = message.into();
        if self.inner.db.is_none() {
            return;
        }
        if self.inner.written.load(Ordering::Relaxed) >= MAX_LINES_PER_RUN {
            if !self.inner.truncation_reported.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    run_id = %self.inner.run_id,
                    "task log truncated at {} lines; the full log is still in the container log",
                    MAX_LINES_PER_RUN
                );
            }
            return;
        }
        let mut buffer = self.inner.buffer.lock().expect("log buffer poisoned");
        buffer.push(TaskLogLine {
            ts: now(),
            level: level.to_string(),
            message,
        });
        let full = buffer.len() >= FLUSH_AT_LINES;
        drop(buffer);
        if full {
            let sink = self.clone();
            // Detached so a slow database cannot stall the task that is logging.
            tokio::spawn(async move { sink.flush().await });
        }
    }

    pub fn info(&self, message: impl Into<String>) {
        self.line("info", message)
    }

    pub fn warn(&self, message: impl Into<String>) {
        self.line("warn", message)
    }

    pub fn error(&self, message: impl Into<String>) {
        self.line("error", message)
    }

    /// Write whatever is buffered. Best-effort -- losing the UI's copy of a log
    /// line is not worth failing a multi-hour ingest over.
    pub async fn flush(&self) {
        let Some(db) = &self.inner.db else { return };
        let lines = {
            let mut buffer = self.inner.buffer.lock().expect("log buffer poisoned");
            if buffer.is_empty() {
                return;
            }
            std::mem::take(&mut *buffer)
        };
        let count = lines.len() as u64;
        let chunk = TaskLogChunk {
            run_id: self.inner.run_id.clone(),
            seq: self.inner.seq.fetch_add(1, Ordering::Relaxed),
            ts: now(),
            lines,
        };
        if let Err(e) = db
            .collection::<TaskLogChunk>(LOGS_COLLECTION)
            .insert_one(&chunk)
            .await
        {
            tracing::warn!("failed to write task log chunk: {}", e);
            return;
        }
        self.inner.written.fetch_add(count, Ordering::Relaxed);
    }
}

/// Read a run's log lines after `after_seq`, for the UI's tail.
pub async fn read_after(
    db: &Database,
    run_id: &str,
    after_seq: Option<u64>,
) -> Result<Vec<TaskLogChunk>, mongodb::error::Error> {
    use futures::TryStreamExt;
    let mut filter = doc! { "run_id": run_id };
    if let Some(seq) = after_seq {
        filter.insert("seq", doc! { "$gt": seq as i64 });
    }
    let cursor = db
        .collection::<TaskLogChunk>(LOGS_COLLECTION)
        .find(filter)
        .sort(doc! { "seq": 1 })
        .await?;
    cursor.try_collect().await
}

/// Drop a run's logs, for when the run itself is deleted.
pub async fn delete_for_run(db: &Database, run_id: &str) -> Result<(), mongodb::error::Error> {
    db.collection::<TaskLogChunk>(LOGS_COLLECTION)
        .delete_many(doc! { "run_id": run_id })
        .await?;
    Ok(())
}
