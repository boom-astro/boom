//! The `copy_cutouts` task: copy alert cutouts between MongoDB deployments.
//!
//! A one-off migration tool: copy a survey's cutout collection from one
//! deployment to another, typically before repointing BOOM at new storage.
//!
//! The usual sequence is a full copy, then a second run with `min_candid` set
//! to the highest candid the first copied, to catch alerts that arrived while
//! it ran. The task reports that value, so the follow-up is a resubmission with
//! one parameter changed.
//!
//! **Idempotent.** Documents are keyed on candid and duplicates are counted
//! rather than treated as errors, so re-running copies only what is missing.
//! That is also what makes an interrupted run cheap to resume.
//!
//! Both endpoints are given as connection URIs. Those carry passwords, so the
//! parameters are redacted everywhere they are read back -- see
//! `tasks::redact` and `docs/task-system.md`.

use super::batch::PROGRESS_EVERY;
use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::enums::Survey;
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson};
use mongodb::error::{ErrorKind, InsertManyError};
use mongodb::options::ClientOptions;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "copy_cutouts";

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CopyCutoutsParams {
    /// Source MongoDB URI, including the database name.
    pub src_uri: String,
    /// Destination MongoDB URI, including the database name.
    pub dst_uri: String,
    pub survey: Survey,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Copy only documents with `_id >= this`. The previous run reports the
    /// value to use.
    #[serde(default)]
    pub min_candid: Option<i64>,
    /// Concurrent write tasks, overlapping source reads with destination
    /// writes.
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,
    /// Report counts without copying.
    #[serde(default)]
    pub dry_run: bool,
}

fn default_batch_size() -> usize {
    1_000
}

fn default_parallelism() -> usize {
    4
}

const MAX_BATCH_SIZE: usize = 100_000;
const MAX_PARALLELISM: usize = 64;

impl CopyCutoutsParams {
    pub fn validate_params(&self) -> Result<(), String> {
        for (name, uri) in [("src_uri", &self.src_uri), ("dst_uri", &self.dst_uri)] {
            if !uri.starts_with("mongodb://") && !uri.starts_with("mongodb+srv://") {
                return Err(format!("{name} must be a mongodb:// or mongodb+srv:// URI"));
            }
        }
        // Copying a collection onto itself would count every document as a
        // duplicate and report success, which looks like a completed migration.
        if self.src_uri == self.dst_uri {
            return Err("src_uri and dst_uri are the same deployment".to_string());
        }
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        if self.parallelism == 0 || self.parallelism > MAX_PARALLELISM {
            return Err(format!(
                "parallelism must be between 1 and {MAX_PARALLELISM}"
            ));
        }
        Ok(())
    }
}

/// Copy the cutouts.
pub async fn run(
    ctx: &TaskContext,
    params: CopyCutoutsParams,
) -> Result<serde_json::Value, super::TaskError> {
    let failed = super::TaskError::Failed;

    let src_db = connect(&params.src_uri).await?;
    let dst_db = connect(&params.dst_uri).await?;

    let collection_name = format!("{}_alerts_cutouts", params.survey);
    let src = src_db.collection::<mongodb::bson::Document>(&collection_name);
    let dst = dst_db.collection::<mongodb::bson::Document>(&collection_name);

    let filter = match params.min_candid {
        Some(min) => doc! { "_id": { "$gte": Bson::Int64(min) } },
        None => doc! {},
    };

    let estimated_total = if params.min_candid.is_some() {
        src.count_documents(filter.clone())
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("count_documents failed: {}", e);
                0
            })
    } else {
        src.estimated_document_count().await.unwrap_or_else(|e| {
            tracing::warn!("estimated_document_count failed: {}", e);
            0
        })
    };

    if params.dry_run {
        let dst_count = dst.estimated_document_count().await.unwrap_or(0);
        ctx.info(format!(
            "dry run: ~{estimated_total} document(s) to copy; destination holds {dst_count}"
        ));
        return Ok(serde_json::json!({
            "collection": collection_name,
            "source_estimated": estimated_total,
            "destination_count": dst_count,
            "dry_run": true,
        }));
    }

    ctx.info(format!(
        "copying ~{estimated_total} document(s) of {collection_name} from {} to {}          (parallelism={})",
        src_db.name(),
        dst_db.name(),
        params.parallelism
    ));
    if let Some(min) = params.min_candid {
        ctx.info(format!("incremental: _id >= {min}"));
    }

    // Channel carries (batch, batch_start_candid).
    // batch_start_candid lets a writer report a precise resume point on fatal error.
    // Capacity is parallelism*2 so the reader stays slightly ahead of writers.
    let (tx, rx) = mpsc::channel::<(Vec<mongodb::bson::Document>, i64)>(params.parallelism * 2);
    let rx = Arc::new(Mutex::new(rx));

    let total_inserted = Arc::new(AtomicU64::new(0));
    let total_skipped = Arc::new(AtomicU64::new(0));

    let mut writer_handles = Vec::with_capacity(params.parallelism);
    for _ in 0..params.parallelism {
        let rx = Arc::clone(&rx);
        let dst = dst.clone();
        let inserted = Arc::clone(&total_inserted);
        let skipped = Arc::clone(&total_skipped);

        writer_handles.push(tokio::spawn(async move {
            loop {
                // Hold the lock only for recv() — released before flush_batch so all
                // writers can be in flush_batch concurrently.
                let item = {
                    let mut rx = rx.lock().await;
                    rx.recv().await
                };
                let (batch, batch_start) = match item {
                    Some(b) => b,
                    None => break,
                };
                match flush_batch(&dst, &batch).await {
                    Ok((ins, dup)) => {
                        // The writers hold the only count of what actually
                        // landed; the reader knows what it queued, which runs
                        // ahead of the destination.
                        inserted.fetch_add(ins, Ordering::Relaxed);
                        skipped.fetch_add(dup, Ordering::Relaxed);
                    }
                    Err(e) => {
                        // Reported rather than fatal to the process: the run
                        // fails with a resume point, and the worker survives to
                        // run something else.
                        return Err(format!(
                            "write failed: {e}; resume with min_candid {batch_start}"
                        ));
                    }
                }
            }
            Ok(())
        }));
    }

    // Reader runs on the main task, feeding batches into the channel.
    let mut cursor = src
        .find(filter)
        .sort(doc! { "_id": 1 })
        .no_cursor_timeout(true)
        .await
        .map_err(|e| failed(format!("failed to open source cursor: {e}")))?;

    let mut batch: Vec<mongodb::bson::Document> = Vec::with_capacity(params.batch_size);
    let mut batch_start: i64 = i64::MIN;
    let mut max_candid_read: i64 = i64::MIN;

    let mut read: u64 = 0;
    let mut last_reported: u64 = 0;

    loop {
        // Cancelling in the reader rather than the writers: they drain what is
        // queued and finish their batches, so a cancelled run leaves whole
        // documents copied and a usable resume point.
        if ctx.is_canceled() {
            ctx.warn(format!(
                "canceled after reading {read} document(s); resume with min_candid                  {max_candid_read}"
            ));
            drop(tx);
            for h in writer_handles {
                let _ = h.await;
            }
            return Err(super::TaskError::Canceled);
        }

        match cursor.try_next().await {
            Ok(Some(doc)) => {
                let id = match doc.get("_id") {
                    Some(Bson::Int64(v)) => *v,
                    Some(Bson::Int32(v)) => *v as i64,
                    other => {
                        // Every document in the batch is keyed on candid; one
                        // that is not means the wrong collection, and copying
                        // it would produce a destination nothing can read back.
                        return Err(failed(format!(
                            "_id is missing or not an integer (got {other:?}); the source \
                             collection may have the wrong _id type"
                        )));
                    }
                };
                if batch.is_empty() {
                    batch_start = id;
                }
                if id > max_candid_read {
                    max_candid_read = id;
                }
                batch.push(doc);
                read += 1;
                if read - last_reported >= PROGRESS_EVERY {
                    last_reported = read;
                    let written = total_inserted.load(Ordering::Relaxed)
                        + total_skipped.load(Ordering::Relaxed);
                    ctx.progress(
                        written,
                        estimated_total.max(read),
                        format!("{read} read, {written} written"),
                    )
                    .await;
                }
                if batch.len() >= params.batch_size {
                    let ready =
                        std::mem::replace(&mut batch, Vec::with_capacity(params.batch_size));
                    let start = batch_start;
                    batch_start = i64::MIN;
                    if tx.send((ready, start)).await.is_err() {
                        // Every writer has exited; the failure that killed them
                        // is collected when the handles are awaited below.
                        return Err(failed(format!(
                            "all writers stopped; resume with min_candid {start}"
                        )));
                    }
                }
            }
            Ok(None) => {
                if !batch.is_empty() {
                    let start = batch_start;
                    if tx.send((batch, start)).await.is_err() {
                        return Err(failed(format!(
                            "all writers stopped; resume with min_candid {start}"
                        )));
                    }
                }
                break;
            }
            Err(e) => {
                // batch_start is i64::MIN when the error falls between batches;
                // use max_candid_read (last successfully read _id) in that case.
                let resume = if !batch.is_empty() {
                    batch_start
                } else {
                    max_candid_read
                };
                return Err(failed(if resume == i64::MIN {
                    format!("cursor error before any document was read: {e}")
                } else {
                    format!("cursor error: {e}; resume with min_candid {resume}")
                }));
            }
        }
    }

    drop(tx); // close channel; writers drain remaining batches then exit

    // Every handle is awaited even after one fails, so no writer is left
    // inserting into a destination the caller believes is finished with.
    let mut writer_error: Option<String> = None;
    for h in writer_handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => writer_error = writer_error.or(Some(e)),
            Err(e) => writer_error = writer_error.or(Some(format!("writer panicked: {e}"))),
        }
    }
    if let Some(e) = writer_error {
        return Err(failed(e));
    }

    let inserted = total_inserted.load(Ordering::Relaxed);
    let skipped = total_skipped.load(Ordering::Relaxed);
    ctx.info(format!(
        "copied {inserted} document(s), skipped {skipped} duplicate(s)"
    ));
    if max_candid_read > i64::MIN {
        ctx.info(format!(
            "max candid copied: {max_candid_read}; for incremental catch-up,              submit again with min_candid {max_candid_read}"
        ));
    }

    ctx.record_mutation(
        MutationTarget {
            database: dst_db.name().to_string(),
            collection: collection_name.clone(),
            catalog: None,
            survey: Some(params.survey.to_string().to_lowercase()),
        },
        // New documents written into a destination from an external source.
        Operation::Ingest,
        doc! {
            // Redacted on the way into the ledger; see tasks::redact.
            "src_uri": &params.src_uri,
            "dst_uri": &params.dst_uri,
            "documents_inserted": inserted as i64,
            "duplicates_skipped": skipped as i64,
            "max_candid_copied": max_candid_read,
            "min_candid": params.min_candid,
            "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                .unwrap_or(Bson::Null),
        },
    )
    .await;

    Ok(serde_json::json!({
        "collection": collection_name,
        "documents_inserted": inserted,
        "duplicates_skipped": skipped,
        "max_candid_copied": max_candid_read,
        "dry_run": false,
    }))
}

/// Open the database a URI names.
///
/// The errors are `InvalidParams` rather than `Failed`: a malformed URI or one
/// missing its database name is a bad submission, not a run that went wrong,
/// and the distinction is what the client sees.
///
/// The URI is never quoted back in these messages -- it holds a password, and
/// an error string is one of the few places redaction does not reach.
async fn connect(uri: &str) -> Result<mongodb::Database, super::TaskError> {
    let mut opts = ClientOptions::parse(uri)
        .await
        .map_err(|e| super::TaskError::InvalidParams(format!("invalid MongoDB URI: {e}")))?;
    let db_name = opts.default_database.take().ok_or_else(|| {
        super::TaskError::InvalidParams(
            "the URI must include a database name, as mongodb://host:port/dbname".to_string(),
        )
    })?;
    let client = mongodb::Client::with_options(opts)
        .map_err(|e| super::TaskError::Failed(format!("failed to build a client: {e}")))?;
    Ok(client.database(&db_name))
}

/// insert_many with ordered=false, treating E11000 (duplicate key) as skips.
/// Returns (inserted, skipped_as_duplicates) or a fatal Err.
async fn flush_batch(
    dst: &mongodb::Collection<mongodb::bson::Document>,
    batch: &[mongodb::bson::Document],
) -> Result<(u64, u64), mongodb::error::Error> {
    match dst.insert_many(batch).ordered(false).await {
        Ok(result) => Ok((result.inserted_ids.len() as u64, 0)),
        Err(e) => {
            if let ErrorKind::InsertMany(InsertManyError {
                ref write_errors,
                ref write_concern_error,
                ..
            }) = *e.kind
            {
                let has_non_dup = write_errors
                    .as_ref()
                    .map(|errs| errs.iter().any(|we| we.code != 11000))
                    .unwrap_or(false);
                if !has_non_dup && write_concern_error.is_none() {
                    let dups = write_errors.as_ref().map(|e| e.len()).unwrap_or(0) as u64;
                    return Ok((batch.len() as u64 - dups, dups));
                }
            }
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(src: &str, dst: &str) -> CopyCutoutsParams {
        CopyCutoutsParams {
            src_uri: src.to_string(),
            dst_uri: dst.to_string(),
            survey: Survey::Ztf,
            batch_size: default_batch_size(),
            min_candid: None,
            parallelism: default_parallelism(),
            dry_run: false,
        }
    }

    #[test]
    fn both_endpoints_must_be_mongodb_uris() {
        assert!(params("mongodb://a/db", "mongodb+srv://b/db")
            .validate_params()
            .is_ok());
        assert!(params("https://a/db", "mongodb://b/db")
            .validate_params()
            .is_err());
    }

    #[test]
    fn copying_a_deployment_onto_itself_is_refused() {
        // Every document would be a duplicate and the run would report success,
        // which looks exactly like a completed migration.
        assert!(params("mongodb://a/db", "mongodb://a/db")
            .validate_params()
            .is_err());
    }

    #[test]
    fn parallelism_and_batch_size_are_bounded() {
        let mut p = params("mongodb://a/db", "mongodb://b/db");
        p.parallelism = MAX_PARALLELISM + 1;
        assert!(p.validate_params().is_err());
        p.parallelism = default_parallelism();
        p.batch_size = 0;
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn credentials_never_reach_a_client_reading_the_run_back() {
        // The whole reason a URI parameter is acceptable: the worker sees the
        // real value, everything that reads it back sees a masked one.
        let params = serde_json::json!({
            "src_uri": "mongodb://alice:hunter2@old.example.org/boom",
            "dst_uri": "mongodb://bob:s3cret@new.example.org/boom",
            "survey": "ztf",
        });
        let redacted = crate::tasks::redact::redact_params(&params);
        let rendered = redacted.to_string();
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(!rendered.contains("s3cret"), "{rendered}");
        // ...while still saying which deployments were involved.
        assert!(rendered.contains("old.example.org"));
        assert!(rendered.contains("new.example.org"));
    }

    #[test]
    fn the_task_is_registered_and_retryable() {
        // Duplicates are counted rather than fatal, so a resumed run copies
        // only what is missing.
        assert!(crate::tasks::is_retryable(TASK_TYPE));
    }

    #[test]
    fn single_flight_is_keyed_by_destination_and_survey() {
        // Two copies into the same collection would race; copying different
        // surveys, or into different deployments, is independent.
        let key = crate::tasks::single_flight_key(
            TASK_TYPE,
            &serde_json::json!({
                "src_uri": "mongodb://a/db",
                "dst_uri": "mongodb://b/db",
                "survey": "ztf",
            }),
        );
        assert_eq!(
            key,
            Some(mongodb::bson::doc! { "dst_uri": "mongodb://b/db", "survey": "ztf" })
        );
    }
}
