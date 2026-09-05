//! Batched `update_many` over a collection, shared by the migration tasks.
//!
//! Both `migrate_snr` and `migrate_fp_flux` walk a collection, collect ids, and
//! apply an aggregation pipeline in batches. The loop was duplicated in both
//! binaries; it lives here once so a fix to cancellation or progress reporting
//! reaches both.

use super::context::TaskContext;
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};

/// How often to publish progress while a batched update runs.
pub const PROGRESS_EVERY: u64 = 50_000;

#[derive(thiserror::Error, Debug)]
pub enum BatchError {
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
    #[error("canceled after {modified} documents")]
    Canceled { modified: i64 },
}

pub async fn run_batched_update(
    ctx: &TaskContext,
    collection: &mongodb::Collection<Document>,
    filter: Document,
    pipeline: Vec<Document>,
    batch_size: usize,
    estimated_total: u64,
    label: &str,
) -> Result<i64, BatchError> {
    let mut cursor = collection
        .find(filter)
        .projection(doc! { "_id": 1 })
        .no_cursor_timeout(true)
        .await?;

    let mut ids: Vec<Bson> = Vec::with_capacity(batch_size);
    let mut total_modified: i64 = 0;
    let mut seen: u64 = 0;
    let mut last_reported: u64 = 0;

    while let Some(d) = cursor.try_next().await? {
        let Some(id) = d.get("_id") else {
            // A document without an _id cannot exist in Mongo; skipping rather
            // than unwrapping keeps a malformed cursor row from killing a
            // migration that is otherwise fine.
            continue;
        };
        ids.push(id.clone());

        if ids.len() >= batch_size {
            // Checked between batches, not mid-batch: an update_many is atomic
            // per document, so a batch boundary is the only point where
            // stopping leaves a state that is easy to describe.
            if ctx.is_canceled() {
                ctx.warn(format!(
                    "{label} canceled after {total_modified} documents; \
                     the work already done stands, and re-running resumes it \
                     because the migration recomputes from the raw fields"
                ));
                return Err(BatchError::Canceled {
                    modified: total_modified,
                });
            }

            let n = ids.len() as u64;
            let batch_filter = doc! { "_id": { "$in": &ids } };
            let result = collection
                .update_many(batch_filter, pipeline.clone())
                .await?;
            total_modified += result.modified_count as i64;
            seen += n;
            ids.clear();

            if seen - last_reported >= PROGRESS_EVERY {
                last_reported = seen;
                ctx.progress(
                    seen,
                    estimated_total.max(seen),
                    format!("{label}: {total_modified} documents updated"),
                )
                .await;
            }
        }
    }

    if !ids.is_empty() {
        let batch_filter = doc! { "_id": { "$in": &ids } };
        let result = collection.update_many(batch_filter, pipeline).await?;
        total_modified += result.modified_count as i64;
    }

    Ok(total_modified)
}
