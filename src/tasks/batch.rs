//! Batched `update_many` over a collection, shared by the migration tasks.
//!
//! Both `migrate_snr` and `migrate_fp_flux` walk a collection, collect ids, and
//! apply an aggregation pipeline in batches. The loop was duplicated in both
//! binaries; it lives here once so a fix to cancellation or progress reporting
//! reaches both.

use super::context::TaskContext;
use crate::utils::db::merge_filters;
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::Hint;

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
    run_batched_update_after(
        ctx,
        collection,
        filter,
        pipeline,
        batch_size,
        estimated_total,
        label,
        None,
    )
    .await
}

fn id_text(id: &Bson) -> String {
    match id {
        Bson::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_batched_update_after(
    ctx: &TaskContext,
    collection: &mongodb::Collection<Document>,
    filter: Document,
    pipeline: Vec<Document>,
    batch_size: usize,
    estimated_total: u64,
    label: &str,
    mut after: Option<Bson>,
) -> Result<i64, BatchError> {
    let mut total_modified: i64 = 0;
    let mut seen: u64 = 0;
    let mut last_reported: u64 = 0;

    loop {
        // Checked between batches, not mid-batch: an update_many is atomic
        // per document, so a batch boundary is the only point where
        // stopping leaves a state that is easy to describe.
        if ctx.is_canceled() {
            let resume = after
                .as_ref()
                .map(|id| format!(", last _id {}", id_text(id)))
                .unwrap_or_default();
            ctx.warn(format!(
                "{label} canceled after {total_modified} documents{resume}; \
                 the work already done stands, and re-running resumes it \
                 because the migration recomputes from the raw fields"
            ));
            return Err(BatchError::Canceled {
                modified: total_modified,
            });
        }

        let page_filter = match &after {
            Some(id) => merge_filters(&filter, &doc! { "_id": { "$gt": id.clone() } }),
            None => filter.clone(),
        };
        // Read whole before writing: an idle cursor dies with its session.
        let ids: Vec<Bson> = collection
            .find(page_filter)
            .projection(doc! { "_id": 1 })
            .sort(doc! { "_id": 1 })
            .hint(Hint::Keys(doc! { "_id": 1 }))
            .limit(batch_size as i64)
            .batch_size(batch_size as u32)
            .await?
            .try_collect::<Vec<Document>>()
            .await?
            .into_iter()
            .filter_map(|d| d.get("_id").cloned())
            .collect();
        let Some(last) = ids.last().cloned() else {
            break;
        };

        let n = ids.len() as u64;
        let result = collection
            .update_many(doc! { "_id": { "$in": &ids } }, pipeline.clone())
            .await?;
        total_modified += result.modified_count as i64;
        seen += n;

        if seen - last_reported >= PROGRESS_EVERY {
            last_reported = seen;
            ctx.progress(
                seen,
                estimated_total.max(seen),
                format!(
                    "{label}: {total_modified} documents updated, last _id {}",
                    id_text(&last)
                ),
            )
            .await;
        }
        after = Some(last);
        if (n as usize) < batch_size {
            break;
        }
    }

    Ok(total_modified)
}
