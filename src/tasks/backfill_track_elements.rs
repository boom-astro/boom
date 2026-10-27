use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use super::TaskError;
use crate::utils::tracks::{conic, StoredOrbit, TRACKS_COLLECTION};
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use mongodb::{Client, Namespace};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const TASK_TYPE: &str = "backfill_track_elements";

const MAX_BATCH_SIZE: usize = 10_000;

fn default_batch_size() -> usize {
    1_000
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackfillTrackElementsParams {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl BackfillTrackElementsParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        Ok(())
    }
}

fn missing() -> Document {
    doc! { "orbit": { "$type": "object" }, "orbit.e": { "$exists": false } }
}

fn update_for(namespace: &Namespace, track: &Document) -> Option<WriteModel> {
    let id = track.get("_id")?;
    let orbit = track.get_document("orbit").ok()?;
    let (e, q) = conic(&mongodb::bson::from_document::<StoredOrbit>(orbit.clone()).ok()?);
    Some(WriteModel::UpdateOne(
        UpdateOneModel::builder()
            .namespace(namespace.clone())
            // Matched on the orbit as read, so a track rewritten meanwhile keeps its own.
            .filter(doc! { "_id": id.clone(), "orbit": orbit.clone() })
            .update(doc! { "$set": { "orbit.e": e, "orbit.q": q } })
            .build(),
    ))
}

fn failed(e: mongodb::error::Error) -> TaskError {
    TaskError::Failed(e.to_string())
}

async fn flush(client: &Client, batch: &mut Vec<WriteModel>) -> Result<u64, TaskError> {
    if batch.is_empty() {
        return Ok(0);
    }
    let result = client
        .bulk_write(std::mem::take(batch))
        .ordered(false)
        .await
        .map_err(failed)?;
    Ok(result.modified_count as u64)
}

pub async fn run(
    ctx: &TaskContext,
    params: BackfillTrackElementsParams,
) -> Result<serde_json::Value, TaskError> {
    let db = ctx.db();
    let tracks = db.collection::<Document>(TRACKS_COLLECTION);
    let namespace = tracks.namespace();
    let total = tracks.count_documents(missing()).await.map_err(failed)?;
    ctx.info(format!("{total} track(s) with an orbit and no e or q"));

    let mut cursor = tracks
        .find(missing())
        .projection(doc! { "orbit": 1 })
        .await
        .map_err(failed)?;
    let mut batch = Vec::with_capacity(params.batch_size);
    let (mut seen, mut written, mut unreadable) = (0u64, 0u64, 0u64);
    while let Some(track) = cursor.try_next().await.map_err(failed)? {
        seen += 1;
        match update_for(&namespace, &track) {
            Some(write) => batch.push(write),
            None => unreadable += 1,
        }
        if batch.len() >= params.batch_size {
            written += flush(db.client(), &mut batch).await?;
            ctx.progress(seen, total.max(seen), format!("{written} written"))
                .await;
            if ctx.is_canceled() {
                ctx.warn(format!(
                    "canceled after {written} track(s); re-running resumes from the ones \
                     still missing e and q"
                ));
                return Err(TaskError::Canceled);
            }
        }
    }
    written += flush(db.client(), &mut batch).await?;
    ctx.progress(seen, total.max(seen), format!("{written} written"))
        .await;
    if unreadable > 0 {
        ctx.warn(format!("{unreadable} track(s) had an unreadable orbit"));
    }

    if written > 0 {
        ctx.record_mutation(
            MutationTarget {
                database: db.name().to_string(),
                collection: TRACKS_COLLECTION.to_string(),
                catalog: None,
                survey: Some("ztf".to_string()),
            },
            Operation::Recompute,
            doc! {
                "fields": ["orbit.e", "orbit.q"],
                "written": written as i64,
            },
        )
        .await;
    }

    Ok(serde_json::json!({
        "tracks": seen,
        "written": written,
        "unreadable": unreadable,
    }))
}
