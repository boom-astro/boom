use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use super::TaskError;
use crate::utils::tracks::{
    conic, expected_rms_arcsec, track_flags, StoredOrbit, StoredTrack, TRACKS_COLLECTION,
};
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use mongodb::{Client, Collection, Namespace};
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

fn missing_quality() -> Document {
    doc! { "bound_fit_residual_arcsec": { "$type": "number" }, "rms_ratio": { "$exists": false } }
}

fn quality_update(namespace: &Namespace, track: &Document) -> Option<WriteModel> {
    let id = track.get("_id")?;
    let stored = mongodb::bson::from_document::<StoredTrack>(track.clone()).ok()?;
    let rms = stored.bound_fit_residual_arcsec?;
    let expected = expected_rms_arcsec(&stored.member_keys())?;
    Some(WriteModel::UpdateOne(
        UpdateOneModel::builder()
            .namespace(namespace.clone())
            .filter(doc! {
                "_id": id.clone(),
                "members": track.get("members")?.clone(),
                "bound_fit_residual_arcsec": rms,
            })
            .update(
                doc! { "$set": { "expected_rms_arcsec": expected, "rms_ratio": rms / expected } },
            )
            .build(),
    ))
}

fn missing_flags() -> Document {
    doc! { "flags": { "$exists": false } }
}

fn flags_update(namespace: &Namespace, track: &Document) -> Option<WriteModel> {
    let id = track.get("_id")?;
    let stored = mongodb::bson::from_document::<StoredTrack>(track.clone()).ok()?;
    if stored.epochs.len() != stored.members.len() {
        return None;
    }
    let flags = track_flags(&stored.member_keys(), &stored.epochs);
    Some(WriteModel::UpdateOne(
        UpdateOneModel::builder()
            .namespace(namespace.clone())
            .filter(doc! { "_id": id.clone(), "members": track.get("members")?.clone() })
            .update(doc! { "$set": { "flags": flags } })
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

#[derive(Default)]
struct Pass {
    seen: u64,
    written: u64,
    unreadable: u64,
}

async fn backfill(
    ctx: &TaskContext,
    tracks: &Collection<Document>,
    (filter, projection): (Document, Document),
    update: fn(&Namespace, &Document) -> Option<WriteModel>,
    what: &str,
    batch_size: usize,
) -> Result<Pass, TaskError> {
    let db = ctx.db();
    let namespace = tracks.namespace();
    let total = tracks
        .count_documents(filter.clone())
        .await
        .map_err(failed)?;
    ctx.info(format!("{total} track(s) with {what}"));

    let mut cursor = tracks
        .find(filter)
        .projection(projection)
        .await
        .map_err(failed)?;
    let mut batch = Vec::with_capacity(batch_size);
    let mut pass = Pass::default();
    while let Some(track) = cursor.try_next().await.map_err(failed)? {
        pass.seen += 1;
        match update(&namespace, &track) {
            Some(write) => batch.push(write),
            None => pass.unreadable += 1,
        }
        if batch.len() >= batch_size {
            pass.written += flush(db.client(), &mut batch).await?;
            ctx.progress(
                pass.seen,
                total.max(pass.seen),
                format!("{} written", pass.written),
            )
            .await;
            if ctx.is_canceled() {
                ctx.warn(format!(
                    "canceled after {} track(s); re-running resumes from the ones \
                     still missing them",
                    pass.written
                ));
                return Err(TaskError::Canceled);
            }
        }
    }
    pass.written += flush(db.client(), &mut batch).await?;
    ctx.progress(
        pass.seen,
        total.max(pass.seen),
        format!("{} written", pass.written),
    )
    .await;
    if pass.unreadable > 0 {
        ctx.warn(format!(
            "{} track(s) with {what} could not be read",
            pass.unreadable
        ));
    }
    Ok(pass)
}

pub async fn run(
    ctx: &TaskContext,
    params: BackfillTrackElementsParams,
) -> Result<serde_json::Value, TaskError> {
    let db = ctx.db();
    let tracks = db.collection::<Document>(TRACKS_COLLECTION);
    let elements = backfill(
        ctx,
        &tracks,
        (missing(), doc! { "orbit": 1 }),
        update_for,
        "an orbit and no e or q",
        params.batch_size,
    )
    .await?;
    let quality = backfill(
        ctx,
        &tracks,
        (
            missing_quality(),
            doc! {
                "members": 1, "member_surveys": 1, "bound_fit_residual_arcsec": 1,
                "n_detections": 1, "n_nights": 1, "arc_days": 1, "first_jd": 1, "last_jd": 1,
            },
        ),
        quality_update,
        "a residual and no rms ratio",
        params.batch_size,
    )
    .await?;

    let flags = backfill(
        ctx,
        &tracks,
        (
            missing_flags(),
            doc! {
                "members": 1, "member_surveys": 1, "epochs": 1,
                "n_detections": 1, "n_nights": 1, "arc_days": 1, "first_jd": 1, "last_jd": 1,
            },
        ),
        flags_update,
        "no flags",
        params.batch_size,
    )
    .await?;

    let written = elements.written + quality.written + flags.written;
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
                "fields": ["orbit.e", "orbit.q", "expected_rms_arcsec", "rms_ratio", "flags"],
                "written": written as i64,
            },
        )
        .await;
    }

    Ok(serde_json::json!({
        "tracks": elements.seen,
        "written": elements.written,
        "unreadable": elements.unreadable,
        "quality_tracks": quality.seen,
        "quality_written": quality.written,
        "quality_unreadable": quality.unreadable,
        "flags_tracks": flags.seen,
        "flags_written": flags.written,
        "flags_unreadable": flags.unreadable,
    }))
}
