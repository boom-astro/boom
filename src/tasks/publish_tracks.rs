use super::context::TaskContext;
use super::TaskError;
use crate::utils::track_results::TrackPublisher;
use crate::utils::tracks::{track_by_id, StoredTrack};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const TASK_TYPE: &str = "publish_tracks";

const MAX_TRACKS: usize = 10_000;
const BATCH_SIZE: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PublishTracksParams {
    /// Track ids, e.g. BT000575.
    pub track_ids: Vec<String>,
    /// Only these filters; empty runs every filter reading `track`.
    #[serde(default)]
    pub filter_ids: Vec<String>,
}

impl PublishTracksParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.track_ids.is_empty() || self.track_ids.len() > MAX_TRACKS {
            return Err(format!(
                "track_ids must hold between 1 and {MAX_TRACKS} ids"
            ));
        }
        Ok(())
    }
}

fn failed(e: impl std::fmt::Display) -> TaskError {
    TaskError::Failed(e.to_string())
}

pub async fn run(
    ctx: &TaskContext,
    params: PublishTracksParams,
) -> Result<serde_json::Value, TaskError> {
    let db = ctx.db();
    let publisher = TrackPublisher::new(ctx.config()).await.map_err(failed)?;

    let mut tracks: Vec<StoredTrack> = Vec::new();
    let mut missing: Vec<String> = Vec::new();
    for id in &params.track_ids {
        match track_by_id(db, id).await.map_err(failed)? {
            Some(track) if tracks.iter().all(|kept| kept.id != track.id) => tracks.push(track),
            Some(_) => {}
            None => missing.push(id.clone()),
        }
    }
    if !missing.is_empty() {
        ctx.warn(format!(
            "{} track(s) not found: {}",
            missing.len(),
            missing.join(", ")
        ));
    }

    let filter_ids = (!params.filter_ids.is_empty()).then_some(params.filter_ids.as_slice());
    let total = tracks.len() as u64;
    let mut done = 0;
    let mut published = 0;
    for batch in tracks.chunks(BATCH_SIZE) {
        published += publisher
            .publish_tracks(db, batch, filter_ids)
            .await
            .map_err(failed)?;
        done += batch.len() as u64;
        ctx.progress(done, total, format!("{published} alert(s) published"))
            .await;
        if ctx.is_canceled() {
            ctx.warn(format!(
                "canceled after {done} track(s); re-running publishes them again"
            ));
            return Err(TaskError::Canceled);
        }
    }

    Ok(serde_json::json!({
        "tracks": total,
        "missing": missing,
        "published": published,
    }))
}
