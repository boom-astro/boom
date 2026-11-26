use crate::api::models::response;
use crate::utils::enums::Survey;
use crate::utils::tracks::{list_tracks, TrackQuery};
use actix_web::{get, web, HttpResponse};
use mongodb::Database;
use serde::Deserialize;
use utoipa::IntoParams;

const DEFAULT_LIST_LIMIT: i64 = 50;
const MAX_LIST_LIMIT: i64 = 500;
const DEFAULT_WINDOW_SECONDS: f64 = 86_400.0;

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListTracksQuery {
    /// Tracks written at or after this time, Unix seconds (default: one day ago)
    updated_since: Option<f64>,
    /// Skip the tracks written at exactly updated_since with an id up to this one
    after_id: Option<String>,
    /// Only tracks not matched to a known object (default false)
    unknown_only: Option<bool>,
    /// Minimum eccentricity of the stored orbit, 1 for unbound
    min_e: Option<f64>,
    /// Maximum perihelion distance of the stored orbit, au
    max_q: Option<f64>,
    /// Minimum number of nights
    min_nights: Option<i32>,
    /// Maximum fit residual over the rms expected from the members' sites
    max_rms_ratio: Option<f64>,
    /// Minimum digest2 NEO score (0-100), set on fast tracks
    min_neo_score: Option<f64>,
    /// Maximum number of tracks (1-500, default 50)
    limit: Option<i64>,
}

pub(crate) async fn list(
    db: &Database,
    params: ListTracksQuery,
    survey: Option<Survey>,
) -> HttpResponse {
    let query = TrackQuery {
        updated_since: params.updated_since.unwrap_or_else(|| {
            chrono::Utc::now().timestamp_millis() as f64 / 1000.0 - DEFAULT_WINDOW_SECONDS
        }),
        after_id: params.after_id,
        unknown_only: params.unknown_only.unwrap_or(false),
        min_e: params.min_e,
        max_q: params.max_q,
        min_nights: params.min_nights,
        max_rms_ratio: params.max_rms_ratio,
        min_neo_score: params.min_neo_score,
        min_last_jd: None,
        survey,
    };
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    match list_tracks(db, &query, limit as usize).await {
        Ok(page) => response::ok_ser(&format!("{} tracks found", page.tracks.len()), page),
        Err(error) => response::internal_error(&format!("error listing tracks: {}", error)),
    }
}

/// List linked moving-object tracks of every survey by write time, oldest first
#[utoipa::path(
    get,
    path = "/tracks",
    params(ListTracksQuery),
    responses(
        (status = 200, description = "Tracks, and in next the updated_since and after_id of the last track scanned", body = serde_json::Value),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/tracks")]
pub async fn get_all_tracks(
    params: web::Query<ListTracksQuery>,
    db: web::Data<Database>,
) -> HttpResponse {
    list(&db, params.into_inner(), None).await
}
