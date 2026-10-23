use crate::api::models::response;
use crate::utils::enums::Survey;
use crate::utils::tracks::{list_tracks, track_by_id, TrackQuery};
use actix_web::{get, web, HttpResponse};
use mongodb::Database;
use serde::Deserialize;

const DEFAULT_LIST_LIMIT: i64 = 50;
const MAX_LIST_LIMIT: i64 = 500;
const DEFAULT_WINDOW_SECONDS: f64 = 86_400.0;

#[derive(Debug, Deserialize)]
pub struct ListTracksQuery {
    updated_since: Option<f64>,
    after_id: Option<String>,
    unknown_only: Option<bool>,
    min_e: Option<f64>,
    max_q: Option<f64>,
    min_nights: Option<i32>,
    limit: Option<i64>,
}

/// List linked moving-object tracks by write time, oldest first
#[utoipa::path(
    get,
    path = "/surveys/{survey}/tracks",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (only ztf has tracks)"),
        ("updated_since" = Option<f64>, Query, description = "Tracks written at or after this time, Unix seconds (default: one day ago)"),
        ("after_id" = Option<String>, Query, description = "Skip the tracks written at exactly updated_since with an id up to this one"),
        ("unknown_only" = Option<bool>, Query, description = "Only tracks not matched to a known object (default false)"),
        ("min_e" = Option<f64>, Query, description = "Minimum eccentricity of the stored orbit, 1 for unbound"),
        ("max_q" = Option<f64>, Query, description = "Maximum perihelion distance of the stored orbit, au"),
        ("min_nights" = Option<i32>, Query, description = "Minimum number of nights"),
        ("limit" = Option<i64>, Query, description = "Maximum number of tracks (1-500, default 50)"),
    ),
    responses(
        (status = 200, description = "Tracks, and in next the updated_since and after_id of the last track scanned", body = serde_json::Value),
        (status = 404, description = "No tracks for this survey"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey}/tracks")]
pub async fn get_tracks(
    survey: web::Path<Survey>,
    params: web::Query<ListTracksQuery>,
    db: web::Data<Database>,
) -> HttpResponse {
    let survey = survey.into_inner();
    if survey != Survey::Ztf {
        return response::not_found(&format!("no tracks for survey {}", survey));
    }
    let params = params.into_inner();
    let query = TrackQuery {
        updated_since: params.updated_since.unwrap_or_else(|| {
            chrono::Utc::now().timestamp_millis() as f64 / 1000.0 - DEFAULT_WINDOW_SECONDS
        }),
        after_id: params.after_id,
        unknown_only: params.unknown_only.unwrap_or(false),
        min_e: params.min_e,
        max_q: params.max_q,
        min_nights: params.min_nights,
    };
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    match list_tracks(&db, &query, limit as usize).await {
        Ok(page) => response::ok_ser(&format!("{} tracks found", page.tracks.len()), page),
        Err(error) => response::internal_error(&format!("error listing tracks: {}", error)),
    }
}

/// Get a linked moving-object track with all of its members
#[utoipa::path(
    get,
    path = "/surveys/{survey}/tracks/{track_id}",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (only ztf has tracks)"),
        ("track_id" = String, Path, description = "Track id, e.g. BT000001"),
    ),
    responses(
        (status = 200, description = "Track found", body = serde_json::Value),
        (status = 404, description = "Track not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey}/tracks/{track_id}")]
pub async fn get_track(path: web::Path<(Survey, String)>, db: web::Data<Database>) -> HttpResponse {
    let (survey, track_id) = path.into_inner();
    if survey != Survey::Ztf {
        return response::not_found(&format!("no tracks for survey {}", survey));
    }
    match track_by_id(&db, &track_id).await {
        Ok(Some(track)) => response::ok_ser(&format!("track {} found", track_id), track),
        Ok(None) => response::not_found(&format!("no track {}", track_id)),
        Err(error) => response::internal_error(&format!("error getting track: {}", error)),
    }
}
