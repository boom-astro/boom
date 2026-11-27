use crate::api::models::response;
use crate::api::routes::tracks::{list, ListTracksQuery};
use crate::utils::discovery::member_residuals;
use crate::utils::enums::Survey;
use crate::utils::heliolinc::{state_to_elements, State};
use crate::utils::sso_geometry::OrbitalElements;
use crate::utils::tracks::{track_by_id, StoredTrack};
use actix_web::{get, web, HttpResponse};
use mongodb::Database;
use serde::Serialize;

/// List linked moving-object tracks with a detection from this survey, by write time, oldest first
#[utoipa::path(
    get,
    path = "/surveys/{survey}/tracks",
    params(
        ("survey" = Survey, Path, description = "Name of the survey"),
        ListTracksQuery,
    ),
    responses(
        (status = 200, description = "Tracks, and in next the updated_since and after_id of the last track scanned", body = serde_json::Value),
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
    list(&db, params.into_inner(), Some(survey.into_inner())).await
}

#[derive(Serialize)]
struct TrackDetail {
    #[serde(flatten)]
    track: StoredTrack,
    elements: Option<OrbitalElements>,
    residuals: Vec<Option<[f64; 2]>>,
}

/// Get a linked moving-object track with all of its members
#[utoipa::path(
    get,
    path = "/surveys/{survey}/tracks/{track_id}",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (tracks are shared across surveys)"),
        ("track_id" = String, Path, description = "Track id, e.g. BT000001"),
    ),
    responses(
        (status = 200, description = "Track found, with the osculating elements of its orbit and each member's observed minus computed position in arcseconds (RA on a great circle, Dec), parallel to members", body = serde_json::Value),
        (status = 404, description = "Track not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey}/tracks/{track_id}")]
pub async fn get_track(path: web::Path<(Survey, String)>, db: web::Data<Database>) -> HttpResponse {
    let (_, track_id) = path.into_inner();
    let track = match track_by_id(&db, &track_id).await {
        Ok(Some(track)) => track.explicit(),
        Ok(None) => return response::not_found(&format!("no track {}", track_id)),
        Err(error) => return response::internal_error(&format!("error getting track: {}", error)),
    };
    let residuals = match member_residuals(&db, &track).await {
        Ok(residuals) => residuals,
        Err(error) => {
            return response::internal_error(&format!("error getting residuals: {}", error))
        }
    };
    let elements = track.orbit.and_then(|orbit| {
        state_to_elements(
            &State {
                pos: orbit.pos,
                vel: orbit.vel,
            },
            orbit.epoch_jd,
        )
    });
    response::ok_ser(
        &format!("track {} found", track_id),
        TrackDetail {
            track,
            elements,
            residuals,
        },
    )
}
