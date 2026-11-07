use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use crate::utils::enums::Survey;
use crate::utils::tracks::public_track_by_id;
use actix_web::{get, web, HttpResponse};
use mongodb::Database;

/// Get a linked moving-object track, restricted to its public detections
#[utoipa::path(
    get,
    path = "/babamul/surveys/{survey}/tracks/{track_id}",
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
pub async fn get_track(
    path: web::Path<(Survey, String)>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    if current_user.is_none() {
        return HttpResponse::Unauthorized().body("Unauthorized");
    }
    let (survey, track_id) = path.into_inner();
    if survey != Survey::Ztf {
        return response::not_found(&format!("no tracks for survey {}", survey));
    }
    match public_track_by_id(&db, &track_id).await {
        Ok(Some(track)) => response::ok_ser(&format!("track {} found", track_id), track),
        Ok(None) => response::not_found(&format!("no track {}", track_id)),
        Err(error) => response::internal_error(&format!("error getting track: {}", error)),
    }
}
