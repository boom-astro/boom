use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use crate::utils::enums::Survey;
use crate::{alert::AlertCutout, utils::lightcurves::Band};
use actix_web::{get, web, HttpResponse};
use base64::prelude::*;
use mongodb::{bson::doc, Collection, Database};
use utoipa::ToSchema;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema, Clone)]
enum WhichCutouts {
    First,
    Last,
    Brightest,
    Faintest,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct AlertCandidOnly {
    #[serde(rename = "_id")]
    candid: i64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct CutoutQuery {
    survey: Survey,
    candid: Option<i64>,
    object_id: Option<String>,
    which: Option<WhichCutouts>,
    band: Option<Band>,
}

#[utoipa::path(
    get,
    path = "/babamul/surveys/{survey}/cutouts",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
        ("candid" = Option<i64>, Query, description = "Candid of the alert to retrieve cutouts for"),
        ("objectId" = Option<String>, Query, description = "Object ID to retrieve cutouts for"),
        ("which" = Option<WhichCutouts>, Query, description = "Which cutouts to retrieve if multiple alerts match the objectId (first, last, brightest, faintest)"),
        ("band" = Option<Band>, Query, description = "Band to retrieve cutouts for")
    ),
    responses(
        (status = 200, description = "Cutouts retrieved successfully", body = serde_json::Value),
        (status = 404, description = "Cutouts not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey}/cutouts")]
pub async fn get_cutouts(
    path: web::Path<Survey>,
    query: web::Query<CutoutQuery>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    let _current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };
    let survey = path.into_inner();

    let cutout_collection: Collection<AlertCutout> =
        db.collection(&format!("{}_alerts_cutouts", survey));

    if let Some(candid) = query.candid {
        let cutouts = match cutout_collection
            .find_one(doc! {
                "_id": candid,
            })
            .await
        {
            Ok(Some(cutouts)) => cutouts,
            Ok(None) => {
                return response::not_found(&format!("no cutouts found for candid {}", candid));
            }
            Err(error) => {
                return response::internal_error(&format!("error getting documents: {}", error));
            }
        };
        let resp = serde_json::json!({
            "candid": candid,
            "cutoutScience": BASE64_STANDARD.encode(&cutouts.cutout_science),
            "cutoutTemplate": BASE64_STANDARD.encode(&cutouts.cutout_template),
            "cutoutDifference": BASE64_STANDARD.encode(&cutouts.cutout_difference),
        });
        return response::ok(
            &format!("cutouts found for candid: {}", candid),
            serde_json::json!(resp),
        );
    }

    if let Some(object_id) = &query.object_id {
        let alert_collection = db.collection::<AlertCandidOnly>(&format!("{}_alerts", survey));
        // here we first find the alerts matching the object id,
        // sorted according to the "which" parameter (default to brightest),
        // and finally we get the cutouts for the selected alert
        let which = query
            .which
            .as_ref()
            .unwrap_or(&WhichCutouts::Brightest)
            .clone();
        let find_options = match which {
            WhichCutouts::First => mongodb::options::FindOneOptions::builder()
                .sort(doc! { "candidate.jd": 1 })
                .build(),
            WhichCutouts::Last => mongodb::options::FindOneOptions::builder()
                .sort(doc! { "candidate.jd": -1 })
                .build(),
            WhichCutouts::Brightest => mongodb::options::FindOneOptions::builder()
                .sort(doc! { "candidate.magpsf": -1 }) // Lowest mag is brightest, so sort in descending order
                .build(),
            WhichCutouts::Faintest => mongodb::options::FindOneOptions::builder()
                .sort(doc! { "candidate.magpsf": 1 }) // Highest mag is faintest, so sort in ascending order
                .build(),
        };

        let mut filter = doc! { "objectId": object_id };
        if let Some(band) = &query.band {
            filter.insert("candidate.band", band.to_string());
        }
        if survey == Survey::Ztf {
            // for ZTF, we also want to filter by programid 1 (public alerts) to avoid returning cutouts for private alerts
            filter.insert("candidate.programid", 1);
        }
        let candid = match alert_collection
            .find_one(filter)
            .projection(doc! { "_id": 1 })
            .with_options(find_options)
            .await
        {
            Ok(Some(alert)) => alert.candid,
            Ok(None) => {
                return response::not_found(&format!("no alerts found for objectId {}", object_id));
            }
            Err(error) => {
                return response::internal_error(&format!("error getting documents: {}", error));
            }
        };

        let cutouts = match cutout_collection
            .find_one(doc! {
                "_id": candid,
            })
            .await
        {
            Ok(Some(cutouts)) => cutouts,
            Ok(None) => {
                return response::not_found(&format!(
                    "no cutouts found for objectId {}",
                    object_id
                ));
            }
            Err(error) => {
                return response::internal_error(&format!("error getting documents: {}", error));
            }
        };

        let resp = serde_json::json!({
            "objectId": object_id,
            "candid": candid,
            "cutoutScience": BASE64_STANDARD.encode(&cutouts.cutout_science),
            "cutoutTemplate": BASE64_STANDARD.encode(&cutouts.cutout_template),
            "cutoutDifference": BASE64_STANDARD.encode(&cutouts.cutout_difference),
        });
        return response::ok(
            &format!("cutouts found for objectId: {}", object_id),
            serde_json::json!(resp),
        );
    }

    response::not_found("candid or objectId query parameter must be provided")
}
