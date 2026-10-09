use crate::api::cutouts::{
    cutouts_to_json, fetch_alert_jds, retrieve_batch_cutouts, validate_batch_candids,
    AlertCandidOnly, AlertJdOnly, BatchCutoutQuery, CutoutQuery, WhichCutouts,
};
use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use crate::utils::cutouts::{CutoutStorage, CutoutStorageError};
use crate::utils::enums::Survey;
use crate::utils::lightcurves::Band;
use actix_web::{get, post, web, HttpResponse};
use mongodb::{bson::doc, Database};
use std::collections::{HashMap, HashSet};

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
    cutout_storages: web::Data<HashMap<Survey, CutoutStorage>>,
) -> HttpResponse {
    let _current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };
    let survey = path.into_inner();
    if survey != Survey::Ztf && survey != Survey::Lsst {
        return response::bad_request(&format!(
            "Unsupported survey: {}. Supported surveys are: ztf, lsst",
            survey
        ));
    }

    let cutout_storage = match cutout_storages.get(&survey) {
        Some(storage) => storage,
        None => {
            return response::internal_error("cutout storage not available for this survey");
        }
    };

    if let Some(candid) = query.candid {
        let cutouts = match cutout_storage.retrieve_cutouts(candid, false).await {
            Ok(cutouts) => cutouts,
            Err(CutoutStorageError::CutoutsNotFound) => {
                return response::not_found(&format!("no cutouts found for candid {}", candid));
            }
            Err(error) => {
                tracing::error!("Error retrieving cutouts from storage: {}", error);
                return response::internal_error("error retrieving cutouts from storage");
            }
        };
        let resp = cutouts_to_json(&cutouts);
        return response::ok(&format!("cutouts found for candid: {}", candid), resp);
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
                .sort(doc! { "candidate.magpsf": 1 }) // Lowest mag is brightest, so sort in ascending order
                .build(),
            WhichCutouts::Faintest => mongodb::options::FindOneOptions::builder()
                .sort(doc! { "candidate.magpsf": -1 }) // Highest mag is faintest, so sort in descending order
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

        let cutouts = match cutout_storage.retrieve_cutouts(candid, false).await {
            Ok(cutouts) => cutouts,
            Err(CutoutStorageError::CutoutsNotFound) => {
                return response::not_found(&format!(
                    "no cutouts found for objectId {} (candid: {})",
                    object_id, candid
                ));
            }
            Err(error) => {
                tracing::error!("Error retrieving cutouts from storage: {}", error);
                return response::internal_error("error retrieving cutouts from storage");
            }
        };

        let resp = cutouts_to_json(&cutouts);
        return response::ok(&format!("cutouts found for objectId: {}", object_id), resp);
    }

    response::bad_request("candid or objectId query parameter must be provided")
}

/// Get image cutouts for a batch of alerts
///
/// Returns the cutouts found, in request order, and lists the candids that
/// have none under `missing` rather than failing the whole request. For ZTF,
/// only public (programid 1) alerts are served; any other candid is reported
/// as missing.
#[utoipa::path(
    post,
    path = "/babamul/surveys/{survey}/cutouts",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
    ),
    request_body = BatchCutoutQuery,
    responses(
        (status = 200, description = "Cutouts retrieved successfully", body = serde_json::Value),
        (status = 400, description = "Invalid request"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[post("/surveys/{survey}/cutouts")]
pub async fn get_batch_cutouts(
    path: web::Path<Survey>,
    body: web::Json<BatchCutoutQuery>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
    cutout_storages: web::Data<HashMap<Survey, CutoutStorage>>,
) -> HttpResponse {
    if current_user.is_none() {
        return HttpResponse::Unauthorized().body("Unauthorized");
    }
    let survey = path.into_inner();
    if survey != Survey::Ztf && survey != Survey::Lsst {
        return response::bad_request(&format!(
            "Unsupported survey: {}. Supported surveys are: ztf, lsst",
            survey
        ));
    }
    let cutout_storage = match cutout_storages.get(&survey) {
        Some(storage) => storage,
        None => {
            return response::internal_error("cutout storage not available for this survey");
        }
    };
    let requested = match validate_batch_candids(&body.candids) {
        Ok(candids) => candids,
        Err(message) => return response::bad_request(&message),
    };

    // One alerts query gives each candid's jd and, for ZTF, doubles as the
    // gate: only public alerts may be served. Candids that aren't are dropped
    // before touching storage and reported as missing, so the response doesn't
    // reveal whether a private alert exists.
    let alert_collection = db.collection::<AlertJdOnly>(&format!("{}_alerts", survey));
    let filter = if survey == Survey::Ztf {
        doc! { "candidate.programid": 1 }
    } else {
        doc! {}
    };
    let jds = match fetch_alert_jds(&alert_collection, &requested, filter).await {
        Ok(jds) => jds,
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    };
    let allowed: Vec<i64> = if survey == Survey::Ztf {
        requested
            .iter()
            .copied()
            .filter(|c| jds.contains_key(c))
            .collect()
    } else {
        requested.clone()
    };

    let mut data = if allowed.is_empty() {
        serde_json::json!({ "cutouts": [], "missing": [] })
    } else {
        match retrieve_batch_cutouts(cutout_storage, &allowed, &jds).await {
            Ok(data) => data,
            Err(error) => {
                tracing::error!("Error retrieving cutouts from storage: {}", error);
                return response::internal_error("error retrieving cutouts from storage");
            }
        }
    };
    // Report every requested candid without cutouts, in request order,
    // whether storage had none or it was filtered out above.
    let served: HashSet<i64> = data["cutouts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c["candid"].as_i64())
        .collect();
    data["missing"] = requested
        .iter()
        .copied()
        .filter(|c| !served.contains(c))
        .collect::<Vec<_>>()
        .into();

    response::ok(
        &format!("cutouts retrieved for {} candids", requested.len()),
        data,
    )
}
