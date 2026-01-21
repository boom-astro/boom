use crate::alert::{
    AlertCutout, LsstAliases, LsstCandidate, LsstForcedPhot, LsstObject, LsstPrvCandidate,
    ZtfAliases, ZtfCandidate, ZtfForcedPhot, ZtfObject, ZtfPrvCandidate,
};
use crate::api::models::response;
use crate::api::routes::babamul::surveys::alerts::{EnrichedLsstAlert, EnrichedZtfAlert};
use crate::api::routes::babamul::BabamulUser;
use crate::enrichment::{LsstAlertProperties, ZtfAlertClassifications, ZtfAlertProperties};
use crate::utils::enums::Survey;
use actix_web::{get, web, HttpResponse};
use base64::prelude::*;
use futures::TryStreamExt;
use mongodb::{bson::doc, Collection, Database};
use utoipa::ToSchema;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct ZtfObj {
    candid: i64,
    object_id: String,
    candidate: ZtfCandidate,
    properties: ZtfAlertProperties,
    cutout_science: serde_json::Value,
    cutout_template: serde_json::Value,
    cutout_difference: serde_json::Value,
    prv_candidates: Vec<ZtfPrvCandidate>,
    prv_nondetections: Vec<ZtfPrvCandidate>,
    fp_hists: Vec<ZtfForcedPhot>,
    classifications: ZtfAlertClassifications,
    cross_matches: serde_json::Value,
    aliases: ZtfAliases,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct LsstObj {
    candid: i64,
    object_id: String,
    candidate: LsstCandidate,
    properties: LsstAlertProperties,
    cutout_science: serde_json::Value,
    cutout_template: serde_json::Value,
    cutout_difference: serde_json::Value,
    prv_candidates: Vec<LsstPrvCandidate>,
    fp_hists: Vec<LsstForcedPhot>,
    cross_matches: serde_json::Value,
    aliases: LsstAliases,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct ObjResponse {
    status: String,
    message: String,
    data: ZtfObj,
}

/// Fetch an object from a given survey's alert stream by its object ID
#[utoipa::path(
    get,
    path = "/babamul/surveys/{survey}/objects/{object_id}",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
        ("object_id" = String, Path, description = "ID of the object to retrieve"),
    ),
    responses(
        (status = 200, description = "Object found", body = ObjResponse),
        (status = 404, description = "Object not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/babamul/surveys/{survey}/objects/{object_id}")]
pub async fn get_object(
    path: web::Path<(Survey, String)>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    // TODO: implement permissions for Babamul users, so we
    // can constrain access to certain surveys' datapoints
    let _current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };

    // Find options for getting most recent alert from alerts collection
    let find_options_recent = mongodb::options::FindOptions::builder()
        .sort(doc! {
            "candidate.jd": -1,
        })
        .limit(1)
        .build();

    let (survey, object_id) = path.into_inner();
    match survey {
        Survey::Ztf => {
            let alerts_collection: Collection<EnrichedZtfAlert> =
                db.collection(&format!("{}_alerts", survey));
            let cutout_collection: Collection<AlertCutout> =
                db.collection(&format!("{}_alerts_cutouts", survey));
            let aux_collection: Collection<ZtfObject> =
                db.collection(&format!("{}_alerts_aux", survey));

            // Get the most recent alert for the object
            let mut alert_cursor = match alerts_collection
                .find(doc! {
                    "objectId": &object_id,
                })
                .with_options(find_options_recent)
                .await
            {
                Ok(cursor) => cursor,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error retrieving latest alert for object {}: {}",
                        object_id, error
                    ));
                }
            };
            let newest_alert = match alert_cursor.try_next().await {
                Ok(Some(alert)) => alert,
                Ok(None) => {
                    return response::not_found(&format!("no object found with id {}", object_id));
                }
                Err(error) => {
                    return response::internal_error(&format!(
                        "error getting documents: {}",
                        error
                    ));
                }
            };

            // using the candid, query the cutout collection for the cutouts
            let candid = newest_alert.candid;
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
                    return response::internal_error(&format!(
                        "error getting documents: {}",
                        error
                    ));
                }
            };

            // Get crossmatches and light curve data from aux collection
            let aux_entry = match aux_collection
                .find_one(doc! {
                    "_id": &object_id,
                })
                .await
            {
                Ok(entry) => match entry {
                    Some(doc) => doc,
                    None => {
                        return response::not_found(&format!(
                            "no aux entry found for object id {}",
                            object_id
                        ));
                    }
                },
                Err(error) => {
                    return response::internal_error(&format!(
                        "error getting documents: {}",
                        error
                    ));
                }
            };

            let obj = ZtfObj {
                candid: newest_alert.candid,
                object_id: object_id.clone(),
                candidate: newest_alert.candidate,
                properties: newest_alert.properties,
                cutout_science: serde_json::json!(BASE64_STANDARD.encode(&cutouts.cutout_science)),
                cutout_template: serde_json::json!(BASE64_STANDARD.encode(&cutouts.cutout_template)),
                cutout_difference: serde_json::json!(
                    BASE64_STANDARD.encode(&cutouts.cutout_difference)
                ),
                prv_candidates: aux_entry.prv_candidates,
                prv_nondetections: aux_entry.prv_nondetections,
                fp_hists: aux_entry.fp_hists,
                classifications: newest_alert.classifications,
                cross_matches: serde_json::json!(aux_entry.cross_matches),
                aliases: aux_entry.aliases.unwrap_or_default(),
            };
            return response::ok(
                &format!("object found with object_id: {}", object_id),
                serde_json::json!(obj),
            );
        }
        Survey::Lsst => {
            let alerts_collection: Collection<EnrichedLsstAlert> =
                db.collection(&format!("{}_alerts", survey));
            let cutout_collection: Collection<AlertCutout> =
                db.collection(&format!("{}_alerts_cutouts", survey));
            let aux_collection: Collection<LsstObject> =
                db.collection(&format!("{}_alerts_aux", survey));

            // Get the most recent alert for the object
            let mut alert_cursor = match alerts_collection
                .find(doc! {
                    "objectId": &object_id,
                })
                .with_options(find_options_recent)
                .await
            {
                Ok(cursor) => cursor,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error retrieving latest alert for object {}: {}",
                        object_id, error
                    ));
                }
            };
            let newest_alert = match alert_cursor.try_next().await {
                Ok(Some(alert)) => alert,
                Ok(None) => {
                    return response::not_found(&format!("no object found with id {}", object_id));
                }
                Err(error) => {
                    return response::internal_error(&format!(
                        "error getting documents: {}",
                        error
                    ));
                }
            };
            // using the candid, query the cutout collection for the cutouts
            let candid = newest_alert.candid;
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
                    return response::internal_error(&format!(
                        "error getting documents: {}",
                        error
                    ));
                }
            };
            // Get crossmatches and light curve data from aux collection
            let aux_entry = match aux_collection
                .find_one(doc! {
                    "_id": &object_id,
                })
                .await
            {
                Ok(entry) => match entry {
                    Some(doc) => doc,
                    None => {
                        return response::not_found(&format!(
                            "no aux entry found for object id {}",
                            object_id
                        ));
                    }
                },
                Err(error) => {
                    return response::internal_error(&format!(
                        "error getting documents: {}",
                        error
                    ));
                }
            };

            let obj = LsstObj {
                candid: newest_alert.candid,
                object_id: object_id.clone(),
                candidate: newest_alert.candidate,
                properties: newest_alert.properties,
                cutout_science: serde_json::json!(BASE64_STANDARD.encode(&cutouts.cutout_science)),
                cutout_template: serde_json::json!(BASE64_STANDARD.encode(&cutouts.cutout_template)),
                cutout_difference: serde_json::json!(
                    BASE64_STANDARD.encode(&cutouts.cutout_difference)
                ),
                prv_candidates: aux_entry.prv_candidates,
                fp_hists: aux_entry.fp_hists,
                cross_matches: serde_json::json!(aux_entry.cross_matches),
                aliases: aux_entry.aliases.unwrap_or_default(),
            };
            return response::ok(
                &format!("object found with object_id: {}", object_id),
                serde_json::json!(obj),
            );
        }
        _ => {
            return response::bad_request(
                "Invalid survey specified, only ZTF and LSST are supported",
            );
        }
    }
}
