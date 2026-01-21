use crate::alert::{
    AlertCutout, LsstCandidate, LsstForcedPhot, LsstObject, ZtfCandidate, ZtfForcedPhot, ZtfObject,
    ZtfPrvCandidate, LSST_ZTF_XMATCH_RADIUS, ZTF_LSST_XMATCH_RADIUS,
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
struct LsstMatch {
    object_id: String,
    ra: f64,
    dec: f64,
    prv_candidates: Vec<LsstCandidate>,
    fp_hists: Vec<LsstForcedPhot>,
    distance_arcsec: f64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct ZtfSurveyMatches {
    lsst: Option<LsstMatch>,
}

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
    classifications_history: Vec<ZtfAlertClassifications>,
    cross_matches: serde_json::Value,
    survey_matches: ZtfSurveyMatches,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct ZtfMatch {
    object_id: String,
    ra: f64,
    dec: f64,
    prv_candidates: Vec<ZtfPrvCandidate>,
    prv_nondetections: Vec<ZtfPrvCandidate>,
    fp_hists: Vec<ZtfForcedPhot>,
    distance_arcsec: f64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct LsstSurveyMatches {
    ztf: Option<ZtfMatch>,
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
    survey_matches: LsstSurveyMatches,
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
            let lsst_aux_collection: Collection<LsstObject> =
                db.collection(&format!("LSST_alerts_aux"));

            // We get all the alerts, to build the classification history and find the newest
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
            let mut newest_alert = None;
            let mut classifications_history = vec![];
            loop {
                match alert_cursor.try_next().await {
                    Ok(Some(alert)) => {
                        // Push classification to history
                        classifications_history.push(alert.classifications.clone());

                        // Update newest_alert only if not set yet (first iteration)
                        if newest_alert.is_none() {
                            newest_alert = Some(alert);
                        }
                    }
                    Ok(None) => break, // No more alerts
                    Err(error) => {
                        return response::internal_error(&format!(
                            "error getting documents: {}",
                            error
                        ));
                    }
                }
            }
            let newest_alert = match newest_alert {
                Some(alert) => alert,
                None => {
                    return response::not_found(&format!("no object found with id {}", object_id));
                }
            };
            // reverse classification history, to have it in chronological order
            classifications_history.reverse();

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

            // Get the nearest LsstObject if any. We use a near query on the aux collection
            let (ra, dec) = aux_entry.coordinates.get_radec();
            let nearest_lsst = match lsst_aux_collection
                .find_one(doc! {
                    "coordinates.radec_geojson": {
                        "$nearSphere": [ra - 180.0, dec],
                        "$maxDistance": ZTF_LSST_XMATCH_RADIUS,
                    },
                })
                .await
            {
                Ok(entry) => entry,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error getting nearest lsst object: {}",
                        error
                    ));
                }
            };

            let survey_matches = ZtfSurveyMatches {
                lsst: match nearest_lsst {
                    Some(lsst_obj) => {
                        let lsst_radec = lsst_obj.coordinates.get_radec();
                        Some(LsstMatch {
                            object_id: lsst_obj.object_id,
                            ra: lsst_radec.0,
                            dec: lsst_radec.1,
                            prv_candidates: lsst_obj.prv_candidates,
                            fp_hists: lsst_obj.fp_hists,
                            distance_arcsec: flare::spatial::great_circle_distance(
                                ra,
                                dec,
                                lsst_radec.0,
                                lsst_radec.1,
                            ) * 3600.0,
                        })
                    }
                    None => None,
                },
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
                classifications_history,
                cross_matches: serde_json::json!(aux_entry.cross_matches),
                survey_matches,
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
            let ztf_aux_collection: Collection<ZtfObject> =
                db.collection(&format!("ZTF_alerts_aux"));

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

            // Get the nearest ZtfObject if any. We use a near query on the aux collection
            let (ra, dec) = aux_entry.coordinates.get_radec();
            let nearest_ztf = match ztf_aux_collection
                .find_one(doc! {
                    "coordinates.radec_geojson": {
                        "$nearSphere": [ra - 180.0, dec],
                        "$maxDistance": LSST_ZTF_XMATCH_RADIUS,
                    },
                })
                .await
            {
                Ok(entry) => entry,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error getting nearest ztf object: {}",
                        error
                    ));
                }
            };

            let survey_matches = LsstSurveyMatches {
                ztf: match nearest_ztf {
                    Some(ztf_obj) => {
                        let ztf_radec = ztf_obj.coordinates.get_radec();
                        Some(ZtfMatch {
                            object_id: ztf_obj.object_id,
                            ra: ztf_radec.0,
                            dec: ztf_radec.1,
                            prv_candidates: ztf_obj.prv_candidates,
                            prv_nondetections: ztf_obj.prv_nondetections,
                            fp_hists: ztf_obj.fp_hists,
                            distance_arcsec: flare::spatial::great_circle_distance(
                                ra,
                                dec,
                                ztf_radec.0,
                                ztf_radec.1,
                            ) * 3600.0,
                        })
                    }
                    None => None,
                },
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
                survey_matches,
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
