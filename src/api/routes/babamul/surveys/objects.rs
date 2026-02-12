use crate::alert::{
    AlertCutout, LsstCandidate, LsstForcedPhot, LsstObject, LsstPrvCandidate, ZtfCandidate,
    ZtfForcedPhot, ZtfObject, ZtfPrvCandidate, LSST_ZTF_XMATCH_RADIUS, ZTF_LSST_XMATCH_RADIUS,
};
use crate::api::models::response;
use crate::api::routes::babamul::surveys::alerts::{EnrichedLsstAlert, EnrichedZtfAlert};
use crate::api::routes::babamul::BabamulUser;
use crate::enrichment::{LsstAlertProperties, ZtfAlertClassifications, ZtfAlertProperties};
use crate::utils::enums::Survey;
use crate::utils::spatial::Coordinates;
use actix_web::{get, post, web, HttpResponse};
use base64::prelude::*;
use futures::TryStreamExt;
use mongodb::{bson::doc, Collection, Database};
use regex::Regex;
use std::collections::HashMap;
use std::sync::OnceLock;
use utoipa::ToSchema;

static ZTF_PREFIX_REGEX: OnceLock<Regex> = OnceLock::new();
static ZTF_NO_PREFIX_REGEX: OnceLock<Regex> = OnceLock::new();
static LSST_PREFIX_REGEX: OnceLock<Regex> = OnceLock::new();

fn get_ztf_prefix_regex() -> &'static Regex {
    ZTF_PREFIX_REGEX.get_or_init(|| Regex::new(r"^ZTF(\d{1,2})([a-zA-Z]{0,7})$").unwrap())
}

fn get_ztf_no_prefix_regex() -> &'static Regex {
    ZTF_NO_PREFIX_REGEX.get_or_init(|| Regex::new(r"^(\d{2})([a-zA-Z]{1,7})$").unwrap())
}

fn get_lsst_prefix_regex() -> &'static Regex {
    LSST_PREFIX_REGEX.get_or_init(|| Regex::new(r"^LSST(\d+)$").unwrap())
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct LsstMatch {
    #[serde(rename = "objectId")]
    object_id: String,
    ra: f64,
    dec: f64,
    prv_candidates: Vec<LsstPrvCandidate>,
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
    #[serde(rename = "objectId")]
    object_id: String,
    candidate: ZtfCandidate,
    properties: Option<ZtfAlertProperties>,
    #[serde(rename = "cutoutScience")]
    cutout_science: serde_json::Value,
    #[serde(rename = "cutoutTemplate")]
    cutout_template: serde_json::Value,
    #[serde(rename = "cutoutDifference")]
    cutout_difference: serde_json::Value,
    prv_candidates: Vec<ZtfPrvCandidate>,
    prv_nondetections: Vec<ZtfPrvCandidate>,
    fp_hists: Vec<ZtfForcedPhot>,
    classifications: Option<ZtfAlertClassifications>,
    classifications_history: Vec<ZtfAlertClassifications>,
    cross_matches: serde_json::Value,
    survey_matches: ZtfSurveyMatches,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct ZtfMatch {
    #[serde(rename = "objectId")]
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
    #[serde(rename = "objectId")]
    object_id: String,
    candidate: LsstCandidate,
    properties: Option<LsstAlertProperties>,
    #[serde(rename = "cutoutScience")]
    cutout_science: serde_json::Value,
    #[serde(rename = "cutoutTemplate")]
    cutout_template: serde_json::Value,
    #[serde(rename = "cutoutDifference")]
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
#[get("/surveys/{survey}/objects/{object_id}")]
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
                    "candidate.programid": 1, // Babamul only returns public ZTF alerts
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
                        if let Some(classifications) = &alert.classifications {
                            classifications_history.push(classifications.clone());
                        }

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
                // Limit photometry to programid 1 (public ZTF alerts)
                prv_candidates: aux_entry
                    .prv_candidates
                    .into_iter()
                    .filter(|c| c.prv_candidate.programid == 1)
                    .collect(),
                prv_nondetections: aux_entry
                    .prv_nondetections
                    .into_iter()
                    .filter(|c| c.prv_candidate.programid == 1)
                    .collect(),
                fp_hists: aux_entry
                    .fp_hists
                    .into_iter()
                    .filter(|c| c.fp_hist.programid == 1)
                    .collect(),
                classifications: newest_alert.classifications,
                classifications_history,
                cross_matches: serde_json::json!(aux_entry.cross_matches),
                survey_matches,
            };
            return response::ok_ser(&format!("object found with object_id: {}", object_id), obj);
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
                            // Limit photometry to programid 1 (public ZTF alerts)
                            prv_candidates: ztf_obj
                                .prv_candidates
                                .into_iter()
                                .filter(|c| c.prv_candidate.programid == 1)
                                .collect(),
                            prv_nondetections: ztf_obj
                                .prv_nondetections
                                .into_iter()
                                .filter(|c| c.prv_candidate.programid == 1)
                                .collect(),
                            fp_hists: ztf_obj
                                .fp_hists
                                .into_iter()
                                .filter(|c| c.fp_hist.programid == 1)
                                .collect(),
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
            return response::ok_ser(&format!("object found with object_id: {}", object_id), obj);
        }
        _ => {
            return response::bad_request(
                "Invalid survey specified, only ZTF and LSST are supported",
            );
        }
    }
}

fn ztf_bad_formatting_message(value: &str) -> String {
    format!(
        "Invalid objectId format: {}. ZTF names must look like ZTF + YY + 7 letters (partial is accepted, can omit the ZTF prefix)",
        value
    )
}

/// Infer survey from objectId value and return normalized id
fn infer_survey_from_objectid(value: &str) -> Result<(Survey, String), String> {
    let trimmed = value.trim();
    let upper = trimmed.to_ascii_uppercase();

    // Handle bare prefix: Z, ZT, or ZTF without any suffix
    if upper == "Z" || upper == "ZT" || upper == "ZTF" {
        return Ok((Survey::Ztf, "ZTF".to_string()));
    }

    // ZTF with complete prefix: only accept full "ZTF" when followed by digits/letters
    let ztf_prefix_re = get_ztf_prefix_regex();
    if let Some(caps) = ztf_prefix_re.captures(&upper) {
        let digits = caps.get(1).unwrap().as_str();
        let letters = caps.get(2).map(|m| m.as_str()).unwrap_or("");

        // If we have letters, require exactly 2 digits
        if !letters.is_empty() && digits.len() != 2 {
            return Err(ztf_bad_formatting_message(value));
        }

        let normalized = format!("ZTF{}{}", digits, letters.to_lowercase());
        return Ok((Survey::Ztf, normalized));
    }

    // ZTF without prefix: 2 digits followed by up to 7 letters -> prepend ZTF
    let ztf_no_prefix_re = get_ztf_no_prefix_regex();
    if let Some(caps) = ztf_no_prefix_re.captures(trimmed) {
        let digits = caps.get(1).unwrap().as_str();
        let letters = caps.get(2).unwrap().as_str();
        return Ok((
            Survey::Ztf,
            format!("ZTF{}{}", digits, letters.to_lowercase()),
        ));
    }

    // Let's have a similar logic for LSST. If we start with L, LS, LSS, or LSST
    if upper == "L" || upper == "LS" || upper == "LSS" || upper == "LSST" {
        return Ok((Survey::Lsst, "".to_string()));
    }

    // then if we have LSST + digits (any length is fine), accept that and return just the digits
    let lsst_re = get_lsst_prefix_regex();
    if let Some(caps) = lsst_re.captures(&upper) {
        let digits = caps.get(1).unwrap().as_str();
        return Ok((Survey::Lsst, digits.to_string()));
    }

    // LSST numeric id
    if trimmed.parse::<u64>().is_ok() {
        return Ok((Survey::Lsst, trimmed.to_string()));
    }

    Err(format!(
        "Invalid objectId format: {}. Could not infer survey from given value",
        value
    ))
}

#[derive(Debug, serde::Deserialize)]
pub struct SearchObjectsQuery {
    object_id: String,
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    10
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct SearchObjectResult {
    #[serde(rename = "objectId")]
    object_id: String,
    ra: f64,
    dec: f64,
    survey: Survey,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ObjectMini {
    #[serde(rename = "_id")]
    object_id: String,
    coordinates: Coordinates,
}

/// Search for objects by partial object ID across surveys. Supports ZTF and LSST, and returns id, ra, dec for up to `limit` results.
#[utoipa::path(
    get,
    path = "/babamul/objects",
    params(
        ("object_id" = String, Query, description = "Partial object ID to search for"),
        ("limit" = Option<u32>, Query, description = "Maximum number of results to return (1-100, default 10)"),
    ),
    responses(
        (status = 200, description = "Search results", body = Vec<SearchObjectResult>),
        (status = 400, description = "Invalid objectId format"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/objects")]
pub async fn get_objects(
    query: web::Query<SearchObjectsQuery>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    let _current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };

    // Validate limit is within bounds
    let limit = if query.limit < 1 || query.limit > 100 {
        return response::bad_request("Limit must be between 1 and 100");
    } else {
        query.limit as i64
    };

    // Infer survey from objectId (and normalize id casing for ZTF)
    let (survey, normalized_id) = match infer_survey_from_objectid(&query.object_id) {
        Ok(pair) => pair,
        Err(e) => return response::bad_request(&e),
    };

    let collection = db.collection::<ObjectMini>(&format!("{}_alerts_aux", survey));

    // Anchor regex to the start so we only match ordered prefixes
    let filter = doc! {
        "_id": {
            "$regex": format!("^{}", normalized_id),
        }
    };

    match collection
        .find(filter)
        .sort(doc! { "_id": 1 })
        .limit(limit)
        .await
    {
        Ok(mut cursor) => {
            let mut results = vec![];
            loop {
                match cursor.try_next().await {
                    Ok(Some(obj)) => {
                        let (ra, dec) = obj.coordinates.get_radec();
                        results.push(SearchObjectResult {
                            object_id: obj.object_id,
                            ra,
                            dec,
                            survey: survey.clone(),
                        });
                    }
                    Ok(None) => break,
                    Err(error) => {
                        return response::internal_error(&format!(
                            "error searching objects: {}",
                            error
                        ));
                    }
                }
            }
            response::ok_ser(&format!("Found {} objects", results.len()), results)
        }
        Err(error) => response::internal_error(&format!("error searching objects: {}", error)),
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct ObjectsConeSearchQuery {
    coordinates: HashMap<String, [f64; 2]>,
    radius_arcsec: f64,
}

/// Perform a cone search around given coordinates for a specified survey.
#[utoipa::path(
    post,
    path = "/babamul/surveys/{survey}/objects/cone_search",
    params(
        ("survey" = Survey, Path, description = "Survey to search in (e.g., ztf, lsst)"),
    ),
    request_body = ObjectsConeSearchQuery,
    responses(
        (status = 200, description = "Cone search results", body = HashMap<String, Vec<SearchObjectResult>>),
        (status = 400, description = "Invalid query parameters"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[post("/surveys/{survey}/objects/cone_search")]
pub async fn cone_search_objects(
    path: web::Path<Survey>,
    query: web::Json<ObjectsConeSearchQuery>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    let _current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };

    let radius_arcsec = query.radius_arcsec;
    if radius_arcsec <= 0.0 || radius_arcsec > 600.0 {
        return response::bad_request("radius_arcsec must be between 0 and 600");
    }

    let radius_radians = (radius_arcsec / 3600.0).to_radians();
    let mut results: HashMap<String, Vec<SearchObjectResult>> = HashMap::new();

    let survey = path.into_inner();
    let collection = db.collection::<ObjectMini>(&format!("{}_alerts_aux", survey));
    for (name, coords) in query.coordinates.iter() {
        let filter = doc! {
            "coordinates.radec_geojson": {
                "$nearSphere": [coords[0] - 180.0, coords[1]],
                "$maxDistance": radius_radians,
            }
        };

        match collection.find(filter).await {
            Ok(mut cursor) => {
                let mut matches = vec![];
                while let Ok(Some(obj)) = cursor.try_next().await {
                    matches.push(SearchObjectResult {
                        object_id: obj.object_id,
                        ra: obj.coordinates.get_radec().0,
                        dec: obj.coordinates.get_radec().1,
                        survey: survey.clone(),
                    });
                }
                results.insert(name.clone(), matches);
            }
            Err(error) => {
                return response::internal_error(&format!(
                    "error performing cone search for {}: {}",
                    name, error
                ));
            }
        }
    }
    response::ok_ser("Cone search completed", results)
}
