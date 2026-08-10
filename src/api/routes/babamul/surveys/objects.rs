use crate::alert::{
    LsstCandidate, LsstForcedPhot, LsstObject, LsstPrvCandidate, ZtfCandidate, ZtfForcedPhot,
    ZtfObject, ZtfPrvCandidate, LSST_ZTF_XMATCH_RADIUS, ZTF_LSST_XMATCH_RADIUS,
};
use crate::api::cutouts::AlertCandidOnly;
use crate::api::models::response;
use crate::api::routes::babamul::surveys::alerts::{EnrichedLsstAlert, EnrichedZtfAlert};
use crate::api::routes::babamul::BabamulUser;
use crate::enrichment::models::{
    classify_with_service, find_model_spec, HyraxBackend, HyraxInput, HyraxModel,
    HyraxModelRegistry, HyraxModelSpec, HyraxPredictError, Model, ServiceBackend, HYRAX_MODELS,
};
use crate::enrichment::{LsstAlertProperties, ZtfAlertClassifications, ZtfAlertProperties};
use crate::utils::cutouts::{CutoutStorage, CutoutStorageError};
use crate::utils::enums::Survey;
use crate::utils::spatial::Coordinates;
use actix_web::{get, post, web, HttpResponse};
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
    object_id: Option<String>,
    ra: Option<f64>,
    dec: Option<f64>,
    radius: Option<f64>,
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
    survey: Survey,
    ra: f64,
    dec: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    distance_arcsec: Option<f64>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ObjectMini {
    #[serde(rename = "_id")]
    object_id: String,
    coordinates: Coordinates,
}

/// Search for objects by partial object ID or sky position across surveys.
///
/// Provide either `object_id` (survey is auto-inferred) or all three of `ra` / `dec` / `radius`
/// for a cross-survey cone search over both ZTF and LSST. The two modes are mutually exclusive.
#[utoipa::path(
    get,
    path = "/babamul/objects",
    params(
        ("object_id" = Option<String>, Query, description = "Partial object ID to search for (mutually exclusive with ra/dec/radius)"),
        ("ra" = Option<f64>, Query, description = "Right ascension in degrees [0, 360) for cone search"),
        ("dec" = Option<f64>, Query, description = "Declination in degrees [-90, 90] for cone search"),
        ("radius" = Option<f64>, Query, description = "Search radius in arcseconds (0, 600] for cone search"),
        ("limit" = Option<u32>, Query, description = "Maximum number of results to return (1-100, default 10)"),
    ),
    responses(
        (status = 200, description = "Search results", body = Vec<SearchObjectResult>),
        (status = 400, description = "Invalid query parameters"),
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

    let limit = if query.limit < 1 || query.limit > 100 {
        return response::bad_request("Limit must be between 1 and 100");
    } else {
        query.limit as i64
    };

    let has_object_id = query.object_id.is_some();
    let has_position = query.ra.is_some() || query.dec.is_some() || query.radius.is_some();

    if has_object_id && has_position {
        return response::bad_request("Provide either object_id or ra/dec/radius, not both");
    }
    if !has_object_id && !has_position {
        return response::bad_request("Must provide either object_id or ra/dec/radius");
    }

    if has_object_id {
        let object_id = query.object_id.as_deref().unwrap();
        let (survey, normalized_id) = match infer_survey_from_objectid(object_id) {
            Ok(pair) => pair,
            Err(e) => return response::bad_request(&e),
        };

        let collection = db.collection::<ObjectMini>(&format!("{}_alerts_aux", survey));
        let filter = doc! {
            "_id": { "$regex": format!("^{}", normalized_id) }
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
                                distance_arcsec: None,
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
    } else {
        let (ra, dec, radius_arcsec) = match (query.ra, query.dec, query.radius) {
            (Some(ra), Some(dec), Some(r)) => (ra, dec, r),
            _ => {
                return response::bad_request(
                    "Must provide ra, dec, and radius together for position search",
                )
            }
        };

        if ra < 0.0 || ra >= 360.0 {
            return response::bad_request("ra must be in [0, 360)");
        }
        if dec < -90.0 || dec > 90.0 {
            return response::bad_request("dec must be in [-90, 90]");
        }
        if radius_arcsec <= 0.0 || radius_arcsec > 600.0 {
            return response::bad_request(
                "radius must be greater than 0 and at most 600 arcseconds",
            );
        }

        let radius_radians = (radius_arcsec / 3600.0_f64).to_radians();
        let near_filter = doc! {
            "coordinates.radec_geojson": {
                "$nearSphere": [ra - 180.0, dec],
                "$maxDistance": radius_radians,
            }
        };

        let mut results: Vec<SearchObjectResult> = vec![];

        let ztf_collection = db.collection::<ObjectMini>("ZTF_alerts_aux");
        match ztf_collection.find(near_filter.clone()).limit(limit).await {
            Ok(mut cursor) => {
                while let Ok(Some(obj)) = cursor.try_next().await {
                    let (obj_ra, obj_dec) = obj.coordinates.get_radec();
                    results.push(SearchObjectResult {
                        object_id: obj.object_id,
                        ra: obj_ra,
                        dec: obj_dec,
                        survey: Survey::Ztf,
                        distance_arcsec: Some(
                            flare::spatial::great_circle_distance(ra, dec, obj_ra, obj_dec)
                                * 3600.0,
                        ),
                    });
                }
            }
            Err(error) => {
                return response::internal_error(&format!(
                    "error searching ZTF objects: {}",
                    error
                ));
            }
        }

        let lsst_collection = db.collection::<ObjectMini>("LSST_alerts_aux");
        match lsst_collection.find(near_filter).limit(limit).await {
            Ok(mut cursor) => {
                while let Ok(Some(obj)) = cursor.try_next().await {
                    let (obj_ra, obj_dec) = obj.coordinates.get_radec();
                    results.push(SearchObjectResult {
                        object_id: obj.object_id,
                        ra: obj_ra,
                        dec: obj_dec,
                        survey: Survey::Lsst,
                        distance_arcsec: Some(
                            flare::spatial::great_circle_distance(ra, dec, obj_ra, obj_dec)
                                * 3600.0,
                        ),
                    });
                }
            }
            Err(error) => {
                return response::internal_error(&format!(
                    "error searching LSST objects: {}",
                    error
                ));
            }
        }

        results.sort_by(|a, b| {
            a.distance_arcsec
                .unwrap_or(f64::MAX)
                .partial_cmp(&b.distance_arcsec.unwrap_or(f64::MAX))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit as usize);

        response::ok_ser(&format!("Found {} objects", results.len()), results)
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
    path = "/babamul/surveys/{survey}/objects/cone-search",
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
#[post("/surveys/{survey}/objects/cone-search")]
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

    // we must have more than 0 and less than 1000 coordinate pairs
    // to prevent expensive queries that could potentially timeout the server
    let coordinates = &query.coordinates;
    if coordinates.is_empty() || coordinates.len() > 1000 {
        return response::bad_request(
            "Must provide between 1 and 1000 coordinate pairs for cone search",
        );
    }

    let radius_radians = (radius_arcsec / 3600.0).to_radians();
    let mut results: HashMap<String, Vec<SearchObjectResult>> = HashMap::new();

    let survey = path.into_inner();
    let collection = db.collection::<ObjectMini>(&format!("{}_alerts_aux", survey));
    for (object_name, radec) in coordinates {
        if radec.len() != 2 {
            return response::bad_request(&format!(
                "Invalid coordinates for {}: must be an array of [ra, dec]",
                object_name
            ));
        }

        let ra = radec[0];
        let dec = radec[1];
        if ra < 0.0 || ra >= 360.0 {
            return response::bad_request(&format!(
                "Invalid RA for object {}: must be in [0, 360)",
                object_name
            ));
        }
        if dec < -90.0 || dec > 90.0 {
            return response::bad_request(&format!(
                "Invalid Dec for object {}: must be in [-90, 90]",
                object_name
            ));
        }
        let filter = doc! {
            "coordinates.radec_geojson": {
                "$geoWithin": {
                    "$centerSphere": [
                        [ra - 180.0, dec],
                        radius_radians
                    ]
                }
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
                        distance_arcsec: None,
                    });
                }
                results.insert(object_name.clone(), matches);
            }
            Err(error) => {
                return response::internal_error(&format!(
                    "error performing cone search for {}: {}",
                    object_name, error
                ));
            }
        }
    }
    response::ok_ser("Cone search completed", results)
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct XmatchResponse {
    status: String,
    message: String,
    data: serde_json::Value,
}

/// Fetch an object's cross-matches (from a given survey's alert stream) by its object ID
#[utoipa::path(
    get,
    path = "/babamul/surveys/{survey}/objects/{object_id}/cross-matches",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
        ("object_id" = String, Path, description = "ID of the object to retrieve"),
    ),
    responses(
        (status = 200, description = "Object found", body = XmatchResponse),
        (status = 404, description = "Object not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey}/objects/{object_id}/cross-matches")]
pub async fn get_object_xmatches(
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

    let (survey, object_id) = path.into_inner();
    if survey != Survey::Ztf && survey != Survey::Lsst {
        return response::bad_request(&format!(
            "Unsupported survey: {}. Supported surveys are: ztf, lsst",
            survey
        ));
    }
    let aux_collection: Collection<mongodb::bson::Document> =
        db.collection(&format!("{}_alerts_aux", survey));

    let aux_entry = match aux_collection
        .find_one(doc! {
            "_id": &object_id,
        })
        .projection(doc! {
            "_id": 1,
            "cross_matches": 1,
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
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    };

    let cross_matches: HashMap<String, Vec<mongodb::bson::Document>> =
        match aux_entry.get_document("cross_matches") {
            Ok(matches_doc) => matches_doc
                .iter()
                .map(|(catalog, matches)| {
                    let matches_array = match matches.as_array() {
                        Some(arr) => arr
                            .iter()
                            .filter_map(|m| m.as_document().cloned())
                            .collect::<Vec<mongodb::bson::Document>>(),
                        None => vec![],
                    };
                    (catalog.clone(), matches_array)
                })
                .collect::<HashMap<String, Vec<mongodb::bson::Document>>>(),
            Err(_) => HashMap::new(),
        };

    let response = XmatchResponse {
        status: "success".to_string(),
        message: format!("Found cross-matches for object {}", object_id),
        data: serde_json::json!(cross_matches),
    };
    HttpResponse::Ok().json(response)
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct BatchXmatchQuery {
    #[serde(rename = "objectIds", alias = "object_ids")]
    object_ids: Vec<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct BatchXmatchResponse {
    status: String,
    message: String,
    // Map of object ID to their cross-matches, where cross-matches are represented as a map of catalog name
    // to list of matches (as JSON values since the structure can vary widely between catalogs)
    data: HashMap<String, HashMap<String, Vec<serde_json::Value>>>,
}

/// Fetch cross-matches for a batch of object IDs.
#[utoipa::path(
    post,
    path = "/babamul/surveys/{survey}/objects/cross-matches",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
    ),
    request_body = BatchXmatchQuery,
    responses(
        (status = 200, description = "Cross-matches found", body = BatchXmatchResponse),
        (status = 400, description = "Invalid request"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[post("/surveys/{survey}/objects/cross-matches")]
pub async fn get_objects_xmatches(
    path: web::Path<(Survey,)>,
    query: web::Json<BatchXmatchQuery>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    let _current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };
    let survey = path.into_inner().0;
    if survey != Survey::Ztf && survey != Survey::Lsst {
        return response::bad_request(&format!(
            "Unsupported survey: {}. Supported surveys are: ztf, lsst",
            survey
        ));
    }
    let aux_collection: Collection<mongodb::bson::Document> =
        db.collection(&format!("{}_alerts_aux", survey));

    let object_ids = &query.object_ids;
    // We require at least 1 and at most 1000 object IDs to prevent expensive queries that could potentially timeout the server
    if object_ids.is_empty() || object_ids.len() > 1000 {
        return response::bad_request("Must provide between 1 and 1000 object IDs");
    }
    let mut cursor = match aux_collection
        .find(doc! {
            "_id": { "$in": object_ids },
        })
        .projection(doc! {
            "_id": 1,
            "cross_matches": 1,
        })
        .await
    {
        Ok(cursor) => cursor,
        Err(error) => {
            return response::internal_error(&format!("error querying database: {}", error));
        }
    };

    let mut cross_matches_map = HashMap::new();
    while let Ok(Some(doc)) = cursor.try_next().await {
        let object_id = match doc.get_str("_id") {
            Ok(id) => id.to_string(),
            Err(_) => continue,
        };
        let cross_matches: HashMap<String, Vec<serde_json::Value>> = match doc
            .get_document("cross_matches")
        {
            Ok(matches_doc) => matches_doc
                .iter()
                .map(|(catalog, matches)| {
                    let matches_array = match matches.as_array() {
                        Some(arr) => arr
                            .iter()
                            .filter_map(|m| m.as_document().cloned())
                            .map(|doc| serde_json::to_value(doc).unwrap_or(serde_json::Value::Null))
                            .collect::<Vec<serde_json::Value>>(),
                        None => vec![],
                    };
                    (catalog.clone(), matches_array)
                })
                .collect::<HashMap<String, Vec<serde_json::Value>>>(),
            Err(_) => HashMap::new(),
        };
        cross_matches_map.insert(object_id, cross_matches);
    }

    let response = BatchXmatchResponse {
        status: "success".to_string(),
        message: format!(
            "Found cross-matches for {} objects",
            cross_matches_map.len()
        ),
        data: cross_matches_map,
    };
    HttpResponse::Ok().json(response)
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct ClassificationModelInfo {
    /// Identifier to send back in the `model` field of a classify request.
    id: String,
    name: String,
    description: String,
    /// Class labels this model emits, in output order. Empty when they are not known
    /// ahead of a run: single-score models, and models whose backend names its own
    /// classes in the result.
    classes: Vec<String>,
}

/// List the Hyrax models that can be run on demand against objects of a survey.
///
/// Every model registered for the survey is reported and every one of them can be
/// selected: whether a model's backend can answer is decided when it is run, not by
/// inspecting the server's `data/models/` directory here.
#[utoipa::path(
    get,
    path = "/babamul/surveys/{survey}/classification-models",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
    ),
    responses(
        (status = 200, description = "Available models", body = Vec<ClassificationModelInfo>),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey}/classification-models")]
pub async fn get_classification_models(path: web::Path<Survey>) -> HttpResponse {
    let survey = path.into_inner();
    let models: Vec<ClassificationModelInfo> = HYRAX_MODELS
        .iter()
        .filter(|spec| spec.surveys.contains(&survey))
        .map(|spec| ClassificationModelInfo {
            id: spec.id.to_string(),
            name: spec.name.to_string(),
            description: spec.description.to_string(),
            classes: spec.classes.iter().map(|c| c.to_string()).collect(),
        })
        .collect();

    response::ok_ser(&format!("Found {} models", models.len()), models)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct ClassifyObjectQuery {
    /// Id of the model to run, as returned by the classification-models endpoint.
    model: String,
}

/// The scored alert, as far as a Hyrax model is concerned: the candidate block and
/// whatever enrichment the ingest workers already attached to it.
///
/// Deliberately its own type rather than a reuse of the object endpoint's response:
/// the classify path wants the alert whose image is being scored, not the newest
/// one, and it has no use for cross-survey matches.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ScoredZtfAlert {
    candidate: ZtfCandidate,
    properties: Option<ZtfAlertProperties>,
    classifications: Option<ZtfAlertClassifications>,
}

/// LSST counterpart of [`ScoredZtfAlert`]. LSST alerts carry no `classifications`.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ScoredLsstAlert {
    candidate: LsstCandidate,
    properties: Option<LsstAlertProperties>,
}

/// The object's light curve and cross-matches, from its aux document.
///
/// Split out from [`fetch_hyrax_object_inputs`] because service-backed models score
/// the light curve alone: they want neither cutouts nor a scored alert, and making
/// them pay for those lookups would mean refusing an object that the service could
/// happily classify.
///
/// A missing aux entry is not fatal — refusing to score an object just because its
/// light curve has not been backfilled would be worse than reporting empty
/// photometry and letting the model say it has nothing to work with.
async fn fetch_object_photometry(
    db: &Database,
    survey: &Survey,
    object_id: &str,
) -> Result<(serde_json::Value, serde_json::Value), HttpResponse> {
    let aux_filter = doc! { "_id": object_id };

    match survey {
        Survey::Ztf => {
            let aux: Option<ZtfObject> = db
                .collection("ZTF_alerts_aux")
                .find_one(aux_filter)
                .await
                .map_err(aux_lookup_error(object_id))?;

            Ok(match aux {
                // Limit photometry to programid 1 (public ZTF alerts), as the object
                // endpoint does.
                Some(aux) => (
                    serde_json::json!({
                        "prv_candidates": aux.prv_candidates
                            .into_iter()
                            .filter(|c| c.prv_candidate.programid == 1)
                            .collect::<Vec<_>>(),
                        "prv_nondetections": aux.prv_nondetections
                            .into_iter()
                            .filter(|c| c.prv_candidate.programid == 1)
                            .collect::<Vec<_>>(),
                        "fp_hists": aux.fp_hists
                            .into_iter()
                            .filter(|c| c.fp_hist.programid == 1)
                            .collect::<Vec<_>>(),
                    }),
                    serde_json::json!(aux.cross_matches),
                ),
                None => (
                    serde_json::json!({
                        "prv_candidates": [],
                        "prv_nondetections": [],
                        "fp_hists": [],
                    }),
                    serde_json::Value::Null,
                ),
            })
        }
        Survey::Lsst => {
            let aux: Option<LsstObject> = db
                .collection("LSST_alerts_aux")
                .find_one(aux_filter)
                .await
                .map_err(aux_lookup_error(object_id))?;

            Ok(match aux {
                Some(aux) => (
                    serde_json::json!({
                        "prv_candidates": aux.prv_candidates,
                        "fp_hists": aux.fp_hists,
                    }),
                    serde_json::json!(aux.cross_matches),
                ),
                None => (
                    serde_json::json!({
                        "prv_candidates": [],
                        "fp_hists": [],
                    }),
                    serde_json::Value::Null,
                ),
            })
        }
        _ => Err(response::bad_request(
            "Invalid survey specified, only ZTF and LSST are supported",
        )),
    }
}

/// Gather the photometry and metadata that ride along with every ONNX Hyrax run.
///
/// Two indexed lookups: the aux document for the light curve and cross-matches, and
/// the scored alert itself for its candidate block. `candid` is the alert whose
/// cutouts were handed to the model, so the metadata describes the same image the
/// model is looking at.
async fn fetch_hyrax_object_inputs(
    db: &Database,
    survey: &Survey,
    object_id: &str,
    candid: i64,
) -> Result<(serde_json::Value, serde_json::Value), HttpResponse> {
    let (photometry, cross_matches) = fetch_object_photometry(db, survey, object_id).await?;

    let alert_filter = doc! { "_id": candid };
    let alert = match survey {
        Survey::Ztf => {
            let alert: Option<ScoredZtfAlert> = db
                .collection("ZTF_alerts")
                .find_one(alert_filter)
                .await
                .map_err(alert_lookup_error(candid))?;
            serde_json::to_value(alert)
        }
        Survey::Lsst => {
            let alert: Option<ScoredLsstAlert> = db
                .collection("LSST_alerts")
                .find_one(alert_filter)
                .await
                .map_err(alert_lookup_error(candid))?;
            serde_json::to_value(alert)
        }
        _ => {
            return Err(response::bad_request(
                "Invalid survey specified, only ZTF and LSST are supported",
            ));
        }
    };

    let alert = match alert {
        Ok(alert) => alert,
        Err(error) => {
            return Err(response::internal_error(&format!(
                "error serializing alert {}: {}",
                candid, error
            )));
        }
    };

    let metadata = serde_json::json!({
        "objectId": object_id,
        "candid": candid,
        "survey": survey.to_string(),
        "alert": alert,
        "cross_matches": cross_matches,
    });

    Ok((photometry, metadata))
}

/// Error mapper for the aux-document lookup, kept out of the branches above so both
/// surveys report the failure the same way.
fn aux_lookup_error(object_id: &str) -> impl Fn(mongodb::error::Error) -> HttpResponse + '_ {
    move |error| {
        response::internal_error(&format!(
            "error getting aux entry for object {}: {}",
            object_id, error
        ))
    }
}

/// Error mapper for the scored-alert lookup.
fn alert_lookup_error(candid: i64) -> impl Fn(mongodb::error::Error) -> HttpResponse {
    move |error| response::internal_error(&format!("error getting alert {}: {}", candid, error))
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct ClassifyObjectResult {
    /// Id of the model that produced this result.
    model: String,
    /// Candid of the alert whose cutouts were scored. Absent for models that score
    /// the whole light curve rather than one alert's image.
    #[serde(skip_serializing_if = "Option::is_none")]
    candid: Option<i64>,
    /// Class name -> probability, for multiclass models.
    #[serde(skip_serializing_if = "Option::is_none")]
    classes: Option<HashMap<String, f32>>,
    /// Single score, for models that emit one value.
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<f32>,
    /// Highest-probability class, when the backend names one itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pred_class: Option<String>,
    /// Evidential uncertainty: how little evidence the model had to go on, from 0
    /// (well-evidenced) to 1. Only evidential models report it.
    #[serde(skip_serializing_if = "Option::is_none")]
    vacuity: Option<f32>,
    /// Entropy of the predicted class distribution.
    #[serde(skip_serializing_if = "Option::is_none")]
    predictive_entropy: Option<f32>,
    /// How many photometry points the model actually used.
    #[serde(skip_serializing_if = "Option::is_none")]
    n_events_used: Option<usize>,
}

/// Candid of the alert a classify request should score: the object's brightest
/// public alert, matching the default the cutouts endpoint uses so the scored image
/// is the one the UI is showing.
async fn find_brightest_candid(
    db: &Database,
    survey: &Survey,
    object_id: &str,
) -> Result<i64, HttpResponse> {
    let mut filter = doc! { "objectId": object_id };
    if *survey == Survey::Ztf {
        filter.insert("candidate.programid", 1);
    }
    let alert_collection = db.collection::<AlertCandidOnly>(&format!("{}_alerts", survey));
    match alert_collection
        .find_one(filter)
        .projection(doc! { "_id": 1 })
        .with_options(
            mongodb::options::FindOneOptions::builder()
                // Lowest mag is brightest, so sort in ascending order
                .sort(doc! { "candidate.magpsf": 1 })
                .build(),
        )
        .await
    {
        Ok(Some(alert)) => Ok(alert.candid),
        Ok(None) => Err(response::not_found(&format!(
            "no alerts found for objectId {}",
            object_id
        ))),
        Err(error) => Err(response::internal_error(&format!(
            "error getting documents: {}",
            error
        ))),
    }
}

/// Shape a model's raw output into the response the UI expects, labelling the
/// scores with the spec's class names for multiclass models.
fn classification_response(
    spec: &'static HyraxModelSpec,
    object_id: &str,
    candid: i64,
    scores: &[f32],
) -> HttpResponse {
    if scores.is_empty() {
        return response::internal_error(&format!("model {} returned no scores", spec.id));
    }

    let result = if spec.classes.is_empty() {
        ClassifyObjectResult {
            model: spec.id.to_string(),
            candid: Some(candid),
            score: Some(scores[0]),
            ..Default::default()
        }
    } else if scores.len() >= spec.classes.len() {
        ClassifyObjectResult {
            model: spec.id.to_string(),
            candid: Some(candid),
            classes: Some(
                spec.classes
                    .iter()
                    .zip(scores)
                    .map(|(label, score)| (label.to_string(), *score))
                    .collect(),
            ),
            ..Default::default()
        }
    } else {
        // The artifact disagrees with the class labels in its spec; surfacing the
        // raw first score would silently mislabel it.
        tracing::error!(
            model = spec.id,
            expected = spec.classes.len(),
            got = scores.len(),
            "Hyrax model returned fewer scores than it has class labels"
        );
        return response::internal_error(&format!(
            "model {} returned {} scores but declares {} classes",
            spec.id,
            scores.len(),
            spec.classes.len()
        ));
    };

    response::ok_ser(&format!("Ran {} on object {}", spec.id, object_id), result)
}

/// Run a service-backed model: hand the object's light curve to the sidecar that
/// owns it and translate what comes back.
///
/// No candid is reported, and none is looked up: these models score the whole light
/// curve rather than the image of one alert, so tying the result to a single
/// candidate would misdescribe it.
async fn classify_with_backend_service(
    spec: &'static HyraxModelSpec,
    backend: &ServiceBackend,
    db: &Database,
    survey: &Survey,
    object_id: &str,
) -> HttpResponse {
    let (photometry, _cross_matches) = match fetch_object_photometry(db, survey, object_id).await {
        Ok(inputs) => inputs,
        Err(response) => return response,
    };

    match classify_with_service(spec, backend, object_id, &photometry).await {
        Ok(result) => {
            let value = ClassifyObjectResult {
                model: spec.id.to_string(),
                classes: Some(result.classes),
                pred_class: Some(result.pred_class),
                vacuity: Some(result.vacuity),
                predictive_entropy: Some(result.predictive_entropy),
                n_events_used: Some(result.n_events_used),
                ..Default::default()
            };
            response::ok_ser(&format!("Ran {} on object {}", spec.id, object_id), value)
        }
        Err(HyraxPredictError::ServiceUnreachable { url, reason }) => {
            tracing::error!(
                model = spec.id,
                url,
                reason,
                "Hyrax inference service is unreachable"
            );
            response::service_unavailable(&format!(
                "Model {} is not available: its inference service could not be reached",
                spec.id
            ))
        }
        Err(HyraxPredictError::ServiceStatus { status, detail }) => {
            tracing::error!(
                model = spec.id,
                status,
                detail,
                "Hyrax inference service refused the request"
            );
            // A 4xx means the object is the problem — no usable photometry, most
            // often — so report it as such and pass the service's own message
            // through rather than flattening it into a server fault.
            if (400..500).contains(&status) {
                response::bad_request(&format!(
                    "Model {} could not classify {}: {}",
                    spec.id, object_id, detail
                ))
            } else {
                response::internal_error(&format!("error running model {}: {}", spec.id, detail))
            }
        }
        Err(error) => {
            tracing::error!(model = spec.id, %error, "Hyrax service inference failed");
            response::internal_error(&format!("error running model {}: {}", spec.id, error))
        }
    }
}

/// Run a Hyrax model against a single object, on demand.
///
/// What the model is given depends on its backend: an ONNX model scores the cutout
/// triplet of the object's brightest public alert, while a service-backed model
/// scores the object's whole light curve.
///
/// Results are returned to the caller only — unlike the classifications produced by
/// the enrichment worker at ingest time, they are not persisted onto the object.
#[utoipa::path(
    post,
    path = "/babamul/surveys/{survey}/objects/{object_id}/classify",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
        ("object_id" = String, Path, description = "ID of the object to classify"),
    ),
    request_body = ClassifyObjectQuery,
    responses(
        (status = 200, description = "Classification result", body = ClassifyObjectResult),
        (status = 400, description = "Unknown model, model not supported for this survey, or the object could not be classified"),
        (status = 404, description = "Object or cutouts not found"),
        (status = 503, description = "Model artifact not installed, or its inference service is unreachable"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[post("/surveys/{survey}/objects/{object_id}/classify")]
pub async fn classify_object(
    path: web::Path<(Survey, String)>,
    query: web::Json<ClassifyObjectQuery>,
    db: web::Data<Database>,
    cutout_storages: web::Data<HashMap<Survey, CutoutStorage>>,
    hyrax_models: web::Data<HyraxModelRegistry>,
) -> HttpResponse {
    let (survey, object_id) = path.into_inner();
    let model_id = query.into_inner().model;

    let spec = match find_model_spec(&model_id) {
        Some(spec) => spec,
        None => {
            return response::bad_request(&format!("Unknown model: {}", model_id));
        }
    };
    if !spec.surveys.contains(&survey) {
        return response::bad_request(&format!(
            "Model {} does not support survey {}",
            spec.id, survey
        ));
    }
    // Nothing else is checked before the run: a model the server cannot actually
    // serve fails below, with the reason its backend gives, rather than being
    // refused here on the strength of what is on local disk.

    // Service-backed models take the light curve alone, so they branch off before
    // the cutout and scored-alert lookups the ONNX path needs.
    if let HyraxBackend::Service(backend) = &spec.backend {
        return classify_with_backend_service(spec, backend, &db, &survey, &object_id).await;
    }

    let cutout_storage = match cutout_storages.get(&survey) {
        Some(storage) => storage,
        None => {
            return response::internal_error("cutout storage not available for this survey");
        }
    };

    let candid = match find_brightest_candid(&db, &survey, &object_id).await {
        Ok(candid) => candid,
        Err(response) => return response,
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

    let triplet = match HyraxModel::get_triplet(&[&cutouts]) {
        Ok(triplet) => triplet,
        Err(error) => {
            return response::bad_request(&format!(
                "could not build a cutout triplet for candid {}: {}",
                candid, error
            ));
        }
    };

    // Every Hyrax run is handed the object's photometry and metadata alongside the
    // cutouts, whether or not the model's spec binds them to an ONNX input.
    let (photometry, metadata) =
        match fetch_hyrax_object_inputs(&db, &survey, &object_id, candid).await {
            Ok(inputs) => inputs,
            Err(response) => return response,
        };
    let input = HyraxInput {
        triplet,
        photometry,
        metadata,
    };

    // ONNX inference is CPU-bound and serializes on the model's mutex, so keep it
    // off the actix worker threads.
    let registry = hyrax_models.clone();
    let model_id_for_task = model_id.clone();
    let scores = match web::block(move || registry.predict(&model_id_for_task, &input)).await {
        Ok(Ok(scores)) => scores,
        Ok(Err(HyraxPredictError::ArtifactNotFound(path))) => {
            tracing::error!(model = spec.id, path, "Hyrax model artifact is missing");
            return response::service_unavailable(&format!(
                "Model {} is not installed on this server",
                spec.id
            ));
        }
        Ok(Err(error)) => {
            tracing::error!(model = spec.id, %error, "Hyrax inference failed");
            return response::internal_error(&format!(
                "error running model {}: {}",
                spec.id, error
            ));
        }
        Err(error) => {
            tracing::error!(model = spec.id, %error, "Hyrax inference task failed");
            return response::internal_error("error running model");
        }
    };

    classification_response(spec, &object_id, candid, &scores)
}
