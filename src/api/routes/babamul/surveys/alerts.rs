use crate::alert::{LsstCandidate, ZtfCandidate};
use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use crate::enrichment::{LsstAlertProperties, ZtfAlertClassifications, ZtfAlertProperties};
use crate::utils::enums::Survey;
use crate::utils::moc::{
    is_in_moc, moc_from_fits_bytes, moc_from_skymap_bytes, moc_to_covering_cones,
    select_covering_depth,
};
use actix_web::{get, post, web, HttpResponse};
use base64::prelude::*;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    Collection, Database,
};
use std::collections::HashMap;
use utoipa::ToSchema;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct EnrichedZtfAlert {
    #[serde(alias = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: ZtfCandidate,
    pub properties: Option<ZtfAlertProperties>,
    pub classifications: Option<ZtfAlertClassifications>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct EnrichedLsstAlert {
    #[serde(alias = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: LsstCandidate,
    pub properties: Option<LsstAlertProperties>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct AlertsQuery {
    object_id: Option<String>,
    ra: Option<f64>,
    dec: Option<f64>,
    radius_arcsec: Option<f64>,
    start_jd: Option<f64>,
    end_jd: Option<f64>,
    min_magpsf: Option<f64>,
    max_magpsf: Option<f64>,
    #[serde(alias = "min_reliability")]
    min_drb: Option<f64>,
    #[serde(alias = "max_reliability")]
    max_drb: Option<f64>,
    is_rock: Option<bool>,
    is_star: Option<bool>,
    is_near_brightstar: Option<bool>,
    is_stationary: Option<bool>,
    limit: Option<u32>,
    skip: Option<u64>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
enum AlertsQueryResult {
    ZtfAlerts(Vec<EnrichedZtfAlert>),
    LsstAlerts(Vec<EnrichedLsstAlert>),
}

#[utoipa::path(
    get,
    path = "/babamul/surveys/{survey}/alerts",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
        ("object_id" = Option<String>, Query, description = "Object ID to filter alerts"),
        ("ra" = Option<f64>, Query, description = "Right Ascension in degrees for cone search"),
        ("dec" = Option<f64>, Query, description = "Declination in degrees for cone search"),
        ("radius_arcsec" = Option<f64>, Query, description = "Radius in arcseconds for cone search"),
        ("start_jd" = Option<f64>, Query, description = "Start Julian Date for time range filter"),
        ("end_jd" = Option<f64>, Query, description = "End Julian Date for time range filter"),
        ("min_magpsf" = Option<f64>, Query, description = "Minimum magpsf for brightness filter"),
        ("max_magpsf" = Option<f64>, Query, description = "Maximum magpsf for brightness filter"),
        ("min_drb" = Option<f64>, Query, description = "Minimum DRB score for classification filter"),
        ("max_drb" = Option<f64>, Query, description = "Maximum DRB score for classification filter"),
        ("is_rock" = Option<bool>, Query, description = "Whether to filter for likely rock candidates"),
        ("is_star" = Option<bool>, Query, description = "Whether to filter for likely star candidates"),
        ("is_near_brightstar" = Option<bool>, Query, description = "Whether to filter for candidates near bright stars"),
        ("is_stationary" = Option<bool>, Query, description = "Whether to filter for stationary candidates"),
        ("limit" = Option<u32>, Query, description = "Maximum number of alerts to return"),
        ("skip" = Option<u64>, Query, description = "Number of alerts to skip (for pagination)"),
    ),
    responses(
        (status = 200, description = "Alerts retrieved successfully", body = AlertsQueryResult),
        (status = 400, description = "Invalid survey or query parameters"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey}/alerts")]
pub async fn get_alerts(
    path: web::Path<Survey>,
    query: web::Query<AlertsQuery>,
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

    let limit = query.limit.unwrap_or(100000);
    if limit == 0 || limit > 100000 {
        return response::bad_request("Invalid limit, must be between 1 and 100000");
    }
    let skip = query.skip.unwrap_or(0);

    let mut filter_doc = if survey == Survey::Ztf {
        doc! {"candidate.programid": 1} // Babamul only returns public ZTF alerts
    } else {
        doc! {}
    };

    // We need to have at least object_id OR position OR time range (less than 1 jd)
    if query.object_id.is_none()
        && (query.ra.is_none() || query.dec.is_none() || query.radius_arcsec.is_none())
    {
        match (query.start_jd, query.end_jd) {
            (Some(start_jd), Some(end_jd)) => {
                if end_jd - start_jd > 1.0 {
                    return response::bad_request(
                        "Time range too large, maximum allowed is 1 Julian Date",
                    );
                }
            }
            _ => {
                return response::bad_request(
                    "Must provide either object_id or (ra, dec, radius_arcsec) or (start_jd, end_jd)",
                );
            }
        }
    }
    // we can't have both object_id and position filters
    if query.object_id.is_some()
        && query.ra.is_some()
        && query.dec.is_some()
        && query.radius_arcsec.is_some()
    {
        return response::bad_request("Cannot provide both object_id and position filters");
    }

    // Build the filter document based on the query parameters
    if let Some(object_id) = &query.object_id {
        filter_doc.insert("objectId", object_id);
    } else if let (Some(ra), Some(dec), Some(radius_arcsec)) =
        (query.ra, query.dec, query.radius_arcsec)
    {
        if radius_arcsec <= 0.0 || radius_arcsec > 600.0 {
            return response::bad_request(
                "Invalid radius, must be greater than 0 and less than or equal to 600 arcseconds (10 arcminutes)",
            );
        }
        // Add cone search filter
        filter_doc.insert(
            "coordinates.radec_geojson",
            doc! {
                "$geoWithin": {
                    "$centerSphere": [
                        [ra - 180.0, dec],
                        (radius_arcsec / 3600.0).to_radians()
                    ]
                }
            },
        );
    }

    if query.start_jd.is_some() || query.end_jd.is_some() {
        let mut jd_filter = Document::new();
        if let Some(start_jd) = query.start_jd {
            jd_filter.insert("$gte", start_jd);
        }
        if let Some(end_jd) = query.end_jd {
            jd_filter.insert("$lte", end_jd);
        }
        filter_doc.insert("candidate.jd", jd_filter);
    }

    if query.min_magpsf.is_some() || query.max_magpsf.is_some() {
        let mut magpsf_filter = Document::new();
        if let Some(min_magpsf) = query.min_magpsf {
            magpsf_filter.insert("$gte", min_magpsf);
        }
        if let Some(max_magpsf) = query.max_magpsf {
            magpsf_filter.insert("$lte", max_magpsf);
        }
        filter_doc.insert("candidate.magpsf", magpsf_filter);
    }

    // we should handle having one OR the other and not requiring both min and max for the DRB filter
    if query.min_drb.is_some() || query.max_drb.is_some() {
        let drb_key = match survey {
            Survey::Ztf => "candidate.drb",
            Survey::Lsst => "candidate.reliability",
            _ => {
                return response::bad_request(
                    "Invalid survey specified, only ZTF and LSST are supported",
                );
            }
        };
        let mut drb_filter = Document::new();
        if let Some(min_drb) = query.min_drb {
            drb_filter.insert("$gte", min_drb);
        }
        if let Some(max_drb) = query.max_drb {
            drb_filter.insert("$lte", max_drb);
        }
        filter_doc.insert(drb_key, drb_filter);
    }

    if let Some(is_rock) = query.is_rock {
        filter_doc.insert("properties.rock", is_rock);
    }
    if let Some(is_star) = query.is_star {
        filter_doc.insert("properties.star", is_star);
    }
    if let Some(is_near_brightstar) = query.is_near_brightstar {
        filter_doc.insert("properties.near_brightstar", is_near_brightstar);
    }
    if let Some(is_stationary) = query.is_stationary {
        filter_doc.insert("properties.stationary", is_stationary);
    }

    match survey {
        Survey::Ztf => {
            let alerts_collection: Collection<EnrichedZtfAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut alert_cursor = match alerts_collection
                .find(filter_doc)
                .sort(doc! { "_id": 1 })
                .skip(skip)
                .limit(limit as i64)
                .await
            {
                Ok(cursor) => cursor,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error retrieving alerts for survey {}: {}",
                        survey, error
                    ));
                }
            };

            let mut results: Vec<EnrichedZtfAlert> = Vec::new();
            while let Some(alert_doc) = match alert_cursor.try_next().await {
                Ok(Some(doc)) => Some(doc),
                Ok(None) => None,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error getting documents: {}",
                        error
                    ));
                }
            } {
                results.push(alert_doc);
            }
            return response::ok(
                &format!("found {} alerts matching query", results.len()),
                serde_json::json!(results),
            );
        }
        Survey::Lsst => {
            let alerts_collection: Collection<EnrichedLsstAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut alert_cursor = match alerts_collection
                .find(filter_doc)
                .sort(doc! { "_id": 1 })
                .skip(skip)
                .limit(limit as i64)
                .await
            {
                Ok(cursor) => cursor,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error retrieving alerts for objects: {}",
                        error
                    ));
                }
            };

            let mut results: Vec<EnrichedLsstAlert> = Vec::new();
            while let Some(alert_doc) = match alert_cursor.try_next().await {
                Ok(Some(doc)) => Some(doc),
                Ok(None) => None,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error getting documents: {}",
                        error
                    ));
                }
            } {
                results.push(alert_doc);
            }
            return response::ok(
                &format!("found {} alerts matching query", results.len()),
                serde_json::json!(results),
            );
        }
        _ => {
            return response::bad_request(
                "Invalid survey specified, only ZTF and LSST are supported",
            );
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct AlertsConeSearchQuery {
    coordinates: HashMap<String, [f64; 2]>,
    radius_arcsec: f64,
    start_jd: Option<f64>,
    end_jd: Option<f64>,
    min_magpsf: Option<f64>,
    max_magpsf: Option<f64>,
    #[serde(alias = "min_reliability")]
    min_drb: Option<f64>,
    #[serde(alias = "max_reliability")]
    max_drb: Option<f64>,
    is_rock: Option<bool>,
    is_star: Option<bool>,
    is_near_brightstar: Option<bool>,
    is_stationary: Option<bool>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
enum AlertsConeSearchResult {
    ZtfAlerts(HashMap<String, Vec<EnrichedZtfAlert>>),
    LsstAlerts(HashMap<String, Vec<EnrichedLsstAlert>>),
}

#[utoipa::path(
    post,
    path = "/babamul/surveys/{survey}/alerts/cone-search",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
    ),
    request_body = AlertsConeSearchQuery,
    responses(
        (status = 200, description = "Alerts retrieved successfully", body = AlertsConeSearchResult),
        (status = 400, description = "Invalid survey or query parameters"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[post("/surveys/{survey}/alerts/cone-search")]
pub async fn cone_search_alerts(
    path: web::Path<Survey>,
    query: web::Json<AlertsConeSearchQuery>,
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
    let coordinates = &query.coordinates;
    // we must have more than 0 and less than 1000 coordinate pairs
    // to prevent expensive queries that could potentially timeout the server
    if coordinates.is_empty() || coordinates.len() > 1000 {
        return response::bad_request(
            "Invalid number of coordinate pairs, must be between 1 and 1000",
        );
    }
    let radius_arcsec = query.radius_arcsec;
    if radius_arcsec <= 0.0 || radius_arcsec > 600.0 {
        return response::bad_request(
            "Invalid radius, must be greater than 0 and less than or equal to 600 arcseconds (10 arcminutes)",
        );
    }
    let radius_radians = (radius_arcsec / 3600.0).to_radians();

    let mut base_filter_doc = if survey == Survey::Ztf {
        doc! {"candidate.programid": 1} // Babamul only returns public ZTF alerts
    } else {
        doc! {}
    };

    if query.start_jd.is_some() || query.end_jd.is_some() {
        let mut jd_filter = Document::new();
        if let Some(start_jd) = query.start_jd {
            jd_filter.insert("$gte", start_jd);
        }
        if let Some(end_jd) = query.end_jd {
            jd_filter.insert("$lte", end_jd);
        }
        base_filter_doc.insert("candidate.jd", jd_filter);
    }
    if query.min_magpsf.is_some() || query.max_magpsf.is_some() {
        let mut magpsf_filter = Document::new();
        if let Some(min_magpsf) = query.min_magpsf {
            magpsf_filter.insert("$gte", min_magpsf);
        }
        if let Some(max_magpsf) = query.max_magpsf {
            magpsf_filter.insert("$lte", max_magpsf);
        }
        base_filter_doc.insert("candidate.magpsf", magpsf_filter);
    }
    if query.min_drb.is_some() || query.max_drb.is_some() {
        let drb_key = match survey {
            Survey::Ztf => "candidate.drb",
            Survey::Lsst => "candidate.reliability",
            _ => {
                return response::bad_request(
                    "Invalid survey specified, only ZTF and LSST are supported",
                );
            }
        };
        let mut drb_filter = Document::new();
        if let Some(min_drb) = query.min_drb {
            drb_filter.insert("$gte", min_drb);
        }
        if let Some(max_drb) = query.max_drb {
            drb_filter.insert("$lte", max_drb);
        }
        base_filter_doc.insert(drb_key, drb_filter);
    }
    if let Some(is_rock) = query.is_rock {
        base_filter_doc.insert("properties.rock", is_rock);
    }
    if let Some(is_star) = query.is_star {
        base_filter_doc.insert("properties.star", is_star);
    }
    if let Some(is_near_brightstar) = query.is_near_brightstar {
        base_filter_doc.insert("properties.near_brightstar", is_near_brightstar);
    }
    if let Some(is_stationary) = query.is_stationary {
        base_filter_doc.insert("properties.stationary", is_stationary);
    }

    match survey {
        Survey::Ztf => {
            let alerts_collection: Collection<EnrichedZtfAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut results: HashMap<String, Vec<EnrichedZtfAlert>> = HashMap::new();
            let mut alert_count = 0;
            let mut coordinates_with_matches_count = 0;
            for (object_name, radec) in coordinates {
                if radec.len() != 2 {
                    return response::bad_request(&format!(
                        "Invalid coordinates for object {}: expected [RA, Dec]",
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
                let center_sphere = doc! {
                    "coordinates.radec_geojson": {
                        "$geoWithin": {
                            "$centerSphere": [
                                [ra - 180.0, dec],
                                radius_radians
                            ]
                        }
                    }
                };
                let mut filter_doc = base_filter_doc.clone();
                // filter_doc.extend(center_sphere);
                // we need to make sure that the condition on coordinates is at the start of the filter document to take advantage of geospatial indexing
                filter_doc = center_sphere
                    .into_iter()
                    .chain(filter_doc.into_iter())
                    .collect();

                let mut alert_cursor = match alerts_collection.find(filter_doc).await {
                    Ok(cursor) => cursor,
                    Err(error) => {
                        return response::internal_error(&format!(
                            "error retrieving alerts for survey {}: {}",
                            survey, error
                        ));
                    }
                };

                let mut alert_results: Vec<EnrichedZtfAlert> = Vec::new();
                while let Some(alert_doc) = match alert_cursor.try_next().await {
                    Ok(Some(doc)) => Some(doc),
                    Ok(None) => None,
                    Err(error) => {
                        return response::internal_error(&format!(
                            "error getting documents: {}",
                            error
                        ));
                    }
                } {
                    alert_results.push(alert_doc);
                    alert_count += 1;
                }
                if !alert_results.is_empty() {
                    coordinates_with_matches_count += 1;
                }
                results.insert(object_name.clone(), alert_results);
            }
            return response::ok(
                &format!(
                    "found cross-matches for {}/{} coordinates, with a total {} alerts",
                    coordinates_with_matches_count,
                    coordinates.len(),
                    alert_count
                ),
                serde_json::json!(results),
            );
        }
        Survey::Lsst => {
            // similar to above but for LSST collection
            let alerts_collection: Collection<EnrichedLsstAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut results: HashMap<String, Vec<EnrichedLsstAlert>> = HashMap::new();
            let mut alert_count = 0;
            let mut coordinates_with_matches_count = 0;
            for (object_name, radec) in coordinates {
                if radec.len() != 2 {
                    return response::bad_request(&format!(
                        "Invalid coordinates for object {}: expected [RA, Dec]",
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
                let center_sphere = doc! {
                    "coordinates.radec_geojson": {
                        "$geoWithin": {
                            "$centerSphere": [
                                [ra - 180.0, dec],
                                radius_radians
                            ]
                        }
                    }
                };
                let mut filter_doc = base_filter_doc.clone();
                // we need to make sure that the condition on coordinates is at the start of the filter document to take advantage of geospatial indexing
                filter_doc = center_sphere
                    .into_iter()
                    .chain(filter_doc.into_iter())
                    .collect();
                let mut alert_cursor = match alerts_collection.find(filter_doc).await {
                    Ok(cursor) => cursor,
                    Err(error) => {
                        return response::internal_error(&format!(
                            "error retrieving alerts for survey {}: {}",
                            survey, error
                        ));
                    }
                };

                let mut alert_results: Vec<EnrichedLsstAlert> = Vec::new();
                while let Some(alert_doc) = match alert_cursor.try_next().await {
                    Ok(Some(doc)) => Some(doc),
                    Ok(None) => None,
                    Err(error) => {
                        return response::internal_error(&format!(
                            "error getting documents: {}",
                            error
                        ));
                    }
                } {
                    alert_results.push(alert_doc);
                    alert_count += 1;
                }
                if !alert_results.is_empty() {
                    coordinates_with_matches_count += 1;
                }
                results.insert(object_name.clone(), alert_results);
            }
            return response::ok(
                &format!(
                    "found cross-matches for {}/{} coordinates, with a total {} alerts",
                    coordinates_with_matches_count,
                    coordinates.len(),
                    alert_count
                ),
                serde_json::json!(results),
            );
        }
        _ => {
            return response::bad_request(
                "Invalid survey specified, only ZTF and LSST are supported",
            );
        }
    }
}

/// Maximum time window for MOC search queries (7 days).
const MOC_SEARCH_MAX_TIME_WINDOW_JD: f64 = 7.0;
/// Maximum number of covering cones before rejecting the query.
const MOC_SEARCH_MAX_CONES: usize = 500;
/// MongoDB server-side query timeout for MOC search (30 seconds).
const MOC_SEARCH_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct AlertsMocSearchQuery {
    /// Base64-encoded MOC FITS file (exactly one of moc_fits_base64 or skymap_fits_base64 required)
    moc_fits_base64: Option<String>,
    /// Base64-encoded HEALPix skymap FITS file
    skymap_fits_base64: Option<String>,
    /// Credible level for skymap thresholding (default: 0.9, required if skymap_fits_base64 is provided)
    credible_level: Option<f64>,
    /// Start of time window (required, Julian Date)
    start_jd: f64,
    /// End of time window (required, Julian Date, max 7 days after start_jd)
    end_jd: f64,
    min_magpsf: Option<f64>,
    max_magpsf: Option<f64>,
    #[serde(alias = "min_reliability")]
    min_drb: Option<f64>,
    #[serde(alias = "max_reliability")]
    max_drb: Option<f64>,
    is_rock: Option<bool>,
    is_star: Option<bool>,
    is_near_brightstar: Option<bool>,
    is_stationary: Option<bool>,
    limit: Option<u32>,
}

#[utoipa::path(
    post,
    path = "/babamul/surveys/{survey}/alerts/moc-search",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
    ),
    request_body = AlertsMocSearchQuery,
    responses(
        (status = 200, description = "Alerts within the MOC region", body = AlertsQueryResult),
        (status = 400, description = "Invalid query parameters or MOC data"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[post("/surveys/{survey}/alerts/moc-search")]
pub async fn moc_search_alerts(
    path: web::Path<Survey>,
    query: web::Json<AlertsMocSearchQuery>,
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

    // Validate time window (required, capped at 7 days)
    let time_window = query.end_jd - query.start_jd;
    if time_window <= 0.0 {
        return response::bad_request("end_jd must be greater than start_jd");
    }
    if time_window > MOC_SEARCH_MAX_TIME_WINDOW_JD {
        return response::bad_request(&format!(
            "Time window too large ({:.1} days), maximum allowed is {} days",
            time_window, MOC_SEARCH_MAX_TIME_WINDOW_JD
        ));
    }

    // Parse the MOC from the request
    let moc = match (&query.moc_fits_base64, &query.skymap_fits_base64) {
        (Some(moc_b64), None) => {
            let bytes = match BASE64_STANDARD.decode(moc_b64) {
                Ok(b) => b,
                Err(e) => {
                    return response::bad_request(&format!(
                        "Invalid base64 in moc_fits_base64: {}",
                        e
                    ));
                }
            };
            match moc_from_fits_bytes(&bytes) {
                Ok(moc) => moc,
                Err(e) => {
                    return response::bad_request(&e.to_string());
                }
            }
        }
        (None, Some(skymap_b64)) => {
            let credible_level = query.credible_level.unwrap_or(0.9);
            if !(0.0..=1.0).contains(&credible_level) {
                return response::bad_request("credible_level must be between 0.0 and 1.0");
            }
            let bytes = match BASE64_STANDARD.decode(skymap_b64) {
                Ok(b) => b,
                Err(e) => {
                    return response::bad_request(&format!(
                        "Invalid base64 in skymap_fits_base64: {}",
                        e
                    ));
                }
            };
            match moc_from_skymap_bytes(&bytes, credible_level) {
                Ok(moc) => moc,
                Err(e) => {
                    return response::bad_request(&e);
                }
            }
        }
        (Some(_), Some(_)) => {
            return response::bad_request(
                "Provide exactly one of moc_fits_base64 or skymap_fits_base64, not both",
            );
        }
        (None, None) => {
            return response::bad_request(
                "Must provide either moc_fits_base64 or skymap_fits_base64",
            );
        }
    };

    let limit = query.limit.unwrap_or(10000).min(10000);
    if limit == 0 {
        return response::bad_request("limit must be between 1 and 10000");
    }

    // Compute covering cones for the MOC
    let depth = select_covering_depth(&moc);
    let cones = moc_to_covering_cones(&moc, depth);
    if cones.is_empty() {
        return response::ok(
            "MOC region is empty, no alerts to search",
            serde_json::json!([]),
        );
    }
    if cones.len() > MOC_SEARCH_MAX_CONES {
        return response::bad_request(&format!(
            "MOC region too large: {} covering cones at depth {} (max {}). Use a smaller credible level or a more targeted MOC.",
            cones.len(),
            depth,
            MOC_SEARCH_MAX_CONES
        ));
    }

    // Build the query with JD range first (uses the candidate.jd index to narrow quickly),
    // then spatial $or to filter by sky region, then post-filter with the full-resolution MOC.
    let jd_filter = doc! { "candidate.jd": { "$gte": query.start_jd, "$lte": query.end_jd } };

    let or_conditions: Vec<Document> = cones
        .iter()
        .map(|&(ra, dec, radius_rad)| {
            doc! {
                "coordinates.radec_geojson": {
                    "$geoWithin": {
                        "$centerSphere": [
                            [ra - 180.0, dec],
                            radius_rad
                        ]
                    }
                }
            }
        })
        .collect();

    // Start with JD filter (indexed), then add spatial $or, then other filters
    let mut filter_doc = jd_filter;
    filter_doc.insert("$or", or_conditions);

    if survey == Survey::Ztf {
        filter_doc.insert("candidate.programid", 1);
    }
    if query.min_magpsf.is_some() || query.max_magpsf.is_some() {
        let mut magpsf_filter = Document::new();
        if let Some(min_magpsf) = query.min_magpsf {
            magpsf_filter.insert("$gte", min_magpsf);
        }
        if let Some(max_magpsf) = query.max_magpsf {
            magpsf_filter.insert("$lte", max_magpsf);
        }
        filter_doc.insert("candidate.magpsf", magpsf_filter);
    }
    if query.min_drb.is_some() || query.max_drb.is_some() {
        let drb_key = match survey {
            Survey::Ztf => "candidate.drb",
            Survey::Lsst => "candidate.reliability",
            _ => {
                return response::bad_request(
                    "Invalid survey specified, only ZTF and LSST are supported",
                );
            }
        };
        let mut drb_filter = Document::new();
        if let Some(min_drb) = query.min_drb {
            drb_filter.insert("$gte", min_drb);
        }
        if let Some(max_drb) = query.max_drb {
            drb_filter.insert("$lte", max_drb);
        }
        filter_doc.insert(drb_key, drb_filter);
    }
    if let Some(is_rock) = query.is_rock {
        filter_doc.insert("properties.rock", is_rock);
    }
    if let Some(is_star) = query.is_star {
        filter_doc.insert("properties.star", is_star);
    }
    if let Some(is_near_brightstar) = query.is_near_brightstar {
        filter_doc.insert("properties.near_brightstar", is_near_brightstar);
    }
    if let Some(is_stationary) = query.is_stationary {
        filter_doc.insert("properties.stationary", is_stationary);
    }

    match survey {
        Survey::Ztf => {
            let alerts_collection: Collection<EnrichedZtfAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut alert_cursor = match alerts_collection
                .find(filter_doc)
                .max_time(MOC_SEARCH_QUERY_TIMEOUT)
                .limit((limit as i64) * 2) // over-fetch since we post-filter
                .await
            {
                Ok(cursor) => cursor,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error retrieving alerts for survey {}: {}",
                        survey, error
                    ));
                }
            };

            let mut results: Vec<EnrichedZtfAlert> = Vec::new();
            while let Some(alert_doc) = match alert_cursor.try_next().await {
                Ok(Some(doc)) => Some(doc),
                Ok(None) => None,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error getting documents: {}",
                        error
                    ));
                }
            } {
                // Post-filter: check that the alert is actually inside the MOC
                let ra = alert_doc.candidate.candidate.ra;
                let dec = alert_doc.candidate.candidate.dec;
                if is_in_moc(&moc, ra, dec) {
                    results.push(alert_doc);
                    if results.len() >= limit as usize {
                        break;
                    }
                }
            }
            response::ok(
                &format!(
                    "found {} alerts within MOC region ({} covering cones at depth {})",
                    results.len(),
                    cones.len(),
                    depth
                ),
                serde_json::json!(results),
            )
        }
        Survey::Lsst => {
            let alerts_collection: Collection<EnrichedLsstAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut alert_cursor = match alerts_collection
                .find(filter_doc)
                .max_time(MOC_SEARCH_QUERY_TIMEOUT)
                .limit((limit as i64) * 2)
                .await
            {
                Ok(cursor) => cursor,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error retrieving alerts for survey {}: {}",
                        survey, error
                    ));
                }
            };

            let mut results: Vec<EnrichedLsstAlert> = Vec::new();
            while let Some(alert_doc) = match alert_cursor.try_next().await {
                Ok(Some(doc)) => Some(doc),
                Ok(None) => None,
                Err(error) => {
                    return response::internal_error(&format!(
                        "error getting documents: {}",
                        error
                    ));
                }
            } {
                let ra = alert_doc.candidate.dia_source.ra;
                let dec = alert_doc.candidate.dia_source.dec;
                if is_in_moc(&moc, ra, dec) {
                    results.push(alert_doc);
                    if results.len() >= limit as usize {
                        break;
                    }
                }
            }
            response::ok(
                &format!(
                    "found {} alerts within MOC region ({} covering cones at depth {})",
                    results.len(),
                    cones.len(),
                    depth
                ),
                serde_json::json!(results),
            )
        }
        _ => response::bad_request("Invalid survey specified, only ZTF and LSST are supported"),
    }
}
