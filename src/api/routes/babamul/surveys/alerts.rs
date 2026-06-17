use crate::alert::{LsstCandidate, ZtfCandidate};
use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use crate::enrichment::{LsstAlertProperties, ZtfAlertClassifications, ZtfAlertProperties};
use crate::utils::cosmology::luminosity_distance_mpc;
use crate::utils::enums::Survey;
use crate::utils::moc::{
    credible_volume_to_2d_moc, is_in_moc, moc_from_fits_bytes, moc_from_skymap_bytes,
    moc_to_covering_cones, parse_3d_skymap_bytes, select_covering_depth, CredibleVolumeIndex,
    HpxMoc,
};
use actix_web::{get, post, web, HttpResponse};
use base64::prelude::*;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Bson, Document},
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
    is_positive: Option<bool>,
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
        ("is_positive" = Option<bool>, Query, description = "Whether to filter for positive/negative difference sources"),
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

    if let Some(is_positive) = query.is_positive {
        filter_doc.insert("candidate.isdiffpos", is_positive);
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

/// Run one cone search per coordinate pair and group the matching alerts by object
/// name. `base_filter_doc` holds the non-spatial filters shared across every cone;
/// the per-cone `$centerSphere` condition is prepended so the geospatial index is
/// used. The alert type is the only thing that differs between surveys.
async fn cone_search_by_coordinates<T>(
    db: &Database,
    survey: Survey,
    coordinates: &HashMap<String, [f64; 2]>,
    base_filter_doc: Document,
    radius_radians: f64,
) -> HttpResponse
where
    T: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + Unpin,
{
    let alerts_collection: Collection<T> = db.collection(&format!("{}_alerts", survey));
    let mut results: HashMap<String, Vec<T>> = HashMap::new();
    let mut alert_count = 0;
    let mut coordinates_with_matches_count = 0;
    for (object_name, radec) in coordinates {
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
        // we need to make sure that the condition on coordinates is at the start of the
        // filter document to take advantage of geospatial indexing
        let filter_doc: Document = center_sphere
            .into_iter()
            .chain(base_filter_doc.clone())
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

        let mut alert_results: Vec<T> = Vec::new();
        while let Some(alert_doc) = match alert_cursor.try_next().await {
            Ok(Some(doc)) => Some(doc),
            Ok(None) => None,
            Err(error) => {
                return response::internal_error(&format!("error getting documents: {}", error));
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
    response::ok(
        &format!(
            "found cross-matches for {}/{} coordinates, with a total {} alerts",
            coordinates_with_matches_count,
            coordinates.len(),
            alert_count
        ),
        serde_json::json!(results),
    )
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
            cone_search_by_coordinates::<EnrichedZtfAlert>(
                &db,
                survey,
                coordinates,
                base_filter_doc,
                radius_radians,
            )
            .await
        }
        Survey::Lsst => {
            cone_search_by_coordinates::<EnrichedLsstAlert>(
                &db,
                survey,
                coordinates,
                base_filter_doc,
                radius_radians,
            )
            .await
        }
        _ => response::bad_request("Invalid survey specified, only ZTF and LSST are supported"),
    }
}

/// Maximum time window for MOC search queries (7 days).
const MOC_SEARCH_MAX_TIME_WINDOW_JD: f64 = 7.0;
/// Maximum number of covering cones before rejecting the query.
const MOC_SEARCH_MAX_CONES: usize = 500;
/// MongoDB server-side query timeout for MOC search (30 seconds).
const MOC_SEARCH_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Stream alerts matching `filter_doc`, keep only those whose coordinates fall
/// inside `moc` (the spatial `$or` is a coarse pre-filter at the cone level), and
/// build the success response. `coords` extracts (ra, dec) from each alert, which
/// is the only thing that differs between surveys.
async fn collect_moc_alerts<T, F>(
    db: &Database,
    survey: Survey,
    filter_doc: Document,
    moc: &HpxMoc,
    limit: u32,
    cones_len: usize,
    depth: u8,
    coords: F,
) -> HttpResponse
where
    T: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + Unpin,
    F: Fn(&T) -> (f64, f64),
{
    let alerts_collection: Collection<T> = db.collection(&format!("{}_alerts", survey));
    let mut alert_cursor = match alerts_collection
        .find(filter_doc)
        .max_time(MOC_SEARCH_QUERY_TIMEOUT)
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

    let mut results: Vec<T> = Vec::new();
    while let Some(alert_doc) = match alert_cursor.try_next().await {
        Ok(Some(doc)) => Some(doc),
        Ok(None) => None,
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    } {
        // Post-filter: check that the alert is actually inside the MOC
        let (ra, dec) = coords(&alert_doc);
        if is_in_moc(moc, ra, dec) {
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
            cones_len,
            depth
        ),
        serde_json::json!(results),
    )
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct AlertsMocSearchQuery {
    /// Base64-encoded MOC FITS file (exactly one of moc_fits_base64 or skymap_fits_base64 required)
    moc_fits_base64: Option<String>,
    /// Base64-encoded HEALPix skymap FITS file
    skymap_fits_base64: Option<String>,
    /// Credible level for skymap thresholding (optional, defaults to 0.9 if omitted when skymap_fits_base64 is provided)
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
    mut query: web::Json<AlertsMocSearchQuery>,
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

    // Validate which MOC source was provided before doing any heavy work.
    let moc_b64 = query.moc_fits_base64.take();
    let skymap_b64 = query.skymap_fits_base64.take();
    match (&moc_b64, &skymap_b64) {
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
        _ => {}
    }
    if moc_b64.is_some() && query.credible_level.is_some() {
        return response::bad_request(
            "credible_level only applies to skymap_fits_base64, not a pre-built moc_fits_base64",
        );
    }
    let credible_level = query.credible_level.unwrap_or(0.9);

    let limit = query.limit.unwrap_or(10000).min(10000);
    if limit == 0 {
        return response::bad_request("limit must be between 1 and 10000");
    }

    // Decoding the base64 payload (up to ~70 MB), parsing the MOC/skymap, and
    // computing the covering cones is pure CPU work with no `.await` points.
    // Running it on the async worker would block that worker for the whole
    // computation, so offload it to the blocking thread pool.
    let computation = web::block(
        move || -> Result<(HpxMoc, u8, Vec<(f64, f64, f64)>), String> {
            let moc = match (moc_b64, skymap_b64) {
                (Some(moc_b64), _) => {
                    let bytes = BASE64_STANDARD
                        .decode(moc_b64)
                        .map_err(|e| format!("Invalid base64 in moc_fits_base64: {}", e))?;
                    moc_from_fits_bytes(&bytes)?
                }
                (None, Some(skymap_b64)) => {
                    if !(0.0..=1.0).contains(&credible_level) {
                        return Err("credible_level must be between 0.0 and 1.0".to_string());
                    }
                    let bytes = BASE64_STANDARD
                        .decode(skymap_b64)
                        .map_err(|e| format!("Invalid base64 in skymap_fits_base64: {}", e))?;
                    moc_from_skymap_bytes(&bytes, credible_level)?
                }
                // Presence is validated above: exactly one source is provided.
                (None, None) => unreachable!("MOC source presence validated before web::block"),
            };
            let depth = select_covering_depth(&moc);
            let cones = moc_to_covering_cones(&moc, depth);
            Ok((moc, depth, cones))
        },
    )
    .await;

    let (moc, depth, cones) = match computation {
        Ok(Ok(result)) => result,
        Ok(Err(message)) => return response::bad_request(&message),
        Err(e) => {
            return response::internal_error(&format!("error processing MOC: {}", e));
        }
    };

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
            collect_moc_alerts::<EnrichedZtfAlert, _>(
                &db,
                survey,
                filter_doc,
                &moc,
                limit,
                cones.len(),
                depth,
                |alert| (alert.candidate.candidate.ra, alert.candidate.candidate.dec),
            )
            .await
        }
        Survey::Lsst => {
            collect_moc_alerts::<EnrichedLsstAlert, _>(
                &db,
                survey,
                filter_doc,
                &moc,
                limit,
                cones.len(),
                depth,
                |alert| {
                    (
                        alert.candidate.dia_source.ra,
                        alert.candidate.dia_source.dec,
                    )
                },
            )
            .await
        }
        _ => response::bad_request("Invalid survey specified, only ZTF and LSST are supported"),
    }
}

// ── 3D credible-volume search ─────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct AlertsSkymap3dSearchQuery {
    /// Base64-encoded LIGO BAYESTAR FITS file (required)
    pub bayestar_fits_base64: String,
    /// Credible level for the 3D volume test (defaults to 0.9)
    pub credible_level: Option<f64>,
    /// Start of time window (Julian Date, required)
    pub start_jd: f64,
    /// End of time window (Julian Date, required; max 7 days after start_jd)
    pub end_jd: f64,
    pub min_magpsf: Option<f64>,
    pub max_magpsf: Option<f64>,
    #[serde(alias = "min_reliability")]
    pub min_drb: Option<f64>,
    #[serde(alias = "max_reliability")]
    pub max_drb: Option<f64>,
    pub is_rock: Option<bool>,
    pub is_star: Option<bool>,
    pub is_near_brightstar: Option<bool>,
    pub is_stationary: Option<bool>,
    pub limit: Option<u32>,
}

#[derive(Debug, serde::Serialize, ToSchema)]
pub struct ZtfAlertSkymap3dResult {
    #[serde(flatten)]
    pub alert: EnrichedZtfAlert,
    /// Best searched_prob_vol across NED cross-match candidates (None if no NED match with valid z).
    pub host_searched_prob_vol: Option<f64>,
}

#[derive(Debug, serde::Serialize, ToSchema)]
pub struct LsstAlertSkymap3dResult {
    #[serde(flatten)]
    pub alert: EnrichedLsstAlert,
    /// Best searched_prob_vol across NED cross-match candidates (None if no NED match with valid z).
    pub host_searched_prob_vol: Option<f64>,
}

#[utoipa::path(
    post,
    path = "/babamul/surveys/{survey}/alerts/skymap-3d-search",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
    ),
    request_body = AlertsSkymap3dSearchQuery,
    responses(
        (status = 200, description = "Alerts inside the 3D credible volume", body = Vec<ZtfAlertSkymap3dResult>),
        (status = 400, description = "Invalid query parameters or FITS data"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[post("/surveys/{survey}/alerts/skymap-3d-search")]
pub async fn alerts_skymap_3d_search(
    path: web::Path<Survey>,
    query: web::Json<AlertsSkymap3dSearchQuery>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    let _current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };
    let survey = path.into_inner();

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

    let credible_level = query.credible_level.unwrap_or(0.9);
    if !(0.0..=1.0).contains(&credible_level) {
        return response::bad_request("credible_level must be between 0.0 and 1.0");
    }

    // Decode and parse the BAYESTAR FITS
    let fits_bytes = match BASE64_STANDARD.decode(&query.bayestar_fits_base64) {
        Ok(b) => b,
        Err(e) => {
            return response::bad_request(&format!("Invalid base64 in bayestar_fits_base64: {}", e))
        }
    };
    let skymap = match parse_3d_skymap_bytes(&fits_bytes) {
        Ok(s) => s,
        Err(e) => return response::bad_request(&format!("Failed to parse BAYESTAR FITS: {}", e)),
    };

    // Build the density-sorted index and 2D MOC prefilter
    let idx = CredibleVolumeIndex::build(&skymap, 200);
    let moc_2d = credible_volume_to_2d_moc(&skymap, &idx, credible_level);

    let depth = select_covering_depth(&moc_2d);
    let cones = moc_to_covering_cones(&moc_2d, depth);
    if cones.is_empty() {
        return response::ok(
            "3D credible volume projects to an empty sky region",
            serde_json::json!([]),
        );
    }
    if cones.len() > MOC_SEARCH_MAX_CONES {
        return response::bad_request(&format!(
            "2D projection too large: {} covering cones at depth {} (max {}). Use a smaller credible level.",
            cones.len(), depth, MOC_SEARCH_MAX_CONES
        ));
    }

    let limit = query.limit.unwrap_or(10000).min(10000);
    if limit == 0 {
        return response::bad_request("limit must be between 1 and 10000");
    }

    // Build the MongoDB filter (same structure as moc_search_alerts)
    let jd_filter = doc! { "candidate.jd": { "$gte": query.start_jd, "$lte": query.end_jd } };
    let or_conditions: Vec<Document> = cones
        .iter()
        .map(|&(ra, dec, radius_rad)| {
            doc! {
                "coordinates.radec_geojson": {
                    "$geoWithin": {
                        "$centerSphere": [[ra - 180.0, dec], radius_rad]
                    }
                }
            }
        })
        .collect();

    let mut filter_doc = jd_filter;
    filter_doc.insert("$or", or_conditions);

    if survey == Survey::Ztf {
        filter_doc.insert("candidate.programid", 1);
    }
    if query.min_magpsf.is_some() || query.max_magpsf.is_some() {
        let mut f = Document::new();
        if let Some(v) = query.min_magpsf {
            f.insert("$gte", v);
        }
        if let Some(v) = query.max_magpsf {
            f.insert("$lte", v);
        }
        filter_doc.insert("candidate.magpsf", f);
    }
    if query.min_drb.is_some() || query.max_drb.is_some() {
        let drb_key = match survey {
            Survey::Ztf => "candidate.drb",
            Survey::Lsst => "candidate.reliability",
            _ => {
                return response::bad_request(
                    "Invalid survey specified, only ZTF and LSST are supported",
                )
            }
        };
        let mut f = Document::new();
        if let Some(v) = query.min_drb {
            f.insert("$gte", v);
        }
        if let Some(v) = query.max_drb {
            f.insert("$lte", v);
        }
        filter_doc.insert(drb_key, f);
    }
    if let Some(v) = query.is_rock {
        filter_doc.insert("properties.rock", v);
    }
    if let Some(v) = query.is_star {
        filter_doc.insert("properties.star", v);
    }
    if let Some(v) = query.is_near_brightstar {
        filter_doc.insert("properties.near_brightstar", v);
    }
    if let Some(v) = query.is_stationary {
        filter_doc.insert("properties.stationary", v);
    }

    match survey {
        Survey::Ztf => {
            let alerts_col: Collection<EnrichedZtfAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut cursor = match alerts_col
                .find(filter_doc)
                .max_time(MOC_SEARCH_QUERY_TIMEOUT)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    return response::internal_error(&format!("error querying alerts: {}", e))
                }
            };

            // Phase 1: spatial post-filter
            const SPATIAL_CAP: usize = 50_000;
            let mut spatial_candidates: Vec<EnrichedZtfAlert> = Vec::new();
            loop {
                match cursor.try_next().await {
                    Ok(Some(doc)) => {
                        let ra = doc.candidate.candidate.ra;
                        let dec = doc.candidate.candidate.dec;
                        if is_in_moc(&moc_2d, ra, dec) {
                            spatial_candidates.push(doc);
                            if spatial_candidates.len() >= SPATIAL_CAP {
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return response::internal_error(&format!("error reading cursor: {}", e))
                    }
                }
            }

            // Phase 2: batch aux lookup for NED cross-matches
            let object_ids: Vec<Bson> = spatial_candidates
                .iter()
                .map(|a| Bson::String(a.object_id.clone()))
                .collect();

            let aux_col: Collection<Document> = db.collection(&format!("{}_alerts_aux", survey));
            let mut aux_cursor = match aux_col
                .find(doc! { "_id": { "$in": object_ids } })
                .projection(doc! { "_id": 1, "cross_matches": 1 })
                .await
            {
                Ok(c) => c,
                Err(e) => return response::internal_error(&format!("error querying aux: {}", e)),
            };

            // objectId → best z from NED + DESI_DR1
            let mut host_z_map: HashMap<String, Vec<f64>> = HashMap::new();
            loop {
                match aux_cursor.try_next().await {
                    Ok(Some(aux_doc)) => {
                        let oid = match aux_doc.get_str("_id") {
                            Ok(s) => s.to_string(),
                            Err(_) => continue,
                        };
                        let cm = aux_doc.get_document("cross_matches").ok();
                        let z_values: Vec<f64> = [("NED", "z"), ("DESI_DR1", "z")]
                            .iter()
                            .flat_map(|(catalog, z_key)| {
                                cm.and_then(|cm| cm.get_array(*catalog).ok())
                                    .into_iter()
                                    .flatten()
                                    .filter_map(|v| v.as_document())
                                    .filter(|m| {
                                        if *catalog == "DESI_DR1" {
                                            m.get_str("spectype")
                                                .map(|s| s != "STAR")
                                                .unwrap_or(true)
                                        } else {
                                            true
                                        }
                                    })
                                    .filter_map(|m| m.get_f64(*z_key).ok())
                                    .filter(|&z| z.is_finite() && z > 0.0)
                                    .collect::<Vec<_>>()
                            })
                            .collect();
                        host_z_map.insert(oid, z_values);
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return response::internal_error(&format!(
                            "error reading aux cursor: {}",
                            e
                        ))
                    }
                }
            }

            // Phase 3: 3D test per alert
            let mut results: Vec<ZtfAlertSkymap3dResult> = Vec::new();
            for alert in spatial_candidates {
                let ra = alert.candidate.candidate.ra;
                let dec = alert.candidate.candidate.dec;

                let z_values = host_z_map
                    .get(&alert.object_id)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);

                if z_values.is_empty() {
                    // No NED match with valid z — pass through on 2D test alone
                    results.push(ZtfAlertSkymap3dResult {
                        alert,
                        host_searched_prob_vol: None,
                    });
                } else {
                    // Find best (lowest) searched_prob_vol across all NED matches
                    let best_spv = z_values
                        .iter()
                        .filter_map(|&z| {
                            let d_mpc = luminosity_distance_mpc(z);
                            idx.searched_prob_vol_at(&skymap, ra, dec, d_mpc)
                        })
                        .fold(f64::INFINITY, f64::min);

                    if best_spv.is_finite() && best_spv <= credible_level {
                        results.push(ZtfAlertSkymap3dResult {
                            alert,
                            host_searched_prob_vol: Some(best_spv),
                        });
                    }
                    // else: has NED matches but none inside the credible volume — drop
                }

                if results.len() >= limit as usize {
                    break;
                }
            }

            response::ok(
                &format!(
                    "found {} alerts inside {:.0}% 3D credible volume ({} covering cones at depth {})",
                    results.len(),
                    credible_level * 100.0,
                    cones.len(),
                    depth
                ),
                serde_json::json!(results),
            )
        }
        Survey::Lsst => {
            let alerts_col: Collection<EnrichedLsstAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut cursor = match alerts_col
                .find(filter_doc)
                .max_time(MOC_SEARCH_QUERY_TIMEOUT)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    return response::internal_error(&format!("error querying alerts: {}", e))
                }
            };

            const SPATIAL_CAP: usize = 50_000;
            let mut spatial_candidates: Vec<EnrichedLsstAlert> = Vec::new();
            loop {
                match cursor.try_next().await {
                    Ok(Some(doc)) => {
                        let ra = doc.candidate.dia_source.ra;
                        let dec = doc.candidate.dia_source.dec;
                        if is_in_moc(&moc_2d, ra, dec) {
                            spatial_candidates.push(doc);
                            if spatial_candidates.len() >= SPATIAL_CAP {
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return response::internal_error(&format!("error reading cursor: {}", e))
                    }
                }
            }

            let object_ids: Vec<Bson> = spatial_candidates
                .iter()
                .map(|a| Bson::String(a.object_id.clone()))
                .collect();

            let aux_col: Collection<Document> = db.collection(&format!("{}_alerts_aux", survey));
            let mut aux_cursor = match aux_col
                .find(doc! { "_id": { "$in": object_ids } })
                .projection(doc! { "_id": 1, "cross_matches": 1 })
                .await
            {
                Ok(c) => c,
                Err(e) => return response::internal_error(&format!("error querying aux: {}", e)),
            };

            let mut host_z_map: HashMap<String, Vec<f64>> = HashMap::new();
            loop {
                match aux_cursor.try_next().await {
                    Ok(Some(aux_doc)) => {
                        let oid = match aux_doc.get_str("_id") {
                            Ok(s) => s.to_string(),
                            Err(_) => continue,
                        };
                        let cm = aux_doc.get_document("cross_matches").ok();
                        let z_values: Vec<f64> = [("NED", "z"), ("DESI_DR1", "z")]
                            .iter()
                            .flat_map(|(catalog, z_key)| {
                                cm.and_then(|cm| cm.get_array(*catalog).ok())
                                    .into_iter()
                                    .flatten()
                                    .filter_map(|v| v.as_document())
                                    .filter(|m| {
                                        if *catalog == "DESI_DR1" {
                                            m.get_str("spectype")
                                                .map(|s| s != "STAR")
                                                .unwrap_or(true)
                                        } else {
                                            true
                                        }
                                    })
                                    .filter_map(|m| m.get_f64(*z_key).ok())
                                    .filter(|&z| z.is_finite() && z > 0.0)
                                    .collect::<Vec<_>>()
                            })
                            .collect();
                        host_z_map.insert(oid, z_values);
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return response::internal_error(&format!(
                            "error reading aux cursor: {}",
                            e
                        ))
                    }
                }
            }

            let mut results: Vec<LsstAlertSkymap3dResult> = Vec::new();
            for alert in spatial_candidates {
                let ra = alert.candidate.dia_source.ra;
                let dec = alert.candidate.dia_source.dec;

                let z_values = host_z_map
                    .get(&alert.object_id)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);

                if z_values.is_empty() {
                    results.push(LsstAlertSkymap3dResult {
                        alert,
                        host_searched_prob_vol: None,
                    });
                } else {
                    let best_spv = z_values
                        .iter()
                        .filter_map(|&z| {
                            let d_mpc = luminosity_distance_mpc(z);
                            idx.searched_prob_vol_at(&skymap, ra, dec, d_mpc)
                        })
                        .fold(f64::INFINITY, f64::min);

                    if best_spv.is_finite() && best_spv <= credible_level {
                        results.push(LsstAlertSkymap3dResult {
                            alert,
                            host_searched_prob_vol: Some(best_spv),
                        });
                    }
                }

                if results.len() >= limit as usize {
                    break;
                }
            }

            response::ok(
                &format!(
                    "found {} alerts inside {:.0}% 3D credible volume ({} covering cones at depth {})",
                    results.len(),
                    credible_level * 100.0,
                    cones.len(),
                    depth
                ),
                serde_json::json!(results),
            )
        }
        _ => response::bad_request("Invalid survey specified, only ZTF and LSST are supported"),
    }
}
