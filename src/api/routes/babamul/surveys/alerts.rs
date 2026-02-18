use crate::alert::{LsstCandidate, ZtfCandidate};
use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use crate::enrichment::{LsstAlertProperties, ZtfAlertClassifications, ZtfAlertProperties};
use crate::utils::enums::Survey;
use actix_web::{get, post, web, HttpResponse};
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
        // if the radius is > 600 arcsec (10 arcmin), reject the query to avoid expensive searches
        if radius_arcsec > 600.0 {
            return response::bad_request(
                "Radius too large, maximum allowed is 600 arcseconds (10 arcminutes)",
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

    if query.is_rock.is_some() {
        filter_doc.insert("properties.rock", query.is_rock.unwrap());
    }
    if query.is_star.is_some() {
        filter_doc.insert("properties.star", query.is_star.unwrap());
    }
    if query.is_near_brightstar.is_some() {
        filter_doc.insert(
            "properties.near_brightstar",
            query.is_near_brightstar.unwrap(),
        );
    }
    if query.is_stationary.is_some() {
        filter_doc.insert("properties.stationary", query.is_stationary.unwrap());
    }

    match survey {
        Survey::Ztf => {
            let alerts_collection: Collection<EnrichedZtfAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut alert_cursor = match alerts_collection
                .find(filter_doc)
                .sort(doc! { "_id": 1 })
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
                &format!("found {} objects matching query", results.len()),
                serde_json::json!(results),
            );
        }
        Survey::Lsst => {
            let alerts_collection: Collection<EnrichedLsstAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut alert_cursor = match alerts_collection
                .find(filter_doc)
                .sort(doc! { "_id": 1 })
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
                &format!("found {} objects matching query", results.len()),
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
    path = "/babamul/surveys/{survey}/alerts/cone_search",
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
#[post("/surveys/{survey}/alerts/cone_search")]
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
    if query.is_rock.is_some() {
        base_filter_doc.insert("properties.rock", query.is_rock.unwrap());
    }
    if query.is_star.is_some() {
        base_filter_doc.insert("properties.star", query.is_star.unwrap());
    }
    if query.is_near_brightstar.is_some() {
        base_filter_doc.insert(
            "properties.near_brightstar",
            query.is_near_brightstar.unwrap(),
        );
    }
    if query.is_stationary.is_some() {
        base_filter_doc.insert("properties.stationary", query.is_stationary.unwrap());
    }

    match survey {
        Survey::Ztf => {
            let alerts_collection: Collection<EnrichedZtfAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut results: HashMap<String, Vec<EnrichedZtfAlert>> = HashMap::new();
            for (object_id, radec) in coordinates {
                let ra = radec[0] - 180.0;
                let dec = radec[1];
                let center_sphere = doc! {
                    "coordinates.radec_geojson": {
                        "$geoWithin": {
                            "$centerSphere": [
                                [ra, dec],
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
                }
                results.insert(object_id.clone(), alert_results);
            }
            return response::ok(
                &format!("found alerts matching query"),
                serde_json::json!(results),
            );
        }
        Survey::Lsst => {
            // similar to above but for LSST collection
            let alerts_collection: Collection<EnrichedLsstAlert> =
                db.collection(&format!("{}_alerts", survey));
            let mut results: HashMap<String, Vec<EnrichedLsstAlert>> = HashMap::new();
            for (object_id, radec) in coordinates {
                let ra = radec[0] - 180.0;
                let dec = radec[1];
                let center_sphere = doc! {
                    "coordinates.radec_geojson": {
                        "$geoWithin": {
                            "$centerSphere": [
                                [ra, dec],
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
                }
                results.insert(object_id.clone(), alert_results);
            }
            return response::ok(
                &format!("found alerts matching query"),
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
