use crate::alert::{AlertCutout, LsstCandidate, ZtfCandidate};
use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use crate::enrichment::{LsstAlertProperties, ZtfAlertClassifications, ZtfAlertProperties};
use crate::utils::enums::Survey;
use actix_web::{get, web, HttpResponse};
use base64::prelude::*;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    Collection, Database,
};
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
    min_sgscore1: Option<f64>,
    max_sgscore1: Option<f64>,
    min_distpsnr1: Option<f64>,
    max_distpsnr1: Option<f64>,
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
        ("min_sgscore1" = Option<f64>, Query, description = "Minimum sgscore1 for star-galaxy classification filter (ZTF only)"),
        ("max_sgscore1" = Option<f64>, Query, description = "Maximum sgscore1 for star-galaxy classification filter (ZTF only)"),
        ("min_distpsnr1" = Option<f64>, Query, description = "Minimum distpsnr1 for star-galaxy classification filter (ZTF only)"),
        ("max_distpsnr1" = Option<f64>, Query, description = "Maximum distpsnr1 for star-galaxy classification filter (ZTF only)"),
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
    let mut filter_doc = Document::new();
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
    } else {
        return response::bad_request(
            "Either object_id or (ra, dec, radius_arcsec) must be provided",
        );
    }
    if let (Some(start_jd), Some(end_jd)) = (query.start_jd, query.end_jd) {
        filter_doc.insert(
            "candidate.jd",
            doc! {
                "$gte": start_jd,
                "$lte": end_jd,
            },
        );
    }
    if let (Some(min_magpsf), Some(max_magpsf)) = (query.min_magpsf, query.max_magpsf) {
        filter_doc.insert(
            "candidate.magpsf",
            doc! {
                "$gte": min_magpsf,
                "$lte": max_magpsf,
            },
        );
    }
    if let (Some(min_drb), Some(max_drb)) = (query.min_drb, query.max_drb) {
        filter_doc.insert(
            "candidate.drb",
            doc! {
                "$gte": min_drb,
                "$lte": max_drb,
            },
        );
    }
    if let (Some(min_sgscore1), Some(max_sgscore1), Some(min_distpsnr1), Some(max_distpsnr1)) = (
        query.min_sgscore1,
        query.max_sgscore1,
        query.min_distpsnr1,
        query.max_distpsnr1,
    ) {
        // we don't support these yet for surveys other than ZTF
        if survey != Survey::Ztf {
            return response::bad_request("sgscore1 and distpsnr1 filters are only supported for ZTF survey (other surveys coming soon)");
        }
        filter_doc.insert(
            "candidate.sgscore1",
            doc! {
                "$gte": min_sgscore1,
                "$lte": max_sgscore1,
            },
        );
        filter_doc.insert(
            "candidate.distpsnr1",
            doc! {
                "$gte": min_distpsnr1,
                "$lte": max_distpsnr1,
            },
        );
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

// we also want an endpoint that given a candid, grabs the cutouts from the cutouts collection
#[utoipa::path(
    get,
    path = "/babamul/surveys/{survey}/alerts/{candid}/cutouts",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
        ("candid" = i64, Path, description = "Candid of the alert to retrieve cutouts for"),
    ),
    responses(
        (status = 200, description = "Cutouts retrieved successfully", body = serde_json::Value),
        (status = 404, description = "Cutouts not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey}/alerts/{candid}/cutouts")]
pub async fn get_alert_cutouts(
    path: web::Path<(Survey, i64)>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    let _current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };
    let (survey, candid) = path.into_inner();

    let cutout_collection: Collection<AlertCutout> =
        db.collection(&format!("{}_alerts_cutouts", survey));
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
        "cutout_science": BASE64_STANDARD.encode(&cutouts.cutout_science),
        "cutout_template": BASE64_STANDARD.encode(&cutouts.cutout_template),
        "cutout_difference": BASE64_STANDARD.encode(&cutouts.cutout_difference),
    });
    return response::ok(
        &format!("cutouts found for candid: {}", candid),
        serde_json::json!(resp),
    );
}
