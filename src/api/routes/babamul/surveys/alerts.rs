use crate::alert::{LsstCandidate, ZtfCandidate};
use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use crate::enrichment::{LsstAlertProperties, ZtfAlertClassifications, ZtfAlertProperties};
use crate::utils::cosmology::luminosity_distance_mpc;
use crate::utils::enums::Survey;
use crate::utils::moc::{
    credible_volume_to_2d_moc, is_in_moc, moc_from_fits_bytes, moc_from_skymap_bytes,
    parse_3d_skymap_bytes, select_covering_depth_bounded, Cone, CredibleVolumeIndex, HpxMoc,
    LIGO3dskymap, Skymap3dError,
};
use crate::utils::spatial::get_f64_from_doc;
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
        // MongoDB only picks the geospatial index if this key comes first.
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

const MOC_SEARCH_MAX_TIME_WINDOW_JD: f64 = 7.0;
const MOC_SEARCH_MAX_CONES: usize = 500;
const MOC_SEARCH_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Bounds memory before the host-galaxy cross-match lookup.
const SKYMAP_3D_SPATIAL_CAP: usize = 50_000;

enum SkymapSearchMode {
    Moc(HpxMoc),
    Skymap2d(HpxMoc),
    Skymap3d {
        skymap: Box<LIGO3dskymap>,
        idx: CredibleVolumeIndex,
        moc_2d: HpxMoc,
        credible_level: f64,
    },
}

impl SkymapSearchMode {
    /// Exact for `Moc`/`Skymap2d`, only a pre-filter for `Skymap3d`.
    fn moc_2d(&self) -> &HpxMoc {
        match self {
            SkymapSearchMode::Moc(moc) | SkymapSearchMode::Skymap2d(moc) => moc,
            SkymapSearchMode::Skymap3d { moc_2d, .. } => moc_2d,
        }
    }
}

/// Host-galaxy redshifts from an alert's `cross_matches`, best-first and
/// deduplicated by 3-arcsec proximity so one galaxy isn't counted per catalog.
///
/// Priority: 0 = DESI spec (zwarn=0), 1 = NED SPEC, 2 = DESI spec (zwarn!=0)
/// and LSDR10 spec, 3 = NED PHOT, 4 = LSDR10 photo-z.
fn extract_host_redshifts(cross_matches: Option<&Document>) -> Vec<f64> {
    /// `priority` ranks a row (lower = better) or returns None to skip it.
    fn extract_catalog_zs(
        cross_matches: Option<&Document>,
        catalog: &str,
        z_field: &str,
        priority: impl Fn(&Document) -> Option<u8>,
        ranked: &mut Vec<(u8, f64, f64, f64)>,
    ) {
        let Some(arr) = cross_matches.and_then(|cm| cm.get_array(catalog).ok()) else {
            return;
        };
        for v in arr {
            let Some(m) = v.as_document() else { continue };
            let Some(p) = priority(m) else { continue };
            // Document::get_f64 rejects the Int32 these catalogs sometimes store.
            let Some(z) = get_f64_from_doc(m, z_field).filter(|&z| z > 0.0) else {
                continue;
            };
            let Some(ra) = get_f64_from_doc(m, "ra") else {
                continue;
            };
            let Some(dec) = get_f64_from_doc(m, "dec") else {
                continue;
            };
            ranked.push((p, ra, dec, z));
        }
    }

    let mut ranked: Vec<(u8, f64, f64, f64)> = Vec::new(); // (priority, ra, dec, z)

    extract_catalog_zs(
        cross_matches,
        "DESI_DR1",
        "z",
        |m| {
            if m.get_str("spectype").map(|s| s == "STAR").unwrap_or(false) {
                return None;
            }
            Some(if get_f64_from_doc(m, "zwarn").unwrap_or(1.0) == 0.0 {
                0
            } else {
                2
            })
        },
        &mut ranked,
    );
    extract_catalog_zs(
        cross_matches,
        "NED",
        "z",
        |m| {
            Some(
                if m.get_str("z_tech").map(|s| s == "SPEC").unwrap_or(false) {
                    1
                } else {
                    3
                },
            )
        },
        &mut ranked,
    );
    // A Legacy row carrying both is deduplicated below, keeping the spectroscopic one.
    extract_catalog_zs(cross_matches, "LSDR10", "z_spec", |_| Some(2), &mut ranked);
    extract_catalog_zs(
        cross_matches,
        "LSDR10",
        "z_phot_median",
        |_| Some(4),
        &mut ranked,
    );

    ranked.sort_by_key(|&(p, _, _, _)| p);
    const DEDUP_ARCSEC: f64 = 3.0;
    let mut kept: Vec<(f64, f64, f64)> = Vec::new(); // (ra, dec, z)
    for (_, ra, dec, z) in ranked {
        let is_dup = kept.iter().any(|&(kra, kdec, _)| {
            let dra = (ra - kra) * dec.to_radians().cos();
            let ddec = dec - kdec;
            (dra * dra + ddec * ddec).sqrt() * 3600.0 < DEDUP_ARCSEC
        });
        if !is_dup {
            kept.push((ra, dec, z));
        }
    }
    kept.into_iter().map(|(_, _, z)| z).collect()
}

/// The spatial `$or` is only a covering-cone pre-filter, so each alert is re-tested
/// against the real region here. `truncated` means `SKYMAP_3D_SPATIAL_CAP` was hit,
/// not the caller's `limit`.
async fn collect_skymap_alerts<T, F>(
    db: &Database,
    survey: Survey,
    filter_doc: Document,
    mode: &SkymapSearchMode,
    limit: u32,
    coords: F,
    object_id: fn(&T) -> &str,
) -> Result<(Vec<(T, Option<f64>)>, bool), HttpResponse>
where
    T: serde::de::DeserializeOwned + Send + Sync + Unpin,
    F: Fn(&T) -> (f64, f64),
{
    let alerts_collection: Collection<T> = db.collection(&format!("{}_alerts", survey));
    let mut cursor = alerts_collection
        .find(filter_doc)
        .max_time(MOC_SEARCH_QUERY_TIMEOUT)
        .await
        .map_err(|e| response::internal_error(&format!("error querying alerts: {}", e)))?;

    match mode {
        SkymapSearchMode::Moc(moc) | SkymapSearchMode::Skymap2d(moc) => {
            let mut results = Vec::new();
            while let Some(alert) = cursor
                .try_next()
                .await
                .map_err(|e| response::internal_error(&format!("error reading cursor: {}", e)))?
            {
                let (ra, dec) = coords(&alert);
                if is_in_moc(moc, ra, dec) {
                    results.push((alert, None));
                    if results.len() >= limit as usize {
                        break;
                    }
                }
            }
            Ok((results, false))
        }
        SkymapSearchMode::Skymap3d {
            skymap,
            idx,
            moc_2d,
            credible_level,
        } => {
            let mut spatial_candidates: Vec<T> = Vec::new();
            let mut truncated = false;
            while let Some(alert) = cursor
                .try_next()
                .await
                .map_err(|e| response::internal_error(&format!("error reading cursor: {}", e)))?
            {
                let (ra, dec) = coords(&alert);
                if is_in_moc(moc_2d, ra, dec) {
                    spatial_candidates.push(alert);
                    if spatial_candidates.len() >= SKYMAP_3D_SPATIAL_CAP {
                        truncated = true;
                        break;
                    }
                }
            }

            let object_ids: Vec<Bson> = spatial_candidates
                .iter()
                .map(|a| Bson::String(object_id(a).to_string()))
                .collect();
            let aux_col: Collection<Document> = db.collection(&format!("{}_alerts_aux", survey));
            let mut aux_cursor = aux_col
                .find(doc! { "_id": { "$in": object_ids } })
                .projection(doc! { "_id": 1, "cross_matches": 1 })
                .await
                .map_err(|e| response::internal_error(&format!("error querying aux: {}", e)))?;

            let mut host_z_map: HashMap<String, Vec<f64>> = HashMap::new();
            while let Some(aux_doc) = aux_cursor.try_next().await.map_err(|e| {
                response::internal_error(&format!("error reading aux cursor: {}", e))
            })? {
                let Ok(oid) = aux_doc.get_str("_id") else {
                    continue;
                };
                let z_values = extract_host_redshifts(aux_doc.get_document("cross_matches").ok());
                host_z_map.insert(oid.to_string(), z_values);
            }

            // luminosity_distance_mpc is a 200-step integration; alerts share hosts.
            let mut dist_cache: HashMap<u64, f64> = HashMap::new();
            let mut results = Vec::new();
            for alert in spatial_candidates {
                let (ra, dec) = coords(&alert);
                let z_values = host_z_map
                    .get(object_id(&alert))
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);

                if z_values.is_empty() {
                    // No host: the 2D test is all we have, so let it through.
                    results.push((alert, None));
                } else {
                    let best_spv = z_values
                        .iter()
                        .filter_map(|&z| {
                            let d_mpc = *dist_cache
                                .entry(z.to_bits())
                                .or_insert_with(|| luminosity_distance_mpc(z));
                            idx.searched_prob_vol_at(skymap, ra, dec, d_mpc)
                        })
                        .fold(f64::INFINITY, f64::min);

                    if best_spv.is_finite() && best_spv <= *credible_level {
                        results.push((alert, Some(best_spv)));
                    }
                }

                if results.len() >= limit as usize {
                    break;
                }
            }
            Ok((results, truncated))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn collect_and_wrap_skymap_alerts<T, F, R, W>(
    db: &Database,
    survey: Survey,
    filter_doc: Document,
    mode: &SkymapSearchMode,
    limit: u32,
    coords: F,
    object_id: fn(&T) -> &str,
    wrap: W,
) -> Result<(serde_json::Value, bool), HttpResponse>
where
    T: serde::de::DeserializeOwned + Send + Sync + Unpin,
    F: Fn(&T) -> (f64, f64),
    R: serde::Serialize,
    W: Fn(T, Option<f64>) -> R,
{
    let (pairs, truncated) =
        collect_skymap_alerts(db, survey, filter_doc, mode, limit, coords, object_id).await?;
    let data = serde_json::json!(pairs
        .into_iter()
        .map(|(alert, host_searched_prob_vol)| wrap(alert, host_searched_prob_vol))
        .collect::<Vec<_>>());
    Ok((data, truncated))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct AlertsSkymapSearchQuery {
    /// Base64-encoded MOC FITS file (exactly one of moc_fits_base64 or skymap_fits_base64 required)
    moc_fits_base64: Option<String>,
    /// Base64-encoded HEALPix skymap FITS file. Automatically classified as a 3D
    /// LIGO/Virgo/KAGRA BAYESTAR localization (distance-aware credible-volume
    /// test, refined against any cross-matched host-galaxy redshift) if it carries
    /// DISTMU/DISTSIGMA/DISTNORM columns, otherwise as a plain 2D probability skymap.
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

#[derive(Debug, serde::Serialize, ToSchema)]
pub struct ZtfAlertSkymapSearchResult {
    #[serde(flatten)]
    pub alert: EnrichedZtfAlert,
    /// Best searched_prob_vol across cross-matched host-galaxy candidates, for a 3D
    /// (BAYESTAR) search with a redshift match. Null for MOC/2D-skymap searches, or
    /// when the alert had no cross-matched host.
    pub host_searched_prob_vol: Option<f64>,
}

#[derive(Debug, serde::Serialize, ToSchema)]
pub struct LsstAlertSkymapSearchResult {
    #[serde(flatten)]
    pub alert: EnrichedLsstAlert,
    /// Best searched_prob_vol across cross-matched host-galaxy candidates, for a 3D
    /// (BAYESTAR) search with a redshift match. Null for MOC/2D-skymap searches, or
    /// when the alert had no cross-matched host.
    pub host_searched_prob_vol: Option<f64>,
}

#[utoipa::path(
    post,
    path = "/babamul/surveys/{survey}/alerts/skymap-search",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
    ),
    request_body = AlertsSkymapSearchQuery,
    responses(
        (status = 200, description = "Alerts within the MOC/skymap region; items are LsstAlertSkymapSearchResult for the lsst survey", body = Vec<ZtfAlertSkymapSearchResult>),
        (status = 400, description = "Invalid query parameters or MOC/skymap data"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[post("/surveys/{survey}/alerts/skymap-search")]
pub async fn skymap_search_alerts(
    path: web::Path<Survey>,
    mut query: web::Json<AlertsSkymapSearchQuery>,
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
    if skymap_b64.is_some() && !(0.0..=1.0).contains(&credible_level) {
        return response::bad_request("credible_level must be between 0.0 and 1.0");
    }

    let limit = query.limit.unwrap_or(10000).min(10000);
    if limit == 0 {
        return response::bad_request("limit must be between 1 and 10000");
    }

    // Pure CPU with no await points, so it must not run on the async worker.
    let computation = web::block(
        move || -> Result<(SkymapSearchMode, u8, Vec<Cone>), String> {
            let mode = match (moc_b64, skymap_b64) {
                (Some(moc_b64), _) => {
                    let bytes = BASE64_STANDARD
                        .decode(moc_b64)
                        .map_err(|e| format!("Invalid base64 in moc_fits_base64: {}", e))?;
                    SkymapSearchMode::Moc(moc_from_fits_bytes(&bytes)?)
                }
                (None, Some(skymap_b64)) => {
                    let bytes = BASE64_STANDARD
                        .decode(skymap_b64)
                        .map_err(|e| format!("Invalid base64 in skymap_fits_base64: {}", e))?;
                    match parse_3d_skymap_bytes(&bytes) {
                        Ok(skymap) => {
                            let idx = CredibleVolumeIndex::build(&skymap, 200);
                            let moc_2d = credible_volume_to_2d_moc(&skymap, &idx, credible_level);
                            SkymapSearchMode::Skymap3d {
                                skymap: Box::new(skymap),
                                idx,
                                moc_2d,
                                credible_level,
                            }
                        }
                        Err(Skymap3dError::NotThreeDimensional) => SkymapSearchMode::Skymap2d(
                            moc_from_skymap_bytes(&bytes, credible_level)?,
                        ),
                        Err(e @ Skymap3dError::Invalid(_)) => {
                            return Err(format!("Invalid 3D skymap FITS: {}", e));
                        }
                    }
                }
                (None, None) => unreachable!("spatial source presence validated before web::block"),
            };
            let (depth, cones) = select_covering_depth_bounded(mode.moc_2d(), MOC_SEARCH_MAX_CONES);
            Ok((mode, depth, cones))
        },
    )
    .await;

    let (mode, depth, cones) = match computation {
        Ok(Ok(result)) => result,
        Ok(Err(message)) => return response::bad_request(&message),
        Err(e) => {
            return response::internal_error(&format!("error processing spatial region: {}", e));
        }
    };

    if cones.is_empty() {
        return response::ok(
            "search region is empty, no alerts to search",
            serde_json::json!([]),
        );
    }
    if cones.len() > MOC_SEARCH_MAX_CONES {
        return response::bad_request(&format!(
            "Search region too large: {} covering cones at depth {} (max {}). Use a smaller credible level or a more targeted MOC.",
            cones.len(),
            depth,
            MOC_SEARCH_MAX_CONES
        ));
    }

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

    let (data, truncated) = match survey {
        Survey::Ztf => {
            match collect_and_wrap_skymap_alerts::<EnrichedZtfAlert, _, _, _>(
                &db,
                survey,
                filter_doc,
                &mode,
                limit,
                |alert| (alert.candidate.candidate.ra, alert.candidate.candidate.dec),
                |alert| alert.object_id.as_str(),
                |alert, host_searched_prob_vol| ZtfAlertSkymapSearchResult {
                    alert,
                    host_searched_prob_vol,
                },
            )
            .await
            {
                Ok(result) => result,
                Err(resp) => return resp,
            }
        }
        Survey::Lsst => {
            match collect_and_wrap_skymap_alerts::<EnrichedLsstAlert, _, _, _>(
                &db,
                survey,
                filter_doc,
                &mode,
                limit,
                |alert| {
                    (
                        alert.candidate.dia_source.ra,
                        alert.candidate.dia_source.dec,
                    )
                },
                |alert| alert.object_id.as_str(),
                |alert, host_searched_prob_vol| LsstAlertSkymapSearchResult {
                    alert,
                    host_searched_prob_vol,
                },
            )
            .await
            {
                Ok(result) => result,
                Err(resp) => return resp,
            }
        }
        _ => {
            return response::bad_request(
                "Invalid survey specified, only ZTF and LSST are supported",
            )
        }
    };

    let n = data.as_array().map(|a| a.len()).unwrap_or(0);
    let mut message = match &mode {
        SkymapSearchMode::Moc(_) => format!(
            "found {} alerts within MOC region ({} covering cones at depth {})",
            n,
            cones.len(),
            depth
        ),
        SkymapSearchMode::Skymap2d(_) => format!(
            "found {} alerts within {:.0}% credible region ({} covering cones at depth {})",
            n,
            credible_level * 100.0,
            cones.len(),
            depth
        ),
        SkymapSearchMode::Skymap3d { credible_level, .. } => format!(
            "found {} alerts inside {:.0}% 3D credible volume ({} covering cones at depth {})",
            n,
            credible_level * 100.0,
            cones.len(),
            depth
        ),
    };
    if truncated {
        message.push_str(&format!(
            " (warning: spatial pre-filter capped at {} candidates before the 3D test; \
             results may be incomplete — narrow the time window or credible level)",
            SKYMAP_3D_SPATIAL_CAP
        ));
    }

    response::ok(&message, data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{doc, Document};

    /// A cross-match block holding one row per catalog at the same position.
    fn cross_matches(rows: Vec<(&str, Document)>) -> Document {
        let mut d = Document::new();
        for (catalog, row) in rows {
            d.insert(catalog, vec![row]);
        }
        d
    }

    #[test]
    fn test_reads_legacy_redshifts_from_lsdr10() {
        let cm = cross_matches(vec![(
            "LSDR10",
            doc! { "ra": 10.0, "dec": 20.0, "z_phot_median": 0.31, "z_spec": -99.0 },
        )]);
        assert_eq!(extract_host_redshifts(Some(&cm)), vec![0.31]);
    }

    #[test]
    fn test_absent_legacy_redshift_sentinel_is_not_a_redshift() {
        // Legacy writes -99 where it has none; a negative z is never valid.
        let cm = cross_matches(vec![(
            "LSDR10",
            doc! { "ra": 10.0, "dec": 20.0, "z_phot_median": -99.0, "z_spec": -99.0 },
        )]);
        assert!(extract_host_redshifts(Some(&cm)).is_empty());
    }

    #[test]
    fn test_legacy_spectroscopic_outranks_its_own_photometric() {
        let cm = cross_matches(vec![(
            "LSDR10",
            doc! { "ra": 10.0, "dec": 20.0, "z_spec": 0.42, "z_phot_median": 0.31 },
        )]);
        // One physical source, so the photo-z is deduplicated away.
        assert_eq!(extract_host_redshifts(Some(&cm)), vec![0.42]);
    }

    #[test]
    fn test_desi_spectroscopic_outranks_legacy() {
        let cm = cross_matches(vec![
            (
                "LSDR10",
                doc! { "ra": 10.0, "dec": 20.0, "z_phot_median": 0.31, "z_spec": -99.0 },
            ),
            (
                "DESI_DR1",
                doc! { "ra": 10.0, "dec": 20.0, "z": 0.40, "zwarn": 0_i64 },
            ),
        ]);
        assert_eq!(extract_host_redshifts(Some(&cm)), vec![0.40]);
    }

    #[test]
    fn test_desi_zwarn_is_read_whatever_its_bson_int_width() {
        // Int32 zwarn used to read as "flagged", demoting every clean spec-z.
        let cm = cross_matches(vec![
            (
                "NED",
                doc! { "ra": 10.0, "dec": 20.0, "z": 0.11, "z_tech": "SPEC" },
            ),
            (
                "DESI_DR1",
                doc! { "ra": 10.0, "dec": 20.0, "z": 0.40, "zwarn": 0_i32 },
            ),
        ]);
        assert_eq!(extract_host_redshifts(Some(&cm)), vec![0.40]);
    }

    #[test]
    fn test_desi_flagged_redshift_ranks_below_ned_spec() {
        let cm = cross_matches(vec![
            (
                "NED",
                doc! { "ra": 10.0, "dec": 20.0, "z": 0.11, "z_tech": "SPEC" },
            ),
            (
                "DESI_DR1",
                doc! { "ra": 10.0, "dec": 20.0, "z": 0.40, "zwarn": 4_i32 },
            ),
        ]);
        assert_eq!(extract_host_redshifts(Some(&cm)), vec![0.11]);
    }

    #[test]
    fn test_distinct_positions_are_both_kept() {
        let cm = cross_matches(vec![(
            "NED",
            doc! { "ra": 10.0, "dec": 20.0, "z": 0.11, "z_tech": "SPEC" },
        )]);
        let mut with_far = cm.clone();
        with_far.insert(
            "LSDR10",
            vec![doc! { "ra": 11.0, "dec": 20.0, "z_phot_median": 0.25, "z_spec": -99.0 }],
        );
        let got = extract_host_redshifts(Some(&with_far));
        assert_eq!(
            got.len(),
            2,
            "a degree apart is not the same galaxy: {got:?}"
        );
    }
}
