use crate::{
    api::catalogs::WATCHLIST_PREFIX,
    conf,
    utils::{enums::Survey, o11y::logging::as_error},
};
use flare::spatial::{great_circle_distance, radec2lb};
use futures::stream::StreamExt;
use itertools::Itertools;
use mongodb::bson::{doc, Bson};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{instrument, trace, warn};

#[derive(thiserror::Error, Debug)]
pub enum XmatchError {
    #[error("value access error from bson")]
    BsonValueAccess(#[from] mongodb::bson::document::ValueAccessError),
    #[error("error from mongodb")]
    Mongodb(#[from] mongodb::error::Error),
    #[error("distance_key field is null")]
    NullDistanceKey,
    #[error("distance_max field is null")]
    NullDistanceMax,
    #[error("distance_max_near field is null")]
    NullDistanceMaxNear,
    #[error("failed to convert the bson data into a document")]
    AsDocumentError,
}

/// Field on a watchlist catalog document under which we record the alert
/// object_ids of each survey that crossmatched against it.
pub fn watchlist_match_field(survey: &Survey) -> String {
    format!("matching_{}_objects", survey.to_string().to_lowercase())
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct GeoJsonPoint {
    r#type: String,
    coordinates: Vec<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Coordinates {
    radec_geojson: GeoJsonPoint,
    l: Option<f64>,
    b: Option<f64>,
}

impl Coordinates {
    pub fn new(ra: f64, dec: f64) -> Self {
        let (l, b) = radec2lb(ra, dec);
        Coordinates {
            radec_geojson: GeoJsonPoint {
                r#type: "Point".to_string(),
                coordinates: vec![ra - 180.0, dec],
            },
            l: Some(l),
            b: Some(b),
        }
    }

    /// Get RA and Dec from the stored GeoJSON coordinates (formatting RA back to [0, 360])
    pub fn get_radec(&self) -> (f64, f64) {
        let ra = self.radec_geojson.coordinates[0] + 180.0;
        let dec = self.radec_geojson.coordinates[1];
        (ra, dec)
    }
}

/// Like [`get_f64_from_doc`] but silent when the field is absent or null.
///
/// Catalogs legitimately leave optional measurements empty -- NED-LVS has no
/// diameter for about a fifth of its rows -- so this must not log.
pub fn get_opt_f64_from_doc(doc: &mongodb::bson::Document, key: &str) -> Option<f64> {
    let value = match doc.get(key) {
        Some(Bson::Double(v)) => *v,
        Some(Bson::Int32(v)) => *v as f64,
        Some(Bson::Int64(v)) => *v as f64,
        _ => return None,
    };
    value.is_finite().then_some(value)
}

/// The `$match` stage selecting candidate rows for one catalog.
///
/// Normally a single cone. With angular-size matching, a second cone is added
/// for rows whose angular size reaches beyond the first; gating that branch on
/// the size keeps the wide search off the bulk of the catalog.
fn cone_match_stage(
    xmatch_config: &conf::CatalogXmatchConfig,
    ra_geojson: f64,
    dec_geojson: f64,
) -> mongodb::bson::Document {
    let cone = |radius: f64| {
        doc! {
            "coordinates.radec_geojson": {
                "$geoWithin": { "$centerSphere": [[ra_geojson, dec_geojson], radius] }
            }
        }
    };

    match (
        &xmatch_config.angular_size_key,
        xmatch_config.angular_size_radius_max,
    ) {
        (Some(size_key), Some(radius_max)) => doc! {
            "$match": {
                "$or": [
                    cone(xmatch_config.radius),
                    { "$and": [
                        { size_key: { "$gt": xmatch_config.angular_size_threshold_arcsec() } },
                        cone(radius_max),
                    ]},
                ]
            }
        },
        _ => doc! { "$match": cone(xmatch_config.radius) },
    }
}

/// The per-catalog stages: select, project, and collect into one row.
fn catalog_pipeline(
    xmatch_config: &conf::CatalogXmatchConfig,
    ra_geojson: f64,
    dec_geojson: f64,
) -> Vec<mongodb::bson::Document> {
    vec![
        cone_match_stage(xmatch_config, ra_geojson, dec_geojson),
        doc! { "$project": &xmatch_config.projection },
        doc! { "$group": { "_id": Bson::Null, "matches": { "$push": "$$ROOT" } } },
        doc! { "$project": { "_id": 0, "matches": 1, "catalog": &xmatch_config.catalog } },
    ]
}

pub fn get_f64_from_doc(doc: &mongodb::bson::Document, key: &str) -> Option<f64> {
    let value = match doc.get(key) {
        Some(Bson::Double(v)) => *v,
        Some(Bson::Int32(v)) => *v as f64,
        Some(Bson::Int64(v)) => *v as f64,
        _ => {
            trace!("no valid {} in doc", key);
            return None;
        }
    };
    // if the value is out of bounds, return None
    if value.is_nan() || value.is_infinite() {
        warn!("{} is NaN or infinite", key);
        return None;
    }
    Some(value)
}

/// Effective match radius in arcsec for a `use_distance` catalog row at
/// redshift `z`. For very nearby objects (z < 0.01) we use the fixed
/// `distance_max_near`; otherwise the radius scales as
/// `distance_max * 0.05 / z`.
pub fn cm_radius_arcsec(z: f64, distance_max: f64, distance_max_near: f64) -> f64 {
    if z < 0.01 {
        distance_max_near
    } else {
        distance_max * (0.05 / z)
    }
}

/// Projected distance in kpc from an angular separation (arcsec) at redshift
/// `z`. Returns `-1.0` for very nearby objects (z <= 0.005), where the
/// physical distance is meaningless and `-1.0` is used as a sort sentinel
/// (sorted before positive values).
pub fn distance_kpc_from_arcsec(distance_arcsec: f64, z: f64) -> f64 {
    if z > 0.005 {
        distance_arcsec * (z / 0.05)
    } else {
        -1.0
    }
}

#[instrument(skip(xmatch_configs, db), fields(database = db.name()), err)]
pub async fn xmatch(
    ra: f64,
    dec: f64,
    object_id: &str,
    survey: &Survey,
    xmatch_configs: &[conf::CatalogXmatchConfig],
    db: &mongodb::Database,
) -> Result<HashMap<String, Vec<mongodb::bson::Document>>, XmatchError> {
    // TODO, make the xmatch config a hashmap for faster access
    // while looping over the xmatch results of the batched queries
    if xmatch_configs.is_empty() {
        return Ok(HashMap::new());
    }
    let ra_geojson = ra - 180.0;
    let dec_geojson = dec;

    let mut x_matches_pipeline = catalog_pipeline(&xmatch_configs[0], ra_geojson, dec_geojson);

    // then for all the other xmatch_configs, use a unionWith stage
    for xmatch_config in xmatch_configs.iter().skip(1) {
        x_matches_pipeline.push(doc! {
            "$unionWith": {
                "coll": xmatch_config.collection_name(),
                "pipeline": catalog_pipeline(xmatch_config, ra_geojson, dec_geojson)
            }
        });
    }

    let collection: mongodb::Collection<mongodb::bson::Document> =
        db.collection(xmatch_configs[0].collection_name());
    let mut cursor = collection
        .aggregate(x_matches_pipeline)
        .await
        .inspect_err(as_error!("failed to aggregate"))?;

    let mut xmatch_results = HashMap::new();
    // pre add the catalogs + empty vec to the xmatch_results
    // this allows us to have a consistent output structure
    for xmatch_config in xmatch_configs.iter() {
        xmatch_results.insert(xmatch_config.catalog.clone(), vec![]);
    }

    while let Some(result) = cursor.next().await {
        let doc = result.inspect_err(as_error!("failed to get next document"))?;
        let catalog = doc
            .get_str("catalog")
            .inspect_err(as_error!("failed to get catalog"))?;
        let matches = doc
            .get_array("matches")
            .inspect_err(as_error!("failed to get matches"))?;

        let xmatch_config = xmatch_configs
            .iter()
            .find(|x| x.catalog == catalog)
            .expect("this should never panic, the doc was derived from the catalogs");

        if let Some(size_key) = &xmatch_config.angular_size_key {
            // Each row gets a match radius from its own angular size, so a
            // large galaxy is kept for a transient far out in its disk while a
            // small one is not.
            let matches_filtered: Vec<mongodb::bson::Document> = matches
                .iter()
                .filter_map(|m| m.as_document().cloned())
                .filter_map(|mut m| {
                    let xmatch_ra = get_f64_from_doc(&m, "ra")?;
                    let xmatch_dec = get_f64_from_doc(&m, "dec")?;
                    let distance_arcsec =
                        great_circle_distance(ra, dec, xmatch_ra, xmatch_dec) * 3600.0;
                    let angular_size = get_opt_f64_from_doc(&m, size_key);
                    if distance_arcsec > xmatch_config.match_radius_arcsec(angular_size) {
                        return None;
                    }
                    m.insert("distance_arcsec", distance_arcsec);
                    Some(m)
                })
                .sorted_by(|a, b| {
                    let da = get_f64_from_doc(a, "distance_arcsec").unwrap_or(f64::INFINITY);
                    let db = get_f64_from_doc(b, "distance_arcsec").unwrap_or(f64::INFINITY);
                    da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
                })
                .take(xmatch_config.max_results.unwrap_or(usize::MAX))
                .collect();
            xmatch_results
                .get_mut(catalog)
                .unwrap()
                .extend(matches_filtered);
        } else if !xmatch_config.use_distance {
            // to each document, add a distance_arcsec field
            // and limit the number of results to max_results if specified
            let matches_cloned: Vec<mongodb::bson::Document> = matches
                .iter()
                .filter_map(|m| m.as_document().cloned())
                .filter_map(|mut m| {
                    let xmatch_ra = match get_f64_from_doc(&m, "ra") {
                        Some(v) => v,
                        None => {
                            return None;
                        }
                    };
                    let xmatch_dec = match get_f64_from_doc(&m, "dec") {
                        Some(v) => v,
                        None => {
                            return None;
                        }
                    };
                    let distance_arcsec =
                        great_circle_distance(ra, dec, xmatch_ra, xmatch_dec) * 3600.0; // convert to arcsec
                    m.insert("distance_arcsec", distance_arcsec);
                    Some(m)
                })
                .sorted_by(|a, b| {
                    let da = get_f64_from_doc(a, "distance_arcsec").unwrap_or(f64::INFINITY);
                    let db = get_f64_from_doc(b, "distance_arcsec").unwrap_or(f64::INFINITY);
                    da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
                })
                .take(xmatch_config.max_results.unwrap_or(usize::MAX))
                .collect();
            xmatch_results
                .get_mut(catalog)
                .unwrap()
                .extend(matches_cloned);
        } else {
            let distance_key = xmatch_config
                .distance_key
                .as_ref()
                .ok_or(XmatchError::NullDistanceKey)?;
            let distance_max = xmatch_config
                .distance_max
                .ok_or(XmatchError::NullDistanceMax)?;
            let distance_max_near = xmatch_config
                .distance_max_near
                .ok_or(XmatchError::NullDistanceMaxNear)?;

            let mut matches_filtered: Vec<mongodb::bson::Document> = vec![];
            for xmatch_doc in matches.iter() {
                let xmatch_doc = xmatch_doc
                    .as_document()
                    .ok_or(XmatchError::AsDocumentError)?;

                let xmatch_ra = match get_f64_from_doc(&xmatch_doc, "ra") {
                    Some(v) => v,
                    None => {
                        continue;
                    }
                };
                let xmatch_dec = match get_f64_from_doc(&xmatch_doc, "dec") {
                    Some(v) => v,
                    None => {
                        continue;
                    }
                };
                let doc_z = match get_f64_from_doc(&xmatch_doc, distance_key) {
                    Some(v) => v,
                    None => {
                        continue;
                    }
                };

                let cm_radius = cm_radius_arcsec(doc_z, distance_max, distance_max_near);
                let distance_arcsec =
                    great_circle_distance(ra, dec, xmatch_ra, xmatch_dec) * 3600.0;

                if distance_arcsec < cm_radius {
                    let distance_kpc = distance_kpc_from_arcsec(distance_arcsec, doc_z);
                    let mut xmatch_doc = xmatch_doc.clone();
                    xmatch_doc.insert("distance_arcsec", distance_arcsec);
                    xmatch_doc.insert("distance_kpc", distance_kpc);
                    matches_filtered.push(xmatch_doc);
                }
            }
            // sort to have nearby galaxies (distance_kpc = -1.0) first, sorted by distance_arcsec
            // then those with distance_kpc != -1.0 sorted by distance_kpc and distance_arcsec
            matches_filtered.sort_by(|a, b| {
                let da_arcsec = get_f64_from_doc(a, "distance_arcsec").unwrap_or(f64::INFINITY);
                let db_arcsec = get_f64_from_doc(b, "distance_arcsec").unwrap_or(f64::INFINITY);
                let da_kpc = get_f64_from_doc(a, "distance_kpc").unwrap_or(f64::INFINITY);
                let db_kpc = get_f64_from_doc(b, "distance_kpc").unwrap_or(f64::INFINITY);

                // First sort by distance_kpc, treating -1.0 as smaller than any positive value
                if da_kpc == -1.0 && db_kpc != -1.0 {
                    std::cmp::Ordering::Less
                } else if da_kpc != -1.0 && db_kpc == -1.0 {
                    std::cmp::Ordering::Greater
                } else if da_kpc != db_kpc {
                    da_kpc
                        .partial_cmp(&db_kpc)
                        .unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    // If distance_kpc are equal, sort by distance_arcsec
                    da_arcsec
                        .partial_cmp(&db_arcsec)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
            });
            xmatch_results
                .get_mut(catalog)
                .unwrap()
                .extend(matches_filtered);
        }
    }

    // Watchlist catalogs are kept out of the alert _aux.cross_matches (they
    // would otherwise leak through the API). Instead, we record the alert's
    // object_id on each matched watchlist document under a per-survey field.
    let watchlist_catalogs: Vec<String> = xmatch_results
        .keys()
        .filter(|name| name.starts_with(WATCHLIST_PREFIX))
        .cloned()
        .collect();
    if !watchlist_catalogs.is_empty() {
        let field = watchlist_match_field(survey);
        for catalog in watchlist_catalogs {
            let matches = xmatch_results.remove(&catalog).unwrap_or_default();
            if matches.is_empty() {
                continue;
            }
            let matched_ids: Vec<Bson> = matches
                .iter()
                .filter_map(|m| m.get("_id").cloned())
                .collect();
            if matched_ids.is_empty() {
                continue;
            }
            let collection: mongodb::Collection<mongodb::bson::Document> = db.collection(&catalog);
            collection
                .update_many(
                    doc! { "_id": { "$in": &matched_ids } },
                    doc! { "$addToSet": { &field: object_id } },
                )
                .await
                .inspect_err(as_error!("failed to record watchlist crossmatch"))?;
        }
    }

    Ok(xmatch_results)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// NED_LVS-shaped config: 300" base cone, per-row radius from `diam`,
    /// capped at 6 deg.
    fn angular_size_config() -> conf::CatalogXmatchConfig {
        conf::CatalogXmatchConfig::new(
            "NED_LVS",
            None,
            300.0,
            doc! {},
            false,
            None,
            None,
            None,
            Some(50),
            Some("diam".to_string()),
            2.0,
            Some(21600.0),
        )
    }

    fn plain_config() -> conf::CatalogXmatchConfig {
        conf::CatalogXmatchConfig::new(
            "NED",
            None,
            300.0,
            doc! {},
            false,
            None,
            None,
            None,
            None,
            None,
            1.0,
            None,
        )
    }

    #[test]
    fn test_plain_config_uses_the_cone_radius() {
        let config = plain_config();
        assert!((config.match_radius_arcsec(None) - 300.0).abs() < 1e-6);
        // A size is irrelevant without angular-size matching enabled.
        assert!((config.match_radius_arcsec(Some(11400.0)) - 300.0).abs() < 1e-6);
    }

    #[test]
    fn test_large_galaxy_gets_a_larger_radius() {
        let config = angular_size_config();
        // M31: diam 11400" -> semi-major 5700" -> 2x = 11400"
        let r = config.match_radius_arcsec(Some(11400.0));
        assert!((r - 11400.0).abs() < 1e-6, "got {r}");
        // A transient 0.4 deg (1440") out is now inside the match radius,
        // where the flat 300" cone would have dropped it.
        assert!(1440.0 <= r);
    }

    #[test]
    fn test_small_galaxy_does_not_shrink_below_the_base_cone() {
        let config = angular_size_config();
        // A 10" dwarf would scale to 10", but the base cone still applies.
        assert!((config.match_radius_arcsec(Some(10.0)) - 300.0).abs() < 1e-6);
        // Rows with no size at all fall back to the base cone too.
        assert!((config.match_radius_arcsec(None) - 300.0).abs() < 1e-6);
    }

    #[test]
    fn test_radius_is_capped() {
        let config = angular_size_config();
        // A degenerate 100 deg diameter must not produce an unbounded radius.
        let r = config.match_radius_arcsec(Some(360_000.0));
        assert!((r - 21600.0).abs() < 1e-6, "got {r}");
    }

    #[test]
    fn test_threshold_is_where_scaling_overtakes_the_cone() {
        let config = angular_size_config();
        // scale*size/2 > 300  <=>  size > 300
        let threshold = config.angular_size_threshold_arcsec();
        assert!((threshold - 300.0).abs() < 1e-6, "got {threshold}");
        // Just below the threshold the base cone still wins, so such rows are
        // correctly excluded from the extended branch.
        assert!((config.match_radius_arcsec(Some(threshold - 1.0)) - 300.0).abs() < 1e-6);
        assert!(config.match_radius_arcsec(Some(threshold + 100.0)) > 300.0);
    }

    #[test]
    fn test_match_stage_adds_the_gated_second_cone() {
        let stage = cone_match_stage(&angular_size_config(), 10.0, 20.0);
        let branches = stage
            .get_document("$match")
            .unwrap()
            .get_array("$or")
            .unwrap();
        assert_eq!(branches.len(), 2);

        // The wide branch must be gated on size, or it would drag the whole
        // catalog through a 6 degree cone on every alert.
        let wide = branches[1]
            .as_document()
            .unwrap()
            .get_array("$and")
            .unwrap();
        let gate = wide[0].as_document().unwrap();
        assert!(gate.contains_key("diam"));
    }

    #[test]
    fn test_match_stage_is_a_single_cone_without_angular_size() {
        let stage = cone_match_stage(&plain_config(), 10.0, 20.0);
        let m = stage.get_document("$match").unwrap();
        assert!(m.get("$or").is_none());
        assert!(m.contains_key("coordinates.radec_geojson"));
    }

    #[test]
    fn test_opt_f64_is_quiet_about_absent_values() {
        let doc = doc! { "diam": 444.0, "null_diam": Bson::Null, "int_diam": 12i32 };
        assert_eq!(get_opt_f64_from_doc(&doc, "diam"), Some(444.0));
        assert_eq!(get_opt_f64_from_doc(&doc, "int_diam"), Some(12.0));
        assert_eq!(get_opt_f64_from_doc(&doc, "null_diam"), None);
        assert_eq!(get_opt_f64_from_doc(&doc, "missing"), None);
        assert_eq!(get_opt_f64_from_doc(&doc! {"d": f64::NAN}, "d"), None);
    }
}
