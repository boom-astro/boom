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
/// redshift `z`. Below [`NEARBY_REDSHIFT`] the fixed `distance_max_near`
/// applies; otherwise the radius scales as `distance_max * 0.05 / z`.
pub fn cm_radius_arcsec(z: f64, distance_max: f64, distance_max_near: f64) -> f64 {
    if z <= NEARBY_REDSHIFT {
        distance_max_near
    } else {
        distance_max * (0.05 / z)
    }
}

/// Redshift below which a projected physical distance is not meaningful: the
/// peculiar velocity of a nearby galaxy dominates its recession, and a star
/// sits here too.
pub const NEARBY_REDSHIFT: f64 = 0.005;

/// Projected distance in kpc from an angular separation (arcsec) at redshift
/// `z`. Returns `-1.0` below [`NEARBY_REDSHIFT`], where the physical distance
/// is meaningless.
pub fn distance_kpc_from_arcsec(distance_arcsec: f64, z: f64) -> f64 {
    if z > NEARBY_REDSHIFT {
        distance_arcsec * (z / 0.05)
    } else {
        -1.0
    }
}

/// Whether a catalog row describes a star, per the catalog's own type column.
///
/// Catalogs that do not label object type report `false`, which leaves their
/// ordering as it was.
fn is_stellar(
    doc: &mongodb::bson::Document,
    type_key: Option<&String>,
    stellar: &[String],
) -> bool {
    let Some(key) = type_key else { return false };
    match doc.get_str(key.as_str()) {
        Ok(value) => stellar.iter().any(|s| s.eq_ignore_ascii_case(value.trim())),
        Err(_) => false,
    }
}

/// Rank of a match for host-galaxy ordering; lower sorts first.
///
/// A galaxy below [`NEARBY_REDSHIFT`] is ranked ahead of everything, because a
/// transient can sit well outside it in arcseconds and still be inside the
/// galaxy -- that is what the missing kpc distance means for a host.
///
/// A star gets no such credit. It shares the missing distance but is not a host,
/// and ranking it by that sentinel put any star within the search radius ahead of
/// every real candidate, however much closer they were.
fn host_rank(doc: &mongodb::bson::Document, type_key: Option<&String>, stellar: &[String]) -> u8 {
    let kpc = get_f64_from_doc(doc, "distance_kpc").unwrap_or(f64::INFINITY);
    if is_stellar(doc, type_key, stellar) {
        2
    } else if kpc == -1.0 {
        0
    } else {
        1
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

    let mut x_matches_pipeline = vec![
        doc! {
            "$match": {
                "coordinates.radec_geojson": {
                    "$geoWithin": {
                        "$centerSphere": [[ra_geojson, dec_geojson], xmatch_configs[0].radius]
                    }
                }
            }
        },
        doc! {
            "$project": &xmatch_configs[0].projection
        },
        doc! {
            "$group": {
                "_id": Bson::Null,
                "matches": {
                    "$push": "$$ROOT"
                }
            }
        },
        doc! {
            "$project": {
                "_id": 0,
                "matches": 1,
                "catalog": &xmatch_configs[0].catalog
            }
        },
    ];

    // then for all the other xmatch_configs, use a unionWith stage
    for xmatch_config in xmatch_configs.iter().skip(1) {
        x_matches_pipeline.push(doc! {
            "$unionWith": {
                "coll": &xmatch_config.catalog,
                "pipeline": [
                    doc! {
                        "$match": {
                            "coordinates.radec_geojson": {
                                "$geoWithin": {
                                    "$centerSphere": [[ra_geojson, dec_geojson], xmatch_config.radius]
                                }
                            }
                        }
                    },
                    doc! {
                        "$project": &xmatch_config.projection
                    },
                    doc! {
                        "$group": {
                            "_id": Bson::Null,
                            "matches": {
                                "$push": "$$ROOT"
                            }
                        }
                    },
                    doc! {
                        "$project": {
                            "_id": 0,
                            "matches": 1,
                            "catalog": &xmatch_config.catalog
                        }
                    }
                ]
            }
        });
    }

    let collection: mongodb::Collection<mongodb::bson::Document> =
        db.collection(&xmatch_configs[0].catalog);
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

        if !xmatch_config.use_distance {
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
            // Nearby galaxies first, then candidates by projected distance, then
            // stars. Within a group, by angular separation.
            let type_key = xmatch_config.type_key.as_ref();
            let stellar = xmatch_config.stellar_types.as_slice();
            matches_filtered.sort_by(|a, b| {
                let by_rank = host_rank(a, type_key, stellar).cmp(&host_rank(b, type_key, stellar));
                if by_rank != std::cmp::Ordering::Equal {
                    return by_rank;
                }

                let da_kpc = get_f64_from_doc(a, "distance_kpc").unwrap_or(f64::INFINITY);
                let db_kpc = get_f64_from_doc(b, "distance_kpc").unwrap_or(f64::INFINITY);
                let by_kpc = da_kpc
                    .partial_cmp(&db_kpc)
                    .unwrap_or(std::cmp::Ordering::Equal);
                if by_kpc != std::cmp::Ordering::Equal {
                    return by_kpc;
                }

                let da_arcsec = get_f64_from_doc(a, "distance_arcsec").unwrap_or(f64::INFINITY);
                let db_arcsec = get_f64_from_doc(b, "distance_arcsec").unwrap_or(f64::INFINITY);
                da_arcsec
                    .partial_cmp(&db_arcsec)
                    .unwrap_or(std::cmp::Ordering::Equal)
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
mod host_ordering_tests {
    use super::*;
    use mongodb::bson::doc;

    fn row(spectype: &str, z: f64, arcsec: f64) -> mongodb::bson::Document {
        doc! {
            "spectype": spectype,
            "z": z,
            "distance_arcsec": arcsec,
            "distance_kpc": distance_kpc_from_arcsec(arcsec, z),
        }
    }

    fn order(mut rows: Vec<mongodb::bson::Document>) -> Vec<String> {
        let key = "spectype".to_string();
        let stellar = vec!["STAR".to_string()];
        rows.sort_by(|a, b| {
            let by_rank =
                host_rank(a, Some(&key), &stellar).cmp(&host_rank(b, Some(&key), &stellar));
            if by_rank != std::cmp::Ordering::Equal {
                return by_rank;
            }
            let ka = get_f64_from_doc(a, "distance_kpc").unwrap_or(f64::INFINITY);
            let kb = get_f64_from_doc(b, "distance_kpc").unwrap_or(f64::INFINITY);
            ka.partial_cmp(&kb)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    let aa = get_f64_from_doc(a, "distance_arcsec").unwrap_or(f64::INFINITY);
                    let ab = get_f64_from_doc(b, "distance_arcsec").unwrap_or(f64::INFINITY);
                    aa.partial_cmp(&ab).unwrap_or(std::cmp::Ordering::Equal)
                })
        });
        rows.iter()
            .map(|r| {
                format!(
                    "{}@{}",
                    r.get_str("spectype").unwrap(),
                    r.get_f64("distance_arcsec").unwrap()
                )
            })
            .collect()
    }

    /// The reported case: a star at z ~ 0 shares the missing projected distance
    /// with a nearby galaxy, and used to be ranked ahead of a closer galaxy.
    #[test]
    fn test_a_distant_star_does_not_outrank_a_closer_galaxy() {
        let ranked = order(vec![
            row("STAR", 0.000_123, 21.85),
            row("GALAXY", 0.032_35, 17.16),
        ]);
        assert_eq!(ranked, vec!["GALAXY@17.16", "STAR@21.85"]);
    }

    /// A genuinely nearby galaxy keeps its place at the front: a transient can
    /// sit far from its centre in arcseconds and still be inside it.
    #[test]
    fn test_a_nearby_galaxy_still_ranks_first() {
        let ranked = order(vec![row("GALAXY", 0.08, 2.0), row("GALAXY", 0.001, 25.0)]);
        assert_eq!(ranked, vec!["GALAXY@25", "GALAXY@2"]);
    }

    /// Stars are kept, just ranked below every real candidate, and ordered among
    /// themselves by separation.
    #[test]
    fn test_stars_sort_last_and_by_separation() {
        let ranked = order(vec![
            row("STAR", 0.0, 3.0),
            row("GALAXY", 0.05, 12.0),
            row("STAR", 0.0, 1.0),
        ]);
        assert_eq!(ranked, vec!["GALAXY@12", "STAR@1", "STAR@3"]);
    }

    /// A catalog with no type column behaves as it did before.
    #[test]
    fn test_without_a_type_column_nothing_is_treated_as_stellar() {
        let nearby = row("GALAXY", 0.001, 25.0);
        assert_eq!(host_rank(&nearby, None, &[]), 0);
        let star = row("STAR", 0.0, 3.0);
        assert_eq!(
            host_rank(&star, None, &[]),
            0,
            "unlabelled rows keep the old rank"
        );
        assert_eq!(
            host_rank(&star, Some(&"spectype".to_string()), &["STAR".to_string()]),
            2
        );
    }
}
