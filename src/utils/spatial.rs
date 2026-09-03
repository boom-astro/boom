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

/// Angular separation, arcsec, within which a match is treated as coincident
/// with the transient. A source this close is the most likely counterpart
/// whatever it is, so type and redshift stop mattering.
pub const COINCIDENT_ARCSEC: f64 = 1.0;

/// Rank of a match for host ordering; lower sorts first.
///
/// 0. Spatially coincident, any type. A star sitting on the transient is the
///    thing to look at first, whether or not it can be a host.
/// 1. A galaxy below [`NEARBY_REDSHIFT`], which has no meaningful projected
///    distance. A transient can sit well outside such a galaxy in arcseconds
///    and still be inside it.
/// 2. Everything else with a projected distance, ordered by it.
/// 3. Stars that are not coincident. They have no projected distance and cannot
///    host anything, so they never compete in 2 -- ranking them by the missing
///    distance put any star in the search radius ahead of every real candidate,
///    however much closer those were.
fn host_rank(doc: &mongodb::bson::Document, type_key: Option<&String>, stellar: &[String]) -> u8 {
    let arcsec = get_f64_from_doc(doc, "distance_arcsec").unwrap_or(f64::INFINITY);
    if arcsec < COINCIDENT_ARCSEC {
        return 0;
    }
    if is_stellar(doc, type_key, stellar) {
        return 3;
    }
    let kpc = get_f64_from_doc(doc, "distance_kpc").unwrap_or(f64::INFINITY);
    if kpc == -1.0 {
        1
    } else {
        2
    }
}

/// Sort key: rank, then projected distance where that rank is ordered by it,
/// then angular separation.
///
/// Only rank 2 carries a usable kpc distance. Ordering the other ranks by it
/// would reintroduce the sentinel problem inside each group.
fn host_sort_key(
    doc: &mongodb::bson::Document,
    type_key: Option<&String>,
    stellar: &[String],
) -> (u8, f64, f64) {
    let rank = host_rank(doc, type_key, stellar);
    let kpc = if rank == 2 {
        get_f64_from_doc(doc, "distance_kpc").unwrap_or(f64::INFINITY)
    } else {
        0.0
    };
    let arcsec = get_f64_from_doc(doc, "distance_arcsec").unwrap_or(f64::INFINITY);
    (rank, kpc, arcsec)
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
            let type_key = xmatch_config.type_key.as_ref();
            let stellar = xmatch_config.stellar_types.as_slice();
            matches_filtered.sort_by(|a, b| {
                let (ra_, ka, aa) = host_sort_key(a, type_key, stellar);
                let (rb, kb, ab) = host_sort_key(b, type_key, stellar);
                ra_.cmp(&rb)
                    .then_with(|| ka.partial_cmp(&kb).unwrap_or(std::cmp::Ordering::Equal))
                    .then_with(|| aa.partial_cmp(&ab).unwrap_or(std::cmp::Ordering::Equal))
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
            let (ra_, ka, aa) = host_sort_key(a, Some(&key), &stellar);
            let (rb, kb, ab) = host_sort_key(b, Some(&key), &stellar);
            ra_.cmp(&rb)
                .then_with(|| ka.partial_cmp(&kb).unwrap_or(std::cmp::Ordering::Equal))
                .then_with(|| aa.partial_cmp(&ab).unwrap_or(std::cmp::Ordering::Equal))
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

    /// A source sitting on the transient is the first thing to look at, whether
    /// or not it could be a host.
    #[test]
    fn test_a_coincident_source_ranks_first_whatever_it_is() {
        let ranked = order(vec![
            row("GALAXY", 0.02, 4.0),
            row("STAR", 0.0, 0.4),
            row("GALAXY", 0.001, 20.0),
        ]);
        assert_eq!(ranked, vec!["STAR@0.4", "GALAXY@20", "GALAXY@4"]);
    }

    /// A genuinely nearby galaxy keeps its place ahead of the kpc-ordered ones:
    /// a transient can sit far from its centre and still be inside it.
    #[test]
    fn test_a_nearby_galaxy_outranks_a_projected_distance() {
        let ranked = order(vec![row("GALAXY", 0.08, 2.0), row("GALAXY", 0.001, 25.0)]);
        assert_eq!(ranked, vec!["GALAXY@25", "GALAXY@2"]);
    }

    /// Non-coincident stars never compete on projected distance, and order among
    /// themselves by separation.
    #[test]
    fn test_stars_sort_last_and_by_separation() {
        let ranked = order(vec![
            row("STAR", 0.0, 3.0),
            row("GALAXY", 0.05, 12.0),
            row("STAR", 0.0, 1.5),
        ]);
        assert_eq!(ranked, vec!["GALAXY@12", "STAR@1.5", "STAR@3"]);
    }

    /// Robert's call: a QSO is a plausible counterpart, so it ranks as a galaxy
    /// does rather than as a star.
    #[test]
    fn test_a_qso_is_ranked_as_a_galaxy() {
        let key = "spectype".to_string();
        let stellar = vec!["STAR".to_string()];
        let qso = row("QSO", 0.001, 20.0);
        assert_eq!(host_rank(&qso, Some(&key), &stellar), 1);
        let star = row("STAR", 0.001, 20.0);
        assert_eq!(host_rank(&star, Some(&key), &stellar), 3);
    }

    /// A catalog with no type column behaves as it did before.
    #[test]
    fn test_without_a_type_column_nothing_is_treated_as_stellar() {
        let star = row("STAR", 0.0, 3.0);
        assert_eq!(
            host_rank(&star, None, &[]),
            1,
            "unlabelled rows keep the old rank"
        );
        assert_eq!(
            host_rank(&star, Some(&"spectype".to_string()), &["STAR".to_string()]),
            3
        );
    }
}
