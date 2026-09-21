use crate::{
    api::catalogs::WATCHLIST_PREFIX,
    conf,
    utils::{enums::Survey, o11y::logging::as_error},
};
use flare::spatial::{great_circle_distance, radec2lb};
use futures::stream::StreamExt;
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
    /// HEALPix NESTED index at [`HPX_DEPTH`]. `None` on alerts written before
    /// this field existed, which a range query cannot distinguish from a
    /// position outside the region -- see `moc_hpx_stage`.
    #[serde(skip_serializing_if = "Option::is_none")]
    hpx: Option<i64>,
}

/// Depth of the stored HEALPix index, matching healpix-alchemy's HPX_MAX_ORDER
/// so a MOC range maps onto it by a bit shift with nothing approximated.
pub const HPX_DEPTH: u8 = 29;

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
            hpx: Some(
                cdshealpix::nested::get(HPX_DEPTH).hash(ra.to_radians(), dec.to_radians()) as i64,
            ),
        }
    }

    /// The stored HEALPix index, absent on alerts written before it existed.
    pub fn hpx(&self) -> Option<i64> {
        self.hpx
    }

    /// Get RA and Dec from the stored GeoJSON coordinates (formatting RA back to [0, 360])
    pub fn get_radec(&self) -> (f64, f64) {
        let ra = self.radec_geojson.coordinates[0] + 180.0;
        let dec = self.radec_geojson.coordinates[1];
        (ra, dec)
    }
}

fn bson_f64(doc: &mongodb::bson::Document, key: &str) -> Option<f64> {
    match doc.get(key) {
        Some(Bson::Double(v)) => Some(*v),
        Some(Bson::Int32(v)) => Some(*v as f64),
        Some(Bson::Int64(v)) => Some(*v as f64),
        _ => None,
    }
}

/// Like [`get_f64_from_doc`] but silent: catalogs legitimately leave optional
/// measurements empty, so this must not log.
pub fn get_opt_f64_from_doc(doc: &mongodb::bson::Document, key: &str) -> Option<f64> {
    bson_f64(doc, key).filter(|v| v.is_finite())
}

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
    let Some(value) = bson_f64(doc, key) else {
        trace!("no valid {} in doc", key);
        return None;
    };
    if !value.is_finite() {
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

/// Value of `distance_kpc` on a row below [`NEARBY_REDSHIFT`], where a
/// projected physical distance is meaningless.
pub const NO_PROJECTED_DISTANCE: f64 = -1.0;

/// Projected distance in kpc from an angular separation (arcsec) at redshift
/// `z`. Returns [`NO_PROJECTED_DISTANCE`] below [`NEARBY_REDSHIFT`].
pub fn distance_kpc_from_arcsec(distance_arcsec: f64, z: f64) -> f64 {
    if z > NEARBY_REDSHIFT {
        distance_arcsec * (z / 0.05)
    } else {
        NO_PROJECTED_DISTANCE
    }
}

/// Redshift of a catalog row, for catalogs that match on distance.
///
/// Legacy writes -99 for "no photo-z"; fold it to 0 rather than drop the row,
/// which would also discard any `z_spec` it carries.
pub fn row_redshift(
    config: &conf::CatalogXmatchConfig,
    doc: &mongodb::bson::Document,
) -> Option<f64> {
    let key = config.distance_key.as_ref()?;
    get_f64_from_doc(doc, key).map(|z| if z >= 0.0 { z } else { 0.0 })
}

/// Radius in arcsec within which one catalog row is accepted.
///
/// A catalog may declare both rules, and a row is kept if either reaches it:
/// the distance rule covers rows with a redshift but no measured extent, the
/// size rule covers large galaxies the distance rule cuts off too early.
/// Neither can see past the cone the database was asked for.
pub fn row_match_radius_arcsec(
    config: &conf::CatalogXmatchConfig,
    doc: &mongodb::bson::Document,
) -> f64 {
    let base = conf::radians_to_arcsec(config.radius);
    let sized = config
        .angular_size_key
        .as_ref()
        .map(|key| config.match_radius_arcsec(get_opt_f64_from_doc(doc, key)));
    let distance = config.use_distance.then(|| {
        let z = match row_redshift(config, doc) {
            Some(z) => z,
            // No redshift, so the distance rule says nothing about this row.
            None => return 0.0,
        };
        let max = config.distance_max.expect("validated in config");
        let max_near = config.distance_max_near.expect("validated in config");
        cm_radius_arcsec(z, max, max_near).min(base)
    });
    match (sized, distance) {
        (None, None) => base,
        (a, b) => a.unwrap_or(0.0).max(b.unwrap_or(0.0)),
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
    if kpc == NO_PROJECTED_DISTANCE {
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

        let type_key = xmatch_config.type_key.as_ref();
        let stellar = xmatch_config.stellar_types.as_slice();
        let mut matches_filtered: Vec<mongodb::bson::Document> = matches
            .iter()
            .filter_map(|m| m.as_document().cloned())
            .filter_map(|mut m| {
                let xmatch_ra = get_f64_from_doc(&m, "ra")?;
                let xmatch_dec = get_f64_from_doc(&m, "dec")?;
                let distance_arcsec =
                    great_circle_distance(ra, dec, xmatch_ra, xmatch_dec) * 3600.0;
                if distance_arcsec > row_match_radius_arcsec(xmatch_config, &m) {
                    return None;
                }
                m.insert("distance_arcsec", distance_arcsec);
                // Written whenever the row carries a redshift, whatever rule
                // matched it: how it was found must not decide what it stores.
                if let Some(z) = row_redshift(xmatch_config, &m) {
                    m.insert("distance_kpc", distance_kpc_from_arcsec(distance_arcsec, z));
                }
                Some(m)
            })
            .collect();
        matches_filtered.sort_by(|a, b| {
            let (ra_, ka, aa) = host_sort_key(a, type_key, stellar);
            let (rb, kb, ab) = host_sort_key(b, type_key, stellar);
            ra_.cmp(&rb)
                .then_with(|| ka.partial_cmp(&kb).unwrap_or(std::cmp::Ordering::Equal))
                .then_with(|| aa.partial_cmp(&ab).unwrap_or(std::cmp::Ordering::Equal))
        });
        matches_filtered.truncate(xmatch_config.max_results.unwrap_or(usize::MAX));
        xmatch_results
            .get_mut(catalog)
            .unwrap()
            .extend(matches_filtered);
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

    /// NED-shaped config: 300" query cone, per-row radius from `diam`,
    /// floored at 5" and capped at 6 deg.
    fn angular_size_config() -> conf::CatalogXmatchConfig {
        conf::CatalogXmatchConfig {
            catalog: "NED".to_string(),
            radius: conf::arcsec_to_radians(300.0),
            max_results: Some(50),
            angular_size_key: Some("diam".to_string()),
            angular_size_scale: 2.0,
            angular_size_radius_max: Some(conf::arcsec_to_radians(21600.0)),
            angular_size_radius_min: conf::arcsec_to_radians(5.0),
            ..Default::default()
        }
    }

    fn plain_config() -> conf::CatalogXmatchConfig {
        conf::CatalogXmatchConfig {
            catalog: "NED".to_string(),
            radius: conf::arcsec_to_radians(300.0),
            ..Default::default()
        }
    }

    #[test]
    fn test_plain_config_uses_the_cone_radius() {
        let config = plain_config();
        assert!((config.match_radius_arcsec(None) - 300.0).abs() < 1e-6);
        assert!((config.match_radius_arcsec(Some(11400.0)) - 300.0).abs() < 1e-6);
    }

    #[test]
    fn test_large_galaxy_gets_a_larger_radius() {
        let config = angular_size_config();
        // M31: diam 11400" -> semi-major 5700" -> 2x = 11400", past 1440".
        let r = config.match_radius_arcsec(Some(11400.0));
        assert!((r - 11400.0).abs() < 1e-6, "got {r}");
        assert!(1440.0 <= r);
    }

    #[test]
    fn test_small_galaxy_is_matched_within_its_own_extent() {
        let config = angular_size_config();
        // diam 10" -> semi-major 5" -> 2x = 10", well inside the query cone.
        assert!((config.match_radius_arcsec(Some(10.0)) - 10.0).abs() < 1e-6);
    }

    #[test]
    fn test_a_row_with_no_size_falls_back_to_the_floor() {
        let config = angular_size_config();
        assert!((config.match_radius_arcsec(None) - 5.0).abs() < 1e-6);
        assert!((config.match_radius_arcsec(Some(0.0)) - 5.0).abs() < 1e-6);
        assert!((config.match_radius_arcsec(Some(f64::NAN)) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_radius_is_capped() {
        let config = angular_size_config();
        let r = config.match_radius_arcsec(Some(360_000.0));
        assert!((r - 21600.0).abs() < 1e-6, "got {r}");
    }

    #[test]
    fn test_threshold_is_where_scaling_overtakes_the_cone() {
        let config = angular_size_config();
        // scale * size / 2 > 300 <=> size > 300.
        let threshold = config.angular_size_threshold_arcsec();
        assert!((threshold - 300.0).abs() < 1e-6, "got {threshold}");
        assert!(config.match_radius_arcsec(Some(threshold - 1.0)) < 300.0);
        assert!(config.match_radius_arcsec(Some(threshold + 100.0)) > 300.0);
    }

    /// The real NED entry: both rules at once.
    fn ned_config() -> conf::CatalogXmatchConfig {
        conf::CatalogXmatchConfig {
            use_distance: true,
            distance_key: Some("z".to_string()),
            distance_max: Some(30.0),
            distance_max_near: Some(300.0),
            ..angular_size_config()
        }
    }

    #[test]
    fn test_a_row_keeps_its_distance_radius_when_it_has_no_extent() {
        let config = ned_config();
        // Nearby, so the fixed near radius applies rather than 1.5 / z.
        let near = doc! { "z": 0.001 };
        assert!((row_match_radius_arcsec(&config, &near) - 300.0).abs() < 1e-6);
        // Further out the radius shrinks with distance: 30 * 0.05 / 0.05.
        let far = doc! { "z": 0.05 };
        assert!((row_match_radius_arcsec(&config, &far) - 30.0).abs() < 1e-6);
    }

    #[test]
    fn test_the_wider_of_the_two_rules_wins() {
        let config = ned_config();
        // M31-sized but far enough that the distance rule alone would cut it
        // at 30": its own extent reaches much further.
        let large = doc! { "z": 0.05, "diam": 11400.0 };
        assert!((row_match_radius_arcsec(&config, &large) - 11400.0).abs() < 1e-6);
        // And a small nearby galaxy keeps the 300" the distance rule gives it,
        // which its 10" extent would have thrown away.
        let small = doc! { "z": 0.001, "diam": 10.0 };
        assert!((row_match_radius_arcsec(&config, &small) - 300.0).abs() < 1e-6);
    }

    #[test]
    fn test_a_row_with_neither_rule_falls_back_to_the_floor() {
        let config = ned_config();
        assert!((row_match_radius_arcsec(&config, &doc! {}) - 5.0).abs() < 1e-6);
    }

    /// The database is only asked for the base cone, so a distance rule that
    /// reaches past it would match rows that were never fetched.
    #[test]
    fn test_the_distance_rule_cannot_see_past_the_query_cone() {
        let config = conf::CatalogXmatchConfig {
            radius: conf::arcsec_to_radians(30.0),
            angular_size_key: None,
            angular_size_radius_max: None,
            ..ned_config()
        };
        assert!((row_match_radius_arcsec(&config, &doc! { "z": 0.001 }) - 30.0).abs() < 1e-6);
    }

    /// Legacy writes -99 for "no photo-z". Folding it to 0 keeps the row, which
    /// may still carry a spectroscopic redshift.
    #[test]
    fn test_the_absent_redshift_sentinel_is_folded_to_zero() {
        let config = ned_config();
        assert_eq!(row_redshift(&config, &doc! { "z": -99.0 }), Some(0.0));
        assert_eq!(row_redshift(&config, &doc! { "z": 0.02 }), Some(0.02));
        assert_eq!(row_redshift(&config, &doc! {}), None);
    }

    /// `distance_kpc` is written from the row's redshift, so a catalog that
    /// also matches on size must not lose it.
    #[test]
    fn test_a_size_matched_row_still_reports_a_redshift() {
        let config = ned_config();
        let big_galaxy = doc! { "z": 0.02, "diam": 11400.0 };
        assert!(row_match_radius_arcsec(&config, &big_galaxy) > 300.0);
        assert_eq!(row_redshift(&config, &big_galaxy), Some(0.02));
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

        // Gated on size, or every alert drags the catalog through a 6 deg cone.
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
