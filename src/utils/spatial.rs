use crate::{conf, utils::o11y::logging::as_error};
use cdshealpix::nested::get;
use flare::spatial::{great_circle_distance, radec2lb};
use futures::stream::StreamExt;
use itertools::Itertools;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{instrument, warn};

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
    /// HEALPix NESTED index at depth 29 (healpix-alchemy's HPX_MAX_ORDER).
    /// Used for spatial queries and sharding. The coarse shard pixel can be
    /// derived with a bit shift: `hpx >> (2 * (29 - shard_depth))`.
    #[serde(default)]
    pub hpx: i64,
}

impl Coordinates {
    pub fn new(ra: f64, dec: f64) -> Self {
        let (l, b) = radec2lb(ra, dec);
        let ra_rad = ra.to_radians();
        let dec_rad = dec.to_radians();
        let hpx = get(conf::HealpixConfig::FINE_DEPTH).hash(ra_rad, dec_rad) as i64;
        Coordinates {
            radec_geojson: GeoJsonPoint {
                r#type: "Point".to_string(),
                coordinates: vec![ra - 180.0, dec],
            },
            l: Some(l),
            b: Some(b),
            hpx,
        }
    }

    /// Get RA and Dec from the stored GeoJSON coordinates (formatting RA back to [0, 360])
    pub fn get_radec(&self) -> (f64, f64) {
        let ra = self.radec_geojson.coordinates[0] + 180.0;
        let dec = self.radec_geojson.coordinates[1];
        (ra, dec)
    }

    /// Derive the coarse shard pixel from the depth-29 HEALPix index.
    /// This is a single bit shift — no need to store it separately.
    pub fn hpx_shard(&self, shard_depth: u8) -> i64 {
        self.hpx >> (2 * (conf::HealpixConfig::FINE_DEPTH - shard_depth))
    }
}

fn get_f64_from_doc(doc: &mongodb::bson::Document, key: &str) -> Option<f64> {
    let value = match doc.get(key) {
        Some(mongodb::bson::Bson::Double(v)) => *v,
        Some(mongodb::bson::Bson::Int32(v)) => *v as f64,
        Some(mongodb::bson::Bson::Int64(v)) => *v as f64,
        _ => {
            warn!("no valid {} in doc", key);
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

#[instrument(skip(xmatch_configs, db), fields(database = db.name()), err)]
pub async fn xmatch(
    ra: f64,
    dec: f64,
    xmatch_configs: &Vec<conf::CatalogXmatchConfig>,
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
                "_id": mongodb::bson::Bson::Null,
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
                            "_id": mongodb::bson::Bson::Null,
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

                let cm_radius_arcsec = if doc_z < 0.01 {
                    distance_max_near // in arcsec
                } else {
                    distance_max * (0.05 / doc_z) // in arcsec
                };
                let distance_arcsec =
                    great_circle_distance(ra, dec, xmatch_ra, xmatch_dec) * 3600.0; // convert to arcsec

                if distance_arcsec < cm_radius_arcsec {
                    // calculate the distance between objs in kpc
                    // let distance_kpc = angular_separation * (doc_z / 0.05);
                    let distance_kpc = if doc_z > 0.005 {
                        distance_arcsec * (doc_z / 0.05)
                    } else {
                        -1.0
                    };

                    // we make a mutable copy of the xmatch_doc
                    let mut xmatch_doc = xmatch_doc.clone();
                    // add the distance fields to the xmatch_doc
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

    Ok(xmatch_results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdshealpix::nested;
    use std::collections::HashMap;

    #[test]
    fn test_hpx_computed_at_depth_29() {
        let coord = Coordinates::new(180.0, 45.0);
        // hpx should be nonzero — a valid depth-29 NESTED index
        assert_ne!(coord.hpx, 0);

        // Recompute independently and verify
        let expected = get(29).hash(180.0_f64.to_radians(), 45.0_f64.to_radians()) as i64;
        assert_eq!(coord.hpx, expected);
    }

    #[test]
    fn test_shard_is_bit_shift_of_hpx() {
        // Verify that hpx_shard() is equivalent to computing the hash
        // directly at the shard depth — the NESTED scheme guarantees this.
        let coord = Coordinates::new(123.456, -30.789);
        for shard_depth in [0, 1, 2, 4, 8, 14] {
            let from_shift = coord.hpx_shard(shard_depth);
            let from_direct = get(shard_depth)
                .hash(123.456_f64.to_radians(), (-30.789_f64).to_radians())
                as i64;
            assert_eq!(
                from_shift, from_direct,
                "shard mismatch at depth {}: shift={} direct={}",
                shard_depth, from_shift, from_direct
            );
        }
    }

    #[test]
    fn test_sky_halves_separated_by_shard() {
        // At depth 0, HEALPix has 12 base cells. We pick points from
        // two opposite sides of the sky and show they land in different
        // base cells (shard pixels).
        //
        // Northern galactic cap vs southern galactic cap:
        //   - (RA=0, Dec=+60) → near north galactic pole
        //   - (RA=0, Dec=-60) → near south galactic pole
        let north = Coordinates::new(0.0, 60.0);
        let south = Coordinates::new(0.0, -60.0);

        // At depth 0, they must be in different base cells
        let north_cell = north.hpx_shard(0);
        let south_cell = south.hpx_shard(0);
        assert_ne!(
            north_cell, south_cell,
            "opposite sky points should be in different base cells"
        );

        // At depth 4 (the default shard depth, NSIDE=16), they must also differ
        let north_shard = north.hpx_shard(4);
        let south_shard = south.hpx_shard(4);
        assert_ne!(north_shard, south_shard);
    }

    #[test]
    fn test_nearby_points_share_shard() {
        // Two points 1 arcsecond apart should share the same coarse shard pixel.
        // To avoid hitting a pixel boundary, we find the center of a depth-8
        // pixel and offset from there.
        let test_depth: u8 = 8;
        let center = get(test_depth).center(42); // center of pixel 42 at depth 8
        let center_ra = center.0.to_degrees();
        let center_dec = center.1.to_degrees();

        let p1 = Coordinates::new(center_ra, center_dec);
        let p2 = Coordinates::new(center_ra + 1.0 / 3600.0, center_dec); // 1 arcsec east

        // Same shard at depth 4 (~13.4 deg² pixels)
        assert_eq!(p1.hpx_shard(4), p2.hpx_shard(4));

        // Same pixel at depth 8 (~50 arcmin pixels) since we started at center
        assert_eq!(p1.hpx_shard(test_depth), p2.hpx_shard(test_depth));

        // But at depth 29 (~0.4 mas pixels) they should differ
        assert_ne!(p1.hpx, p2.hpx);
    }

    #[test]
    fn test_shard_bins_sky_into_expected_count() {
        // Scatter points across the sky and verify the number of distinct
        // shard pixels matches expectations.
        let shard_depth: u8 = 4;
        let expected_max_pixels = 12 * 4u64.pow(shard_depth as u32); // 3072

        let mut seen_shards: std::collections::HashSet<i64> = std::collections::HashSet::new();
        // Grid the sky
        for ra_idx in 0..36 {
            for dec_idx in 0..18 {
                let ra = ra_idx as f64 * 10.0 + 5.0; // 5, 15, ..., 355
                let dec = dec_idx as f64 * 10.0 - 85.0; // -85, -75, ..., 85
                let coord = Coordinates::new(ra, dec);
                seen_shards.insert(coord.hpx_shard(shard_depth));
            }
        }

        // We should see many distinct shards, but never exceed the max
        assert!(
            seen_shards.len() > 100,
            "should see many distinct shards from a sky grid, got {}",
            seen_shards.len()
        );
        assert!(
            (seen_shards.len() as u64) <= expected_max_pixels,
            "shard count {} exceeds max {} for depth {}",
            seen_shards.len(),
            expected_max_pixels,
            shard_depth
        );
    }

    #[test]
    fn test_cone_coverage_finds_point() {
        // Place a point on the sky, then compute the depth-29 cone coverage
        // around it. The point's hpx index should fall within one of the
        // covering ranges — this is how cone search queries will work.
        let point = Coordinates::new(45.0, 30.0);

        // 10 arcsecond cone around the same position
        let radius_rad = (10.0_f64 / 3600.0).to_radians();
        let bmoc = nested::cone_coverage_approx(
            conf::HealpixConfig::FINE_DEPTH,
            45.0_f64.to_radians(),
            30.0_f64.to_radians(),
            radius_rad,
        );
        let ranges = bmoc.to_ranges();

        // The point's hpx must be inside one of the covering ranges
        let hpx = point.hpx as u64;
        let inside = ranges.iter().any(|r| hpx >= r.start && hpx < r.end);
        assert!(
            inside,
            "point hpx={} should be inside cone coverage ({} ranges)",
            hpx,
            ranges.len()
        );
    }

    #[test]
    fn test_cone_coverage_excludes_distant_point() {
        // A point 1 degree away should NOT be inside a 10 arcsecond cone.
        let distant = Coordinates::new(46.0, 30.0); // ~1 deg away

        let radius_rad = (10.0_f64 / 3600.0).to_radians();
        let bmoc = nested::cone_coverage_approx(
            conf::HealpixConfig::FINE_DEPTH,
            45.0_f64.to_radians(),
            30.0_f64.to_radians(),
            radius_rad,
        );
        let ranges = bmoc.to_ranges();

        let hpx = distant.hpx as u64;
        let inside = ranges.iter().any(|r| hpx >= r.start && hpx < r.end);
        assert!(
            !inside,
            "distant point hpx={} should NOT be inside a 10 arcsec cone",
            hpx
        );
    }

    #[test]
    fn test_shard_aware_cone_search_simulation() {
        // Simulate what a shard-aware cone search looks like:
        // 1. Build a "database" of alerts binned by shard pixel
        // 2. Query a cone — compute which shards to check
        // 3. Only scan those shards, apply a fine HEALPix filter
        // 4. Verify we find all alerts that brute force finds

        let shard_depth: u8 = 4;
        // Use depth 17 for the fine filter in this test (~1.6 arcsec pixels).
        // In production you'd use depth 29, but cone_coverage_approx at depth 29
        // for a 1-degree cone is expensive; depth 17 demonstrates the same principle.
        let fine_depth: u8 = 17;

        // Insert 500 alerts across the sky into a HashMap keyed by shard
        let mut shards: HashMap<i64, Vec<(f64, f64, u64)>> = HashMap::new();
        for i in 0..500 {
            let ra = (i as f64 * 137.508) % 360.0; // golden angle spacing
            let dec = (i as f64 * 0.36) % 180.0 - 90.0;
            let hpx = get(fine_depth).hash(ra.to_radians(), dec.to_radians());
            let shard = (hpx >> (2 * (fine_depth - shard_depth))) as i64;
            shards.entry(shard).or_default().push((ra, dec, hpx));
        }

        // Cone search: 1-degree radius around (RA=180, Dec=0)
        let query_ra = 180.0_f64;
        let query_dec = 0.0_f64;
        let radius_rad = 1.0_f64.to_radians();

        // Step 1: Find which shard pixels overlap the cone
        let shard_bmoc = nested::cone_coverage_approx(
            shard_depth,
            query_ra.to_radians(),
            query_dec.to_radians(),
            radius_rad,
        );
        let shard_ranges = shard_bmoc.to_ranges();
        let relevant_shards: Vec<i64> = shard_ranges
            .iter()
            .flat_map(|r| (r.start..r.end).map(|x| x as i64))
            .collect();

        // Step 2: Find which fine-depth ranges overlap the cone
        let fine_bmoc = nested::cone_coverage_approx(
            fine_depth,
            query_ra.to_radians(),
            query_dec.to_radians(),
            radius_rad,
        );
        let fine_ranges = fine_bmoc.to_ranges();

        // Step 3: Only scan the relevant shards, apply fine filter
        let mut found = Vec::new();
        let mut shards_scanned = 0;
        for shard_id in &relevant_shards {
            if let Some(alerts) = shards.get(shard_id) {
                shards_scanned += 1;
                for &(ra, dec, hpx) in alerts {
                    if fine_ranges.iter().any(|r| hpx >= r.start && hpx < r.end) {
                        found.push((ra, dec));
                    }
                }
            }
        }

        // Brute-force check: count all alerts truly within 1 degree
        let mut brute_force = Vec::new();
        for alerts in shards.values() {
            for &(ra, dec, _) in alerts {
                let dist = flare::spatial::great_circle_distance(query_ra, query_dec, ra, dec);
                if dist < 1.0 {
                    brute_force.push((ra, dec));
                }
            }
        }

        // The HEALPix result should contain all brute-force matches
        // (it may contain a few extras at tile boundaries — that's expected)
        assert!(
            found.len() >= brute_force.len(),
            "HEALPix search found {} but brute force found {} — missed some alerts",
            found.len(),
            brute_force.len()
        );

        // We should have skipped most shards
        let total_shards = shards.len();
        assert!(
            shards_scanned < total_shards,
            "should skip some shards: scanned {} out of {}",
            shards_scanned,
            total_shards
        );
    }

    /// Emulates a 2-machine sharded cluster using in-memory HashMaps.
    ///
    /// 12 HEALPix base cells (depth 0) are split across 2 "machines":
    ///   Machine 0: base cells 0–5  (one half of the sky)
    ///   Machine 1: base cells 6–11 (the other half)
    ///
    /// 1000 alerts are scattered across the sky and routed to the
    /// correct machine. A cone search determines which machine(s)
    /// to query, queries only those, and verifies results match
    /// brute-force while the other machine is never scanned.
    #[test]
    fn test_two_machine_sharding_emulated() {
        // Each "machine" is a Vec of (id, ra, dec, hpx) tuples,
        // standing in for a MongoDB collection on a separate host.
        struct Machine {
            alerts: Vec<(String, f64, f64, i64)>,
        }
        impl Machine {
            fn new() -> Self {
                Machine { alerts: Vec::new() }
            }
            fn insert(&mut self, id: String, ra: f64, dec: f64, hpx: i64) {
                self.alerts.push((id, ra, dec, hpx));
            }
            /// Query: return alerts whose hpx falls within any of the given ranges.
            fn query_hpx_ranges(&self, ranges: &[(i64, i64)]) -> Vec<&(String, f64, f64, i64)> {
                self.alerts
                    .iter()
                    .filter(|(_, _, _, hpx)| {
                        ranges.iter().any(|(lo, hi)| *hpx >= *lo && *hpx < *hi)
                    })
                    .collect()
            }
        }

        // Route by base cell: cells 0–5 → machine 0, cells 6–11 → machine 1
        let route = |hpx: i64| -> usize {
            let base_cell = hpx >> (2 * conf::HealpixConfig::FINE_DEPTH);
            if base_cell < 6 { 0 } else { 1 }
        };

        // --- Step 1: Scatter 1000 alerts, route to correct machine ---
        let mut machines = [Machine::new(), Machine::new()];
        let n_alerts = 1000;

        for i in 0..n_alerts {
            let ra = (i as f64 * 137.508) % 360.0;
            let dec = ((i as f64 / n_alerts as f64) * 180.0) - 90.0;
            let coord = Coordinates::new(ra, dec);
            let idx = route(coord.hpx);
            machines[idx].insert(format!("alert_{}", i), ra, dec, coord.hpx);
        }

        assert!(
            machines[0].alerts.len() > 100,
            "machine 0 should have substantial alerts, got {}",
            machines[0].alerts.len()
        );
        assert!(
            machines[1].alerts.len() > 100,
            "machine 1 should have substantial alerts, got {}",
            machines[1].alerts.len()
        );

        // --- Step 2: Cone search — which machine(s) to query? ---
        let query_ra = 120.0_f64;
        let query_dec = 45.0_f64;
        let radius_deg = 5.0_f64;
        let radius_rad = radius_deg.to_radians();

        // Find which base cells the cone overlaps
        let shard_bmoc = nested::cone_coverage_approx(
            0, // depth 0 = 12 base cells
            query_ra.to_radians(),
            query_dec.to_radians(),
            radius_rad,
        );
        let target_cells: Vec<i64> = shard_bmoc
            .to_ranges()
            .iter()
            .flat_map(|r| (r.start..r.end).map(|x| x as i64))
            .collect();

        let mut need_machine = [false; 2];
        for cell in &target_cells {
            if *cell < 6 { need_machine[0] = true; } else { need_machine[1] = true; }
        }

        // A 5-degree cone is tiny relative to a base cell (~3400 deg²),
        // so it should only need 1 machine
        let machines_needed: usize = need_machine.iter().filter(|&&x| x).count();
        assert_eq!(
            machines_needed, 1,
            "a 5-degree cone should fit in one base cell, but needs {} machines (cells: {:?})",
            machines_needed, target_cells
        );

        // --- Step 3: Query only the needed machine with fine filter ---
        let query_depth: u8 = 17;
        let fine_bmoc = nested::cone_coverage_approx(
            query_depth,
            query_ra.to_radians(),
            query_dec.to_radians(),
            radius_rad,
        );
        let shift = 2 * (conf::HealpixConfig::FINE_DEPTH - query_depth);
        let fine_ranges: Vec<(i64, i64)> = fine_bmoc
            .to_ranges()
            .iter()
            .map(|r| ((r.start << shift) as i64, (r.end << shift) as i64))
            .collect();

        let mut found = Vec::new();
        let mut machines_queried = 0;
        for (idx, needed) in need_machine.iter().enumerate() {
            if !needed {
                continue;
            }
            machines_queried += 1;
            let hits = machines[idx].query_hpx_ranges(&fine_ranges);
            for (id, ra, dec, _) in hits {
                found.push((id.clone(), *ra, *dec));
            }
        }

        // --- Step 4: Verify against brute-force over ALL machines ---
        let mut brute_force_count = 0;
        for machine in &machines {
            for (_, ra, dec, _) in &machine.alerts {
                let dist =
                    flare::spatial::great_circle_distance(query_ra, query_dec, *ra, *dec);
                if dist < radius_deg {
                    brute_force_count += 1;
                }
            }
        }

        assert!(
            found.len() >= brute_force_count,
            "missed alerts: HEALPix found {} but brute force found {}",
            found.len(),
            brute_force_count
        );
        assert!(brute_force_count > 0, "expected some alerts in the cone");
        assert_eq!(
            machines_queried, 1,
            "should only query 1 machine, queried {}",
            machines_queried
        );
    }
}
