use cdshealpix::nested;
use moc::deser::fits::skymap::from_fits_skymap;
use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType, MocType};
use moc::moc::range::RangeMOC;
use moc::moc::{CellMOCIntoIterator, CellMOCIterator, HasMaxDepth};
use moc::qty::Hpx;
use std::io::{BufReader, Cursor};

/// Type alias for the HEALPix MOC type used throughout the codebase.
pub type HpxMoc = RangeMOC<u64, Hpx<u64>>;

/// Parse a MOC from IVOA FITS bytes.
pub fn moc_from_fits_bytes(bytes: &[u8]) -> Result<HpxMoc, String> {
    let reader = BufReader::new(Cursor::new(bytes));
    match from_fits_ivoa(reader) {
        Ok(MocIdxType::U64(MocQtyType::Hpx(MocType::Ranges(moc)))) => {
            Ok(RangeMOC::new(moc.depth_max(), moc.collect()))
        }
        Ok(MocIdxType::U64(MocQtyType::Hpx(MocType::Cells(cell_moc)))) => {
            let depth = cell_moc.depth_max();
            let ranges = cell_moc.into_cell_moc_iter().ranges().collect();
            Ok(RangeMOC::new(depth, ranges))
        }
        Ok(_) => Err("Unexpected MOC type in FITS data".to_string()),
        Err(e) => Err(format!("Failed to parse MOC FITS: {}", e)),
    }
}

/// Parse a HEALPix skymap FITS and threshold at the given cumulative credible level.
///
/// For example, `credible_level = 0.9` returns a MOC covering the 90% credible region.
pub fn moc_from_skymap_bytes(bytes: &[u8], credible_level: f64) -> Result<HpxMoc, String> {
    let reader = BufReader::new(Cursor::new(bytes));
    from_fits_skymap(
        reader,
        0.0,            // skip_value_le_this: don't skip any pixels by value
        0.0,            // cumul_from: start from 0
        credible_level, // cumul_to: stop at the credible level
        false,          // asc=false: accumulate from highest probability densities
        false,          // strict: include cells overlapping the boundary
        false,          // no_split: allow splitting cells at boundary
        false,          // reverse_decent
    )
    .map_err(|e| format!("Failed to parse skymap FITS: {}", e))
}

/// Check if a given (ra_deg, dec_deg) is inside the MOC.
pub fn is_in_moc(moc: &HpxMoc, ra_deg: f64, dec_deg: f64) -> bool {
    let depth = moc.depth_max();
    let ra_rad = ra_deg.to_radians();
    let dec_rad = dec_deg.to_radians();
    let layer = nested::get(depth);
    let cell = layer.hash(ra_rad, dec_rad);
    moc.contains_cell(depth, cell)
}

/// Degrade a MOC to a target depth and return covering cones.
///
/// Each cone is `(ra_deg, dec_deg, radius_rad)` — the center and circumscribing
/// radius of a HEALPix cell at the target depth. These cones fully cover the
/// degraded MOC cells (with some overlap), suitable for a MongoDB `$centerSphere` query.
pub fn moc_to_covering_cones(moc: &HpxMoc, target_depth: u8) -> Vec<(f64, f64, f64)> {
    let degraded = moc.degraded(target_depth);
    degraded
        .flatten_to_fixed_depth_cells()
        .map(|cell_idx| {
            let (lon_rad, lat_rad) = nested::center(target_depth, cell_idx);
            let vertices = nested::vertices(target_depth, cell_idx);

            // Circumscribing radius = max angular distance from center to any vertex
            let radius_rad = vertices
                .iter()
                .map(|(vlon, vlat)| angular_distance(lon_rad, lat_rad, *vlon, *vlat))
                .fold(0.0_f64, f64::max);

            // Add a small safety margin (1%) to handle edge effects
            let radius_rad = radius_rad * 1.01;

            let ra_deg = lon_rad.to_degrees();
            let dec_deg = lat_rad.to_degrees();
            (ra_deg, dec_deg, radius_rad)
        })
        .collect()
}

/// Select an appropriate covering depth based on the MOC's sky coverage.
///
/// Returns a depth that produces a manageable number of covering cones
/// (typically under ~500) for use in a MongoDB `$or` query.
pub fn select_covering_depth(moc: &HpxMoc) -> u8 {
    let coverage_pct = moc.coverage_percentage();
    let coverage_sq_deg = coverage_pct * 41253.0; // full sky ≈ 41253 sq deg

    if coverage_sq_deg < 10.0 {
        7 // ~0.05 sq deg cells
    } else if coverage_sq_deg < 100.0 {
        5 // ~0.84 sq deg cells
    } else if coverage_sq_deg < 1000.0 {
        4 // ~3.36 sq deg cells
    } else {
        3 // ~13.4 sq deg cells
    }
}

/// Haversine angular distance between two points on the sphere (in radians).
fn angular_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_moc_from_fits_bytes() {
        let bytes = std::fs::read("./data/ls_footprint_moc.fits")
            .expect("Failed to read footprint MOC file");
        let moc = moc_from_fits_bytes(&bytes).expect("Failed to parse MOC");
        assert!(moc.depth_max() > 0);
        assert!(moc.coverage_percentage() > 0.0);
        assert!(moc.coverage_percentage() < 1.0);
    }

    #[test]
    fn test_is_in_moc() {
        let bytes = std::fs::read("./data/ls_footprint_moc.fits")
            .expect("Failed to read footprint MOC file");
        let moc = moc_from_fits_bytes(&bytes).expect("Failed to parse MOC");
        // A point in the LSST footprint (southern sky)
        let in_footprint = is_in_moc(&moc, 0.0, -30.0);
        // A point at the north pole (unlikely in LSST footprint)
        let at_north_pole = is_in_moc(&moc, 0.0, 89.0);
        // At least one should differ from the other (footprint is partial sky)
        assert_ne!(in_footprint, at_north_pole);
    }

    #[test]
    fn test_moc_to_covering_cones() {
        let bytes = std::fs::read("./data/ls_footprint_moc.fits")
            .expect("Failed to read footprint MOC file");
        let moc = moc_from_fits_bytes(&bytes).expect("Failed to parse MOC");

        let cones = moc_to_covering_cones(&moc, 4);
        assert!(!cones.is_empty());

        for &(ra, dec, radius) in &cones {
            assert!((0.0..360.0).contains(&ra));
            assert!((-90.0..=90.0).contains(&dec));
            assert!(radius > 0.0);
            // At depth 4, circumscribing radius should be roughly ~1 degree (~0.018 rad)
            assert!(radius < 0.1, "Radius too large: {}", radius);
        }
    }

    #[test]
    fn test_covering_cones_contain_original_moc() {
        // Verify that the covering cones fully contain the original MOC.
        // Pick some points known to be in the MOC and check they fall within at least one cone.
        let bytes = std::fs::read("./data/ls_footprint_moc.fits")
            .expect("Failed to read footprint MOC file");
        let moc = moc_from_fits_bytes(&bytes).expect("Failed to parse MOC");

        let depth = select_covering_depth(&moc);
        let cones = moc_to_covering_cones(&moc, depth);

        // Sample some points from the MOC by taking cell centers at the MOC's depth
        let degraded = moc.degraded(8);
        let sample_cells: Vec<u64> = degraded.flatten_to_fixed_depth_cells().take(100).collect();

        for cell_idx in sample_cells {
            let (lon_rad, lat_rad) = nested::center(8, cell_idx);
            let ra_deg = lon_rad.to_degrees();
            let dec_deg = lat_rad.to_degrees();

            // This point should be inside at least one covering cone
            let in_any_cone = cones.iter().any(|&(cone_ra, cone_dec, cone_radius)| {
                let dist = angular_distance(
                    cone_ra.to_radians(),
                    cone_dec.to_radians(),
                    ra_deg.to_radians(),
                    dec_deg.to_radians(),
                );
                dist <= cone_radius
            });
            assert!(
                in_any_cone,
                "Point ({}, {}) is in MOC but not in any covering cone",
                ra_deg, dec_deg
            );
        }
    }

    #[test]
    fn test_select_covering_depth() {
        let bytes = std::fs::read("./data/ls_footprint_moc.fits")
            .expect("Failed to read footprint MOC file");
        let moc = moc_from_fits_bytes(&bytes).expect("Failed to parse MOC");

        let depth = select_covering_depth(&moc);
        // The LSST footprint covers a significant fraction of the sky
        assert!(depth <= 5);
        assert!(depth >= 3);
    }

    #[test]
    fn test_moc_from_skymap_bytes() {
        let bytes = std::fs::read("./data/glg_healpix_all_bn200524211.fits")
            .expect("Failed to read skymap FITS file");
        let moc = moc_from_skymap_bytes(&bytes, 0.9).expect("Failed to parse skymap");
        assert!(moc.depth_max() > 0);
        let coverage = moc.coverage_percentage();
        // A 90% credible region of a Fermi GBM burst should cover a substantial but not full sky
        assert!(coverage > 0.0, "MOC coverage should be > 0");
        assert!(coverage < 1.0, "MOC coverage should be < 100%");

        let depth = select_covering_depth(&moc);
        let cones = moc_to_covering_cones(&moc, depth);
        assert!(!cones.is_empty(), "Should have covering cones");

        // Verify all cones have valid coordinates
        for &(ra, dec, radius) in &cones {
            assert!((0.0..360.0).contains(&ra), "RA out of range: {}", ra);
            assert!((-90.0..=90.0).contains(&dec), "Dec out of range: {}", dec);
            assert!(radius > 0.0, "Radius should be positive");
        }
    }

    /// Test that GRB 200524A's Fermi GBM skymap contains ZTF20abbiixp,
    /// the confirmed optical counterpart, verifying spatial coincidence,
    /// temporal coincidence, and that the covering-cone query machinery
    /// would find it in a database query.
    ///
    /// Also verifies that a tight credible level (5%) excludes the counterpart,
    /// confirming that the credible level parameter meaningfully restricts
    /// the search region. The counterpart enters the MOC between CL~6-6.5%.
    #[test]
    fn test_grb200524a_counterpart_in_skymap() {
        // ZTF20abbiixp (AT2020kym) — the optical counterpart to GRB 200524A
        let counterpart_ra = 213.0430731_f64;
        let counterpart_dec = 60.9052795_f64;
        let counterpart_jd = 2458993.8065046_f64; // 2020-05-24 ~07:21 UTC

        // GRB 200524A trigger: bn200524211 → 2020-05-24 at 0.211 day fraction ≈ 05:04 UTC
        // JD of the trigger ≈ 2458993.711
        let grb_trigger_jd = 2458993.711_f64;

        let skymap_bytes = std::fs::read("./data/glg_healpix_all_bn200524211.fits")
            .expect("Failed to read GRB 200524A skymap");

        // 1. At 90% credible level: counterpart IS inside the MOC
        let moc_90 =
            moc_from_skymap_bytes(&skymap_bytes, 0.9).expect("Failed to parse skymap at CL=0.9");
        assert!(
            is_in_moc(&moc_90, counterpart_ra, counterpart_dec),
            "ZTF20abbiixp (RA={}, Dec={}) should be inside the 90% credible region",
            counterpart_ra,
            counterpart_dec
        );

        // 2. At 5% credible level: counterpart is NOT inside the MOC
        //    (it enters between CL~6-6.5%, so 5% is just tight enough to exclude it)
        let moc_05 =
            moc_from_skymap_bytes(&skymap_bytes, 0.05).expect("Failed to parse skymap at CL=0.05");
        assert!(
            !is_in_moc(&moc_05, counterpart_ra, counterpart_dec),
            "ZTF20abbiixp should NOT be inside the 5% credible region"
        );

        // 3. Covering cones at 90%: the DB query machinery would find this position
        let depth = select_covering_depth(&moc_90);
        let cones = moc_to_covering_cones(&moc_90, depth);
        let in_any_cone = cones.iter().any(|&(cone_ra, cone_dec, cone_radius)| {
            angular_distance(
                cone_ra.to_radians(),
                cone_dec.to_radians(),
                counterpart_ra.to_radians(),
                counterpart_dec.to_radians(),
            ) <= cone_radius
        });
        assert!(
            in_any_cone,
            "ZTF20abbiixp should fall within at least one covering cone (depth {})",
            depth
        );

        // 4. Covering cones at 5%: none of the cones contain the counterpart
        let depth_05 = select_covering_depth(&moc_05);
        let cones_05 = moc_to_covering_cones(&moc_05, depth_05);
        let in_any_cone_05 = cones_05.iter().any(|&(cone_ra, cone_dec, cone_radius)| {
            angular_distance(
                cone_ra.to_radians(),
                cone_dec.to_radians(),
                counterpart_ra.to_radians(),
                counterpart_dec.to_radians(),
            ) <= cone_radius
        });
        assert!(
            !in_any_cone_05,
            "ZTF20abbiixp should NOT fall within any covering cone at 5% CL"
        );

        // 5. Temporal coincidence: the first ZTF detection is within 1 day of the GRB trigger
        let dt = counterpart_jd - grb_trigger_jd;
        assert!(
            dt > 0.0 && dt < 1.0,
            "ZTF20abbiixp detection (JD {}) should be within 1 day after GRB trigger (JD {}), got dt={:.4} days",
            counterpart_jd,
            grb_trigger_jd,
            dt
        );
    }

    #[test]
    fn test_angular_distance() {
        // Same point
        assert!((angular_distance(0.0, 0.0, 0.0, 0.0)).abs() < 1e-10);
        // Opposite poles
        let dist = angular_distance(
            0.0,
            std::f64::consts::FRAC_PI_2,
            0.0,
            -std::f64::consts::FRAC_PI_2,
        );
        assert!((dist - std::f64::consts::PI).abs() < 1e-10);
        // 90 degrees apart along equator
        let dist = angular_distance(0.0, 0.0, std::f64::consts::FRAC_PI_2, 0.0);
        assert!((dist - std::f64::consts::FRAC_PI_2).abs() < 1e-10);
    }
}
