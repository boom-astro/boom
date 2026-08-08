use cdshealpix::nested;
use fitsio::FitsFile;
use moc::deser::fits::skymap::from_fits_skymap;
use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType, MocType};
use moc::moc::range::RangeMOC;
use moc::moc::{CellMOCIntoIterator, CellMOCIterator, HasMaxDepth};
use moc::qty::Hpx;
use std::collections::HashMap;
use std::io::{BufReader, Cursor};

const SQRT_2PI: f64 = 2.5066282746310002; // (2π)^0.5

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

/// Extract HEALPix order from a UNIQ index.  UNIQ = 4·4^order + ipix.
fn uniq_to_order(uniq: u64) -> u8 {
    ((63 - uniq.leading_zeros()) / 2 - 1) as u8
}

/// Extract the pixel index at native order from a UNIQ index.
fn uniq_to_ipix(uniq: u64) -> u64 {
    let order = uniq_to_order(uniq) as u32;
    uniq - (1u64 << (2 * order + 2))
}

/// Solid angle of one HEALPix pixel at the given order: 4π / (12 · 4^order).
fn pixel_area_from_order(order: u8) -> f64 {
    4.0 * std::f64::consts::PI / (12.0 * (1u64 << (2 * order as u32)) as f64)
}

/// LIGO BAYESTAR 3D skymap in multi-order (UNIQ) representation.
///
/// Both flat HEALPix files (nside header + PROB column) and true multi-order
/// files (UNIQ + PROBDENSITY columns) are supported. Flat files are converted
/// to the UNIQ representation on load so all downstream code is uniform.
pub struct LIGO3dskymap {
    /// UNIQ pixel index per row (encodes order + ipix).
    pub uniq: Vec<u64>,
    /// Per-pixel solid angle in steradians.
    pub pixel_area_sr: Vec<f64>,
    /// Per-pixel probability (dimensionless; sums to ~1).
    pub prob: Vec<f64>,
    pub distmu: Vec<f64>,
    pub distsigma: Vec<f64>,
    pub distnorm: Vec<f64>,
    /// Maximum HEALPix order present in this map.
    pub max_order: u8,
    /// Fast lookup: UNIQ index → row in the above Vecs.
    uniq_to_row: HashMap<u64, usize>,
}

/// Parse a LIGO BAYESTAR 3D skymap FITS file.
///
/// Supports two on-disk layouts:
/// - **Multi-order** (UNIQ column present): `UNIQ` + `PROBDENSITY` per steradian.
/// - **Flat HEALPix** (NSIDE header, no UNIQ column): `PROB` per pixel.
///   Flat maps are converted to the UNIQ representation on load.
pub fn parse_3d_skymap(path: &str) -> Result<LIGO3dskymap, String> {
    let mut fits = FitsFile::open(path).map_err(|e| e.to_string())?;
    let hdu = fits.hdu(1).map_err(|e| e.to_string())?;

    let (uniq, pixel_area_sr, prob) = if let Ok(uniq_i64) = hdu.read_col::<i64>(&mut fits, "UNIQ") {
        // ── Multi-order format ──────────────────────────────────────────
        let uniq: Vec<u64> = uniq_i64.iter().map(|&u| u as u64).collect();
        let areas: Vec<f64> = uniq
            .iter()
            .map(|&u| pixel_area_from_order(uniq_to_order(u)))
            .collect();
        let probdensity: Vec<f64> = hdu
            .read_col(&mut fits, "PROBDENSITY")
            .map_err(|e| format!("PROBDENSITY column missing from UNIQ skymap: {}", e))?;
        let prob: Vec<f64> = probdensity
            .iter()
            .zip(areas.iter())
            .map(|(&pd, &a)| pd * a)
            .collect();
        (uniq, areas, prob)
    } else {
        // ── Flat HEALPix format ─────────────────────────────────────────
        let nside: i64 = hdu
            .read_key(&mut fits, "NSIDE")
            .map_err(|e| e.to_string())?;
        let nside = nside as u32;
        let order = (nside as f64).log2() as u8;
        let area = pixel_area_from_order(order);
        let npix = 12 * (nside as usize).pow(2);
        let prob: Vec<f64> = hdu.read_col(&mut fits, "PROB").map_err(|e| e.to_string())?;
        // Synthesise UNIQ indices: UNIQ[i] = 4·4^order + i
        let base = 1u64 << (2 * order as u32 + 2);
        let uniq: Vec<u64> = (0..npix as u64).map(|i| base + i).collect();
        let areas = vec![area; npix];
        (uniq, areas, prob)
    };

    let distmu: Vec<f64> = hdu
        .read_col(&mut fits, "DISTMU")
        .map_err(|e| e.to_string())?;
    let distsigma: Vec<f64> = hdu
        .read_col(&mut fits, "DISTSIGMA")
        .map_err(|e| e.to_string())?;
    let distnorm: Vec<f64> = hdu
        .read_col(&mut fits, "DISTNORM")
        .map_err(|e| e.to_string())?;

    let max_order = uniq.iter().map(|&u| uniq_to_order(u)).max().unwrap_or(0);
    let uniq_to_row: HashMap<u64, usize> = uniq.iter().enumerate().map(|(i, &u)| (u, i)).collect();

    Ok(LIGO3dskymap {
        uniq,
        pixel_area_sr,
        prob,
        distmu,
        distsigma,
        distnorm,
        max_order,
        uniq_to_row,
    })
}

/// Parse a LIGO BAYESTAR 3D skymap from raw FITS bytes (e.g. from a base64-decoded upload).
///
/// Writes bytes to a temp file then delegates to `parse_3d_skymap`, because fitsio
/// (based on cfitsio) requires a file path.
pub fn parse_3d_skymap_bytes(bytes: &[u8]) -> Result<LIGO3dskymap, String> {
    use std::io::Write;
    let mut tmp = tempfile::NamedTempFile::new().map_err(|e| e.to_string())?;
    tmp.write_all(bytes).map_err(|e| e.to_string())?;
    tmp.flush().map_err(|e| e.to_string())?;
    let path = tmp
        .path()
        .to_str()
        .ok_or("temp file path is not valid UTF-8")?
        .to_string();
    parse_3d_skymap(&path)
}

impl LIGO3dskymap {
    /// Row index of the pixel containing (ra_deg, dec_deg).
    ///
    /// Walks from `max_order` down to order 0, checking parent pixels at each
    /// level, so it works correctly for multi-order (UNIQ) maps where adjacent
    /// pixels can be at different resolutions. Returns `None` only if the map
    /// has a coverage gap at that location (should not happen for a valid map).
    pub fn ang2pix(&self, ra_deg: f64, dec_deg: f64) -> Option<usize> {
        let layer = nested::get(self.max_order);
        let mut ipix = layer.hash(ra_deg.to_radians(), dec_deg.to_radians());
        for order in (0..=self.max_order).rev() {
            let uniq = (1u64 << (2 * order as u32 + 2)) + ipix;
            if let Some(&row) = self.uniq_to_row.get(&uniq) {
                return Some(row);
            }
            if order > 0 {
                ipix >>= 2; // step to parent pixel in NESTED ordering
            }
        }
        None
    }

    /// Per-pixel distance posterior p(D|Ω) using the Singer+2016 ansatz:
    ///   p(D|Ω) = DISTNORM · D² · Normal(D; DISTMU, DISTSIGMA)
    /// Returns 0.0 for pixels with NaN/inf/non-positive σ or DISTNORM.
    pub fn conditional_pdf(&self, row: usize, d_mpc: f64) -> f64 {
        let Some((mu, sigma, norm)) = self.dist_params(row) else {
            return 0.0;
        };
        let gauss = (-0.5 * ((d_mpc - mu) / sigma).powi(2)).exp() / (sigma * SQRT_2PI);
        norm * d_mpc * d_mpc * gauss
    }

    /// Validate and return (DISTMU, DISTSIGMA, DISTNORM) for pixel `row`.
    /// Returns `None` if any value is non-finite, sigma ≤ 0, or norm ≤ 0.
    fn dist_params(&self, row: usize) -> Option<(f64, f64, f64)> {
        let mu = self.distmu[row];
        let sigma = self.distsigma[row];
        let norm = self.distnorm[row];
        if mu.is_finite() && sigma.is_finite() && sigma > 0.0 && norm.is_finite() && norm > 0.0 {
            Some((mu, sigma, norm))
        } else {
            None
        }
    }

    /// 3D probability density dP/dV at pixel `row` and distance `d_mpc`,
    /// in units of sr⁻¹ Mpc⁻³ (probability per steradian per Mpc³).
    ///
    /// dP/dV = (PROB / pixel_area) × DISTNORM × Normal(d; DISTMU, DISTSIGMA)
    ///
    /// The r² factors in the probability element (p(r|Ω) ∝ r²·Normal) and the volume
    /// element (dV = r² dr dΩ) cancel exactly, leaving a pure Gaussian in distance.
    /// Returns `None` for pixels with invalid distance parameters.
    fn pixel_dpdv(&self, row: usize, d_mpc: f64) -> Option<f64> {
        let (mu, sigma, norm) = self.dist_params(row)?;
        let gauss = (-0.5 * ((d_mpc - mu) / sigma).powi(2)).exp() / (sigma * SQRT_2PI);
        Some((self.prob[row] / self.pixel_area_sr[row]) * norm * gauss)
    }
}

/// Density-sorted credible-volume index built from a BAYESTAR 3D skymap.
///
/// Voxels (pixel × distance bin) are sorted by dP/dV descending.
/// The cumulative arrays allow O(log n) lookup of:
/// - the density threshold ξ* for any credible level, and
/// - the credible level at which any candidate enters the credible volume.
pub struct CredibleVolumeIndex {
    sorted_dpdv: Vec<f64>,
    cum_prob: Vec<f64>,
    cum_vol: Vec<f64>,
}

impl CredibleVolumeIndex {
    /// Precompute density-sorted cumulative arrays.
    ///
    /// Works natively at the map's own resolution — no degradation. For
    /// multi-order (UNIQ) skymaps the pixel count is already small. For flat
    /// nside=512 maps two filters keep the voxel array manageable:
    /// - pixels with prob < total_prob × 1e-7 are skipped (retains >99.5% of mass);
    /// - distance bins beyond 5σ from DISTMU are skipped (negligible Gaussian tail).
    ///
    /// The `n_dist_bins` bins span `[0, max(DISTMU + 5σ)]` uniformly.
    pub fn build(skymap: &LIGO3dskymap, n_dist_bins: usize) -> Self {
        let d_max = skymap
            .distmu
            .iter()
            .zip(skymap.distsigma.iter())
            .filter_map(|(&mu, &sigma)| {
                if mu.is_finite() && sigma.is_finite() && sigma > 0.0 {
                    Some(mu + 5.0 * sigma)
                } else {
                    None
                }
            })
            .fold(0.0_f64, f64::max);

        if d_max <= 0.0 {
            return CredibleVolumeIndex {
                sorted_dpdv: vec![],
                cum_prob: vec![],
                cum_vol: vec![],
            };
        }

        let dr = d_max / n_dist_bins as f64;
        let total_prob: f64 = skymap.prob.iter().sum();
        let prob_threshold = total_prob * 1e-7;
        let mut voxels: Vec<(f64, f64, f64)> = Vec::new();

        for row in 0..skymap.prob.len() {
            let p_pix = skymap.prob[row];
            if p_pix < prob_threshold {
                continue;
            }
            let area = skymap.pixel_area_sr[row];
            let Some((mu, sigma, norm)) = skymap.dist_params(row) else {
                continue;
            };

            for j in 0..n_dist_bins {
                let r = (j as f64 + 0.5) * dr;
                let z_sq = ((r - mu) / sigma).powi(2);
                if z_sq > 25.0 {
                    continue; // beyond 5σ: negligible contribution
                }
                let gauss = (-0.5 * z_sq).exp() / (sigma * SQRT_2PI);
                // dP/dV = (PROB / pixel_area) × DISTNORM × Normal(r; DISTMU, DISTSIGMA)
                let dpdv = (p_pix / area) * norm * gauss;
                if dpdv > 0.0 {
                    let dv = area * r * r * dr;
                    let dp = dpdv * dv;
                    voxels.push((dpdv, dp, dv));
                }
            }
        }

        voxels.sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        let n = voxels.len();
        let mut sorted_dpdv = Vec::with_capacity(n);
        let mut cum_prob = Vec::with_capacity(n);
        let mut cum_vol = Vec::with_capacity(n);

        let total_dp: f64 = voxels.iter().map(|(_, dp, _)| dp).sum();
        let norm_factor = if total_dp > 0.0 { 1.0 / total_dp } else { 1.0 };

        let mut running_prob = 0.0_f64;
        let mut running_vol = 0.0_f64;
        for (dpdv, dp, dv) in &voxels {
            sorted_dpdv.push(*dpdv);
            running_prob += dp * norm_factor;
            running_vol += dv;
            cum_prob.push(running_prob);
            cum_vol.push(running_vol);
        }

        CredibleVolumeIndex {
            sorted_dpdv,
            cum_prob,
            cum_vol,
        }
    }

    /// Density threshold ξ* such that voxels with dP/dV ≥ ξ* contain
    /// `credible_level` of the total probability.
    pub fn density_threshold(&self, credible_level: f64) -> f64 {
        let idx = self.cum_prob.partition_point(|&p| p < credible_level);
        if idx >= self.sorted_dpdv.len() {
            0.0
        } else {
            self.sorted_dpdv[idx]
        }
    }

    /// Credible level at which a candidate with density `dpdv` enters the
    /// credible volume. Lower values mean a better-ranked candidate.
    pub fn searched_prob_vol(&self, dpdv: f64) -> f64 {
        // Count voxels with density strictly greater than dpdv (descending array).
        let k = self.sorted_dpdv.partition_point(|&d| d > dpdv);
        if k == 0 {
            0.0
        } else if k >= self.cum_prob.len() {
            1.0
        } else {
            self.cum_prob[k - 1]
        }
    }

    /// Test whether a candidate at (ra_deg, dec_deg, d_mpc) falls inside the
    /// X% credible volume by comparing its per-voxel density to the threshold.
    pub fn contains(
        &self,
        skymap: &LIGO3dskymap,
        ra_deg: f64,
        dec_deg: f64,
        d_mpc: f64,
        credible_level: f64,
    ) -> bool {
        self.searched_prob_vol_at(skymap, ra_deg, dec_deg, d_mpc)
            .is_some_and(|spv| spv <= credible_level)
    }

    /// Credible level at which a candidate at (ra_deg, dec_deg, d_mpc) enters
    /// the credible volume.  Returns `None` if the sky position is outside the map
    /// or if the pixel has invalid distance parameters.
    pub fn searched_prob_vol_at(
        &self,
        skymap: &LIGO3dskymap,
        ra_deg: f64,
        dec_deg: f64,
        d_mpc: f64,
    ) -> Option<f64> {
        let row = skymap.ang2pix(ra_deg, dec_deg)?;
        let dpdv = skymap.pixel_dpdv(row, d_mpc)?;
        Some(self.searched_prob_vol(dpdv))
    }

    /// Total comoving volume enclosed by the X% credible region, in Mpc³.
    pub fn credible_volume_mpc3(&self, credible_level: f64) -> f64 {
        let idx = self.cum_prob.partition_point(|&p| p < credible_level);
        // cum_vol[idx] is the volume after including the first voxel that brings
        // cumulative probability to credible_level — consistent with density_threshold.
        self.cum_vol
            .get(idx)
            .or_else(|| self.cum_vol.last())
            .copied()
            .unwrap_or(0.0)
    }
}

/// Build a 2D MOC covering the sky projection of the X% credible volume.
///
/// Pixels at their native UNIQ order are included when their peak dP/dV
/// (evaluated at D = DISTMU) exceeds the credible-level threshold. This is an
/// over-approximation by design; downstream code must call
/// `CredibleVolumeIndex::contains` per candidate for the exact 3D test.
pub fn credible_volume_to_2d_moc(
    skymap: &LIGO3dskymap,
    idx: &CredibleVolumeIndex,
    credible_level: f64,
) -> HpxMoc {
    let threshold = idx.density_threshold(credible_level);

    // 2D probability floor: require pixels to also lie within the 2D credible
    // region at the same credible_level. Without this, pixels with near-zero sky
    // probability can pass the 3D threshold purely due to a high DISTNORM, pulling
    // in sky regions with no real GW localization support.
    let mut sorted_prob: Vec<f64> = skymap.prob.clone();
    sorted_prob.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    let cumsum: Vec<f64> = sorted_prob
        .iter()
        .scan(0.0, |acc, &p| {
            *acc += p;
            Some(*acc)
        })
        .collect();
    // # let cutoff_idx = cumsum.partition_point(|&s| s <= 0.99);
    let cutoff_idx = cumsum.partition_point(|&s| s <= credible_level);
    let prob_floor_2d = if cutoff_idx < sorted_prob.len() {
        sorted_prob[cutoff_idx]
    } else {
        0.0
    };

    let included = (0..skymap.prob.len())
        .filter(|&row| {
            // Must be within the 2D credible region at credible_level to suppress
            // low-probability sky pixels artificially boosted by tight distance constraints
            if skymap.prob[row] < prob_floor_2d {
                return false;
            }
            // Peak density occurs at D = DISTMU; pixel_dpdv evaluated there gives (PROB/area)·DISTNORM/(σ√2π)
            skymap
                .pixel_dpdv(row, skymap.distmu[row])
                .is_some_and(|dpdv| dpdv >= threshold)
        })
        .map(|row| {
            let u = skymap.uniq[row];
            (uniq_to_order(u), uniq_to_ipix(u))
        });

    RangeMOC::<u64, Hpx<u64>>::from_cells(skymap.max_order, included, None)
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
        // The two points should differ (footprint is partial sky)
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

    /// Verify conditional_pdf integrates to ~1 over distance for a high-prob pixel.
    #[test]
    fn test_conditional_pdf_integrates_to_one() {
        let skymap =
            parse_3d_skymap("./data/S251031cq_bayestar.fits").expect("Failed to parse 3D skymap");

        // Find the pixel with the highest PROB that also has a valid distance fit
        let best_pix = skymap
            .prob
            .iter()
            .enumerate()
            .filter(|&(i, &p)| {
                p > 0.0
                    && skymap.distmu[i].is_finite()
                    && skymap.distsigma[i].is_finite()
                    && skymap.distsigma[i] > 0.0
                    && skymap.distnorm[i].is_finite()
                    && skymap.distnorm[i] > 0.0
            })
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(i, _)| i)
            .expect("No valid pixel found");

        let mu = skymap.distmu[best_pix];
        let sigma = skymap.distsigma[best_pix];

        // Integrate from 0 to mu + 10σ using trapezoidal rule (500 steps)
        let d_max = mu + 10.0 * sigma;
        let n = 500;
        let dr = d_max / n as f64;
        let integral: f64 = (0..=n)
            .map(|k| {
                let d = k as f64 * dr;
                let weight = if k == 0 || k == n { 0.5 } else { 1.0 };
                weight * skymap.conditional_pdf(best_pix, d) * dr
            })
            .sum();

        assert!(
            (integral - 1.0).abs() < 0.01,
            "conditional_pdf should integrate to ~1 for pixel {}, got {:.6}",
            best_pix,
            integral
        );
    }

    /// Verify CredibleVolumeIndex cumulative arrays are monotonic and end at 1.
    #[test]
    fn test_credible_volume_index_build() {
        let skymap =
            parse_3d_skymap("./data/S251031cq_bayestar.fits").expect("Failed to parse 3D skymap");
        let idx = CredibleVolumeIndex::build(&skymap, 200);

        assert!(!idx.sorted_dpdv.is_empty(), "Index should have voxels");
        assert_eq!(idx.sorted_dpdv.len(), idx.cum_prob.len());
        assert_eq!(idx.sorted_dpdv.len(), idx.cum_vol.len());

        // sorted_dpdv must be non-increasing
        for w in idx.sorted_dpdv.windows(2) {
            assert!(
                w[0] >= w[1],
                "sorted_dpdv not monotone: {} < {}",
                w[0],
                w[1]
            );
        }

        // cum_prob must be non-decreasing and end at ~1.0
        for w in idx.cum_prob.windows(2) {
            assert!(w[1] >= w[0], "cum_prob not monotone");
        }
        let last = *idx.cum_prob.last().unwrap();
        assert!(
            (last - 1.0).abs() < 1e-6,
            "cum_prob should end at 1.0, got {}",
            last
        );

        // cum_vol must be non-decreasing and positive
        for w in idx.cum_vol.windows(2) {
            assert!(w[1] >= w[0], "cum_vol not monotone");
        }
        assert!(*idx.cum_vol.last().unwrap() > 0.0);
    }

    /// Tighter credible level requires a higher density threshold.
    #[test]
    fn test_density_threshold_monotonic() {
        let skymap =
            parse_3d_skymap("./data/S251031cq_bayestar.fits").expect("Failed to parse 3D skymap");
        let idx = CredibleVolumeIndex::build(&skymap, 200);

        let threshold_50 = idx.density_threshold(0.5);
        let threshold_90 = idx.density_threshold(0.9);

        assert!(
            threshold_50 >= threshold_90,
            "density_threshold(0.5)={} should be >= density_threshold(0.9)={}",
            threshold_50,
            threshold_90
        );
    }

    /// The highest-PROB pixel must be inside the 2D MOC projection at 90% CL.
    #[test]
    fn test_credible_volume_to_2d_moc_contains_max_pixel() {
        let skymap =
            parse_3d_skymap("./data/S251031cq_bayestar.fits").expect("Failed to parse 3D skymap");
        let idx = CredibleVolumeIndex::build(&skymap, 200);
        let moc = credible_volume_to_2d_moc(&skymap, &idx, 0.9);

        // Find the pixel with highest PROB that has a valid distance fit
        let best_pix = skymap
            .prob
            .iter()
            .enumerate()
            .filter(|&(i, &p)| {
                p > 0.0
                    && skymap.distmu[i].is_finite()
                    && skymap.distsigma[i].is_finite()
                    && skymap.distsigma[i] > 0.0
                    && skymap.distnorm[i].is_finite()
                    && skymap.distnorm[i] > 0.0
            })
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(i, _)| i)
            .expect("No valid pixel found");

        let best_uniq = skymap.uniq[best_pix];
        let order = uniq_to_order(best_uniq);
        let ipix = uniq_to_ipix(best_uniq);
        assert!(
            moc.contains_cell(order, ipix),
            "highest-PROB pixel (row={}, order={}, ipix={}) should be inside the 90% 2D MOC projection",
            best_pix, order, ipix
        );
    }

    /// A candidate placed at the peak-density voxel should have searched_prob_vol ≈ 0.
    #[test]
    fn test_searched_prob_vol_at_max_density_is_low() {
        let skymap =
            parse_3d_skymap("./data/S251031cq_bayestar.fits").expect("Failed to parse 3D skymap");
        let idx = CredibleVolumeIndex::build(&skymap, 200);

        // The maximum dP/dV in the index
        let max_dpdv = idx.sorted_dpdv[0];
        let spv = idx.searched_prob_vol(max_dpdv);

        assert!(
            spv < 0.01,
            "searched_prob_vol at max density should be near 0, got {}",
            spv
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

    /// Verify that `parse_3d_skymap` reads the BAYESTAR FITS file and
    /// returns all four expected per-pixel columns with matching lengths.
    ///
    /// Test file: `./data/S251031cq_bayestar.fits` — a flat HEALPix BAYESTAR
    /// skymap at nside=512 (npix = 12 × 512² = 3,145,728).
    #[test]
    fn test_parse_3d_skymap_columns() {
        let skymap = parse_3d_skymap("./data/S251031cq_bayestar.fits")
            .expect("Failed to parse 3D skymap FITS");

        // Flat nside=512 file → order=9 after parsing
        assert_eq!(skymap.max_order, 9, "max_order should be 9 (nside=512)");
        let expected_npix = skymap.prob.len();
        assert_eq!(
            expected_npix, 3_145_728,
            "npix should be 12 × 512² for this file"
        );
        assert_eq!(
            skymap.uniq.len(),
            expected_npix,
            "UNIQ vec length should match npix"
        );

        // All four columns must be read and have length == npix
        assert_eq!(
            skymap.prob.len(),
            expected_npix,
            "PROB column length should match npix",
        );
        assert_eq!(
            skymap.distmu.len(),
            expected_npix,
            "DISTMU column length should match npix",
        );
        assert_eq!(
            skymap.distsigma.len(),
            expected_npix,
            "DISTSIGMA column length should match npix",
        );
        assert_eq!(
            skymap.distnorm.len(),
            expected_npix,
            "DISTNORM column length should match npix",
        );

        // PROB column has units pix^-1, so summing all pixels should give ~1.0
        // (a small tolerance for floating-point accumulation)
        let prob_sum: f64 = skymap.prob.iter().sum();
        assert!(
            (prob_sum - 1.0).abs() < 1e-3,
            "PROB column should sum to ~1.0, got {}",
            prob_sum,
        );

        // The majority of pixels should have finite ansatz parameters.
        // (Some pixels along low-probability lines of sight have invalid fits — expected.)
        let n_finite = (0..expected_npix)
            .filter(|&i| {
                skymap.distmu[i].is_finite()
                    && skymap.distsigma[i].is_finite()
                    && skymap.distsigma[i] > 0.0
                    && skymap.distnorm[i].is_finite()
            })
            .count();
        let frac_finite = n_finite as f64 / expected_npix as f64;
        assert!(
            frac_finite > 0.5,
            "Expected majority of pixels to have finite (μ, σ, N), got {:.1}%",
            frac_finite * 100.0,
        );
    }
}
