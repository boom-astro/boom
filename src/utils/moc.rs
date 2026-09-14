use cdshealpix::nested;
use fitsio::FitsFile;
use flare::spatial::great_circle_distance;
use moc::deser::fits::skymap::from_fits_skymap;
use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType, MocType};
use moc::moc::range::RangeMOC;
use moc::moc::{CellMOCIntoIterator, CellMOCIterator, HasMaxDepth};
use moc::qty::Hpx;
use std::collections::HashMap;
use std::io::{BufReader, Cursor};

const SQRT_2PI: f64 = 2.5066282746310002;

pub type HpxMoc = RangeMOC<u64, Hpx<u64>>;

/// `(ra_deg, dec_deg, radius_rad)`.
pub type Cone = (f64, f64, f64);

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

/// Threshold a HEALPix skymap at a cumulative credible level (0.9 = the 90% region).
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

/// Underflows below UNIQ 4, which [`parse_3d_skymap`] rejects at load.
fn uniq_to_order(uniq: u64) -> u8 {
    debug_assert!(uniq >= 4, "invalid UNIQ index: {uniq} (minimum is 4)");
    ((63 - uniq.leading_zeros()) / 2 - 1) as u8
}

fn uniq_to_ipix(uniq: u64) -> u64 {
    let order = uniq_to_order(uniq) as u32;
    uniq - (1u64 << (2 * order + 2))
}

fn pixel_area_from_order(order: u8) -> f64 {
    4.0 * std::f64::consts::PI / (12.0 * (1u64 << (2 * order as u32)) as f64)
}

/// A flat HEALPix file is converted to the UNIQ layout on load.
pub struct LIGO3dskymap {
    pub uniq: Vec<u64>,
    pixel_area_sr: Vec<f64>,
    /// Sums to ~1 over the map.
    pub prob: Vec<f64>,
    pub distmu: Vec<f64>,
    pub distsigma: Vec<f64>,
    pub distnorm: Vec<f64>,
    max_order: u8,
    /// `None` for a flat full-sky map, where the row is the pixel index at `max_order`.
    uniq_to_row: Option<HashMap<u64, usize>>,
}

#[derive(Debug)]
pub enum Skymap3dError {
    /// Callers may retry the file as a plain 2D probability skymap.
    NotThreeDimensional,
    /// Carries the distance columns but is unreadable; never downgrade to a 2D search.
    Invalid(String),
}

impl std::fmt::Display for Skymap3dError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Skymap3dError::NotThreeDimensional => {
                write!(f, "not a 3D skymap (no DISTMU/DISTSIGMA/DISTNORM columns)")
            }
            Skymap3dError::Invalid(e) => write!(f, "{}", e),
        }
    }
}

pub fn parse_3d_skymap(path: &str) -> Result<LIGO3dskymap, Skymap3dError> {
    use Skymap3dError::{Invalid, NotThreeDimensional};
    let invalid = |e: String| Invalid(e);

    let mut fits = FitsFile::open(path).map_err(|e| Invalid(e.to_string()))?;
    let hdu = fits.hdu(1).map_err(|e| Invalid(e.to_string()))?;

    // Read the distance columns first: only their absence may fall back to a 2D search.
    let distmu: Vec<f64> = hdu
        .read_col(&mut fits, "DISTMU")
        .map_err(|_| NotThreeDimensional)?;
    let distsigma: Vec<f64> = hdu
        .read_col(&mut fits, "DISTSIGMA")
        .map_err(|_| NotThreeDimensional)?;
    let distnorm: Vec<f64> = hdu
        .read_col(&mut fits, "DISTNORM")
        .map_err(|_| NotThreeDimensional)?;

    let (uniq, pixel_area_sr, prob, uniq_to_row) =
        if let Ok(uniq_i64) = hdu.read_col::<i64>(&mut fits, "UNIQ") {
            let uniq: Vec<u64> = uniq_i64.iter().map(|&u| u as u64).collect();
            if let Some(&bad) = uniq.iter().find(|&&u| u < 4) {
                return Err(invalid(format!(
                    "Invalid UNIQ index in skymap FITS: {bad} (minimum valid UNIQ is 4)"
                )));
            }
            let areas: Vec<f64> = uniq
                .iter()
                .map(|&u| pixel_area_from_order(uniq_to_order(u)))
                .collect();
            let probdensity: Vec<f64> = hdu.read_col(&mut fits, "PROBDENSITY").map_err(|e| {
                invalid(format!(
                    "PROBDENSITY column missing from UNIQ skymap: {}",
                    e
                ))
            })?;
            let prob: Vec<f64> = probdensity
                .iter()
                .zip(areas.iter())
                .map(|(&pd, &a)| pd * a)
                .collect();
            let rows = uniq.iter().enumerate().map(|(i, &u)| (u, i)).collect();
            (uniq, areas, prob, Some(rows))
        } else {
            let ordering: String = hdu.read_key(&mut fits, "ORDERING").map_err(|e| {
                invalid(format!(
                    "skymap has neither a readable int64 UNIQ column nor an ORDERING keyword: {}",
                    e
                ))
            })?;
            if ordering.trim() != "NESTED" {
                return Err(invalid(format!(
                    "Unsupported HEALPix ORDERING {}: only NESTED is supported",
                    ordering.trim()
                )));
            }
            let nside: i64 = hdu
                .read_key(&mut fits, "NSIDE")
                .map_err(|e| invalid(e.to_string()))?;
            let nside = nside as u32;
            if !nside.is_power_of_two() {
                return Err(invalid(format!(
                    "NSIDE must be a power of two, got {}",
                    nside
                )));
            }
            let order = nside.trailing_zeros() as u8;
            let area = pixel_area_from_order(order);
            let npix = 12 * (nside as usize).pow(2);
            let prob: Vec<f64> = hdu
                .read_col(&mut fits, "PROB")
                .map_err(|e| invalid(e.to_string()))?;
            if prob.len() != npix {
                return Err(invalid(format!(
                    "Flat skymap has {} rows but NSIDE={} implies {} pixels; \
                     only implicit full-sky maps are supported",
                    prob.len(),
                    nside,
                    npix
                )));
            }
            let base = 1u64 << (2 * order as u32 + 2);
            let uniq: Vec<u64> = (0..npix as u64).map(|i| base + i).collect();
            let areas = vec![area; npix];
            (uniq, areas, prob, None)
        };

    if distmu.len() != prob.len() || distsigma.len() != prob.len() || distnorm.len() != prob.len() {
        return Err(invalid(
            "DISTMU/DISTSIGMA/DISTNORM lengths do not match the probability column".to_string(),
        ));
    }

    let max_order = uniq.iter().map(|&u| uniq_to_order(u)).max().unwrap_or(0);

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
pub fn parse_3d_skymap_bytes(bytes: &[u8]) -> Result<LIGO3dskymap, Skymap3dError> {
    use std::io::Write;
    let invalid = |e: std::io::Error| Skymap3dError::Invalid(e.to_string());
    let mut tmp = tempfile::NamedTempFile::new().map_err(invalid)?;
    tmp.write_all(bytes).map_err(invalid)?;
    tmp.flush().map_err(invalid)?;
    let path = tmp
        .path()
        .to_str()
        .ok_or_else(|| Skymap3dError::Invalid("temp file path is not valid UTF-8".to_string()))?
        .to_string();
    parse_3d_skymap(&path)
}

impl LIGO3dskymap {
    /// Walks up to coarser parents so multi-order maps resolve. `None` on a coverage gap.
    fn ang2pix(&self, ra_deg: f64, dec_deg: f64) -> Option<usize> {
        let layer = nested::get(self.max_order);
        let mut ipix = layer.hash(ra_deg.to_radians(), dec_deg.to_radians());
        let Some(uniq_to_row) = self.uniq_to_row.as_ref() else {
            return Some(ipix as usize); // flat full-sky map: row == ipix
        };
        for order in (0..=self.max_order).rev() {
            let uniq = (1u64 << (2 * order as u32 + 2)) + ipix;
            if let Some(&row) = uniq_to_row.get(&uniq) {
                return Some(row);
            }
            if order > 0 {
                ipix >>= 2; // step to parent pixel in NESTED ordering
            }
        }
        None
    }

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

    /// dP/dV in sr⁻¹ Mpc⁻³. The r² in p(r|Ω) and in dV cancel, so no r² factor here.
    fn pixel_dpdv(&self, row: usize, d_mpc: f64) -> Option<f64> {
        let (mu, sigma, norm) = self.dist_params(row)?;
        let gauss = (-0.5 * ((d_mpc - mu) / sigma).powi(2)).exp() / (sigma * SQRT_2PI);
        Some((self.prob[row] / self.pixel_area_sr[row]) * norm * gauss)
    }
}

/// Voxels (pixel × distance bin) sorted by dP/dV descending, with cumulative probability.
pub struct CredibleVolumeIndex {
    sorted_dpdv: Vec<f64>,
    cum_prob: Vec<f64>,
}

impl CredibleVolumeIndex {
    /// `n_dist_bins` bins span `[0, max(DISTMU + 5σ)]`.
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
            };
        }

        let dr = d_max / n_dist_bins as f64;
        let total_prob: f64 = skymap.prob.iter().sum();
        let prob_threshold = total_prob * 1e-7;
        let mut voxels: Vec<(f64, f64)> = Vec::new();

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
                let dpdv = (p_pix / area) * norm * gauss;
                if dpdv > 0.0 {
                    voxels.push((dpdv, dpdv * area * r * r * dr));
                }
            }
        }

        voxels.sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        let total_dp: f64 = voxels.iter().map(|(_, dp)| dp).sum();
        let norm_factor = if total_dp > 0.0 { 1.0 / total_dp } else { 1.0 };

        let mut running_prob = 0.0_f64;
        let mut cum_prob = Vec::with_capacity(voxels.len());
        for (_, dp) in &voxels {
            running_prob += dp * norm_factor;
            cum_prob.push(running_prob);
        }

        CredibleVolumeIndex {
            sorted_dpdv: voxels.into_iter().map(|(dpdv, _)| dpdv).collect(),
            cum_prob,
        }
    }

    /// Density ξ* above which voxels hold `credible_level` of the total probability.
    pub fn density_threshold(&self, credible_level: f64) -> f64 {
        let idx = self.cum_prob.partition_point(|&p| p < credible_level);
        if idx >= self.sorted_dpdv.len() {
            0.0
        } else {
            self.sorted_dpdv[idx]
        }
    }

    /// Credible level at which density `dpdv` enters the volume; lower ranks better.
    fn searched_prob_vol(&self, dpdv: f64) -> f64 {
        // sorted_dpdv is descending, so the predicate is `>`, not the usual `<`.
        let k = self.sorted_dpdv.partition_point(|&d| d > dpdv);
        if k == 0 {
            0.0
        } else if k >= self.cum_prob.len() {
            1.0
        } else {
            self.cum_prob[k - 1]
        }
    }

    /// `None` if the position is off the map or its pixel has no valid distance fit.
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
}

/// Deliberately an over-approximation: callers must still run `searched_prob_vol_at`.
pub fn credible_volume_to_2d_moc(
    skymap: &LIGO3dskymap,
    idx: &CredibleVolumeIndex,
    credible_level: f64,
) -> HpxMoc {
    let threshold = idx.density_threshold(credible_level);

    let mut sorted_prob: Vec<f64> = skymap.prob.clone();
    sorted_prob.sort_unstable_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    let cumsum: Vec<f64> = sorted_prob
        .iter()
        .scan(0.0, |acc, &p| {
            *acc += p;
            Some(*acc)
        })
        .collect();
    let cutoff_idx = cumsum.partition_point(|&s| s <= credible_level);
    let prob_floor_2d = if cutoff_idx < sorted_prob.len() {
        sorted_prob[cutoff_idx]
    } else {
        0.0
    };

    let included = (0..skymap.prob.len())
        .filter(|&row| {
            if skymap.prob[row] < prob_floor_2d {
                return false;
            }
            // Peak density is at D = DISTMU.
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

pub fn is_in_moc(moc: &HpxMoc, ra_deg: f64, dec_deg: f64) -> bool {
    let depth = moc.depth_max();
    let ra_rad = ra_deg.to_radians();
    let dec_rad = dec_deg.to_radians();
    let layer = nested::get(depth);
    let cell = layer.hash(ra_rad, dec_rad);
    moc.contains_cell(depth, cell)
}

/// `(ra_deg, dec_deg, radius_rad)` cones circumscribing the cells of a degraded MOC.
pub fn moc_to_covering_cones(moc: &HpxMoc, target_depth: u8) -> Vec<Cone> {
    let degraded = moc.degraded(target_depth);
    degraded
        .flatten_to_fixed_depth_cells()
        .map(|cell_idx| {
            let (lon_rad, lat_rad) = nested::center(target_depth, cell_idx);
            let vertices = nested::vertices(target_depth, cell_idx);

            let radius_rad = vertices
                .iter()
                .map(|(vlon, vlat)| angular_distance(lon_rad, lat_rad, *vlon, *vlat))
                .fold(0.0_f64, f64::max);

            let radius_rad = radius_rad * 1.01; // margin for edge effects

            let ra_deg = lon_rad.to_degrees();
            let dec_deg = lat_rad.to_degrees();
            (ra_deg, dec_deg, radius_rad)
        })
        .collect()
}

/// Area-only, so a fragmented region can still blow past any cone budget: use
/// [`select_covering_depth_bounded`] when the count must be capped.
pub fn select_covering_depth(moc: &HpxMoc) -> u8 {
    let coverage_pct = moc.coverage_percentage();
    let coverage_sq_deg = coverage_pct * 41253.0; // full sky ≈ 41253 sq deg

    if coverage_sq_deg < 10.0 {
        7 // ~0.21 sq deg cells
    } else if coverage_sq_deg < 100.0 {
        5 // ~3.36 sq deg cells
    } else if coverage_sq_deg < 1000.0 {
        4 // ~13.4 sq deg cells
    } else {
        3 // ~53.7 sq deg cells
    }
}

/// Below this the cones stop narrowing the query, so a sky-wide region must be rejected.
const MIN_COVERING_DEPTH: u8 = 3;

/// Coarsens no further than [`MIN_COVERING_DEPTH`], so the caller must still reject
/// a result that is over `max_cones`.
/// Largest number of covering cones a match stage will expand a MOC into.
///
/// Each becomes an `$or` branch, so this bounds the query mongo has to plan.
pub const MOC_MATCH_MAX_CONES: usize = 500;

/// A `$match` stage selecting alerts inside `moc`.
///
/// Prepending this to a filter's own pipeline keeps a skymap search on the same
/// cuts as any other run of that filter, rather than a parallel set. Mongo
/// cannot test a MOC directly, so the region is expanded into covering cones.
pub fn moc_match_stage(moc: &HpxMoc) -> Result<mongodb::bson::Document, String> {
    use mongodb::bson::doc;

    let (depth, cones) = select_covering_depth_bounded(moc, MOC_MATCH_MAX_CONES);
    if cones.is_empty() {
        return Err("MOC covers no sky".to_string());
    }
    if cones.len() > MOC_MATCH_MAX_CONES {
        return Err(format!(
            "search region too large: {} covering cones at depth {} (max {})",
            cones.len(),
            depth,
            MOC_MATCH_MAX_CONES
        ));
    }
    let or_conditions: Vec<mongodb::bson::Document> = cones
        .iter()
        .map(|&(ra, dec, radius_rad)| {
            doc! {
                "coordinates.radec_geojson": {
                    "$geoWithin": { "$centerSphere": [[ra - 180.0, dec], radius_rad] }
                }
            }
        })
        .collect();
    Ok(doc! { "$match": { "$or": or_conditions } })
}

pub fn select_covering_depth_bounded(moc: &HpxMoc, max_cones: usize) -> (u8, Vec<Cone>) {
    let mut depth = select_covering_depth(moc);
    let mut cones = moc_to_covering_cones(moc, depth);
    while depth > MIN_COVERING_DEPTH && cones.len() > max_cones {
        depth -= 1;
        cones = moc_to_covering_cones(moc, depth);
    }
    (depth, cones)
}

/// Radians in, radians out.
fn angular_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    great_circle_distance(
        lon1.to_degrees(),
        lat1.to_degrees(),
        lon2.to_degrees(),
        lat2.to_degrees(),
    )
    .to_radians()
}

#[cfg(test)]
mod tests {
    use super::{moc_match_stage, MOC_MATCH_MAX_CONES};
    use moc::moc::range::RangeMOC;
    use moc::qty::Hpx;

    /// A MOC covering a single cell at the given depth.
    fn one_cell(depth: u8, cell: u64) -> super::HpxMoc {
        RangeMOC::<u64, Hpx<u64>>::from_cells(depth, std::iter::once((depth, cell)), None)
    }

    #[test]
    fn test_stage_is_a_match_of_cone_alternatives() {
        let stage = moc_match_stage(&one_cell(3, 100)).expect("a stage");
        let or = stage
            .get_document("$match")
            .expect("$match")
            .get_array("$or")
            .expect("$or");
        assert!(!or.is_empty());
        for branch in or {
            let d = branch.as_document().expect("a condition");
            assert!(d.contains_key("coordinates.radec_geojson"));
        }
    }

    #[test]
    fn test_longitude_is_written_in_the_stored_convention() {
        // Positions are stored as [ra - 180, dec], so the stage must match that.
        let stage = moc_match_stage(&one_cell(3, 100)).expect("a stage");
        let or = stage
            .get_document("$match")
            .unwrap()
            .get_array("$or")
            .unwrap();
        let centre = or[0]
            .as_document()
            .unwrap()
            .get_document("coordinates.radec_geojson")
            .unwrap()
            .get_document("$geoWithin")
            .unwrap()
            .get_array("$centerSphere")
            .unwrap();
        let lon = centre[0].as_array().unwrap()[0].as_f64().unwrap();
        assert!(
            (-180.0..=180.0).contains(&lon),
            "longitude {lon} is not shifted"
        );
    }

    #[test]
    fn test_an_empty_moc_is_refused() {
        let empty = RangeMOC::<u64, Hpx<u64>>::new_empty(3);
        assert!(moc_match_stage(&empty).is_err());
    }

    /// The whole path a skymap event takes: real localization -> MOC at a
    /// credible level -> the stage mongo actually runs.
    #[test]
    fn test_a_real_fermi_localization_becomes_a_usable_stage() {
        let bytes =
            std::fs::read("./data/glg_healpix_all_bn200524211.fits").expect("the Fermi fixture");
        let moc = super::moc_from_skymap_bytes(&bytes, 0.9).expect("a 90% MOC");
        let stage = moc_match_stage(&moc).expect("a stage");
        let or = stage
            .get_document("$match")
            .expect("$match")
            .get_array("$or")
            .expect("$or");
        assert!(!or.is_empty(), "a real localization must cover some sky");
        assert!(or.len() <= MOC_MATCH_MAX_CONES, "{} cones", or.len());
    }

    /// A tighter credible level is a subset, so it can never need more cones.
    #[test]
    fn test_a_smaller_credible_level_does_not_cost_more_cones() {
        let bytes =
            std::fs::read("./data/glg_healpix_all_bn200524211.fits").expect("the Fermi fixture");
        let count = |level: f64| {
            let moc = super::moc_from_skymap_bytes(&bytes, level).expect("a MOC");
            moc_match_stage(&moc)
                .expect("a stage")
                .get_document("$match")
                .unwrap()
                .get_array("$or")
                .unwrap()
                .len()
        };
        let (fifty, ninety) = (count(0.5), count(0.9));
        assert!(
            fifty <= ninety,
            "50% took {fifty} cones against 90% at {ninety}"
        );
    }

    /// Roughly the area of the largest Fermi localization seen in production.
    #[test]
    fn test_a_five_thousand_square_degree_region_still_yields_a_stage() {
        // Depth 5 cells are ~3.36 deg^2, so ~1500 of them cover ~5000 deg^2.
        let moc = RangeMOC::<u64, Hpx<u64>>::from_cells(5, (0..1500).map(|c| (5, c)), None);
        let stage = moc_match_stage(&moc).expect("a wide region must still search");
        let n = stage
            .get_document("$match")
            .unwrap()
            .get_array("$or")
            .unwrap()
            .len();
        assert!(n <= MOC_MATCH_MAX_CONES, "{n} cones exceeds the budget");
    }

    #[test]
    fn test_the_cone_budget_is_never_exceeded() {
        // All-sky at depth 0 is the widest region a caller could hand over.
        let all_sky = RangeMOC::<u64, Hpx<u64>>::from_cells(0, (0..12).map(|c| (0, c)), None);
        if let Ok(stage) = moc_match_stage(&all_sky) {
            let n = stage
                .get_document("$match")
                .unwrap()
                .get_array("$or")
                .unwrap()
                .len();
            assert!(n <= MOC_MATCH_MAX_CONES, "{n} cones exceeds the budget");
        }
    }

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
        let bytes = std::fs::read("./data/ls_footprint_moc.fits")
            .expect("Failed to read footprint MOC file");
        let moc = moc_from_fits_bytes(&bytes).expect("Failed to parse MOC");

        let depth = select_covering_depth(&moc);
        let cones = moc_to_covering_cones(&moc, depth);

        let degraded = moc.degraded(8);
        let sample_cells: Vec<u64> = degraded.flatten_to_fixed_depth_cells().take(100).collect();

        for cell_idx in sample_cells {
            let (lon_rad, lat_rad) = nested::center(8, cell_idx);
            let ra_deg = lon_rad.to_degrees();
            let dec_deg = lat_rad.to_degrees();

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

    /// ZTF20abbiixp enters GRB 200524A's Fermi GBM MOC between CL ~6 and ~6.5%.
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

    /// The Singer+2016 ansatz must integrate back to the pixel's own PROB.
    #[test]
    fn test_pixel_dpdv_integrates_to_pixel_prob() {
        let skymap =
            parse_3d_skymap("./data/S240618ah_bayestar.fits").expect("Failed to parse 3D skymap");

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

        let n = 500;
        let dr = (mu + 10.0 * sigma) / n as f64;
        let integral: f64 = (0..=n)
            .map(|k| {
                let d = k as f64 * dr;
                let weight = if k == 0 || k == n { 0.5 } else { 1.0 };
                let dpdv = skymap.pixel_dpdv(best_pix, d).unwrap_or(0.0);
                weight * dpdv * d * d * skymap.pixel_area_sr[best_pix] * dr
            })
            .sum();

        let expected = skymap.prob[best_pix];
        assert!(
            (integral - expected).abs() < 0.01 * expected,
            "dP/dV should integrate to PROB={:.6e} for pixel {}, got {:.6e}",
            expected,
            best_pix,
            integral
        );
    }

    #[test]
    fn test_credible_volume_index_build() {
        let skymap =
            parse_3d_skymap("./data/S240618ah_bayestar.fits").expect("Failed to parse 3D skymap");
        let idx = CredibleVolumeIndex::build(&skymap, 200);

        assert!(!idx.sorted_dpdv.is_empty(), "Index should have voxels");
        assert_eq!(idx.sorted_dpdv.len(), idx.cum_prob.len());

        for w in idx.sorted_dpdv.windows(2) {
            assert!(
                w[0] >= w[1],
                "sorted_dpdv not monotone: {} < {}",
                w[0],
                w[1]
            );
        }

        for w in idx.cum_prob.windows(2) {
            assert!(w[1] >= w[0], "cum_prob not monotone");
        }
        let last = *idx.cum_prob.last().unwrap();
        assert!(
            (last - 1.0).abs() < 1e-6,
            "cum_prob should end at 1.0, got {}",
            last
        );
    }

    #[test]
    fn test_density_threshold_monotonic() {
        let skymap =
            parse_3d_skymap("./data/S240618ah_bayestar.fits").expect("Failed to parse 3D skymap");
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

    #[test]
    fn test_credible_volume_to_2d_moc_contains_max_pixel() {
        let skymap =
            parse_3d_skymap("./data/S240618ah_bayestar.fits").expect("Failed to parse 3D skymap");
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

    #[test]
    fn test_searched_prob_vol_at_max_density_is_low() {
        let skymap =
            parse_3d_skymap("./data/S240618ah_bayestar.fits").expect("Failed to parse 3D skymap");
        let idx = CredibleVolumeIndex::build(&skymap, 200);

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

    /// `./data/S240618ah_bayestar.fits` is a flat nside=256 map (npix = 12 × 256²).
    #[test]
    fn test_parse_3d_skymap_columns() {
        let skymap = parse_3d_skymap("./data/S240618ah_bayestar.fits")
            .expect("Failed to parse 3D skymap FITS");

        assert_eq!(skymap.max_order, 8, "max_order should be 8 (nside=256)");
        let expected_npix = skymap.prob.len();
        assert_eq!(
            expected_npix, 786_432,
            "npix should be 12 × 256² for this file"
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

        let prob_sum: f64 = skymap.prob.iter().sum();
        assert!(
            (prob_sum - 1.0).abs() < 1e-3,
            "PROB column should sum to ~1.0, got {}",
            prob_sum,
        );

        // Pixels along low-probability lines of sight legitimately have no valid fit.
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

    fn write_partial_flat_skymap(path: &str, nside: i64, n_rows: usize) {
        use fitsio::tables::{ColumnDataType, ColumnDescription};
        let columns: Vec<_> = ["PROB", "DISTMU", "DISTSIGMA", "DISTNORM"]
            .iter()
            .map(|name| {
                ColumnDescription::new(*name)
                    .with_type(ColumnDataType::Double)
                    .create()
                    .unwrap()
            })
            .collect();
        let mut f = fitsio::FitsFile::create(path).overwrite().open().unwrap();
        let hdu = f.create_table("SKYMAP", &columns).unwrap();
        hdu.write_key(&mut f, "NSIDE", nside).unwrap();
        hdu.write_key(&mut f, "ORDERING", "NESTED").unwrap();
        for (name, value) in [
            ("PROB", 0.1_f64),
            ("DISTMU", 100.0),
            ("DISTSIGMA", 10.0),
            ("DISTNORM", 1.0),
        ] {
            hdu.write_col(&mut f, name, &vec![value; n_rows]).unwrap();
        }
    }

    /// Accepting one would let `ang2pix` return a row past the end of the columns.
    #[test]
    fn test_parse_3d_skymap_rejects_partial_flat_map() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        write_partial_flat_skymap(&path, 4, 10); // NSIDE=4 implies 192 pixels

        match parse_3d_skymap(&path) {
            Err(Skymap3dError::Invalid(e)) => {
                assert!(e.contains("10 rows"), "unexpected message: {e}")
            }
            Err(other) => panic!("expected Invalid, got {other:?}"),
            Ok(_) => panic!("a partial flat skymap must be rejected"),
        }
    }

    #[test]
    fn test_parse_3d_skymap_classifies_2d_skymap() {
        match parse_3d_skymap("./data/glg_healpix_all_bn200524211.fits") {
            Err(Skymap3dError::NotThreeDimensional) => {}
            Err(other) => panic!("expected NotThreeDimensional, got {other:?}"),
            Ok(_) => panic!("a Fermi GBM 2D skymap is not a 3D localization"),
        }
    }

    #[test]
    fn test_select_covering_depth_bounded_handles_fragmentation() {
        let depth = 9u8;
        let layer = cdshealpix::nested::get(depth);
        // 800 islands spread over ~80x40 deg, totalling ~10.5 sq deg.
        let cells = (0..800u32).map(|i| {
            let ra = (i as f64 * 0.61803398875 * 80.0) % 80.0;
            let dec = (-20.0 + (i as f64 * 0.37 * 40.0) % 40.0).clamp(-89.0, 89.0);
            (depth, layer.hash(ra.to_radians(), dec.to_radians()))
        });
        let moc: HpxMoc = RangeMOC::from_cells(depth, cells, None);

        let naive_depth = select_covering_depth(&moc);
        let naive_cones = moc_to_covering_cones(&moc, naive_depth).len();
        assert!(
            naive_cones > 500,
            "test setup: naive heuristic should exceed the cap here (got {} cones)",
            naive_cones
        );

        let (bounded_depth, bounded_cones) = select_covering_depth_bounded(&moc, 500);
        assert!(
            bounded_depth >= MIN_COVERING_DEPTH,
            "should not coarsen below the floor"
        );
        assert!(
            bounded_cones.len() <= 500,
            "bounded selection should respect the cap, got {} cones",
            bounded_cones.len()
        );
    }

    #[test]
    fn test_select_covering_depth_bounded_still_rejects_genuinely_broad_regions() {
        let bytes = std::fs::read("./data/ls_footprint_moc.fits")
            .expect("Failed to read footprint MOC file");
        let moc = moc_from_fits_bytes(&bytes).expect("Failed to parse MOC");

        let (depth, cones) = select_covering_depth_bounded(&moc, 500);
        assert_eq!(
            depth, MIN_COVERING_DEPTH,
            "should coarsen all the way to the floor for a ~67%-of-sky footprint"
        );
        assert!(
            cones.len() > 500,
            "a genuinely broad region should still exceed the cap at the floor, got {} cones",
            cones.len()
        );
    }
}
