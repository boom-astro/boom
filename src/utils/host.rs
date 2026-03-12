use serde::{Deserialize, Serialize};

/// Semi-major axis, semi-minor axis, and position angle of a galaxy's elliptical profile.
#[derive(Debug, Clone)]
pub struct GalaxyEllipse {
    pub ra: f64,
    pub dec: f64,
    pub a_arcsec: f64,
    pub b_arcsec: f64,
    pub pa_deg: f64,
    pub axis_ratio: f64,
}

/// A candidate host galaxy with computed association metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostCandidate {
    pub ra: f64,
    pub dec: f64,
    pub separation_arcsec: f64,
    pub dlr: f64,
    pub dlr_rank: u32,
    pub posterior: f64,
    pub fractional_offset: f64,
    pub redshift: Option<f64>,
    pub objtype: Option<String>,
    pub objname: Option<String>,
}

/// Result of host galaxy association for a transient.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostGalaxyAssociation {
    pub best_host: Option<HostCandidate>,
    pub candidates: Vec<HostCandidate>,
    pub n_candidates_searched: u32,
    pub n_candidates_after_dlr_cut: u32,
    pub p_host_none: f64,
    pub search_radius_arcsec: f64,
}

/// Configuration for host galaxy association.
#[derive(Debug, Clone, Deserialize)]
pub struct HostGalaxyConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_catalog")]
    pub catalog: String,
    #[serde(default = "default_max_dlr")]
    pub max_dlr: f64,
    #[serde(default = "default_min_axis_arcsec")]
    pub min_axis_arcsec: f64,
    #[serde(default = "default_max_candidates")]
    pub max_candidates: usize,
    #[serde(default = "default_exclude_star_like")]
    pub exclude_star_like: bool,
    #[serde(default = "default_star_type_value")]
    pub star_type_value: String,
}

fn default_enabled() -> bool {
    false
}
fn default_catalog() -> String {
    "LS_DR10".to_string()
}
fn default_max_dlr() -> f64 {
    5.0
}
fn default_min_axis_arcsec() -> f64 {
    0.05
}
fn default_max_candidates() -> usize {
    10
}
fn default_exclude_star_like() -> bool {
    true
}
fn default_star_type_value() -> String {
    "PSF".to_string()
}

impl Default for HostGalaxyConfig {
    fn default() -> Self {
        HostGalaxyConfig {
            enabled: default_enabled(),
            catalog: default_catalog(),
            max_dlr: default_max_dlr(),
            min_axis_arcsec: default_min_axis_arcsec(),
            max_candidates: default_max_candidates(),
            exclude_star_like: default_exclude_star_like(),
            star_type_value: default_star_type_value(),
        }
    }
}

/// Intermediate result from DLR computation.
#[derive(Debug, Clone)]
pub struct DlrResult {
    dlr: f64,
    separation_arcsec: f64,
}

/// Gamma(0.75) constant used in Bayesian likelihood.
const GAMMA_0_75: f64 = 1.2254167024651776;

/// Convert Legacy Survey Tractor shape parameters to a galaxy ellipse.
///
/// shape_r: half-light radius in arcsec
/// shape_e1, shape_e2: ellipticity components
/// min_b: minimum semi-minor axis in arcsec (floor)
pub fn tractor_shape_to_ellipse(
    ra: f64,
    dec: f64,
    shape_r: f64,
    shape_e1: f64,
    shape_e2: f64,
    min_b: f64,
) -> Option<GalaxyEllipse> {
    if shape_r <= 0.0 || !shape_r.is_finite() {
        return None;
    }

    let e = (shape_e1 * shape_e1 + shape_e2 * shape_e2).sqrt();
    // Clamp eccentricity to avoid division by zero or negative axis ratio
    let e_clamped = e.min(0.999);
    let q = (1.0 - e_clamped) / (1.0 + e_clamped);
    let pa_rad = 0.5 * shape_e2.atan2(shape_e1);
    let pa_deg = pa_rad.to_degrees().rem_euclid(180.0);

    let a = shape_r;
    let b = (a * q).max(min_b);

    Some(GalaxyEllipse {
        ra,
        dec,
        a_arcsec: a,
        b_arcsec: b,
        pa_deg,
        axis_ratio: q,
    })
}

/// Compute the directional light radius (DLR) for a transient relative to a galaxy.
///
/// DLR is the separation between the transient and galaxy center, normalized by
/// the galaxy's effective radius in the direction of the transient. A DLR of 1.0
/// means the transient is at the galaxy's effective radius along that direction.
///
/// Returns the DLR value (which equals the fractional offset) and the angular
/// separation in arcsec.
pub fn compute_dlr(transient_ra: f64, transient_dec: f64, galaxy: &GalaxyEllipse) -> DlrResult {
    let dec_g_rad = galaxy.dec.to_radians();

    // Tangent-plane offsets in arcsec
    let dx = (transient_ra - galaxy.ra) * dec_g_rad.cos() * 3600.0;
    let dy = (transient_dec - galaxy.dec) * 3600.0;

    let separation_arcsec = (dx * dx + dy * dy).sqrt();

    let pa_rad = galaxy.pa_deg.to_radians();
    let cos_pa = pa_rad.cos();
    let sin_pa = pa_rad.sin();

    // Rotate into galaxy frame
    let x_maj = dx * cos_pa + dy * sin_pa;
    let y_min = -dx * sin_pa + dy * cos_pa;

    // DLR = sqrt((x_maj/a)^2 + (y_min/b)^2)
    // This is the fractional offset: separation normalized by galaxy extent in that direction
    let dlr = ((x_maj / galaxy.a_arcsec).powi(2) + (y_min / galaxy.b_arcsec).powi(2)).sqrt();

    DlrResult {
        dlr,
        separation_arcsec,
    }
}

/// Compute Bayesian posterior probabilities for host galaxy candidates.
///
/// Uses a Gamma(a=0.75) likelihood for the fractional offset and a uniform prior.
/// Returns posteriors normalized so that sum(posteriors) + p_none = 1.
fn compute_posteriors(candidates: &mut [HostCandidate]) {
    if candidates.is_empty() {
        return;
    }

    // Gamma(a=0.75) PDF: f(x) = x^(a-1) * exp(-x) / Gamma(a)
    // With a=0.75: f(x) = x^(-0.25) * exp(-x) / Gamma(0.75)
    let likelihoods: Vec<f64> = candidates
        .iter()
        .map(|c| {
            let x = c.fractional_offset;
            if x <= 0.0 {
                // At zero offset, the Gamma(0.75) PDF diverges, use a large but finite value
                1e6
            } else {
                x.powf(-0.25) * (-x).exp() / GAMMA_0_75
            }
        })
        .collect();

    // Prior: uniform over [0, 10], constant for offsets < 10
    // Since prior is the same for all candidates, it cancels in normalization

    // "None" probabilities (small epsilon for V1)
    let p_outside = f64::EPSILON;
    let p_unobserved = f64::EPSILON;
    let p_hostless = f64::EPSILON;
    let p_none_total = p_outside + p_unobserved + p_hostless;

    let sum_likelihoods: f64 = likelihoods.iter().sum();
    let normalization = sum_likelihoods + p_none_total;

    for (candidate, likelihood) in candidates.iter_mut().zip(likelihoods.iter()) {
        candidate.posterior = likelihood / normalization;
    }
}

/// Main entry point: associate a transient with its most likely host galaxy.
///
/// galaxy_docs: cross-match documents from the configured galaxy catalog.
/// Each document should have fields: ra, dec, type, shape_r, shape_e1, shape_e2,
/// and optionally z (redshift) and _id (object name).
pub fn associate_host(
    transient_ra: f64,
    transient_dec: f64,
    galaxy_docs: &[serde_json::Value],
    config: &HostGalaxyConfig,
) -> HostGalaxyAssociation {
    let n_candidates_searched = galaxy_docs.len() as u32;
    let search_radius_arcsec = 0.0; // Set by the crossmatch config, not here

    let mut candidates: Vec<HostCandidate> = Vec::new();

    for doc in galaxy_docs {
        // Extract required fields
        let ra = match doc.get("ra").and_then(|v| v.as_f64()) {
            Some(v) => v,
            None => continue,
        };
        let dec = match doc.get("dec").and_then(|v| v.as_f64()) {
            Some(v) => v,
            None => continue,
        };

        // Filter star-like sources
        if config.exclude_star_like {
            if let Some(obj_type) = doc.get("type").and_then(|v| v.as_str()) {
                if obj_type == config.star_type_value {
                    continue;
                }
            }
        }

        // Extract shape parameters
        let shape_r = match doc.get("shape_r").and_then(|v| v.as_f64()) {
            Some(v) => v,
            None => continue,
        };
        let shape_e1 = doc.get("shape_e1").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let shape_e2 = doc.get("shape_e2").and_then(|v| v.as_f64()).unwrap_or(0.0);

        // Build ellipse
        let ellipse = match tractor_shape_to_ellipse(
            ra,
            dec,
            shape_r,
            shape_e1,
            shape_e2,
            config.min_axis_arcsec,
        ) {
            Some(e) => e,
            None => continue,
        };

        // Compute DLR
        let dlr_result = compute_dlr(transient_ra, transient_dec, &ellipse);

        // Apply DLR cut
        if dlr_result.dlr > config.max_dlr {
            continue;
        }

        // Extract optional fields
        let redshift = doc.get("z").and_then(|v| v.as_f64());
        let objtype = doc
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let objname = doc
            .get("_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        candidates.push(HostCandidate {
            ra,
            dec,
            separation_arcsec: dlr_result.separation_arcsec,
            dlr: dlr_result.dlr,
            dlr_rank: 0, // assigned after sorting
            posterior: 0.0,
            fractional_offset: dlr_result.dlr, // DLR from GHOST3 IS the fractional offset
            redshift,
            objtype,
            objname,
        });
    }

    // Sort by DLR (ascending)
    candidates.sort_by(|a, b| a.dlr.total_cmp(&b.dlr));

    // Assign ranks
    for (i, candidate) in candidates.iter_mut().enumerate() {
        candidate.dlr_rank = (i + 1) as u32;
    }

    let n_candidates_after_dlr_cut = candidates.len() as u32;

    // Truncate to max_candidates
    candidates.truncate(config.max_candidates);

    // Compute Bayesian posteriors
    compute_posteriors(&mut candidates);

    // Compute p_host_none = 1 - sum(posteriors)
    let p_host_none: f64 = 1.0 - candidates.iter().map(|c| c.posterior).sum::<f64>();

    let best_host = candidates
        .iter()
        .cloned()
        .max_by(|a, b| a.posterior.partial_cmp(&b.posterior).unwrap_or(std::cmp::Ordering::Equal));

    HostGalaxyAssociation {
        best_host,
        candidates,
        n_candidates_searched,
        n_candidates_after_dlr_cut,
        p_host_none,
        search_radius_arcsec,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tractor_shape_to_ellipse_basic() {
        let ellipse = tractor_shape_to_ellipse(180.0, -23.0, 2.0, 0.0, 0.0, 0.05).unwrap();
        assert!((ellipse.a_arcsec - 2.0).abs() < 1e-10);
        // With e1=e2=0, e=0, q=1, so b=a
        assert!((ellipse.b_arcsec - 2.0).abs() < 1e-10);
        assert!((ellipse.axis_ratio - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_tractor_shape_to_ellipse_with_ellipticity() {
        let ellipse = tractor_shape_to_ellipse(180.0, -23.0, 2.0, 0.3, 0.4, 0.05).unwrap();
        let e = (0.3f64 * 0.3 + 0.4 * 0.4).sqrt();
        let expected_q = (1.0 - e) / (1.0 + e);
        assert!((ellipse.axis_ratio - expected_q).abs() < 1e-10);
        assert!((ellipse.a_arcsec - 2.0).abs() < 1e-10);
        assert!((ellipse.b_arcsec - (2.0 * expected_q).max(0.05)).abs() < 1e-10);
    }

    #[test]
    fn test_tractor_shape_invalid_radius() {
        assert!(tractor_shape_to_ellipse(180.0, -23.0, 0.0, 0.0, 0.0, 0.05).is_none());
        assert!(tractor_shape_to_ellipse(180.0, -23.0, -1.0, 0.0, 0.0, 0.05).is_none());
        assert!(tractor_shape_to_ellipse(180.0, -23.0, f64::NAN, 0.0, 0.0, 0.05).is_none());
    }

    #[test]
    fn test_tractor_shape_min_b_floor() {
        // Very high ellipticity should be floored by min_b
        let ellipse = tractor_shape_to_ellipse(180.0, -23.0, 0.1, 0.9, 0.0, 0.05).unwrap();
        assert!(ellipse.b_arcsec >= 0.05);
    }

    #[test]
    fn test_compute_dlr_zero_offset() {
        let galaxy = GalaxyEllipse {
            ra: 180.0,
            dec: -23.0,
            a_arcsec: 2.0,
            b_arcsec: 1.0,
            pa_deg: 0.0,
            axis_ratio: 0.5,
        };
        let result = compute_dlr(180.0, -23.0, &galaxy);
        assert!(result.dlr < 1e-10);
        assert!(result.separation_arcsec < 1e-10);
    }

    #[test]
    fn test_compute_dlr_along_major_axis() {
        let galaxy = GalaxyEllipse {
            ra: 180.0,
            dec: 0.0, // equator for simpler cos(dec)=1
            a_arcsec: 5.0,
            b_arcsec: 2.0,
            pa_deg: 0.0,
            axis_ratio: 0.4,
        };
        // Offset 5 arcsec along RA (which is the major axis when pa=0)
        let offset_ra = 5.0 / 3600.0; // 5 arcsec in degrees
        let result = compute_dlr(180.0 + offset_ra, 0.0, &galaxy);
        // Should be at DLR ~ 1.0 (at the effective radius along major axis)
        assert!(
            (result.dlr - 1.0).abs() < 0.01,
            "DLR along major axis should be ~1.0, got {}",
            result.dlr
        );
    }

    #[test]
    fn test_compute_dlr_along_minor_axis() {
        let galaxy = GalaxyEllipse {
            ra: 180.0,
            dec: 0.0,
            a_arcsec: 5.0,
            b_arcsec: 2.0,
            pa_deg: 0.0,
            axis_ratio: 0.4,
        };
        // Offset 2 arcsec along Dec (minor axis when pa=0)
        let offset_dec = 2.0 / 3600.0;
        let result = compute_dlr(180.0, 0.0 + offset_dec, &galaxy);
        // Should be at DLR ~ 1.0 (at the effective radius along minor axis)
        assert!(
            (result.dlr - 1.0).abs() < 0.01,
            "DLR along minor axis should be ~1.0, got {}",
            result.dlr
        );
    }

    #[test]
    fn test_associate_host_basic() {
        let config = HostGalaxyConfig::default();

        // Create a galaxy doc at a small offset from the transient
        let galaxy = serde_json::json!({
            "ra": 180.001,
            "dec": 0.001,
            "type": "SER",
            "shape_r": 3.0,
            "shape_e1": 0.0,
            "shape_e2": 0.0,
        });

        let result = associate_host(180.0, 0.0, &[galaxy], &config);
        assert_eq!(result.n_candidates_searched, 1);
        assert!(result.best_host.is_some());
        let best = result.best_host.unwrap();
        assert_eq!(best.dlr_rank, 1);
        assert!(best.posterior > 0.0);
    }

    #[test]
    fn test_associate_host_filters_stars() {
        let config = HostGalaxyConfig {
            exclude_star_like: true,
            star_type_value: "PSF".to_string(),
            ..Default::default()
        };

        let star = serde_json::json!({
            "ra": 180.001,
            "dec": 0.001,
            "type": "PSF",
            "shape_r": 1.0,
            "shape_e1": 0.0,
            "shape_e2": 0.0,
        });

        let result = associate_host(180.0, 0.0, &[star], &config);
        assert!(result.best_host.is_none());
        assert_eq!(result.n_candidates_after_dlr_cut, 0);
    }

    #[test]
    fn test_associate_host_dlr_ranking() {
        let config = HostGalaxyConfig::default();

        // Two galaxies: one closer in DLR, one further
        let near = serde_json::json!({
            "ra": 180.0005,
            "dec": 0.0005,
            "type": "SER",
            "shape_r": 5.0,
            "shape_e1": 0.0,
            "shape_e2": 0.0,
        });
        let far = serde_json::json!({
            "ra": 180.003,
            "dec": 0.003,
            "type": "DEV",
            "shape_r": 2.0,
            "shape_e1": 0.0,
            "shape_e2": 0.0,
        });

        let result = associate_host(180.0, 0.0, &[far.clone(), near.clone()], &config);
        assert!(result.candidates.len() >= 1);
        // Best host should be the one with lower DLR
        let best = result.best_host.unwrap();
        assert_eq!(best.dlr_rank, 1);
        // The "near" galaxy with larger shape_r and smaller offset should have lower DLR
        assert!((best.ra - 180.0005).abs() < 1e-6);
    }

    #[test]
    fn test_associate_host_no_galaxies() {
        let config = HostGalaxyConfig::default();
        let result = associate_host(180.0, 0.0, &[], &config);
        assert!(result.best_host.is_none());
        assert_eq!(result.n_candidates_searched, 0);
        assert_eq!(result.n_candidates_after_dlr_cut, 0);
        assert!((result.p_host_none - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_posteriors_sum_to_one() {
        let config = HostGalaxyConfig::default();

        let galaxies: Vec<serde_json::Value> = (0..5)
            .map(|i| {
                serde_json::json!({
                    "ra": 180.0 + (i as f64 + 1.0) * 0.001,
                    "dec": 0.0 + (i as f64 + 1.0) * 0.001,
                    "type": "SER",
                    "shape_r": 3.0,
                    "shape_e1": 0.0,
                    "shape_e2": 0.0,
                })
            })
            .collect();

        let result = associate_host(180.0, 0.0, &galaxies, &config);
        let total: f64 = result.candidates.iter().map(|c| c.posterior).sum::<f64>() + result.p_host_none;
        assert!(
            (total - 1.0).abs() < 1e-10,
            "Posteriors + p_host_none should sum to 1.0, got {}",
            total
        );
    }
}
