//! Match detections to known objects by ephemeris.
//!
//! ZTF labels a detection with an MPC designation and Rubin labels it with its
//! own `ssObjectId`, so the two surveys cannot be compared directly. Predicting
//! where every catalogued object sits at a detection's epoch supplies the
//! missing bridge: it names Rubin's objects in MPC terms, and the residual it
//! leaves measures each survey's astrometry against the same reference.

use crate::utils::heliolinc::radec_from_ecliptic;
use crate::utils::linking::{angular_separation_deg, Detection};
use crate::utils::sso_geometry::{earth_position, heliocentric_position, OrbitalElements};
use std::collections::HashMap;

/// One catalogued object.
#[derive(Debug, Clone)]
pub struct OrbitEntry {
    pub designation: String,
    pub elements: OrbitalElements,
}

/// A detection attributed to a catalogued object.
#[derive(Debug, Clone)]
pub struct Match {
    pub detection_id: i64,
    pub designation: String,
    pub separation_arcsec: f64,
    pub jd: f64,
}

/// How wide to search, at each of the two stages.
#[derive(Debug, Clone)]
pub struct IdentifyConfig {
    /// Shortlisting radius, degrees. Must exceed a night's motion, since the
    /// coarse pass places every object at one epoch per night.
    pub coarse_radius_deg: f64,
    /// Radius a refined prediction must fall inside to count, arcseconds.
    pub match_radius_arcsec: f64,
}

impl Default for IdentifyConfig {
    fn default() -> Self {
        Self {
            coarse_radius_deg: 1.5,
            match_radius_arcsec: 10.0,
        }
    }
}

/// Geocentric apparent position of `elements` at `jd`, degrees.
pub fn predict_radec(elements: &OrbitalElements, jd: f64) -> (f64, f64) {
    let helio = heliocentric_position(elements, jd);
    let earth = earth_position(jd);
    radec_from_ecliptic(&[
        helio[0] - earth[0],
        helio[1] - earth[1],
        helio[2] - earth[2],
    ])
}

/// Attribute each detection to the catalogued object it sits closest to.
///
/// Two stages, because propagating the whole catalogue per detection is
/// wasteful: every object is placed once per night to shortlist candidates,
/// then only those are recomputed at the detection's own epoch.
pub fn identify(
    detections: &[Detection],
    orbits: &[OrbitEntry],
    cfg: &IdentifyConfig,
) -> Vec<Match> {
    let mut by_night: HashMap<i64, Vec<&Detection>> = HashMap::new();
    for d in detections {
        by_night
            .entry((d.jd - 0.5).floor() as i64)
            .or_default()
            .push(d);
    }

    let mut matches = Vec::new();
    for (night, dets) in by_night {
        let epoch = night as f64 + 1.0;
        // Every object placed once, then sorted so a dec band can be swept.
        let mut predicted: Vec<(f64, f64, usize)> = orbits
            .iter()
            .enumerate()
            .map(|(i, o)| {
                let (ra, dec) = predict_radec(&o.elements, epoch);
                (dec, ra, i)
            })
            .collect();
        predicted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let decs: Vec<f64> = predicted.iter().map(|p| p.0).collect();

        for d in dets {
            let lo = decs.partition_point(|&x| x < d.dec - cfg.coarse_radius_deg);
            let hi = decs.partition_point(|&x| x <= d.dec + cfg.coarse_radius_deg);
            let mut best: Option<(f64, usize)> = None;
            for &(night_dec, night_ra, idx) in &predicted[lo..hi] {
                // The dec band still spans every RA, so gate on the full
                // separation before paying for a second propagation.
                if angular_separation_deg(d.ra, d.dec, night_ra, night_dec) > cfg.coarse_radius_deg
                {
                    continue;
                }
                let (pra, pdec) = predict_radec(&orbits[idx].elements, d.jd);
                let sep = angular_separation_deg(d.ra, d.dec, pra, pdec) * 3600.0;
                if sep <= cfg.match_radius_arcsec && best.is_none_or(|(b, _)| sep < b) {
                    best = Some((sep, idx));
                }
            }
            if let Some((sep, idx)) = best {
                matches.push(Match {
                    detection_id: d.id,
                    designation: orbits[idx].designation.clone(),
                    separation_arcsec: sep,
                    jd: d.jd,
                });
            }
        }
    }
    matches
}

/// Median of a set of separations, arcseconds.
pub fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(values[values.len() / 2])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ceres_like() -> OrbitalElements {
        OrbitalElements::elliptical(2460000.5, 2.7658, 0.0785, 10.588, 80.25, 73.6, 100.0)
    }

    fn pallas_like() -> OrbitalElements {
        OrbitalElements::elliptical(2460000.5, 2.7696, 0.2307, 34.93, 172.89, 310.97, 254.25)
    }

    fn catalogue() -> Vec<OrbitEntry> {
        vec![
            OrbitEntry {
                designation: "1".to_string(),
                elements: ceres_like(),
            },
            OrbitEntry {
                designation: "2".to_string(),
                elements: pallas_like(),
            },
        ]
    }

    /// A detection placed exactly where `elements` predicts at `jd`.
    fn detection_of(elements: &OrbitalElements, jd: f64, id: i64) -> Detection {
        let (ra, dec) = predict_radec(elements, jd);
        Detection {
            id,
            jd,
            ra,
            dec,
            mag: None,
            band: None,
        }
    }

    #[test]
    fn test_identifies_the_right_object() {
        let jd = 2460010.3;
        let dets = vec![
            detection_of(&ceres_like(), jd, 1),
            detection_of(&pallas_like(), jd, 2),
        ];
        let found = identify(&dets, &catalogue(), &IdentifyConfig::default());
        assert_eq!(found.len(), 2);
        let by_id: HashMap<i64, &Match> = found.iter().map(|m| (m.detection_id, m)).collect();
        assert_eq!(by_id[&1].designation, "1");
        assert_eq!(by_id[&2].designation, "2");
        assert!(found.iter().all(|m| m.separation_arcsec < 1e-3));
    }

    #[test]
    fn test_leaves_an_unknown_position_unmatched() {
        // A degree off any catalogued object at this epoch.
        let jd = 2460010.3;
        let (ra, dec) = predict_radec(&ceres_like(), jd);
        let dets = vec![Detection {
            id: 9,
            jd,
            ra,
            dec: dec + 1.0,
            mag: None,
            band: None,
        }];
        assert!(identify(&dets, &catalogue(), &IdentifyConfig::default()).is_empty());
    }

    #[test]
    fn test_a_small_offset_is_reported_not_discarded() {
        let jd = 2460010.3;
        let mut d = detection_of(&ceres_like(), jd, 1);
        // Two arcseconds north, the scale of a survey-to-survey difference.
        d.dec += 2.0 / 3600.0;
        let found = identify(&[d], &catalogue(), &IdentifyConfig::default());
        assert_eq!(found.len(), 1);
        assert!(
            (found[0].separation_arcsec - 2.0).abs() < 0.05,
            "separation {}",
            found[0].separation_arcsec
        );
    }

    #[test]
    fn test_matches_across_separate_nights() {
        let dets = vec![
            detection_of(&ceres_like(), 2460010.3, 1),
            detection_of(&ceres_like(), 2460014.3, 2),
        ];
        let found = identify(&dets, &catalogue(), &IdentifyConfig::default());
        assert_eq!(found.len(), 2);
        assert!(found.iter().all(|m| m.designation == "1"));
    }

    #[test]
    fn test_median_of_separations() {
        assert_eq!(median(&mut [3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&mut []), None);
    }
}
