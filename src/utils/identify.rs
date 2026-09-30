//! Match detections to known objects by ephemeris.
//!
//! ZTF labels a detection with an MPC designation and Rubin labels it with its
//! own `ssObjectId`, so the two surveys cannot be compared directly. Predicting
//! where every catalogued object sits at a detection's epoch supplies the
//! missing bridge: it names Rubin's objects in MPC terms, and the residual it
//! leaves measures each survey's astrometry against the same reference.

use crate::utils::heliolinc::radec_from_ecliptic;
use crate::utils::linking::{angular_separation_deg, night_of, Detection};
use crate::utils::orbit_fit::C_AU_PER_DAY;
pub use crate::utils::outburst::median;
use crate::utils::sso_geometry::{
    earth_position, heliocentric_position, observer_position, OrbitalElements, Site, ZTF,
};
use rayon::prelude::*;
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
    /// Where the observations were made from, for the parallax correction.
    pub site: Site,
    /// Radius a refined prediction must fall inside to count, arcseconds.
    ///
    /// Has to cover the error two-body propagation accumulates between the
    /// catalogue's epoch and the observation, which is tens of arcseconds at the
    /// few months MPCORB's single standard epoch implies. Measured against
    /// `ssnamenr` over a night at an 86-day gap: recall is 1.7% at 10", 68.6% at
    /// 20", 98.9% at 30" and saturates at 99.4% by 60", while the false-match
    /// rate is flat at 0.55% from 30" all the way out to 600" -- asteroids are
    /// sparse enough that a wide radius costs almost nothing. The default
    /// doubles the saturation point so a staler catalogue still resolves.
    pub match_radius_arcsec: f64,
}

impl Default for IdentifyConfig {
    fn default() -> Self {
        Self {
            coarse_radius_deg: 1.5,
            site: ZTF,
            match_radius_arcsec: 120.0,
        }
    }
}

/// Apparent position of `elements` at `jd` as seen from `site`, degrees.
///
/// Topocentric rather than geocentric: an Earth radius of offset is several
/// arcseconds against a main-belt asteroid, which is a large share of the
/// residual a catalogue match has to tolerate. Light-time corrected for the
/// same reason, that being the larger of the two at tens of arcseconds.
pub fn predict_radec_from(elements: &OrbitalElements, jd: f64, site: &Site) -> (f64, f64) {
    let observer = observer_position(jd, site);
    radec_from_ecliptic(&light_time_corrected(elements, jd, &observer))
}

/// Vector from `observer` to where `elements` was when its light left, au.
fn light_time_corrected(elements: &OrbitalElements, jd: f64, observer: &[f64; 3]) -> [f64; 3] {
    let mut tau = 0.0;
    let mut los = [0.0; 3];
    for _ in 0..2 {
        let helio = heliocentric_position(elements, jd - tau);
        los = [
            helio[0] - observer[0],
            helio[1] - observer[1],
            helio[2] - observer[2],
        ];
        tau = (los[0] * los[0] + los[1] * los[1] + los[2] * los[2]).sqrt() / C_AU_PER_DAY;
    }
    los
}

/// Apparent position from the Earth's centre, degrees.
pub fn predict_radec(elements: &OrbitalElements, jd: f64) -> (f64, f64) {
    radec_from_ecliptic(&light_time_corrected(elements, jd, &earth_position(jd)))
}

/// Catalogued objects placed on the sky at one epoch, bucketed so a position's
/// neighbours are found without scanning the catalogue.
///
/// Cells are `size` degrees in dec. Each dec band is cut into RA bins at least
/// `size` wide on the sky at the band's poleward edge, so the 3x3 cells around
/// a position hold everything within `size` of it.
struct SkyGrid {
    size: f64,
    cells: HashMap<(i64, i64), Vec<usize>>,
}

impl SkyGrid {
    fn new(positions: &[(f64, f64)], size: f64) -> Self {
        let mut cells: HashMap<(i64, i64), Vec<usize>> = HashMap::new();
        for (i, &(ra, dec)) in positions.iter().enumerate() {
            let band = Self::band(dec, size);
            cells
                .entry((band, Self::bin(ra, band, size)))
                .or_default()
                .push(i);
        }
        SkyGrid { size, cells }
    }

    fn band(dec: f64, size: f64) -> i64 {
        (dec / size).floor() as i64
    }

    /// RA bins in `band`: as many as fit at the band's poleward edge.
    fn bins(band: i64, size: f64) -> i64 {
        let lower = band as f64 * size;
        let edge = lower.abs().max((lower + size).abs()).min(89.999);
        ((360.0 * edge.to_radians().cos() / size).floor() as i64).max(1)
    }

    fn bin(ra: f64, band: i64, size: f64) -> i64 {
        let bins = Self::bins(band, size);
        ((ra.rem_euclid(360.0) / 360.0 * bins as f64).floor() as i64).min(bins - 1)
    }

    /// Indices of every position within `size` of `(ra, dec)`, and some more.
    fn near(&self, ra: f64, dec: f64) -> Vec<usize> {
        let band = Self::band(dec, self.size);
        let mut keys: Vec<(i64, i64)> = Vec::with_capacity(9);
        for b in band - 1..=band + 1 {
            let bins = Self::bins(b, self.size);
            let x = Self::bin(ra, b, self.size);
            for dx in -1..=1 {
                let key = (b, (x + dx).rem_euclid(bins));
                // A band with fewer than three bins wraps onto itself.
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
        }
        keys.iter()
            .filter_map(|k| self.cells.get(k))
            .flatten()
            .copied()
            .collect()
    }
}

/// Attribute each detection to the catalogued object it sits closest to.
///
/// Two stages, because propagating the whole catalogue per detection is
/// wasteful: every object is placed once per night to shortlist candidates,
/// then only those are recomputed at the detection's own epoch. The shortlist
/// comes from a sky grid rather than a dec band spanning every RA, and the
/// detections of a night are matched in parallel.
pub fn identify(
    detections: &[Detection],
    orbits: &[OrbitEntry],
    cfg: &IdentifyConfig,
) -> Vec<Match> {
    let mut by_night: HashMap<i64, Vec<&Detection>> = HashMap::new();
    for d in detections {
        by_night.entry(night_of(d.jd)).or_default().push(d);
    }

    // Sorted, since a HashMap's order would carry into `matches`.
    let mut nights: Vec<(i64, Vec<&Detection>)> = by_night.into_iter().collect();
    nights.sort_by_key(|(night, _)| *night);

    let mut matches = Vec::new();
    for (night, dets) in nights {
        let epoch = night as f64 + 1.0;
        // Every object placed once for the night.
        let placed: Vec<(f64, f64)> = orbits
            .par_iter()
            .map(|o| predict_radec(&o.elements, epoch))
            .collect();
        let grid = SkyGrid::new(&placed, cfg.coarse_radius_deg);

        let found: Vec<Match> = dets
            .par_iter()
            .filter_map(|d| {
                let mut best: Option<(f64, usize)> = None;
                for idx in grid.near(d.ra, d.dec) {
                    let (night_ra, night_dec) = placed[idx];
                    // The grid overreaches, so gate on the separation before
                    // paying for a second propagation.
                    if angular_separation_deg(d.ra, d.dec, night_ra, night_dec)
                        > cfg.coarse_radius_deg
                    {
                        continue;
                    }
                    let (pra, pdec) = predict_radec_from(&orbits[idx].elements, d.jd, &cfg.site);
                    let sep = angular_separation_deg(d.ra, d.dec, pra, pdec) * 3600.0;
                    // Ties go to the lower index, so the result does not
                    // depend on the order the grid returns candidates in.
                    if sep <= cfg.match_radius_arcsec
                        && best.is_none_or(|(b, i)| sep < b || (sep == b && idx < i))
                    {
                        best = Some((sep, idx));
                    }
                }
                best.map(|(sep, idx)| Match {
                    detection_id: d.id,
                    designation: orbits[idx].designation.clone(),
                    separation_arcsec: sep,
                    jd: d.jd,
                })
            })
            .collect();
        matches.extend(found);
    }
    matches
}

/// How much agreement makes a linked track a catalogued object.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KnownRule {
    /// Share of the track's detections that must match one designation.
    pub min_fraction: f64,
    /// Distinct nights those matching detections must span.
    pub min_nights: usize,
    /// How far any agreeing detection's separation may stray from their
    /// median, arcseconds. A catalogued orbit's error grows over months, so the
    /// real object sits at a nearly constant offset over a few nights; an
    /// unrelated neighbour moving almost in parallel drifts by tens of
    /// arcseconds a night.
    pub max_scatter_arcsec: f64,
}

impl Default for KnownRule {
    /// Tuned on synthetic tracks against a catalogue of MPCORB's size, where
    /// 43% of a new object's detections land within 120" of some catalogued
    /// orbit: two thirds agreeing over two nights at a steady separation left
    /// 1 new object in 2,000 designated, and no known one missed. Half
    /// agreeing with no steadiness test designated 43.
    fn default() -> Self {
        Self {
            min_fraction: 2.0 / 3.0,
            min_nights: 2,
            max_scatter_arcsec: 10.0,
        }
    }
}

/// The designation a track's detections agree on, when enough of them do.
///
/// One detection landing near a catalogued orbit is weak evidence: at the
/// default match radius a random position near the ecliptic finds some object
/// a quarter to nearly half of the time. The same object on most of a track's
/// detections, across nights and at a steady offset, is not chance. `matches`
/// holds whatever [`identify`] returned for the track's `n_detections`
/// detections.
pub fn track_designation(
    matches: &[&Match],
    n_detections: usize,
    rule: &KnownRule,
) -> Option<String> {
    let mut votes: HashMap<&str, Vec<&Match>> = HashMap::new();
    for m in matches {
        votes.entry(m.designation.as_str()).or_default().push(m);
    }
    let (designation, agreeing) = votes
        .into_iter()
        // Most detections first; the designation breaks a tie reproducibly.
        .max_by(|a, b| a.1.len().cmp(&b.1.len()).then(b.0.cmp(a.0)))?;
    let nights: std::collections::BTreeSet<i64> = agreeing.iter().map(|m| night_of(m.jd)).collect();
    let separations: Vec<f64> = agreeing.iter().map(|m| m.separation_arcsec).collect();
    let centre = median(&separations)?;
    let steady = separations
        .iter()
        .all(|s| (s - centre).abs() <= rule.max_scatter_arcsec);
    (agreeing.len() as f64 >= rule.min_fraction * n_detections as f64
        && nights.len() >= rule.min_nights
        && steady)
        .then(|| designation.to_string())
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
        // Placed as the site would see it, since that is what `identify`
        // predicts; a geocentric fixture is offset by the parallax.
        let (ra, dec) = predict_radec_from(elements, jd, &ZTF);
        Detection {
            id,
            jd,
            ra,
            dec,
            mag: None,
            mag_err: None,
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
        let (ra, dec) = predict_radec_from(&ceres_like(), jd, &ZTF);
        let dets = vec![Detection {
            id: 9,
            jd,
            ra,
            dec: dec + 1.0,
            mag: None,
            mag_err: None,
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

    /// A spread of orbits, deterministic, reaching high inclinations so some
    /// objects sit near the poles and some near RA 0.
    fn scattered_catalogue(n: usize) -> Vec<OrbitEntry> {
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 / (1u64 << 53) as f64
        };
        (0..n)
            .map(|i| OrbitEntry {
                designation: format!("S{i}"),
                elements: OrbitalElements::elliptical(
                    2460000.5,
                    1.8 + 1.8 * next(),
                    0.3 * next(),
                    90.0 * next(),
                    360.0 * next(),
                    360.0 * next(),
                    360.0 * next(),
                ),
            })
            .collect()
    }

    /// The answer without a shortlist: every object refined at every detection.
    fn brute_force(
        detections: &[Detection],
        orbits: &[OrbitEntry],
        cfg: &IdentifyConfig,
    ) -> Vec<(i64, String)> {
        detections
            .iter()
            .filter_map(|d| {
                let epoch = night_of(d.jd) as f64 + 1.0;
                let mut best: Option<(f64, usize)> = None;
                for (idx, o) in orbits.iter().enumerate() {
                    let (ra, dec) = predict_radec(&o.elements, epoch);
                    if angular_separation_deg(d.ra, d.dec, ra, dec) > cfg.coarse_radius_deg {
                        continue;
                    }
                    let (pra, pdec) = predict_radec_from(&o.elements, d.jd, &cfg.site);
                    let sep = angular_separation_deg(d.ra, d.dec, pra, pdec) * 3600.0;
                    if sep <= cfg.match_radius_arcsec
                        && best.is_none_or(|(b, i)| sep < b || (sep == b && idx < i))
                    {
                        best = Some((sep, idx));
                    }
                }
                best.map(|(_, idx)| (d.id, orbits[idx].designation.clone()))
            })
            .collect()
    }

    /// The sky grid shortlists exactly what a full scan finds, including across
    /// RA 0 and near the poles, where its bins are widest.
    #[test]
    fn test_the_sky_grid_matches_a_full_scan() {
        let orbits = scattered_catalogue(1500);
        let cfg = IdentifyConfig::default();
        let jd = 2460010.3;
        let mut detections: Vec<Detection> = orbits
            .iter()
            .step_by(5)
            .enumerate()
            .map(|(k, o)| {
                // Offset up to a minute of arc, inside the match radius.
                let mut d = detection_of(&o.elements, jd, k as i64);
                d.dec += ((k % 7) as f64 - 3.0) * 20.0 / 3600.0;
                d
            })
            .collect();
        // Positions tied to no object, at the RA seam and the poles.
        for (k, (ra, dec)) in [(359.99, 10.0), (0.01, -5.0), (120.0, 88.5), (300.0, -89.0)]
            .into_iter()
            .enumerate()
        {
            detections.push(Detection {
                id: 10_000 + k as i64,
                jd,
                ra,
                dec,
                mag: None,
                mag_err: None,
                band: None,
            });
        }
        assert!(
            detections.iter().any(|d| d.ra < 1.5 || d.ra > 358.5),
            "the fixture should reach the RA seam"
        );
        assert!(
            detections.iter().any(|d| d.dec.abs() > 60.0),
            "the fixture should reach high declination"
        );

        let mut grid: Vec<(i64, String)> = identify(&detections, &orbits, &cfg)
            .into_iter()
            .map(|m| (m.detection_id, m.designation))
            .collect();
        let mut full = brute_force(&detections, &orbits, &cfg);
        grid.sort();
        full.sort();
        assert!(!full.is_empty());
        assert_eq!(grid, full);
    }

    fn a_match(designation: &str, jd: f64) -> Match {
        Match {
            detection_id: 0,
            designation: designation.to_string(),
            separation_arcsec: 30.0,
            jd,
        }
    }

    #[test]
    fn test_a_track_most_of_whose_detections_match_one_object_is_that_object() {
        let matches = [
            a_match("1", 2460010.8),
            a_match("1", 2460010.85),
            a_match("1", 2460012.8),
        ];
        let refs: Vec<&Match> = matches.iter().collect();
        assert_eq!(
            track_designation(&refs, 4, &KnownRule::default()).as_deref(),
            Some("1")
        );
    }

    /// Scattered chance matches, each to a different object, are not a
    /// designation for the track.
    #[test]
    fn test_chance_matches_to_different_objects_are_not_a_designation() {
        let matches = [
            a_match("1", 2460010.8),
            a_match("2", 2460012.8),
            a_match("3", 2460014.8),
        ];
        let refs: Vec<&Match> = matches.iter().collect();
        assert_eq!(track_designation(&refs, 6, &KnownRule::default()), None);
    }

    /// Agreement confined to one night could be a single chance match repeated.
    #[test]
    fn test_agreement_on_a_single_night_is_not_enough() {
        let matches = [
            a_match("1", 2460010.8),
            a_match("1", 2460010.83),
            a_match("1", 2460010.85),
        ];
        let refs: Vec<&Match> = matches.iter().collect();
        assert_eq!(track_designation(&refs, 4, &KnownRule::default()), None);
    }

    /// A neighbour moving almost in parallel can stay inside the radius for two
    /// nights, but its offset drifts; a catalogued object's offset holds still.
    #[test]
    fn test_a_drifting_offset_is_a_neighbour_not_the_object() {
        let at = |jd: f64, sep: f64| Match {
            separation_arcsec: sep,
            ..a_match("1", jd)
        };
        let drifting = [
            at(2460010.8, 20.0),
            at(2460010.85, 21.0),
            at(2460012.8, 70.0),
        ];
        let refs: Vec<&Match> = drifting.iter().collect();
        assert_eq!(track_designation(&refs, 4, &KnownRule::default()), None);

        let steady = [
            at(2460010.8, 20.0),
            at(2460010.85, 21.0),
            at(2460012.8, 24.0),
        ];
        let refs: Vec<&Match> = steady.iter().collect();
        assert_eq!(
            track_designation(&refs, 4, &KnownRule::default()).as_deref(),
            Some("1")
        );
    }

    #[test]
    fn test_a_minority_of_matching_detections_is_not_enough() {
        let matches = [a_match("1", 2460010.8), a_match("1", 2460012.8)];
        let refs: Vec<&Match> = matches.iter().collect();
        assert_eq!(track_designation(&refs, 8, &KnownRule::default()), None);
    }

    #[test]
    fn test_median_of_separations() {
        assert_eq!(median(&mut [3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&mut []), None);
    }
}
