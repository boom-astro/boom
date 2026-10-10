use crate::utils::heliolinc::State;
use crate::utils::linking::{angular_separation_deg, night_of, Detection};
use crate::utils::orbit_fit::{fit_within, predict_radec, Observation, OrbitFit};
use crate::utils::sso_geometry::Site;
use std::collections::{BTreeMap, HashSet};

const BASE_RADIUS_ARCSEC: f64 = 60.0;
const RADIUS_PER_DAY_ARCSEC: f64 = 20.0;
const MAX_RADIUS_ARCSEC: f64 = 1800.0;
const MIN_PREDICTED_FRACTION: f64 = 2.0 / 3.0;
const TARGETS_FITTED_PER_TRACK: usize = 3;
const MAX_PER_NIGHT: usize = 4;
const MIN_ATTACHED: usize = 2;
const PREFILTER_RADII: f64 = 3.0;
pub const MIN_TARGET_NIGHTS: usize = 3;

#[derive(Debug, Clone)]
pub struct Target {
    pub id: String,
    pub state: State,
    pub epoch_jd: f64,
    pub members: Vec<Detection>,
}

impl Target {
    fn span(&self) -> (f64, f64) {
        self.members
            .iter()
            .fold((f64::MAX, f64::MIN), |(lo, hi), d| {
                (lo.min(d.jd), hi.max(d.jd))
            })
    }

    pub fn nights(&self) -> usize {
        self.members
            .iter()
            .map(|d| night_of(d.jd))
            .collect::<HashSet<_>>()
            .len()
    }

    fn radius_arcsec(&self, jd: f64) -> f64 {
        let (first, last) = self.span();
        let days = (first - jd).max(jd - last).max(0.0);
        (BASE_RADIUS_ARCSEC + RADIUS_PER_DAY_ARCSEC * days).min(MAX_RADIUS_ARCSEC)
    }

    fn separation_arcsec(&self, d: &Detection, site: &Site) -> Option<f64> {
        let (ra, dec) = predict_radec(&self.state, self.epoch_jd, d.jd, site)?;
        Some(angular_separation_deg(ra, dec, d.ra, d.dec) * 3600.0)
    }

    fn predicts(&self, d: &Detection, site: &Site) -> Option<f64> {
        self.separation_arcsec(d, site)
            .filter(|&sep| sep <= self.radius_arcsec(d.jd))
    }

    fn joint_fit(&self, extra: &[Detection], site: &Site, gate_arcsec: f64) -> Option<OrbitFit> {
        let observations: Vec<Observation> = self
            .members
            .iter()
            .chain(extra)
            .map(|d| Observation {
                jd: d.jd,
                ra: d.ra,
                dec: d.dec,
            })
            .collect();
        fit_within(&observations, &self.state, self.epoch_jd, site, gate_arcsec)
    }
}

pub fn match_track<'a>(
    targets: &'a [Target],
    track: &[Detection],
    site: &Site,
    gate_arcsec: f64,
) -> Option<(&'a Target, OrbitFit)> {
    let ids: HashSet<i64> = track.iter().map(|d| d.id).collect();
    let middle = track.get(track.len() / 2)?;
    let mut scored: Vec<(usize, &Target)> = targets
        .iter()
        .filter(|t| {
            t.separation_arcsec(middle, site)
                .is_some_and(|sep| sep <= PREFILTER_RADII * t.radius_arcsec(middle.jd))
        })
        .filter(|t| t.members.iter().all(|m| !ids.contains(&m.id)))
        .map(|t| {
            let predicted = track
                .iter()
                .filter(|d| t.predicts(d, site).is_some())
                .count();
            (predicted, t)
        })
        .filter(|&(n, _)| n > 0 && n as f64 >= MIN_PREDICTED_FRACTION * track.len() as f64)
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.id.cmp(&b.1.id)));
    scored
        .into_iter()
        .take(TARGETS_FITTED_PER_TRACK)
        .find_map(|(_, t)| {
            t.joint_fit(track, site, gate_arcsec)
                .filter(|f| f.rms_arcsec <= gate_arcsec)
                .map(|f| (t, f))
        })
}

pub struct Pool {
    nights: BTreeMap<i64, Vec<Detection>>,
    taken: HashSet<i64>,
}

impl Pool {
    pub fn new(detections: impl IntoIterator<Item = Detection>) -> Self {
        let mut nights: BTreeMap<i64, Vec<Detection>> = BTreeMap::new();
        for d in detections {
            nights.entry(night_of(d.jd)).or_default().push(d);
        }
        for dets in nights.values_mut() {
            dets.sort_by(|a, b| a.dec.total_cmp(&b.dec));
        }
        Pool {
            nights,
            taken: HashSet::new(),
        }
    }

    fn candidates(&self, target: &Target, site: &Site) -> Vec<Detection> {
        let covered: HashSet<i64> = target.members.iter().map(|d| night_of(d.jd)).collect();
        let mut out = Vec::new();
        for (night, dets) in &self.nights {
            if covered.contains(night) || dets.is_empty() {
                continue;
            }
            let mid = dets[dets.len() / 2].jd;
            let (Some((ra, dec)), Some((ra_later, dec_later))) = (
                predict_radec(&target.state, target.epoch_jd, mid, site),
                predict_radec(&target.state, target.epoch_jd, mid + 0.5, site),
            ) else {
                continue;
            };
            let reach = target.radius_arcsec(mid) / 3600.0
                + angular_separation_deg(ra, dec, ra_later, dec_later);
            let lo = dets.partition_point(|d| d.dec < dec - reach);
            let hi = dets.partition_point(|d| d.dec <= dec + reach);
            let mut near: Vec<(f64, Detection)> = dets[lo..hi]
                .iter()
                .filter(|d| !self.taken.contains(&d.id))
                .filter_map(|d| target.predicts(d, site).map(|sep| (sep, *d)))
                .collect();
            near.sort_by(|a, b| a.0.total_cmp(&b.0));
            out.extend(near.into_iter().take(MAX_PER_NIGHT).map(|(_, d)| d));
        }
        out
    }

    pub fn attach(
        &mut self,
        target: &Target,
        site: &Site,
        gate_arcsec: f64,
    ) -> Option<(Vec<Detection>, OrbitFit)> {
        if target.nights() < MIN_TARGET_NIGHTS {
            return None;
        }
        let (first, last) = target.span();
        let (first, last) = (night_of(first), night_of(last));
        let mut candidates = self.candidates(target, site);
        candidates.sort_by_key(|d| {
            let night = night_of(d.jd);
            (night - last).max(first - night)
        });
        let mut grown = target.clone();
        let mut accepted = Vec::new();
        let mut fit = None;
        for d in candidates {
            if grown.predicts(&d, site).is_none() {
                continue;
            }
            let Some(f) = grown
                .joint_fit(&[d], site, gate_arcsec)
                .filter(|f| f.rms_arcsec <= gate_arcsec)
            else {
                continue;
            };
            grown.members.push(d);
            grown.state = f.state;
            grown.epoch_jd = f.epoch_jd;
            accepted.push(d);
            fit = Some(f);
        }
        if accepted.len() < MIN_ATTACHED {
            return None;
        }
        let fit = fit?;
        self.taken.extend(accepted.iter().map(|d| d.id));
        Some((accepted, fit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::identify::predict_radec_from;
    use crate::utils::orbit_fit::fit_orbit;
    use crate::utils::sso_geometry::{heliocentric_position, OrbitalElements, ZTF};

    fn object(mean_anomaly: f64) -> OrbitalElements {
        OrbitalElements::elliptical(2460012.0, 2.44, 0.08, 7.5, 90.0, 141.0, mean_anomaly)
    }

    fn seen(el: &OrbitalElements, nights: &[f64], first_id: i64) -> Vec<Detection> {
        nights
            .iter()
            .enumerate()
            .flat_map(|(n, &start)| {
                (0..2).map(move |visit| {
                    let jd = start + 0.05 * visit as f64;
                    let (ra, dec) = predict_radec_from(el, jd, &ZTF);
                    Detection {
                        id: first_id + (n * 2 + visit) as i64,
                        jd,
                        ra,
                        dec,
                        mag: Some(19.0),
                        mag_err: Some(0.1),
                        band: Some('r'),
                    }
                })
            })
            .collect()
    }

    fn stored(el: &OrbitalElements, nights: &[f64]) -> Target {
        let members = seen(el, nights, 0);
        let epoch = members.iter().map(|d| d.jd).sum::<f64>() / members.len() as f64;
        let at = |jd: f64| heliocentric_position(el, jd);
        let (a, b) = (at(epoch - 0.05), at(epoch + 0.05));
        let truth = State {
            pos: at(epoch),
            vel: [
                (b[0] - a[0]) / 0.1,
                (b[1] - a[1]) / 0.1,
                (b[2] - a[2]) / 0.1,
            ],
        };
        let observations: Vec<Observation> = members
            .iter()
            .map(|d| Observation {
                jd: d.jd,
                ra: d.ra,
                dec: d.dec,
            })
            .collect();
        let fit = fit_orbit(&observations, &truth, epoch, 60, &ZTF).expect("fits");
        Target {
            id: "BT000001".to_string(),
            state: fit.state,
            epoch_jd: epoch,
            members,
        }
    }

    const STORED_NIGHTS: [f64; 4] = [2460010.70, 2460012.72, 2460015.68, 2460017.71];
    const LATER_NIGHTS: [f64; 3] = [2460040.70, 2460042.72, 2460045.68];

    #[test]
    fn test_a_track_found_after_a_gap_joins_the_stored_one() {
        let target = stored(&object(291.0), &STORED_NIGHTS);
        let same = seen(&object(291.0), &LATER_NIGHTS, 100);
        let neighbor = seen(&object(291.05), &LATER_NIGHTS, 200);
        let targets = [target];
        assert!(match_track(&targets, &same, &ZTF, 2.0).is_some());
        assert!(match_track(&targets, &neighbor, &ZTF, 2.0).is_none());
    }

    #[test]
    fn test_leftover_detections_join_only_their_own_track() {
        let target = stored(&object(291.0), &STORED_NIGHTS);
        let same = seen(&object(291.0), &LATER_NIGHTS[..1], 100);
        let neighbor = seen(&object(291.05), &LATER_NIGHTS[1..], 200);
        let mut pool = Pool::new(same.iter().chain(&neighbor).copied());
        let (attached, fit) = pool.attach(&target, &ZTF, 2.0).expect("attaches");
        let ids: HashSet<i64> = attached.iter().map(|d| d.id).collect();
        assert_eq!(ids, same.iter().map(|d| d.id).collect());
        assert!(fit.rms_arcsec <= 2.0);
    }
}
