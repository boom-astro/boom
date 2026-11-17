use crate::utils::heliolinc::{is_bound, Conics, State};
use crate::utils::linking::{angular_separation_deg, night_of, Detection};
use crate::utils::orbit_fit::{
    fit_within, predict_radec, predict_radec_seen_from, Observation, OrbitFit,
};
use crate::utils::sso_geometry::{observer_position, Site};
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
            .map(Detection::night)
            .collect::<HashSet<_>>()
            .len()
    }

    fn radius_arcsec(&self, jd: f64) -> f64 {
        let (first, last) = self.span();
        let days = (first - jd).max(jd - last).max(0.0);
        (BASE_RADIUS_ARCSEC + RADIUS_PER_DAY_ARCSEC * days).min(MAX_RADIUS_ARCSEC)
    }

    fn separation_arcsec(&self, d: &Detection, site: &Site) -> Option<f64> {
        self.separation_seen_from(d, &observer_of(d, site))
    }

    fn separation_seen_from(&self, d: &Detection, observer: &[f64; 3]) -> Option<f64> {
        let (ra, dec) = predict_radec_seen_from(&self.state, self.epoch_jd, d.jd, observer)?;
        Some(angular_separation_deg(ra, dec, d.ra, d.dec) * 3600.0)
    }

    fn predicts(&self, d: &Detection, site: &Site) -> Option<f64> {
        self.separation_arcsec(d, site)
            .filter(|&sep| sep <= self.radius_arcsec(d.jd))
    }

    fn joint_fit(
        &self,
        extra: &[Detection],
        site: &Site,
        gate_arcsec: f64,
        conics: Conics,
    ) -> Option<OrbitFit> {
        let observations: Vec<Observation> = self
            .members
            .iter()
            .chain(extra)
            .map(|d| Observation {
                jd: d.jd,
                ra: d.ra,
                dec: d.dec,
                site: d.site.unwrap_or(*site),
            })
            .collect();
        let conics = if is_bound(&self.state) {
            Conics::Bound
        } else {
            conics
        };
        fit_within(
            &observations,
            &self.state,
            self.epoch_jd,
            gate_arcsec,
            conics,
        )
    }
}

fn observer_of(d: &Detection, site: &Site) -> [f64; 3] {
    observer_position(d.jd, d.site.as_ref().unwrap_or(site))
}

pub fn match_track<'a>(
    targets: &'a [Target],
    track: &[Detection],
    site: &Site,
    gate_arcsec: f64,
    conics: Conics,
) -> Option<(&'a Target, OrbitFit)> {
    let ids: HashSet<i64> = track.iter().map(|d| d.id).collect();
    let middle = track.get(track.len() / 2)?;
    let observer = observer_of(middle, site);
    let mut scored: Vec<(usize, &Target)> = targets
        .iter()
        .filter(|t| {
            t.separation_seen_from(middle, &observer)
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
            t.joint_fit(track, site, gate_arcsec, conics)
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
            nights.entry(d.night()).or_default().push(d);
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
        let covered: HashSet<i64> = target.members.iter().map(Detection::night).collect();
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
        conics: Conics,
    ) -> Option<(Vec<Detection>, OrbitFit)> {
        if target.nights() < MIN_TARGET_NIGHTS {
            return None;
        }
        let (first, last) = target.span();
        let (first, last) = (night_of(first), night_of(last));
        let mut candidates = self.candidates(target, site);
        candidates.sort_by_key(|d| {
            let night = d.night();
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
                .joint_fit(&[d], site, gate_arcsec, conics)
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

/// Which of `observations` one orbit reproduces, from `seed`, which was fitted
/// on only some of them and left `seed_rms_arcsec` there.
///
/// All of them when one orbit fits them all within `gate_arcsec`. Otherwise
/// the ones `seed` reproduces, grown nearest in time first by every other one
/// a joint fit still passes with, as attached detections are. `None` when
/// fewer than three are reproduced, too few to judge the rest by.
pub fn reproduced(
    observations: &[Observation],
    seed: &State,
    epoch_jd: f64,
    seed_rms_arcsec: f64,
    gate_arcsec: f64,
) -> Option<(Vec<bool>, OrbitFit)> {
    let conics = if is_bound(seed) {
        Conics::Bound
    } else {
        Conics::Unbound {
            vinf_au_per_day: f64::INFINITY,
            max_rms_arcsec: f64::INFINITY,
        }
    };
    let fits = |obs: &[Observation], from: &State| {
        fit_within(obs, from, epoch_jd, gate_arcsec, conics).filter(|f| f.rms_arcsec <= gate_arcsec)
    };
    if let Some(fit) = fits(observations, seed) {
        return Some((vec![true; observations.len()], fit));
    }
    let off = |state: &State, obs: &Observation| {
        predict_radec(state, epoch_jd, obs.jd, &obs.site).map_or(f64::INFINITY, |(ra, dec)| {
            angular_separation_deg(ra, dec, obs.ra, obs.dec) * 3600.0
        })
    };
    let tolerance = gate_arcsec.max(5.0 * seed_rms_arcsec);
    let mut kept: Vec<bool> = observations
        .iter()
        .map(|obs| off(seed, obs) <= tolerance)
        .collect();
    let subset = |kept: &[bool]| -> Vec<Observation> {
        observations
            .iter()
            .zip(kept)
            .filter(|(_, k)| **k)
            .map(|(obs, _)| *obs)
            .collect()
    };
    let core = subset(&kept);
    if core.len() < 3 {
        return None;
    }
    let mut fit = fits(&core, seed)?;
    let (mut first, mut last) = core.iter().fold((f64::MAX, f64::MIN), |(lo, hi), obs| {
        (lo.min(obs.jd), hi.max(obs.jd))
    });
    let beyond = |jd: f64, first: f64, last: f64| (first - jd).max(jd - last).max(0.0);
    let mut order: Vec<usize> = (0..observations.len()).filter(|&i| !kept[i]).collect();
    order.sort_by(|&a, &b| {
        beyond(observations[a].jd, first, last).total_cmp(&beyond(observations[b].jd, first, last))
    });
    for i in order {
        let obs = &observations[i];
        let radius = (BASE_RADIUS_ARCSEC + RADIUS_PER_DAY_ARCSEC * beyond(obs.jd, first, last))
            .min(MAX_RADIUS_ARCSEC);
        if off(&fit.state, obs) > radius {
            continue;
        }
        kept[i] = true;
        match fits(&subset(&kept), &fit.state) {
            Some(grown) => {
                fit = grown;
                first = first.min(obs.jd);
                last = last.max(obs.jd);
            }
            None => kept[i] = false,
        }
    }
    Some((kept, fit))
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
                        site: None,
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
                site: ZTF,
            })
            .collect();
        let fit = fit_orbit(&observations, &truth, epoch, 60).expect("fits");
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
        assert!(match_track(&targets, &same, &ZTF, 2.0, Conics::Bound).is_some());
        assert!(match_track(&targets, &neighbor, &ZTF, 2.0, Conics::Bound).is_none());
    }

    #[test]
    fn test_leftover_detections_join_only_their_own_track() {
        let target = stored(&object(291.0), &STORED_NIGHTS);
        let same = seen(&object(291.0), &LATER_NIGHTS[..1], 100);
        let neighbor = seen(&object(291.05), &LATER_NIGHTS[1..], 200);
        let mut pool = Pool::new(same.iter().chain(&neighbor).copied());
        let (attached, fit) = pool
            .attach(&target, &ZTF, 2.0, Conics::Bound)
            .expect("attaches");
        let ids: HashSet<i64> = attached.iter().map(|d| d.id).collect();
        assert_eq!(ids, same.iter().map(|d| d.id).collect());
        assert!(fit.rms_arcsec <= 2.0);
    }

    /// An orbit fitted on three nights of a track keeps every other detection
    /// of its object, across surveys and on a hyperbola too, and lets another
    /// object's go.
    #[test]
    fn test_an_orbit_keeps_its_own_object_and_frees_another() {
        use crate::utils::orbit_fit::fit_orbit_with;
        use crate::utils::sso_geometry::{DECAM, TT_MINUS_UTC_DAYS};
        let borisov = OrbitalElements {
            epoch_jd: 2458826.0536,
            a: 2.006_548 / (1.0 - 3.356_633),
            e: 3.356_633,
            incl: 44.0526,
            node: 308.1489,
            peri: 209.1243,
            mean_anomaly: 0.0,
            q: 2.006_548,
            tp: 2458826.0536,
        };
        for (el, start, odd_site) in [
            (object(291.0), 2460010.70, ZTF),
            (object(291.0), 2460010.70, DECAM),
            (borisov, 2458746.70, ZTF),
        ] {
            let at = |el: &OrbitalElements, jd: f64, site: Site| {
                let (ra, dec) = predict_radec_from(el, jd, &site);
                Observation { jd, ra, dec, site }
            };
            let mut observations: Vec<Observation> = (0..6)
                .flat_map(|night| {
                    let site = if night % 2 == 0 { ZTF } else { odd_site };
                    [0.0, 0.05].map(|visit| (start + 3.0 * f64::from(night) + visit, site))
                })
                .map(|(jd, site)| at(&el, jd, site))
                .collect();
            observations.extend([6.02, 12.02].map(|dt| at(&object(291.3), start + dt, ZTF)));
            let epoch = start + 7.5;
            let helio = |jd: f64| heliocentric_position(&el, jd + TT_MINUS_UTC_DAYS);
            let (before, after) = (helio(epoch - 0.05), helio(epoch + 0.05));
            let truth = State {
                pos: helio(epoch),
                vel: std::array::from_fn(|k| (after[k] - before[k]) / 0.1),
            };
            let conics = if is_bound(&truth) {
                Conics::Bound
            } else {
                Conics::Unbound {
                    vinf_au_per_day: f64::INFINITY,
                    max_rms_arcsec: f64::INFINITY,
                }
            };
            let seed = fit_orbit_with(&observations[4..10], &truth, epoch, 60, None, conics)
                .expect("three nights fit");
            let (kept, _) = reproduced(&observations, &seed.state, epoch, seed.rms_arcsec, 2.0)
                .expect("enough to judge by");
            assert!(
                kept[..12].iter().all(|&k| k),
                "e = {} lost its own detections: {kept:?}",
                el.e
            );
            assert!(!kept[12] && !kept[13], "kept another object's: {kept:?}");
        }
    }
}
