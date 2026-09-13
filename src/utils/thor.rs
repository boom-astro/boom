//! Tracklet-less recovery of moving objects, in the manner of THOR
//! (Moeyens et al. 2021).
//!
//! Linking that starts from tracklets can only see objects detected several
//! times in one night, which is a minority of what a survey actually catches:
//! most objects give one detection per night. Assuming a full test orbit
//! instead of a per-tracklet rate removes that requirement. Every detection is
//! projected into the frame co-moving with the test orbit, where an object on a
//! nearby orbit sits almost still while everything else sweeps past, so single
//! detections spread over several nights cluster on their own.
//!
//! Orbital mechanics stays with the caller: this module is handed where the
//! test orbit appears on the sky at each epoch, and does only the projection
//! and clustering, which is the part worth putting on a GPU. That split is why
//! it is self-contained enough to move into its own crate alongside
//! `villar-pso` when the kernels are written.

use crate::utils::linking::Detection;
use serde::{Deserialize, Serialize};

/// Where the test orbit appears, at each epoch the detections were taken.
///
/// Supplied by the caller so this crate needs no ephemeris of its own. Epochs
/// must be sorted and match the detections to within the cadence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestOrbitTrack {
    pub jd: Vec<f64>,
    /// Degrees.
    pub ra: Vec<f64>,
    /// Degrees.
    pub dec: Vec<f64>,
}

impl TestOrbitTrack {
    /// Interpolated sky position at `jd`, or `None` outside the tabulated span.
    pub fn at(&self, jd: f64) -> Option<(f64, f64)> {
        if self.jd.len() < 2 || jd < *self.jd.first()? || jd > *self.jd.last()? {
            return None;
        }
        let i = match self
            .jd
            .binary_search_by(|probe| probe.partial_cmp(&jd).unwrap_or(std::cmp::Ordering::Equal))
        {
            Ok(exact) => return Some((self.ra[exact], self.dec[exact])),
            Err(0) => 0,
            Err(n) if n >= self.jd.len() => self.jd.len() - 2,
            Err(n) => n - 1,
        };
        let (t0, t1) = (self.jd[i], self.jd[i + 1]);
        let f = if (t1 - t0).abs() < f64::EPSILON {
            0.0
        } else {
            (jd - t0) / (t1 - t0)
        };
        // Interpolate through the wrap rather than across it.
        let dra = ((self.ra[i + 1] - self.ra[i] + 540.0).rem_euclid(360.0)) - 180.0;
        Some((
            (self.ra[i] + f * dra).rem_euclid(360.0),
            self.dec[i] + f * (self.dec[i + 1] - self.dec[i]),
        ))
    }
}

/// How wide to search the residual motion, and what counts as a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Largest residual angular rate considered, degrees/day. An object whose
    /// orbit differs from the test orbit drifts in the co-moving frame; this
    /// bounds how far the test orbit may be from the truth.
    pub max_residual_rate_deg_per_day: f64,
    /// Rate grid steps per axis, across the full range.
    pub rate_steps: usize,
    /// Cluster cell size, arcseconds.
    pub cluster_radius_arcsec: f64,
    /// Detections a cluster needs.
    pub min_detections: usize,
    /// Distinct nights a cluster needs, which is what rejects a single night's
    /// blend of unrelated sources.
    pub min_nights: usize,
    /// Detections further than this from the test orbit are not considered.
    pub max_offset_deg: f64,
    /// Largest scatter about the refitted drift a cluster may have, arcseconds.
    /// A chance grouping does not hold a straight line, so this is the main
    /// defence against the rate grid manufacturing clusters.
    pub max_rms_arcsec: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_residual_rate_deg_per_day: 0.05,
            rate_steps: 21,
            cluster_radius_arcsec: 30.0,
            min_detections: 3,
            min_nights: 3,
            max_offset_deg: 2.0,
            max_rms_arcsec: 2.0,
        }
    }
}

/// Detections that hold still together in the co-moving frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    pub ids: Vec<i64>,
    /// Residual rate the cluster was found at, degrees/day.
    pub rate_x_deg_per_day: f64,
    pub rate_y_deg_per_day: f64,
    /// Scatter about the fitted linear drift, arcseconds.
    pub rms_arcsec: f64,
    pub nights: usize,
}

/// A detection projected into the co-moving frame, degrees.
#[derive(Debug, Clone, Copy)]
pub struct Projected {
    pub id: i64,
    pub jd: f64,
    pub x: f64,
    pub y: f64,
}

/// Gnomonic projection of `(ra, dec)` about `(ra0, dec0)`, degrees.
fn tangent_plane(ra: f64, dec: f64, ra0: f64, dec0: f64) -> Option<(f64, f64)> {
    let (r, d) = (ra.to_radians(), dec.to_radians());
    let (r0, d0) = (ra0.to_radians(), dec0.to_radians());
    let cos_c = d0.sin() * d.sin() + d0.cos() * d.cos() * (r - r0).cos();
    if cos_c <= 1e-12 {
        return None;
    }
    Some((
        (d.cos() * (r - r0).sin() / cos_c).to_degrees(),
        ((d0.cos() * d.sin() - d0.sin() * d.cos() * (r - r0).cos()) / cos_c).to_degrees(),
    ))
}

/// Project every detection into the frame co-moving with the test orbit.
///
/// Detections outside `max_offset_deg` of the test orbit are dropped, since a
/// test orbit only speaks for its own neighbourhood.
pub fn project(detections: &[Detection], track: &TestOrbitTrack, cfg: &Config) -> Vec<Projected> {
    detections
        .iter()
        .filter_map(|d| {
            let (ra0, dec0) = track.at(d.jd)?;
            let (x, y) = tangent_plane(d.ra, d.dec, ra0, dec0)?;
            (x.hypot(y) <= cfg.max_offset_deg).then_some(Projected {
                id: d.id,
                jd: d.jd,
                x,
                y,
            })
        })
        .collect()
}

/// Cluster projected detections that share a residual linear drift.
///
/// One pass per rate on the grid: subtracting a trial drift makes a real
/// object's detections coincide, so a plain spatial cluster then finds it.
pub fn cluster(projected: &[Projected], cfg: &Config) -> Vec<Cluster> {
    if projected.len() < cfg.min_detections {
        return Vec::new();
    }
    let t0 = projected.iter().map(|p| p.jd).sum::<f64>() / projected.len() as f64;
    let cell = cfg.cluster_radius_arcsec / 3600.0;
    let steps = cfg.rate_steps.max(1);
    let span = cfg.max_residual_rate_deg_per_day;

    let mut found: Vec<Cluster> = Vec::new();
    for ix in 0..steps {
        let vx = rate_at(ix, steps, span);
        for iy in 0..steps {
            let vy = rate_at(iy, steps, span);
            // Shift every detection by the trial drift, then group what lands
            // together. Cells only shortlist neighbours: membership is decided
            // on distance, so a cluster straddling a boundary stays whole.
            let shifted: Vec<(f64, f64)> = projected
                .iter()
                .map(|p| {
                    let dt = p.jd - t0;
                    (p.x - vx * dt, p.y - vy * dt)
                })
                .collect();
            let mut grid: std::collections::HashMap<(i64, i64), Vec<usize>> =
                std::collections::HashMap::new();
            for (k, (x, y)) in shifted.iter().enumerate() {
                grid.entry(((x / cell).floor() as i64, (y / cell).floor() as i64))
                    .or_default()
                    .push(k);
            }

            let mut groups: Vec<Vec<usize>> = Vec::new();
            for (k, (x, y)) in shifted.iter().enumerate() {
                let base = ((x / cell).floor() as i64, (y / cell).floor() as i64);
                let mut members = vec![k];
                for dx in -1..=1 {
                    for dy in -1..=1 {
                        let Some(bucket) = grid.get(&(base.0 + dx, base.1 + dy)) else {
                            continue;
                        };
                        for &m in bucket {
                            if m == k {
                                continue;
                            }
                            let (mx, my) = shifted[m];
                            if (mx - x).hypot(my - y) <= cell {
                                members.push(m);
                            }
                        }
                    }
                }
                members.sort_unstable();
                members.dedup();
                groups.push(members);
            }

            for members in groups {
                if members.len() < cfg.min_detections {
                    continue;
                }
                let nights = members
                    .iter()
                    .map(|&k| (projected[k].jd - 0.5).floor() as i64)
                    .collect::<std::collections::HashSet<_>>()
                    .len();
                if nights < cfg.min_nights {
                    continue;
                }
                let candidate = summarise(&members, projected, t0, vx, vy);
                if candidate.rms_arcsec <= cfg.max_rms_arcsec {
                    found.push(candidate);
                }
            }
        }
    }

    // Best first, then drop anything already covered by a better cluster.
    found.sort_by(|a, b| {
        b.ids.len().cmp(&a.ids.len()).then(
            a.rms_arcsec
                .partial_cmp(&b.rms_arcsec)
                .unwrap_or(std::cmp::Ordering::Equal),
        )
    });
    let mut claimed: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut kept = Vec::new();
    for c in found {
        if c.ids.iter().all(|id| claimed.contains(id)) {
            continue;
        }
        claimed.extend(c.ids.iter().copied());
        kept.push(c);
    }
    kept
}

/// Trial rate `i` of `steps`, spread symmetrically about zero.
fn rate_at(i: usize, steps: usize, span: f64) -> f64 {
    if steps == 1 {
        return 0.0;
    }
    -span + 2.0 * span * (i as f64) / ((steps - 1) as f64)
}

/// Describe a cluster, refitting the drift from its own members.
fn summarise(members: &[usize], projected: &[Projected], t0: f64, vx: f64, vy: f64) -> Cluster {
    let n = members.len() as f64;
    let mean_t = members.iter().map(|&k| projected[k].jd).sum::<f64>() / n;
    let (mut sxx, mut sxy, mut syy) = (0.0, 0.0, 0.0);
    let mean_x = members.iter().map(|&k| projected[k].x).sum::<f64>() / n;
    let mean_y = members.iter().map(|&k| projected[k].y).sum::<f64>() / n;
    for &k in members {
        let dt = projected[k].jd - mean_t;
        sxx += dt * dt;
        sxy += dt * (projected[k].x - mean_x);
        syy += dt * (projected[k].y - mean_y);
    }
    // A cluster confined to one instant cannot refit its own drift.
    let (fx, fy) = if sxx > 0.0 {
        (sxy / sxx, syy / sxx)
    } else {
        (vx, vy)
    };
    let mut sq = 0.0;
    for &k in members {
        let dt = projected[k].jd - mean_t;
        let ex = projected[k].x - (mean_x + fx * dt);
        let ey = projected[k].y - (mean_y + fy * dt);
        sq += ex * ex + ey * ey;
    }
    let nights = members
        .iter()
        .map(|&k| (projected[k].jd - 0.5).floor() as i64)
        .collect::<std::collections::HashSet<_>>()
        .len();
    let mut ids: Vec<i64> = members.iter().map(|&k| projected[k].id).collect();
    ids.sort_unstable();
    let _ = t0;
    Cluster {
        ids,
        rate_x_deg_per_day: fx,
        rate_y_deg_per_day: fy,
        rms_arcsec: (sq / n).sqrt() * 3600.0,
        nights,
    }
}

/// Project and cluster in one call.
pub fn recover(detections: &[Detection], track: &TestOrbitTrack, cfg: &Config) -> Vec<Cluster> {
    cluster(&project(detections, track, cfg), cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test orbit that simply drifts west at a steady rate.
    fn track() -> TestOrbitTrack {
        let jd: Vec<f64> = (0..8).map(|k| 2460000.0 + k as f64).collect();
        let ra: Vec<f64> = jd.iter().map(|t| 100.0 + 0.2 * (t - 2460000.0)).collect();
        let dec: Vec<f64> = jd.iter().map(|t| 20.0 - 0.05 * (t - 2460000.0)).collect();
        TestOrbitTrack { jd, ra, dec }
    }

    /// Detections of an object offset from the test orbit and drifting slowly.
    fn object(
        offset_x: f64,
        offset_y: f64,
        drift_x: f64,
        drift_y: f64,
        jds: &[f64],
        id0: i64,
    ) -> Vec<Detection> {
        let t = track();
        let t0 = jds.iter().sum::<f64>() / jds.len() as f64;
        jds.iter()
            .enumerate()
            .map(|(k, &jd)| {
                let (ra0, dec0) = t.at(jd).expect("inside the span");
                let dt = jd - t0;
                let x = offset_x + drift_x * dt;
                let y = offset_y + drift_y * dt;
                Detection {
                    id: id0 + k as i64,
                    jd,
                    ra: ra0 + x / dec0.to_radians().cos(),
                    dec: dec0 + y,
                    mag: None,
                    band: None,
                }
            })
            .collect()
    }

    // One detection a night: exactly what a tracklet-based linker cannot use.
    const SPARSE: [f64; 4] = [2460001.2, 2460002.2, 2460003.2, 2460004.2];

    #[test]
    fn test_track_interpolates_and_refuses_outside_its_span() {
        let t = track();
        let (ra, dec) = t.at(2460002.5).expect("inside");
        assert!((ra - 100.5).abs() < 1e-9, "ra {ra}");
        assert!((dec - 19.875).abs() < 1e-9, "dec {dec}");
        assert!(t.at(2459999.0).is_none());
        assert!(t.at(2460100.0).is_none());
    }

    #[test]
    fn test_recovers_an_object_seen_once_per_night() {
        let dets = object(0.01, -0.02, 0.004, -0.002, &SPARSE, 1);
        let found = recover(&dets, &track(), &Config::default());
        assert_eq!(found.len(), 1, "expected one cluster, got {found:?}");
        assert_eq!(found[0].ids.len(), 4);
        assert_eq!(found[0].nights, 4);
        assert!(found[0].rms_arcsec < 1.0, "rms {}", found[0].rms_arcsec);
    }

    #[test]
    fn test_separates_two_objects_near_the_same_test_orbit() {
        let mut dets = object(0.01, -0.02, 0.004, -0.002, &SPARSE, 1);
        dets.extend(object(-0.03, 0.015, -0.005, 0.003, &SPARSE, 100));
        let found = recover(&dets, &track(), &Config::default());
        // A rate grid can also throw up chance groupings, which downstream
        // orbit fitting rejects; what matters is that both objects come back
        // whole and unmixed.
        let first = found
            .iter()
            .find(|c| c.ids.contains(&1))
            .expect("first object");
        let second = found
            .iter()
            .find(|c| c.ids.contains(&100))
            .expect("second object");
        assert_eq!(first.ids, vec![1, 2, 3, 4]);
        assert_eq!(second.ids, vec![100, 101, 102, 103]);
        assert_eq!(first.nights, 4);
        assert_eq!(second.nights, 4);
    }

    #[test]
    fn test_ignores_detections_far_from_the_test_orbit() {
        let dets = object(5.0, 5.0, 0.0, 0.0, &SPARSE, 1);
        assert!(recover(&dets, &track(), &Config::default()).is_empty());
    }

    #[test]
    fn test_scattered_detections_do_not_cluster() {
        // Same nights, but positions that follow no common drift.
        let t = track();
        let dets: Vec<Detection> = SPARSE
            .iter()
            .enumerate()
            .map(|(k, &jd)| {
                let (ra0, dec0) = t.at(jd).unwrap();
                let wobble = [0.4_f64, -0.9, 0.7, -0.3][k];
                Detection {
                    id: k as i64,
                    jd,
                    ra: ra0 + wobble,
                    dec: dec0 - wobble,
                    mag: None,
                    band: None,
                }
            })
            .collect();
        assert!(recover(&dets, &t, &Config::default()).is_empty());
    }

    #[test]
    fn test_one_night_is_not_a_track() {
        // Four detections, all in a single night, must not pass min_nights.
        let same_night = [2460001.10, 2460001.15, 2460001.20, 2460001.25];
        let dets = object(0.01, -0.02, 0.004, -0.002, &same_night, 1);
        assert!(recover(&dets, &track(), &Config::default()).is_empty());
    }

    #[test]
    fn test_a_drift_beyond_the_grid_is_not_recovered() {
        let cfg = Config {
            max_residual_rate_deg_per_day: 0.01,
            ..Config::default()
        };
        // Drifting far faster than the grid searches.
        let dets = object(0.01, -0.02, 0.5, -0.4, &SPARSE, 1);
        assert!(recover(&dets, &track(), &cfg).is_empty());
    }
}
