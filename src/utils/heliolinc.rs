//! Cross-night linking: group tracklets from different nights, and different
//! surveys, into tracks belonging to one moving object.
//!
//! A tracklet fixes a direction and an on-sky rate but not a distance, so it
//! cannot be propagated on its own. Assuming a heliocentric distance and radial
//! velocity supplies the missing pair, which turns each tracklet into a full
//! heliocentric state. Tracklets of one object agree on that state once
//! propagated to a common epoch, whatever night or survey they came from;
//! unrelated ones scatter. Sweeping a grid of assumptions and clustering the
//! propagated states is then the whole method (Holman et al. 2018).

use crate::utils::linking::Tracklet;
use crate::utils::sso_geometry::{earth_position, heliocentric_position, OrbitalElements};
use std::collections::HashMap;

/// Heliocentric gravitational parameter, au^3/day^2.
const MU: f64 = 0.017_202_098_95 * 0.017_202_098_95;
/// Obliquity of the ecliptic at J2000, degrees.
const OBLIQUITY_DEG: f64 = 23.439_281;
/// Step for differencing Earth's position, days.
const EARTH_DERIV_STEP: f64 = 0.5;

/// One assumed heliocentric distance and radial velocity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Hypothesis {
    /// Heliocentric distance, au.
    pub r_au: f64,
    /// Heliocentric radial velocity, au/day.
    pub rdot_au_per_day: f64,
}

/// A heliocentric state in ecliptic coordinates, au and au/day.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct State {
    pub pos: [f64; 3],
    pub vel: [f64; 3],
}

/// Tracklets that agree on a state once propagated to a common epoch.
#[derive(Debug, Clone)]
pub struct Track {
    /// Indices into the tracklet slice handed to [`link_tracklets`].
    pub members: Vec<usize>,
    pub hypothesis: Hypothesis,
    /// State at the reference epoch, averaged over the members.
    pub state: State,
    /// Nights the members span, by integer JD.
    pub nights: usize,
}

/// Bounds the search and how tightly propagated states must agree.
#[derive(Debug, Clone)]
pub struct LinkConfig {
    pub hypotheses: Vec<Hypothesis>,
    /// Epoch the states are compared at; the middle of the arc is the safest.
    pub reference_jd: f64,
    /// Position agreement required to cluster, au.
    pub position_tol_au: f64,
    /// Velocity agreement required to cluster, au/day.
    pub velocity_tol_au_per_day: f64,
    /// Tracks must draw on at least this many distinct nights.
    pub min_nights: usize,
}

/// A grid over the main belt, the region most of the linkable population sits in.
pub fn main_belt_hypotheses() -> Vec<Hypothesis> {
    let mut out = Vec::new();
    let mut r = 1.8;
    while r <= 3.6 {
        // Radial velocity is bounded by the circular speed at this distance.
        let v_circ = (MU / r).sqrt();
        for k in -2..=2 {
            out.push(Hypothesis {
                r_au: r,
                rdot_au_per_day: 0.35 * v_circ * f64::from(k) / 2.0,
            });
        }
        r += 0.2;
    }
    out
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            hypotheses: main_belt_hypotheses(),
            reference_jd: 0.0,
            position_tol_au: 0.002,
            velocity_tol_au_per_day: 0.0004,
            min_nights: 2,
        }
    }
}

/// Equatorial degrees to an ecliptic unit vector.
fn unit_vector(ra_deg: f64, dec_deg: f64) -> [f64; 3] {
    let (ra, dec) = (ra_deg.to_radians(), dec_deg.to_radians());
    let (x, y, z) = (dec.cos() * ra.cos(), dec.cos() * ra.sin(), dec.sin());
    let (s, c) = OBLIQUITY_DEG.to_radians().sin_cos();
    [x, c * y + s * z, -s * y + c * z]
}

/// Rate of change of the line of sight, per day, from the on-sky rates.
fn unit_vector_rate(t: &Tracklet) -> [f64; 3] {
    // Differencing the unit vector keeps one definition of the projection.
    let step = 0.01;
    let a = unit_vector(
        t.ra_ref - t.ra_rate_deg_per_day * step / 2.0 / t.dec_ref.to_radians().cos().max(1e-6),
        t.dec_ref - t.dec_rate_deg_per_day * step / 2.0,
    );
    let b = unit_vector(
        t.ra_ref + t.ra_rate_deg_per_day * step / 2.0 / t.dec_ref.to_radians().cos().max(1e-6),
        t.dec_ref + t.dec_rate_deg_per_day * step / 2.0,
    );
    [
        (b[0] - a[0]) / step,
        (b[1] - a[1]) / step,
        (b[2] - a[2]) / step,
    ]
}

fn dot(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn cross(a: &[f64; 3], b: &[f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn norm(a: &[f64; 3]) -> f64 {
    dot(a, a).sqrt()
}

/// Earth's heliocentric velocity, au/day, by central difference.
fn earth_velocity(jd: f64) -> [f64; 3] {
    let a = earth_position(jd - EARTH_DERIV_STEP);
    let b = earth_position(jd + EARTH_DERIV_STEP);
    let d = 2.0 * EARTH_DERIV_STEP;
    [(b[0] - a[0]) / d, (b[1] - a[1]) / d, (b[2] - a[2]) / d]
}

/// The heliocentric state a tracklet implies under `h`.
///
/// The distance along the line of sight follows from placing the object on a
/// sphere of radius `r_au`; the range rate then follows from requiring the
/// heliocentric radial velocity to be `rdot_au_per_day`.
pub fn state_from_tracklet(t: &Tracklet, h: &Hypothesis) -> Option<State> {
    let rho = unit_vector(t.ra_ref, t.dec_ref);
    let rho_dot = unit_vector_rate(t);
    let e_pos = earth_position(t.jd_ref);
    let e_vel = earth_velocity(t.jd_ref);

    // |E + d rho| = r, taking the root in front of the observer.
    let b = dot(&e_pos, &rho);
    let c = dot(&e_pos, &e_pos) - h.r_au * h.r_au;
    let disc = b * b - c;
    if disc < 0.0 {
        return None;
    }
    let d = -b + disc.sqrt();
    if d <= 0.0 {
        return None;
    }

    let pos = [
        e_pos[0] + d * rho[0],
        e_pos[1] + d * rho[1],
        e_pos[2] + d * rho[2],
    ];
    let denom = dot(&pos, &rho);
    if denom.abs() < 1e-9 {
        return None;
    }
    let d_dot = (h.r_au * h.rdot_au_per_day - dot(&pos, &e_vel) - d * dot(&pos, &rho_dot)) / denom;
    let vel = [
        e_vel[0] + d_dot * rho[0] + d * rho_dot[0],
        e_vel[1] + d_dot * rho[1] + d * rho_dot[1],
        e_vel[2] + d_dot * rho[2] + d * rho_dot[2],
    ];
    Some(State { pos, vel })
}

/// Osculating elements for a bound state, or `None` when it is not an ellipse.
pub fn state_to_elements(state: &State, epoch_jd: f64) -> Option<OrbitalElements> {
    let r = norm(&state.pos);
    let v2 = dot(&state.vel, &state.vel);
    if r <= 0.0 {
        return None;
    }
    let energy = v2 / 2.0 - MU / r;
    if energy >= 0.0 {
        return None;
    }
    let a = -MU / (2.0 * energy);

    let h_vec = cross(&state.pos, &state.vel);
    let h_norm = norm(&h_vec);
    if h_norm <= 0.0 {
        return None;
    }

    let rv = dot(&state.pos, &state.vel);
    let e_vec = [
        (v2 - MU / r) * state.pos[0] / MU - rv * state.vel[0] / MU,
        (v2 - MU / r) * state.pos[1] / MU - rv * state.vel[1] / MU,
        (v2 - MU / r) * state.pos[2] / MU - rv * state.vel[2] / MU,
    ];
    let e = norm(&e_vec);
    if !(0.0..1.0).contains(&e) {
        return None;
    }

    let incl = (h_vec[2] / h_norm).clamp(-1.0, 1.0).acos();
    let n_vec = [-h_vec[1], h_vec[0], 0.0];
    let n_norm = norm(&n_vec);

    // At zero inclination the node is undefined; put it at the origin of longitude.
    let (node, peri) = if n_norm < 1e-12 {
        (0.0, e_vec[1].atan2(e_vec[0]))
    } else {
        let node = n_vec[1].atan2(n_vec[0]);
        let mut peri = (dot(&n_vec, &e_vec) / (n_norm * e)).clamp(-1.0, 1.0).acos();
        if e_vec[2] < 0.0 {
            peri = 2.0 * std::f64::consts::PI - peri;
        }
        (node, peri)
    };

    let mut nu = (dot(&e_vec, &state.pos) / (e * r)).clamp(-1.0, 1.0).acos();
    if rv < 0.0 {
        nu = 2.0 * std::f64::consts::PI - nu;
    }
    // True to eccentric to mean anomaly.
    let ecc_anom =
        2.0 * ((1.0 - e).sqrt() * (nu / 2.0).sin()).atan2((1.0 + e).sqrt() * (nu / 2.0).cos());
    let mean_anom = ecc_anom - e * ecc_anom.sin();

    Some(OrbitalElements::elliptical(
        epoch_jd,
        a,
        e,
        incl.to_degrees(),
        node.to_degrees().rem_euclid(360.0),
        peri.to_degrees().rem_euclid(360.0),
        mean_anom.to_degrees().rem_euclid(360.0),
    ))
}

/// Propagate a state to `jd` on its own two-body orbit.
pub fn propagate(state: &State, epoch_jd: f64, jd: f64) -> Option<State> {
    let elements = state_to_elements(state, epoch_jd)?;
    let pos = heliocentric_position(&elements, jd);
    // Velocity by central difference, so one propagator serves both.
    let step = 0.05;
    let before = heliocentric_position(&elements, jd - step);
    let after = heliocentric_position(&elements, jd + step);
    let vel = [
        (after[0] - before[0]) / (2.0 * step),
        (after[1] - before[1]) / (2.0 * step),
        (after[2] - before[2]) / (2.0 * step),
    ];
    Some(State { pos, vel })
}

/// Bucket key placing a state in a grid cell of side `tol`.
fn cell(pos: &[f64; 3], tol: f64) -> (i64, i64, i64) {
    (
        (pos[0] / tol).floor() as i64,
        (pos[1] / tol).floor() as i64,
        (pos[2] / tol).floor() as i64,
    )
}

/// Link tracklets into tracks, sweeping every hypothesis in `cfg`.
///
/// A tracklet may appear in more than one track when several hypotheses fit it;
/// the caller decides which to keep.
pub fn link_tracklets(tracklets: &[Tracklet], cfg: &LinkConfig) -> Vec<Track> {
    let mut tracks: Vec<Track> = Vec::new();

    for hypothesis in &cfg.hypotheses {
        // Propagated state per tracklet under this hypothesis.
        let mut states: Vec<(usize, State)> = Vec::new();
        for (i, t) in tracklets.iter().enumerate() {
            let Some(s) = state_from_tracklet(t, hypothesis) else {
                continue;
            };
            if let Some(p) = propagate(&s, t.jd_ref, cfg.reference_jd) {
                states.push((i, p));
            }
        }
        if states.len() < 2 {
            continue;
        }

        // Grid on position so only nearby states are compared.
        let mut grid: HashMap<(i64, i64, i64), Vec<usize>> = HashMap::new();
        for (k, (_, s)) in states.iter().enumerate() {
            grid.entry(cell(&s.pos, cfg.position_tol_au))
                .or_default()
                .push(k);
        }

        let mut used = vec![false; states.len()];
        for k in 0..states.len() {
            if used[k] {
                continue;
            }
            let (_, ref sk) = states[k];
            let base = cell(&sk.pos, cfg.position_tol_au);
            let mut group = vec![k];
            for dx in -1..=1 {
                for dy in -1..=1 {
                    for dz in -1..=1 {
                        let key = (base.0 + dx, base.1 + dy, base.2 + dz);
                        let Some(bucket) = grid.get(&key) else {
                            continue;
                        };
                        for &m in bucket {
                            if m == k || used[m] {
                                continue;
                            }
                            let (_, ref sm) = states[m];
                            let dp = [
                                sm.pos[0] - sk.pos[0],
                                sm.pos[1] - sk.pos[1],
                                sm.pos[2] - sk.pos[2],
                            ];
                            let dv = [
                                sm.vel[0] - sk.vel[0],
                                sm.vel[1] - sk.vel[1],
                                sm.vel[2] - sk.vel[2],
                            ];
                            if norm(&dp) <= cfg.position_tol_au
                                && norm(&dv) <= cfg.velocity_tol_au_per_day
                            {
                                group.push(m);
                            }
                        }
                    }
                }
            }
            if group.len() < 2 {
                continue;
            }

            let members: Vec<usize> = group.iter().map(|&g| states[g].0).collect();
            let nights = members
                .iter()
                .map(|&m| tracklets[m].jd_ref.floor() as i64)
                .collect::<std::collections::HashSet<_>>()
                .len();
            if nights < cfg.min_nights {
                continue;
            }
            for &g in &group {
                used[g] = true;
            }

            let n = group.len() as f64;
            let mut pos = [0.0; 3];
            let mut vel = [0.0; 3];
            for &g in &group {
                let (_, ref s) = states[g];
                for c in 0..3 {
                    pos[c] += s.pos[c] / n;
                    vel[c] += s.vel[c] / n;
                }
            }
            tracks.push(Track {
                members,
                hypothesis: *hypothesis,
                state: State { pos, vel },
                nights,
            });
        }
    }

    // Longest first so a caller taking the best per tracklet sees it first.
    tracks.sort_by(|a, b| {
        b.members
            .len()
            .cmp(&a.members.len())
            .then(b.nights.cmp(&a.nights))
    });
    tracks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::sso_geometry::geometry_at;

    /// Elements roughly those of a main-belt asteroid.
    fn ceres_like() -> OrbitalElements {
        OrbitalElements::elliptical(2460000.5, 2.7658, 0.0785, 10.588, 80.25, 73.6, 100.0)
    }

    /// The tracklet an object would produce at `jd`, from its true ephemeris.
    fn tracklet_for(elements: &OrbitalElements, jd: f64) -> Tracklet {
        let step = 0.02;
        let at = |t: f64| {
            let g = geometry_at(elements, t);
            let helio = heliocentric_position(elements, t);
            let earth = earth_position(t);
            let topo = [
                helio[0] - earth[0],
                helio[1] - earth[1],
                helio[2] - earth[2],
            ];
            // Ecliptic back to equatorial, then to spherical.
            let (s, c) = OBLIQUITY_DEG.to_radians().sin_cos();
            let eq = [
                topo[0],
                c * topo[1] - s * topo[2],
                s * topo[1] + c * topo[2],
            ];
            let ra = eq[1].atan2(eq[0]).to_degrees().rem_euclid(360.0);
            let dec = (eq[2] / norm(&eq)).asin().to_degrees();
            let _ = g;
            (ra, dec)
        };
        let (ra, dec) = at(jd);
        let (ra_a, dec_a) = at(jd - step / 2.0);
        let (ra_b, dec_b) = at(jd + step / 2.0);
        Tracklet::from_motion(
            vec![jd as i64],
            jd,
            ra,
            dec,
            ((ra_b - ra_a + 540.0).rem_euclid(360.0) - 180.0) / step * dec.to_radians().cos(),
            (dec_b - dec_a) / step,
            0.0,
        )
    }

    #[test]
    fn test_state_to_elements_round_trips() {
        let el = ceres_like();
        let jd = 2460010.0;
        let pos = heliocentric_position(&el, jd);
        let step = 0.05;
        let a = heliocentric_position(&el, jd - step);
        let b = heliocentric_position(&el, jd + step);
        let vel = [
            (b[0] - a[0]) / (2.0 * step),
            (b[1] - a[1]) / (2.0 * step),
            (b[2] - a[2]) / (2.0 * step),
        ];
        let recovered = state_to_elements(&State { pos, vel }, jd).expect("bound orbit");
        assert!(
            (recovered.a - el.a).abs() < 1e-3,
            "a {} vs {}",
            recovered.a,
            el.a
        );
        assert!(
            (recovered.e - el.e).abs() < 1e-3,
            "e {} vs {}",
            recovered.e,
            el.e
        );
        assert!((recovered.incl - el.incl).abs() < 1e-2);
    }

    #[test]
    fn test_propagate_matches_the_ephemeris() {
        let el = ceres_like();
        let (from, to) = (2460010.0, 2460040.0);
        let pos = heliocentric_position(&el, from);
        let step = 0.05;
        let a = heliocentric_position(&el, from - step);
        let b = heliocentric_position(&el, from + step);
        let vel = [
            (b[0] - a[0]) / (2.0 * step),
            (b[1] - a[1]) / (2.0 * step),
            (b[2] - a[2]) / (2.0 * step),
        ];
        let moved = propagate(&State { pos, vel }, from, to).expect("propagates");
        let truth = heliocentric_position(&el, to);
        let err = norm(&[
            moved.pos[0] - truth[0],
            moved.pos[1] - truth[1],
            moved.pos[2] - truth[2],
        ]);
        assert!(err < 1e-4, "propagation error {err} au");
    }

    #[test]
    fn test_recovers_the_true_distance_for_a_real_orbit() {
        let el = ceres_like();
        let jd = 2460010.0;
        let t = tracklet_for(&el, jd);
        let truth = norm(&heliocentric_position(&el, jd));
        let h = Hypothesis {
            r_au: truth,
            rdot_au_per_day: 0.0,
        };
        let s = state_from_tracklet(&t, &h).expect("state");
        // The assumed distance is by construction the state's distance.
        assert!((norm(&s.pos) - truth).abs() < 1e-6);
    }

    #[test]
    fn test_links_tracklets_of_one_object_across_nights() {
        let el = ceres_like();
        let jds = [2460010.0, 2460013.0, 2460017.0];
        let tracklets: Vec<Tracklet> = jds.iter().map(|&jd| tracklet_for(&el, jd)).collect();
        let truth_r = norm(&heliocentric_position(&el, jds[1]));
        let cfg = LinkConfig {
            hypotheses: vec![Hypothesis {
                r_au: truth_r,
                rdot_au_per_day: 0.0,
            }],
            reference_jd: jds[1],
            position_tol_au: 0.05,
            velocity_tol_au_per_day: 0.01,
            min_nights: 2,
        };
        let tracks = link_tracklets(&tracklets, &cfg);
        assert!(!tracks.is_empty(), "no track recovered");
        assert_eq!(tracks[0].members.len(), 3);
        assert_eq!(tracks[0].nights, 3);
    }

    #[test]
    fn test_does_not_link_unrelated_directions() {
        let el = ceres_like();
        let mut tracklets = vec![tracklet_for(&el, 2460010.0)];
        // Same night structure, opposite side of the sky.
        let mut other = tracklet_for(&el, 2460013.0);
        other.ra_ref = (other.ra_ref + 120.0).rem_euclid(360.0);
        tracklets.push(other);
        let cfg = LinkConfig {
            hypotheses: vec![Hypothesis {
                r_au: 2.7,
                rdot_au_per_day: 0.0,
            }],
            reference_jd: 2460011.5,
            ..LinkConfig::default()
        };
        assert!(link_tracklets(&tracklets, &cfg).is_empty());
    }

    #[test]
    fn test_hypothesis_grid_spans_the_belt() {
        let grid = main_belt_hypotheses();
        assert!(grid.iter().any(|h| (h.r_au - 2.0).abs() < 0.11));
        assert!(grid.iter().any(|h| (h.r_au - 3.2).abs() < 0.11));
        assert!(grid.iter().any(|h| h.rdot_au_per_day > 0.0));
        assert!(grid.iter().any(|h| h.rdot_au_per_day < 0.0));
    }
}
