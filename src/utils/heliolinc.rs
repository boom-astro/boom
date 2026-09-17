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

use crate::utils::linking::{night_of, Detection, Tracklet};
use crate::utils::orbit_fit::{fit_orbit, rms_arcsec, Observation};
use crate::utils::sso_geometry::{earth_position, heliocentric_position, OrbitalElements};
use rayon::prelude::*;
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
    /// Spread of the member states about their mean position, au. Lower is a
    /// better fit, so it picks between hypotheses that both cluster a track.
    pub rms_au: f64,
    /// How well a fitted orbit reproduces the members' positions, arcseconds.
    /// `None` when there were too few to constrain one.
    pub residual_arcsec: Option<f64>,
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
    /// Largest sky residual a fitted orbit may leave, arcseconds. Candidates
    /// that no orbit explains are rejected rather than ranked.
    pub max_residual_arcsec: f64,
}

/// Largest radial velocity a bound object can have at `r_au`, au/day.
fn escape_speed(r_au: f64) -> f64 {
    (2.0 * MU / r_au).sqrt()
}

/// Hypotheses over `distances` heliocentric distances from `first_au` to
/// `last_au`, spaced geometrically so the closer, faster-changing region is
/// sampled more finely.
///
/// Radial velocity carries the sampling: a wrong choice displaces a propagated
/// state by its error times the baseline, so the step is set from the
/// clustering tolerance and the longest baseline the search spans, while the
/// range covers everything up to escape speed.
fn hypothesis_grid(
    first_au: f64,
    last_au: f64,
    distances: u32,
    baseline_days: f64,
    tol_au: f64,
) -> Vec<Hypothesis> {
    let step = (tol_au / baseline_days).max(f64::MIN_POSITIVE);
    let ratio = (last_au / first_au).powf(1.0 / f64::from(distances.saturating_sub(1).max(1)));
    let mut out = Vec::new();
    for i in 0..distances {
        let r_au = first_au * ratio.powi(i as i32);
        let limit = 0.98 * escape_speed(r_au);
        let arms = (limit / step).floor() as i64;
        for k in -arms..=arms {
            out.push(Hypothesis {
                r_au,
                rdot_au_per_day: step * k as f64,
            });
        }
    }
    out
}

/// A grid over the main belt and beyond, matching the span heliolinx searches.
pub fn main_belt_hypotheses() -> Vec<Hypothesis> {
    hypothesis_grid(1.5, 9.5, 29, 7.0, 0.002)
}

/// A grid over the near-Earth region.
///
/// Overlaps the belt grid deliberately: an object's distance is unknown, and
/// the two populations are separated by orbit rather than by where they happen
/// to be when detected.
pub fn neo_hypotheses() -> Vec<Hypothesis> {
    hypothesis_grid(1.1, 5.6, 18, 7.0, 0.002)
}

/// Both populations, which is what a survey-wide search needs.
pub fn default_hypotheses() -> Vec<Hypothesis> {
    let mut out = neo_hypotheses();
    out.extend(main_belt_hypotheses());
    out
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            hypotheses: default_hypotheses(),
            reference_jd: 0.0,
            position_tol_au: 0.002,
            velocity_tol_au_per_day: 0.0004,
            min_nights: 2,
            max_residual_arcsec: 2.0,
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

/// Right ascension and declination, degrees, for an ecliptic vector.
pub fn radec_from_ecliptic(v: &[f64; 3]) -> (f64, f64) {
    let (s, c) = OBLIQUITY_DEG.to_radians().sin_cos();
    let eq = [v[0], c * v[1] - s * v[2], s * v[1] + c * v[2]];
    let r = (eq[0] * eq[0] + eq[1] * eq[1] + eq[2] * eq[2]).sqrt();
    (
        eq[1].atan2(eq[0]).to_degrees().rem_euclid(360.0),
        (eq[2] / r).asin().to_degrees(),
    )
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

/// Position alone, for callers that discard the velocity.
///
/// A third of the work of [`propagate`], which differences two extra positions
/// to get the velocity. The orbit-fit Jacobian calls this per observation per
/// parameter per iteration, so the saving is the bulk of a fit.
pub fn propagate_position(state: &State, epoch_jd: f64, jd: f64) -> Option<[f64; 3]> {
    let elements = state_to_elements(state, epoch_jd)?;
    Some(heliocentric_position(&elements, jd))
}

/// Where `state` appears on the sky at each of `jds`, degrees.
///
/// `None` if the orbit cannot be propagated to one of them, since a test orbit
/// with a gap in its track cannot anchor a co-moving frame.
pub fn sky_track(state: &State, epoch_jd: f64, jds: &[f64]) -> Option<(Vec<f64>, Vec<f64>)> {
    let mut ras = Vec::with_capacity(jds.len());
    let mut decs = Vec::with_capacity(jds.len());
    for &jd in jds {
        let p = propagate_position(state, epoch_jd, jd)?;
        let e = earth_position(jd);
        let (ra, dec) = radec_from_ecliptic(&[p[0] - e[0], p[1] - e[1], p[2] - e[2]]);
        ras.push(ra);
        decs.push(dec);
    }
    Some((ras, decs))
}

/// Trial orbits through `(ra_deg, dec_deg)` at `epoch_jd`, one per heliocentric
/// distance, each given the circular speed there.
///
/// A tracklet-less search needs whole orbits rather than the distance and
/// radial velocity a tracklet's rate supplies, so the direction is taken from
/// where the field is and the speed from what a bound orbit at that distance
/// must have. Nearby real orbits drift slowly in the frame co-moving with one
/// of these, which is what makes their detections cluster.
pub fn test_orbits(
    ra_deg: f64,
    dec_deg: f64,
    epoch_jd: f64,
    distances_au: &[f64],
) -> Vec<(State, f64)> {
    let look = unit_vector(ra_deg, dec_deg);
    let earth = earth_position(epoch_jd);
    let mut out = Vec::new();

    for &r_au in distances_au {
        // Distance along the line of sight that puts the object at r_au from
        // the Sun: solves |earth + d*look| = r_au.
        let b = dot(&earth, &look);
        let c = dot(&earth, &earth) - r_au * r_au;
        let disc = b * b - c;
        if disc < 0.0 {
            continue;
        }
        let d = -b + disc.sqrt();
        if d <= 0.0 {
            continue;
        }
        let pos = [
            earth[0] + d * look[0],
            earth[1] + d * look[1],
            earth[2] + d * look[2],
        ];

        // Circular speed, perpendicular to the radius and in the orbit plane
        // closest to the ecliptic, which is where most of the population sits.
        let speed = (MU / r_au).sqrt();
        let up = [0.0, 0.0, 1.0];
        let tangent = cross(&up, &pos);
        let n = norm(&tangent);
        if n < 1e-12 {
            continue;
        }
        let vel = [
            speed * tangent[0] / n,
            speed * tangent[1] / n,
            speed * tangent[2] / n,
        ];
        out.push((State { pos, vel }, r_au));
    }
    out
}

/// Bucket key placing a state in a grid cell of side `tol`.
fn cell(pos: &[f64; 3], tol: f64) -> (i64, i64, i64) {
    (
        (pos[0] / tol).floor() as i64,
        (pos[1] / tol).floor() as i64,
        (pos[2] / tol).floor() as i64,
    )
}

/// Whether two propagated states agree closely enough to belong to one object.
fn agree(a: &State, b: &State, cfg: &LinkConfig) -> bool {
    let dp = [
        b.pos[0] - a.pos[0],
        b.pos[1] - a.pos[1],
        b.pos[2] - a.pos[2],
    ];
    let dv = [
        b.vel[0] - a.vel[0],
        b.vel[1] - a.vel[1],
        b.vel[2] - a.vel[2],
    ];
    norm(&dp) <= cfg.position_tol_au && norm(&dv) <= cfg.velocity_tol_au_per_day
}

/// Every state reachable from `seed` through chains of agreeing states.
///
/// Grown transitively rather than taken from the seed alone: an object observed
/// over many nights spreads its propagated states further than one tolerance
/// width, and collecting only the seed's own neighbours splits it into pieces.
/// `used` members are skipped, so a group is never stolen from a kept track.
fn connected_group(
    seed: usize,
    states: &[(usize, State)],
    grid: &HashMap<(i64, i64, i64), Vec<usize>>,
    used: &[bool],
    cfg: &LinkConfig,
) -> Vec<usize> {
    let mut group = vec![seed];
    let mut seen = std::collections::HashSet::from([seed]);
    let mut queue = vec![seed];

    while let Some(current) = queue.pop() {
        let base = cell(&states[current].1.pos, cfg.position_tol_au);
        for dx in -1..=1 {
            for dy in -1..=1 {
                for dz in -1..=1 {
                    let Some(bucket) = grid.get(&(base.0 + dx, base.1 + dy, base.2 + dz)) else {
                        continue;
                    };
                    for &m in bucket {
                        if used[m] || seen.contains(&m) {
                            continue;
                        }
                        if agree(&states[current].1, &states[m].1, cfg) {
                            seen.insert(m);
                            group.push(m);
                            queue.push(m);
                        }
                    }
                }
            }
        }
    }
    group
}

/// Tracks this one hypothesis produces.
///
/// Each hypothesis is independent -- it propagates every tracklet under its own
/// assumption and clusters the result -- so the sweep parallelises over them.
fn tracks_for_hypothesis(
    tracklets: &[Tracklet],
    hypothesis: &Hypothesis,
    cfg: &LinkConfig,
) -> Vec<Track> {
    let mut tracks: Vec<Track> = Vec::new();
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
    // Nothing can cluster with fewer than two states.
    if states.len() < 2 {
        return tracks;
    }

    // Grid on position so only nearby states are compared.
    let mut grid: HashMap<(i64, i64, i64), Vec<usize>> = HashMap::new();
    for (k, (_, s)) in states.iter().enumerate() {
        grid.entry(cell(&s.pos, cfg.position_tol_au))
            .or_default()
            .push(k);
    }

    // Marks a state's component as walked. Components are equivalence
    // classes, so re-seeding from another member only rediscovers the same
    // one; without this a rejected component is re-walked once per member.
    let mut used = vec![false; states.len()];
    for k in 0..states.len() {
        if used[k] {
            continue;
        }
        let group = connected_group(k, &states, &grid, &used, cfg);
        for &g in &group {
            used[g] = true;
        }
        if group.len() < 2 {
            continue;
        }

        let members: Vec<usize> = group.iter().map(|&g| states[g].0).collect();
        let nights = members
            .iter()
            .map(|&m| night_of(tracklets[m].jd_ref))
            .collect::<std::collections::HashSet<_>>()
            .len();
        if nights < cfg.min_nights {
            continue;
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
        let mut sq = 0.0;
        for &g in &group {
            let (_, ref s) = states[g];
            sq += (s.pos[0] - pos[0]).powi(2)
                + (s.pos[1] - pos[1]).powi(2)
                + (s.pos[2] - pos[2]).powi(2);
        }
        tracks.push(Track {
            members,
            hypothesis: *hypothesis,
            state: State { pos, vel },
            nights,
            rms_au: (sq / n).sqrt(),
            residual_arcsec: None,
        });
    }

    tracks
}

/// Score a candidate by how well one orbit reproduces its members' positions.
///
/// A cluster in state space is only a claim that the tracklets agree under some
/// assumed distance; fitting the sky positions tests that claim against the
/// astrometry itself, which is what separates a real track from tracklets that
/// happen to land near each other.
fn score(
    track: &mut Track,
    tracklets: &[Tracklet],
    by_id: &HashMap<i64, &Detection>,
    cfg: &LinkConfig,
) {
    // Every detection of every member. A tracklet's midpoint alone leaves a
    // two-tracklet track fitting six parameters to four residuals, which no
    // amount of iteration can determine, and the residual it reports is then
    // the underdetermination rather than the quality of the link.
    let observations: Vec<Observation> = track
        .members
        .iter()
        .flat_map(|&m| tracklets[m].ids.iter().filter_map(|id| by_id.get(id)))
        .map(|d| Observation {
            jd: d.jd,
            ra: d.ra,
            dec: d.dec,
        })
        .collect();
    // A tracklet built without its detections to hand still carries a midpoint.
    let observations = if observations.len() >= track.members.len() {
        observations
    } else {
        track
            .members
            .iter()
            .map(|&m| Observation {
                jd: tracklets[m].jd_ref,
                ra: tracklets[m].ra_ref,
                dec: tracklets[m].dec_ref,
            })
            .collect()
    };

    track.residual_arcsec = match fit_orbit(&observations, &track.state, cfg.reference_jd, 20) {
        Some(fit) => {
            track.state = fit.state;
            Some(fit.rms_arcsec)
        }
        // Too few positions to refine six parameters, so take the state as it
        // stands rather than discarding a candidate for being short.
        None => rms_arcsec(&track.state, cfg.reference_jd, &observations),
    };
}

/// Link tracklets into tracks, sweeping every hypothesis in `cfg`.
///
/// Every hypothesis contributes its candidates, and the orbit fit decides
/// between those that overlap: a tracklet is reported in whichever surviving
/// track explains its astrometry best, not whichever hypothesis reached it
/// first.
pub fn link_tracklets(
    tracklets: &[Tracklet],
    detections: &[Detection],
    cfg: &LinkConfig,
) -> Vec<Track> {
    let by_id: HashMap<i64, &Detection> = detections.iter().map(|d| (d.id, d)).collect();
    // Collected in hypothesis order, so the result does not depend on thread
    // scheduling and deduplication stays reproducible.
    let mut tracks: Vec<Track> = cfg
        .hypotheses
        .par_iter()
        .flat_map(|hypothesis| tracks_for_hypothesis(tracklets, hypothesis, cfg))
        .collect();

    tracks
        .par_iter_mut()
        .for_each(|track| score(track, tracklets, &by_id, cfg));

    // An orbit nothing explains is not a track, whatever its states did.
    tracks.retain(|t| {
        t.residual_arcsec
            .is_some_and(|r| r <= cfg.max_residual_arcsec)
    });

    // Best-fitting first, then longest, so deduplication keeps the candidate
    // the astrometry supports rather than the one found earliest.
    tracks.sort_by(|a, b| {
        a.residual_arcsec
            .unwrap_or(f64::INFINITY)
            .partial_cmp(&b.residual_arcsec.unwrap_or(f64::INFINITY))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.members.len().cmp(&a.members.len()))
            .then(b.nights.cmp(&a.nights))
    });
    deduplicate(tracks)
}

/// Drop tracks whose members are already covered by a kept track.
///
/// One object clusters under every hypothesis close enough to its true
/// distance, so the same track is found many times over; only the best-fitting
/// version is worth reporting.
pub fn deduplicate(tracks: Vec<Track>) -> Vec<Track> {
    let mut claimed: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut kept: Vec<Track> = Vec::new();
    for track in tracks {
        // A track adds nothing when every tracklet in it is already reported.
        if track.members.iter().all(|m| claimed.contains(m)) {
            continue;
        }
        claimed.extend(track.members.iter().copied());
        kept.push(track);
    }
    kept
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Angular separation, degrees.
    fn angular_gap(ra1: f64, dec1: f64, ra2: f64, dec2: f64) -> f64 {
        crate::utils::linking::angular_separation_deg(ra1, dec1, ra2, dec2)
    }

    /// The near-Earth grid starts inside the belt and overlaps it, since an
    /// object's distance is not known before it is linked.
    #[test]
    fn neo_hypotheses_cover_the_near_earth_region() {
        let neo = neo_hypotheses();
        let closest = neo.iter().map(|h| h.r_au).fold(f64::INFINITY, f64::min);
        assert!(closest <= 1.1, "closest hypothesis is {closest} au");
        assert!(
            neo.iter().any(|h| h.r_au < 1.3),
            "nothing inside the NEO perihelion limit"
        );
    }

    /// Radial velocity is sampled to escape speed: anything faster is unbound
    /// and anything coarser lets a real object fall between hypotheses.
    #[test]
    fn rdot_spans_the_bound_range_at_each_distance() {
        let belt = main_belt_hypotheses();
        let inner = belt.iter().map(|h| h.r_au).fold(f64::INFINITY, f64::min);
        let widest = belt
            .iter()
            .filter(|h| h.r_au == inner)
            .map(|h| h.rdot_au_per_day.abs())
            .fold(0.0, f64::max);
        let escape = escape_speed(inner);
        assert!(
            widest > 0.9 * escape && widest <= escape,
            "rdot reaches {widest} against escape speed {escape} at {inner} au"
        );
    }

    /// The grid is fine enough to compete with the reference implementation,
    /// which searches a few thousand hypotheses rather than a few dozen.
    #[test]
    fn the_grid_is_densely_sampled() {
        let n = default_hypotheses().len();
        assert!(n > 2000, "only {n} hypotheses");
    }

    /// Both grids reach their stated edges, so nothing is lost to the
    /// accumulated error of a geometric progression.
    #[test]
    fn grids_reach_their_last_distance() {
        for (label, grid, first, last) in [
            ("belt", main_belt_hypotheses(), 1.5, 9.5),
            ("neo", neo_hypotheses(), 1.1, 5.6),
        ] {
            let lo = grid.iter().map(|h| h.r_au).fold(f64::INFINITY, f64::min);
            let hi = grid.iter().map(|h| h.r_au).fold(0.0, f64::max);
            assert!((lo - first).abs() < 1e-6, "{label} starts at {lo}");
            assert!((hi - last).abs() < 1e-6, "{label} stops at {hi}");
        }
    }

    /// The default search covers both populations, not the belt alone.
    #[test]
    fn default_hypotheses_span_both_populations() {
        let all = default_hypotheses();
        assert!(all.iter().any(|h| h.r_au < 1.3), "no near-Earth hypotheses");
        assert!(all.iter().any(|h| h.r_au > 3.0), "no outer-belt hypotheses");
        assert_eq!(
            all.len(),
            neo_hypotheses().len() + main_belt_hypotheses().len()
        );
    }

    /// States are grouped through chains, so a track spread over more than one
    /// tolerance width stays whole instead of splitting.
    #[test]
    fn a_chain_of_states_forms_one_group() {
        let cfg = LinkConfig {
            position_tol_au: 0.01,
            velocity_tol_au_per_day: 1.0,
            ..Default::default()
        };
        // Each neighbour is within tolerance; the ends are three times beyond it.
        let states: Vec<(usize, State)> = (0..4)
            .map(|i| {
                (
                    i,
                    State {
                        pos: [0.009 * i as f64, 0.0, 0.0],
                        vel: [0.0; 3],
                    },
                )
            })
            .collect();
        let mut grid: HashMap<(i64, i64, i64), Vec<usize>> = HashMap::new();
        for (k, (_, s)) in states.iter().enumerate() {
            grid.entry(cell(&s.pos, cfg.position_tol_au))
                .or_default()
                .push(k);
        }
        let used = vec![false; states.len()];
        let group = connected_group(0, &states, &grid, &used, &cfg);
        assert_eq!(group.len(), 4, "chain split into {:?}", group);
    }

    /// A cluster the astrometry does not support is rejected, however tightly
    /// its propagated states agreed.
    #[test]
    fn a_track_no_orbit_explains_is_rejected() {
        let good = ceres_like();
        let jds = [2460000.5, 2460002.5, 2460004.5];
        let mut tracklets: Vec<Tracklet> = jds
            .iter()
            .enumerate()
            .map(|(_, &jd)| tracklet_for(&good, jd))
            .collect();
        // Displace one member far off the orbit the others describe.
        tracklets[2].dec_ref += 0.5;

        let cfg = LinkConfig {
            reference_jd: 2460002.5,
            ..Default::default()
        };
        let strict = link_tracklets(&tracklets, &[], &cfg);
        assert!(
            strict.iter().all(|t| t.members.len() < 3),
            "a displaced member was kept in a track"
        );

        let loose = LinkConfig {
            max_residual_arcsec: 1e9,
            ..cfg.clone()
        };
        assert!(
            link_tracklets(&tracklets, &[], &loose).len() >= strict.len(),
            "the gate should only ever remove candidates"
        );
    }

    /// A trial orbit sits at the distance asked for and in the direction looked.
    #[test]
    fn test_orbits_are_placed_where_the_field_is() {
        let (ra, dec, jd) = (100.0, 20.0, 2460000.5);
        for (state, r_au) in test_orbits(ra, dec, jd, &[1.5, 2.5, 3.5]) {
            assert!(
                (norm(&state.pos) - r_au).abs() < 1e-6,
                "{r_au} au orbit sits at {}",
                norm(&state.pos)
            );
            // Seen from Earth it must lie in the direction that was searched.
            let e = earth_position(jd);
            let (sra, sdec) = radec_from_ecliptic(&[
                state.pos[0] - e[0],
                state.pos[1] - e[1],
                state.pos[2] - e[2],
            ]);
            assert!(
                angular_gap(sra, sdec, ra, dec) < 1e-4,
                "points at {sra},{sdec}"
            );
        }
    }

    /// A trial orbit's sky track is continuous and moves like a real body.
    #[test]
    fn test_sky_track_follows_the_orbit() {
        let jds: Vec<f64> = (0..5).map(|k| 2460000.5 + k as f64).collect();
        let (state, _) = test_orbits(100.0, 20.0, 2460000.5, &[2.5]).remove(0);
        let (ras, decs) = sky_track(&state, 2460000.5, &jds).expect("a track");
        assert_eq!(ras.len(), jds.len());
        // A main-belt body moves under a degree a day near opposition.
        for k in 1..ras.len() {
            let step = angular_gap(ras[k - 1], decs[k - 1], ras[k], decs[k]);
            assert!(step > 0.0 && step < 1.0, "moved {step} deg in a day");
        }
    }

    /// A large component that fails the night test is walked once, not once per
    /// member: re-seeding into it is what made dense nights quadratic.
    #[test]
    fn a_rejected_component_is_not_rewalked() {
        use std::time::Instant;

        // One night only, so every group fails min_nights and none is kept.
        let n = 4000;
        let tracklets: Vec<Tracklet> = (0..n)
            .map(|i| {
                Tracklet::from_motion(
                    vec![i as i64],
                    2460000.5,
                    10.0 + 1e-6 * i as f64,
                    5.0,
                    0.2,
                    0.0,
                    0.1,
                )
            })
            .collect();
        let cfg = LinkConfig {
            hypotheses: vec![Hypothesis {
                r_au: 2.5,
                rdot_au_per_day: 0.0,
            }],
            reference_jd: 2460000.5,
            position_tol_au: 1.0,
            velocity_tol_au_per_day: 1.0,
            min_nights: 2,
            max_residual_arcsec: 2.0,
        };

        let started = Instant::now();
        let tracks = link_tracklets(&tracklets, &[], &cfg);
        let elapsed = started.elapsed();

        assert!(tracks.is_empty(), "one night cannot make a track");
        // Generous because the bound only has to separate linear from quadratic:
        // re-walking this 4000-state component runs past two minutes, while
        // walking it once is seconds even on a loaded machine.
        assert!(
            elapsed.as_secs() < 30,
            "linking took {elapsed:?}, suggesting the component is re-walked"
        );
    }

    /// A state already claimed by a kept track is not pulled into another.
    #[test]
    fn used_states_are_left_alone() {
        let cfg = LinkConfig {
            position_tol_au: 0.01,
            velocity_tol_au_per_day: 1.0,
            ..Default::default()
        };
        let states: Vec<(usize, State)> = (0..3)
            .map(|i| {
                (
                    i,
                    State {
                        pos: [0.005 * i as f64, 0.0, 0.0],
                        vel: [0.0; 3],
                    },
                )
            })
            .collect();
        let mut grid: HashMap<(i64, i64, i64), Vec<usize>> = HashMap::new();
        for (k, (_, s)) in states.iter().enumerate() {
            grid.entry(cell(&s.pos, cfg.position_tol_au))
                .or_default()
                .push(k);
        }
        let used = vec![false, true, false];
        let group = connected_group(0, &states, &grid, &used, &cfg);
        assert!(!group.contains(&1), "claimed state was taken: {group:?}");
    }

    /// Elements roughly those of a main-belt asteroid.
    fn ceres_like() -> OrbitalElements {
        OrbitalElements::elliptical(2460000.5, 2.7658, 0.0785, 10.588, 80.25, 73.6, 100.0)
    }

    /// The tracklet an object would produce at `jd`, from its true ephemeris.
    fn tracklet_for(elements: &OrbitalElements, jd: f64) -> Tracklet {
        let step = 0.02;
        let at = |t: f64| {
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
            max_residual_arcsec: 2.0,
        };
        let tracks = link_tracklets(&tracklets, &[], &cfg);
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
        assert!(link_tracklets(&tracklets, &[], &cfg).is_empty());
    }

    #[test]
    fn test_one_object_is_reported_once_across_a_hypothesis_grid() {
        let el = ceres_like();
        let jds = [2460010.0, 2460013.0, 2460017.0];
        let tracklets: Vec<Tracklet> = jds.iter().map(|&jd| tracklet_for(&el, jd)).collect();
        let truth_r = norm(&heliocentric_position(&el, jds[1]));
        // Several neighbouring distances all cluster this object.
        let hypotheses = (-2..=2)
            .map(|k| Hypothesis {
                r_au: truth_r + 0.02 * f64::from(k),
                rdot_au_per_day: 0.0,
            })
            .collect();
        let cfg = LinkConfig {
            hypotheses,
            reference_jd: jds[1],
            position_tol_au: 0.05,
            velocity_tol_au_per_day: 0.01,
            min_nights: 2,
            max_residual_arcsec: 2.0,
        };
        let tracks = link_tracklets(&tracklets, &[], &cfg);
        assert_eq!(tracks.len(), 1, "one object should yield one track");
        assert_eq!(tracks[0].members.len(), 3);
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
