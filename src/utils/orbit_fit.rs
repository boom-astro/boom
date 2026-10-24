//! Differential correction: refine a heliocentric state against the astrometry
//! of a linked track.
//!
//! Linking leaves a state good enough to have clustered, which is far looser
//! than an orbit worth reporting. Gauss-Newton on the six state components,
//! with residuals taken on the sky, turns one into the other and yields the
//! residual that says whether the track was real.

use crate::utils::heliolinc::{
    bound_seed, is_bound, propagate_position, radec_from_ecliptic, state_to_elements, Conics, State,
};
use crate::utils::sso_geometry::{observer_position, OrbitalElements, Site};

/// One astrometric position.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Observation {
    pub jd: f64,
    /// Degrees.
    pub ra: f64,
    /// Degrees.
    pub dec: f64,
    pub site: Site,
}

fn weights(observations: &[Observation]) -> Vec<f64> {
    let inverse: Vec<f64> = observations
        .iter()
        .map(|obs| 1.0 / obs.site.astrometric_sigma_arcsec)
        .collect();
    let scale = (inverse.iter().map(|w| w * w).sum::<f64>() / inverse.len() as f64).sqrt();
    inverse.iter().map(|w| w / scale).collect()
}

/// A fitted orbit and how well it reproduces the observations.
#[derive(Debug, Clone)]
pub struct OrbitFit {
    pub state: State,
    pub epoch_jd: f64,
    pub elements: OrbitalElements,
    pub rms_arcsec: f64,
    pub iterations: usize,
    pub n_obs: usize,
}

/// Speed of light, au/day.
pub const C_AU_PER_DAY: f64 = 173.144_632_674;

/// Steps used to difference the state numerically.
const POS_STEP_AU: f64 = 1e-6;
const VEL_STEP_AU_PER_DAY: f64 = 1e-8;

/// A fit stops once an accepted step improves the rms by less than this,
/// arcseconds. A thousandth of an arcsecond is far below any gate a fit feeds,
/// and chasing smaller gains cost most of a fit's iterations.
const MIN_IMPROVEMENT_ARCSEC: f64 = 1e-3;

/// When to abandon a fit that is not going to pass the gate it feeds.
///
/// A candidate that no orbit explains keeps iterating until it runs out of
/// iterations, and most candidates a search produces are like that. Checking
/// after a few iterations spends the budget on the ones that can pass.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GiveUp {
    /// Iterations to run before checking.
    pub after_iterations: usize,
    /// Abandon if the rms is still above this, arcseconds.
    pub above_arcsec: f64,
}

/// Iterations a screening fit gets to pass its gate.
pub const SCREEN_ITERATIONS: usize = 20;
/// Iterations a fit that passed its gate gets to reach its minimum. A seed far
/// down a short arc's valley takes a few dozen.
pub const CONVERGE_ITERATIONS: usize = 100;
/// Iterations a screening fit gets before [`GiveUp::for_gate`] checks it.
pub const GIVE_UP_AFTER: usize = 8;
/// How far above its gate a screening fit may still be at that check.
pub const GIVE_UP_FACTOR: f64 = 18.0;

impl GiveUp {
    /// Abandon a fit still [`GIVE_UP_FACTOR`] times above `gate_arcsec` after
    /// [`GIVE_UP_AFTER`] iterations.
    ///
    /// The tests in `heliolinc` measure this against the fits it must spare,
    /// from tracklet states and from THOR's test orbits; tighten it only with
    /// them passing.
    pub fn for_gate(gate_arcsec: f64) -> Self {
        GiveUp {
            after_iterations: GIVE_UP_AFTER,
            above_arcsec: GIVE_UP_FACTOR * gate_arcsec,
        }
    }
}

/// Where a state puts the object on the sky at `jd`, degrees.
///
/// Light-time corrected: the object is seen where it was when the light left
/// it, which at 2 au is about 17 minutes and so tens of arcseconds of motion.
/// One correction leaves well under a milliarcsecond, since the geocentric
/// distance barely changes over the light time itself.
pub fn predict_radec(state: &State, epoch_jd: f64, jd: f64, site: &Site) -> Option<(f64, f64)> {
    // From the observer, not the Earth's centre: an Earth radius is several
    // arcseconds at these distances, which the residual gate is tighter than.
    predict_radec_seen_from(state, epoch_jd, jd, &observer_position(jd, site))
}

pub fn predict_radec_seen_from(
    state: &State,
    epoch_jd: f64,
    jd: f64,
    observer: &[f64; 3],
) -> Option<(f64, f64)> {
    apparent(state, epoch_jd, jd, observer, None).map(|(radec, _)| radec)
}

/// Where `state` appears from `observer` at `jd`, and the light time used.
///
/// With `light_time` given, the object is placed at that retarded epoch in one
/// propagation instead of iterating for it. A state nudged by a Jacobian step
/// changes its light time by far less than a second, so the nominal state's
/// light time serves every nudge of it.
fn apparent(
    state: &State,
    epoch_jd: f64,
    jd: f64,
    observer: &[f64; 3],
    light_time: Option<f64>,
) -> Option<((f64, f64), f64)> {
    let passes = if light_time.is_some() { 1 } else { 2 };
    let mut tau = light_time.unwrap_or(0.0);
    let mut topo = [0.0; 3];
    for _ in 0..passes {
        let moved = propagate_position(state, epoch_jd, jd - tau)?;
        topo = [
            moved[0] - observer[0],
            moved[1] - observer[1],
            moved[2] - observer[2],
        ];
        if light_time.is_none() {
            tau = (topo[0] * topo[0] + topo[1] * topo[1] + topo[2] * topo[2]).sqrt() / C_AU_PER_DAY;
        }
    }
    Some((radec_from_ecliptic(&topo), tau))
}

/// Residual in arcseconds, as (RA on a great circle, Dec), and the light time.
fn residual(
    state: &State,
    epoch_jd: f64,
    obs: &Observation,
    observer: &[f64; 3],
    light_time: Option<f64>,
) -> Option<((f64, f64), f64)> {
    let ((ra, dec), tau) = apparent(state, epoch_jd, obs.jd, observer, light_time)?;
    // Fold the RA difference so a wrap does not read as a huge residual.
    let dra =
        ((obs.ra - ra + 540.0).rem_euclid(360.0) - 180.0) * obs.dec.to_radians().cos() * 3600.0;
    Some(((dra, (obs.dec - dec) * 3600.0), tau))
}

/// Nudge one component of a state.
fn perturb(state: &State, k: usize, delta: f64) -> State {
    let mut out = *state;
    if k < 3 {
        out.pos[k] += delta;
    } else {
        out.vel[k - 3] += delta;
    }
    out
}

/// Solve `a x = b` for a small square system, or `None` if it is singular.
fn solve(mut a: Vec<Vec<f64>>, mut b: Vec<f64>) -> Option<Vec<f64>> {
    let n = b.len();
    for col in 0..n {
        // Partial pivoting keeps the elimination stable.
        let pivot = (col..n).max_by(|&i, &j| {
            a[i][col]
                .abs()
                .partial_cmp(&a[j][col].abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
        if a[pivot][col].abs() < 1e-18 {
            return None;
        }
        a.swap(col, pivot);
        b.swap(col, pivot);
        for row in (col + 1)..n {
            let f = a[row][col] / a[col][col];
            for k in col..n {
                a[row][k] -= f * a[col][k];
            }
            b[row] -= f * b[col];
        }
    }
    let mut x = vec![0.0; n];
    for row in (0..n).rev() {
        let mut sum = b[row];
        for k in (row + 1)..n {
            sum -= a[row][k] * x[k];
        }
        x[row] = sum / a[row][row];
    }
    Some(x)
}

/// Root-mean-square residual over the observations, arcseconds.
pub fn rms_arcsec(state: &State, epoch_jd: f64, observations: &[Observation]) -> Option<f64> {
    if observations.is_empty() {
        return None;
    }
    let observers: Vec<[f64; 3]> = observations
        .iter()
        .map(|obs| observer_position(obs.jd, &obs.site))
        .collect();
    rms_and_light_times(
        state,
        epoch_jd,
        observations,
        &observers,
        &weights(observations),
    )
    .map(|(_, rms, _)| rms)
}

/// The weighted and plain rms residuals, arcseconds, and each observation's
/// light time.
fn rms_and_light_times(
    state: &State,
    epoch_jd: f64,
    observations: &[Observation],
    observers: &[[f64; 3]],
    weights: &[f64],
) -> Option<(f64, f64, Vec<f64>)> {
    let (mut weighted, mut plain) = (0.0, 0.0);
    let mut light_times = Vec::with_capacity(observations.len());
    for ((obs, observer), w) in observations.iter().zip(observers).zip(weights) {
        let ((dra, ddec), tau) = residual(state, epoch_jd, obs, observer, None)?;
        let sq = dra * dra + ddec * ddec;
        weighted += w * w * sq;
        plain += sq;
        light_times.push(tau);
    }
    let n = observations.len() as f64;
    Some(((weighted / n).sqrt(), (plain / n).sqrt(), light_times))
}

/// Refine `initial` against `observations` by Gauss-Newton.
///
/// Needs at least three positions to constrain six parameters. Damping is
/// raised whenever a step makes the fit worse, which keeps a poor starting
/// state from diverging.
pub fn fit_orbit(
    observations: &[Observation],
    initial: &State,
    epoch_jd: f64,
    max_iterations: usize,
) -> Option<OrbitFit> {
    fit_orbit_with(
        observations,
        initial,
        epoch_jd,
        max_iterations,
        None,
        Conics::Bound,
    )
}

/// [`fit_orbit`], abandoning the fit early when `give_up` says it cannot pass.
pub fn fit_orbit_with(
    observations: &[Observation],
    initial: &State,
    epoch_jd: f64,
    max_iterations: usize,
    give_up: Option<GiveUp>,
    conics: Conics,
) -> Option<OrbitFit> {
    fit(
        observations,
        initial,
        epoch_jd,
        max_iterations,
        give_up,
        MIN_IMPROVEMENT_ARCSEC,
        conics,
    )
}

/// [`fit_orbit`], run until no step improves the fit rather than until one
/// improves it by little.
///
/// From a poor seed a short arc's fit can crawl along a curved valley, gaining
/// less than [`MIN_IMPROVEMENT_ARCSEC`] an iteration for a while before it
/// drops towards the minimum. [`fit_orbit`] reads the crawl as convergence and
/// reports the rms where it stopped, which then says more about the seed than
/// about the orbit. That is fine for screening candidates against a gate, but a
/// residual that is reported or ranked on should come from here.
pub fn converge_orbit(
    observations: &[Observation],
    initial: &State,
    epoch_jd: f64,
    max_iterations: usize,
    conics: Conics,
) -> Option<OrbitFit> {
    fit(
        observations,
        initial,
        epoch_jd,
        max_iterations,
        None,
        0.0,
        conics,
    )
}

/// Fit `seed` against a residual gate: screened with [`GiveUp::for_gate`],
/// and run to convergence if it passes.
///
/// The fit comes back whether or not it passed, so the caller compares its
/// rms with the gate; one abandoned by the screen is simply above it. `None`
/// when the observations cannot constrain an orbit. An unbound fit that
/// [`keeps_unbound`] rejects gives way to the best bound one.
pub fn fit_within(
    observations: &[Observation],
    seed: &State,
    epoch_jd: f64,
    gate_arcsec: f64,
    conics: Conics,
) -> Option<OrbitFit> {
    let fit = screen_then_converge(observations, seed, epoch_jd, gate_arcsec, conics)?;
    if is_bound(&fit.state) {
        return Some(fit);
    }
    let bound = [*seed, bound_seed(&fit.state)]
        .iter()
        .filter_map(|s| screen_then_converge(observations, s, epoch_jd, gate_arcsec, Conics::Bound))
        .min_by(|a, b| a.rms_arcsec.total_cmp(&b.rms_arcsec));
    let against = bound.as_ref().map(|b| (b.rms_arcsec, &b.state));
    if keeps_unbound(conics, fit.rms_arcsec, against) {
        Some(fit)
    } else {
        bound
    }
}

/// Whether an unbound fit leaving `rms` arcseconds stands against `bound`, the
/// best bound fit of the same astrometry (its rms and state): it must be
/// within the limit `conics` sets, and `bound` must not [`bound_suffices`].
pub fn keeps_unbound(conics: Conics, rms: f64, bound: Option<(f64, &State)>) -> bool {
    let Conics::Unbound { max_rms_arcsec, .. } = conics else {
        return false;
    };
    rms <= max_rms_arcsec
        && !bound.is_some_and(|(bound_rms, state)| bound_suffices(rms, bound_rms, state))
}

/// Eccentricity past which a bound fit is pressed against e = 1, where one kept
/// off hyperbolae stops when it wants one.
const PINNED_ECCENTRICITY: f64 = 0.99;

/// Whether a bound fit at `bound_rms` arcseconds should replace an unbound one
/// at `rms`: it fits as well, or within twice and not pressed against e = 1.
fn bound_suffices(rms: f64, bound_rms: f64, bound: &State) -> bool {
    bound_rms <= rms
        || (bound_rms <= 2.0 * rms
            && state_to_elements(bound, 0.0).is_some_and(|el| el.e < PINNED_ECCENTRICITY))
}

fn screen_then_converge(
    observations: &[Observation],
    seed: &State,
    epoch_jd: f64,
    gate_arcsec: f64,
    conics: Conics,
) -> Option<OrbitFit> {
    let screened = fit_orbit_with(
        observations,
        seed,
        epoch_jd,
        SCREEN_ITERATIONS,
        Some(GiveUp::for_gate(gate_arcsec)),
        conics,
    )?;
    if screened.rms_arcsec > gate_arcsec {
        return Some(screened);
    }
    // Starts where the screen stopped, so it can only improve on it.
    converge_orbit(
        observations,
        &screened.state,
        epoch_jd,
        CONVERGE_ITERATIONS,
        conics,
    )
    .or(Some(screened))
}

fn fit(
    observations: &[Observation],
    initial: &State,
    epoch_jd: f64,
    max_iterations: usize,
    give_up: Option<GiveUp>,
    min_improvement_arcsec: f64,
    conics: Conics,
) -> Option<OrbitFit> {
    if observations.len() < 3 || !conics.admits(initial) {
        return None;
    }
    // The observer depends only on the epoch, so it is placed once per
    // observation rather than once per prediction.
    let observers: Vec<[f64; 3]> = observations
        .iter()
        .map(|obs| observer_position(obs.jd, &obs.site))
        .collect();
    let weights = weights(observations);
    let mut state = *initial;
    let (mut best, mut plain, mut light_times) =
        rms_and_light_times(&state, epoch_jd, observations, &observers, &weights)?;
    let mut lambda = 1e-3;
    let mut iterations = 0;
    let steps = [
        POS_STEP_AU,
        POS_STEP_AU,
        POS_STEP_AU,
        VEL_STEP_AU_PER_DAY,
        VEL_STEP_AU_PER_DAY,
        VEL_STEP_AU_PER_DAY,
    ];

    for _ in 0..max_iterations {
        if let Some(give_up) = give_up {
            if iterations >= give_up.after_iterations && plain > give_up.above_arcsec {
                break;
            }
        }
        iterations += 1;

        let mut ata = vec![vec![0.0; 6]; 6];
        let mut atb = vec![0.0; 6];
        let mut usable = true;
        'obs: for (((obs, observer), &tau), w) in observations
            .iter()
            .zip(&observers)
            .zip(&light_times)
            .zip(&weights)
        {
            // The residual and every nudge of it share one light time, so the
            // differences measure the state change and nothing else.
            let Some(((r_ra, r_dec), _)) = residual(&state, epoch_jd, obs, observer, Some(tau))
            else {
                usable = false;
                break 'obs;
            };
            let mut jac_ra = [0.0; 6];
            let mut jac_dec = [0.0; 6];
            for (k, step) in steps.iter().enumerate() {
                let up = perturb(&state, k, *step);
                if !conics.admits(&up) {
                    usable = false;
                    break 'obs;
                }
                let Some(((ra_up, dec_up), _)) = residual(&up, epoch_jd, obs, observer, Some(tau))
                else {
                    usable = false;
                    break 'obs;
                };
                // Forward differences: half the predictions central ones take,
                // and Gauss-Newton only needs the Jacobian to point the step.
                // Residual falls as the model improves, hence the sign.
                jac_ra[k] = -(ra_up - r_ra) / step;
                jac_dec[k] = -(dec_up - r_dec) / step;
            }
            let w2 = w * w;
            for i in 0..6 {
                atb[i] += w2 * (jac_ra[i] * r_ra + jac_dec[i] * r_dec);
                for j in 0..6 {
                    ata[i][j] += w2 * (jac_ra[i] * jac_ra[j] + jac_dec[i] * jac_dec[j]);
                }
            }
        }
        if !usable {
            break;
        }

        // Levenberg damping on the diagonal.
        let mut damped = ata.clone();
        for (i, row) in damped.iter_mut().enumerate() {
            row[i] *= 1.0 + lambda;
        }
        let Some(delta) = solve(damped, atb.clone()) else {
            break;
        };

        let mut candidate = state;
        // A non-finite step is the same kind of failure: damp rather than abort.
        if delta.iter().any(|d| !d.is_finite()) {
            lambda *= 10.0;
            if lambda > 1e8 {
                break;
            }
            continue;
        }
        for (k, d) in delta.iter().enumerate() {
            candidate = perturb(&candidate, k, *d);
        }

        let trial = conics
            .admits(&candidate)
            .then(|| rms_and_light_times(&candidate, epoch_jd, observations, &observers, &weights))
            .flatten();
        match trial {
            Some((trial, trial_plain, trial_light_times)) if trial < best => {
                let improvement = best - trial;
                state = candidate;
                best = trial;
                plain = trial_plain;
                light_times = trial_light_times;
                lambda = (lambda * 0.5).max(1e-9);
                // Converged once the fit stops moving.
                if improvement < min_improvement_arcsec {
                    break;
                }
            }
            _ => {
                lambda *= 10.0;
                if lambda > 1e8 {
                    break;
                }
            }
        }
    }

    let elements = state_to_elements(&state, epoch_jd)?;
    Some(OrbitFit {
        state,
        epoch_jd,
        elements,
        rms_arcsec: plain,
        iterations,
        n_obs: observations.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::sso_geometry::heliocentric_position;
    use crate::utils::sso_geometry::ZTF;

    fn ceres_like() -> OrbitalElements {
        OrbitalElements::elliptical(2460000.5, 2.7658, 0.0785, 10.588, 80.25, 73.6, 100.0)
    }

    /// True state of `elements` at `jd`.
    fn truth_state(elements: &OrbitalElements, jd: f64) -> State {
        let pos = heliocentric_position(elements, jd);
        let step = 0.05;
        let a = heliocentric_position(elements, jd - step);
        let b = heliocentric_position(elements, jd + step);
        State {
            pos,
            vel: [
                (b[0] - a[0]) / (2.0 * step),
                (b[1] - a[1]) / (2.0 * step),
                (b[2] - a[2]) / (2.0 * step),
            ],
        }
    }

    /// Astrometry the object would produce on the given nights.
    fn observations(elements: &OrbitalElements, epoch: f64, jds: &[f64]) -> Vec<Observation> {
        let state = truth_state(elements, epoch);
        jds.iter()
            .map(|&jd| {
                let (ra, dec) = predict_radec(&state, epoch, jd, &ZTF).expect("ephemeris");
                Observation {
                    jd,
                    ra,
                    dec,
                    site: ZTF,
                }
            })
            .collect()
    }

    const EPOCH: f64 = 2460015.0;
    const NIGHTS: [f64; 6] = [
        2460010.0, 2460010.05, 2460013.0, 2460013.05, 2460020.0, 2460020.05,
    ];

    /// Between two nearby observatories the residual does not reveal a site
    /// error at all: the fit absorbs it into the orbit and still looks clean.
    /// What moves is where that orbit says the object will be, so a search that
    /// skipped the sites would quietly produce unrecoverable predictions.
    /// Measured on real CSS and ZTF astrometry: residual 0.19" to 0.20", with a
    /// median orbit drift of 4.2" after a month.
    #[test]
    fn test_a_nearby_site_error_hides_in_the_orbit_not_the_residual() {
        use crate::utils::sso_geometry::MT_LEMMON;

        let el = ceres_like();
        let truth = truth_state(&el, EPOCH);
        // Palomar and Mt Lemmon, ~500 km apart, as CSS and ZTF are, on the
        // two-night arc a real cross-survey link produces: one survey's
        // tracklet on each night, which is far looser than a long arc.
        let arc: [(f64, Site); 4] = [
            (EPOCH - 1.0, MT_LEMMON),
            (EPOCH - 0.97, MT_LEMMON),
            (EPOCH + 1.0, ZTF),
            (EPOCH + 1.03, ZTF),
        ];
        let honest: Vec<Observation> = arc
            .iter()
            .map(|&(jd, site)| {
                let (ra, dec) = predict_radec(&truth, EPOCH, jd, &site).expect("ephemeris");
                Observation { jd, ra, dec, site }
            })
            .collect();
        let mislabelled: Vec<Observation> = honest
            .iter()
            .map(|o| Observation { site: ZTF, ..*o })
            .collect();

        let start = State {
            pos: [
                truth.pos[0] + 1e-3,
                truth.pos[1] - 8e-4,
                truth.pos[2] + 4e-4,
            ],
            vel: [
                truth.vel[0] + 2e-5,
                truth.vel[1] - 1e-5,
                truth.vel[2] + 5e-6,
            ],
        };
        let right = fit_orbit(&honest, &start, EPOCH, 60).expect("fits");
        let wrong = fit_orbit(&mislabelled, &start, EPOCH, 60).expect("fits");

        // The gate sees nothing: both look like good fits.
        assert!(
            wrong.rms_arcsec < 2.0,
            "the mislabelled fit should still pass the gate, got {:.2}\"",
            wrong.rms_arcsec
        );

        // The orbits disagree about where the object goes.
        let later = EPOCH + 30.0;
        let (r1, d1) = predict_radec(&right.state, EPOCH, later, &ZTF).expect("ephemeris");
        let (r2, d2) = predict_radec(&wrong.state, EPOCH, later, &ZTF).expect("ephemeris");
        let drift = (((r2 - r1 + 540.0).rem_euclid(360.0) - 180.0) * d1.to_radians().cos())
            .hypot(d2 - d1)
            * 3600.0;
        assert!(
            drift > 1.0,
            "a site error should bias the orbit even when the residual hides it, drift {drift:.2}\""
        );
    }

    /// Astrometry from two observatories fits only when each observation says
    /// which one it came from. Told they all came from one, the fit has to
    /// absorb the parallax between them into the orbit, and cannot: it leaves
    /// a residual far above the gate the linker accepts at.
    #[test]
    fn test_an_arc_from_two_sites_needs_its_sites() {
        use crate::utils::sso_geometry::SIDING_SPRING;

        let el = ceres_like();
        let truth = truth_state(&el, EPOCH);
        // Alternate Palomar and Siding Spring: opposite hemispheres, so the
        // baseline between them is the whole Earth.
        let honest: Vec<Observation> = NIGHTS
            .iter()
            .enumerate()
            .map(|(k, &jd)| {
                let site = if k % 2 == 0 { ZTF } else { SIDING_SPRING };
                let (ra, dec) = predict_radec(&truth, EPOCH, jd, &site).expect("ephemeris");
                Observation { jd, ra, dec, site }
            })
            .collect();
        // The same positions, every one claiming to be from Palomar.
        let mislabelled: Vec<Observation> = honest
            .iter()
            .map(|o| Observation { site: ZTF, ..*o })
            .collect();

        let start = State {
            pos: [
                truth.pos[0] + 1e-3,
                truth.pos[1] - 8e-4,
                truth.pos[2] + 4e-4,
            ],
            vel: [
                truth.vel[0] + 2e-5,
                truth.vel[1] - 1e-5,
                truth.vel[2] + 5e-6,
            ],
        };

        let right = fit_orbit(&honest, &start, EPOCH, 60).expect("the honest arc fits");
        let wrong = fit_orbit(&mislabelled, &start, EPOCH, 60).expect("the mislabelled arc fits");

        assert!(
            right.rms_arcsec < 0.1,
            "an arc that names its sites should fit cleanly, got {:.3}\"",
            right.rms_arcsec
        );
        assert!(
            wrong.rms_arcsec > 1.0,
            "mislabelling every site as Palomar should leave a large residual, got {:.3}\"",
            wrong.rms_arcsec
        );
        assert!(
            wrong.rms_arcsec > 10.0 * right.rms_arcsec,
            "the site information should matter by an order of magnitude: {:.3}\" vs {:.3}\"",
            wrong.rms_arcsec,
            right.rms_arcsec
        );
    }

    #[test]
    fn test_recovers_a_perturbed_state() {
        let el = ceres_like();
        let obs = observations(&el, EPOCH, &NIGHTS);
        let truth = truth_state(&el, EPOCH);
        // Displace far more than linking's clustering tolerance allows.
        let start = State {
            pos: [
                truth.pos[0] + 0.01,
                truth.pos[1] - 0.008,
                truth.pos[2] + 0.004,
            ],
            vel: [
                truth.vel[0] + 2e-4,
                truth.vel[1] - 1e-4,
                truth.vel[2] + 5e-5,
            ],
        };
        let before = rms_arcsec(&start, EPOCH, &obs).expect("residual");
        let fit = fit_orbit(&obs, &start, EPOCH, 60).expect("fit");
        assert!(before > 100.0, "start should be far off, was {before}");
        assert!(
            fit.rms_arcsec < 0.1,
            "rms {} after {} iterations",
            fit.rms_arcsec,
            fit.iterations
        );
    }

    /// Nights spread over three months, long enough to bend the arc.
    const LONG_ARC: [f64; 8] = [
        2460010.0, 2460010.05, 2460040.0, 2460040.05, 2460070.0, 2460070.05, 2460100.0, 2460100.05,
    ];

    #[test]
    fn test_recovered_elements_match_the_truth_on_a_long_arc() {
        let el = ceres_like();
        let obs = observations(&el, EPOCH, &LONG_ARC);
        let truth = truth_state(&el, EPOCH);
        let start = State {
            pos: [truth.pos[0] + 0.005, truth.pos[1], truth.pos[2]],
            vel: truth.vel,
        };
        let fit = fit_orbit(&obs, &start, EPOCH, 60).expect("fit");
        assert!(
            (fit.elements.a - el.a).abs() < 0.02,
            "a {} vs {}",
            fit.elements.a,
            el.a
        );
        assert!((fit.elements.e - el.e).abs() < 0.02);
        assert!((fit.elements.incl - el.incl).abs() < 0.5);
    }

    #[test]
    fn test_a_short_arc_fits_the_sky_without_pinning_the_orbit() {
        let el = ceres_like();
        let obs = observations(&el, EPOCH, &NIGHTS);
        let truth = truth_state(&el, EPOCH);
        let start = State {
            pos: [truth.pos[0] + 0.005, truth.pos[1], truth.pos[2]],
            vel: truth.vel,
        };
        let fit = fit_orbit(&obs, &start, EPOCH, 60).expect("fit");
        // Ten days of arc leaves the semimajor axis loose even when the
        // positions are reproduced, so a track this short is not an orbit.
        assert!(
            fit.rms_arcsec < 1.0,
            "sky fit should be good, rms {}",
            fit.rms_arcsec
        );
        assert!(
            (fit.elements.a - el.a).abs() > 0.02,
            "short arc unexpectedly pinned a"
        );
    }

    #[test]
    fn test_rejects_too_few_observations() {
        let el = ceres_like();
        let obs = observations(&el, EPOCH, &NIGHTS[..2]);
        assert!(fit_orbit(&obs, &truth_state(&el, EPOCH), EPOCH, 20).is_none());
    }

    #[test]
    fn test_a_true_state_stays_put() {
        let el = ceres_like();
        let obs = observations(&el, EPOCH, &NIGHTS);
        let truth = truth_state(&el, EPOCH);
        let fit = fit_orbit(&obs, &truth, EPOCH, 20).expect("fit");
        assert!(fit.rms_arcsec < 1e-3, "rms {}", fit.rms_arcsec);
    }

    /// A bound fit pressed against e = 1 does not displace a better hyperbola.
    #[test]
    fn test_a_bound_fit_at_e_1_leaves_a_better_hyperbola() {
        let (r, mu) = (2.0, 0.017_202_098_95_f64.powi(2));
        let at = |speed: f64| State {
            pos: [r, 0.0, 0.0],
            vel: [0.0, speed, 0.0],
        };
        let pinned = at(0.999 * (2.0 * mu / r).sqrt());
        let circular = at((mu / r).sqrt());
        assert!(!bound_suffices(0.1, 0.15, &pinned));
        assert!(bound_suffices(0.1, 0.09, &pinned));
        assert!(bound_suffices(0.1, 0.15, &circular));
        assert!(!bound_suffices(0.1, 0.25, &circular));
    }

    #[test]
    fn test_scattered_positions_do_not_fit() {
        let el = ceres_like();
        let mut obs = observations(&el, EPOCH, &NIGHTS);
        // Push one position a degree away: no orbit passes through them all.
        obs[3].dec += 1.0;
        let fit = fit_orbit(&obs, &truth_state(&el, EPOCH), EPOCH, 60).expect("fit");
        assert!(
            fit.rms_arcsec > 100.0,
            "a bad arc should not fit, rms {}",
            fit.rms_arcsec
        );
    }
}
