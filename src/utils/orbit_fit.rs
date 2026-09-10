//! Differential correction: refine a heliocentric state against the astrometry
//! of a linked track.
//!
//! Linking leaves a state good enough to have clustered, which is far looser
//! than an orbit worth reporting. Gauss-Newton on the six state components,
//! with residuals taken on the sky, turns one into the other and yields the
//! residual that says whether the track was real.

use crate::utils::heliolinc::{propagate, radec_from_ecliptic, state_to_elements, State};
use crate::utils::sso_geometry::{earth_position, OrbitalElements};

/// One astrometric position.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Observation {
    pub jd: f64,
    /// Degrees.
    pub ra: f64,
    /// Degrees.
    pub dec: f64,
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

/// Steps used to difference the state numerically.
const POS_STEP_AU: f64 = 1e-6;
const VEL_STEP_AU_PER_DAY: f64 = 1e-8;

/// Where a state puts the object on the sky at `jd`, degrees.
pub fn predict_radec(state: &State, epoch_jd: f64, jd: f64) -> Option<(f64, f64)> {
    let moved = propagate(state, epoch_jd, jd)?;
    let earth = earth_position(jd);
    let topo = [
        moved.pos[0] - earth[0],
        moved.pos[1] - earth[1],
        moved.pos[2] - earth[2],
    ];
    Some(radec_from_ecliptic(&topo))
}

/// Residual in arcseconds, as (RA on a great circle, Dec).
fn residual(state: &State, epoch_jd: f64, obs: &Observation) -> Option<(f64, f64)> {
    let (ra, dec) = predict_radec(state, epoch_jd, obs.jd)?;
    // Fold the RA difference so a wrap does not read as a huge residual.
    let dra =
        ((obs.ra - ra + 540.0).rem_euclid(360.0) - 180.0) * obs.dec.to_radians().cos() * 3600.0;
    Some((dra, (obs.dec - dec) * 3600.0))
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
    let mut sq = 0.0;
    for obs in observations {
        let (dra, ddec) = residual(state, epoch_jd, obs)?;
        sq += dra * dra + ddec * ddec;
    }
    Some((sq / observations.len() as f64).sqrt())
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
    if observations.len() < 3 {
        return None;
    }
    let mut state = *initial;
    let mut best = rms_arcsec(&state, epoch_jd, observations)?;
    let mut lambda = 1e-3;
    let mut iterations = 0;

    for _ in 0..max_iterations {
        iterations += 1;
        let steps = [
            POS_STEP_AU,
            POS_STEP_AU,
            POS_STEP_AU,
            VEL_STEP_AU_PER_DAY,
            VEL_STEP_AU_PER_DAY,
            VEL_STEP_AU_PER_DAY,
        ];

        // Normal equations from the numerical Jacobian.
        let mut ata = vec![vec![0.0; 6]; 6];
        let mut atb = vec![0.0; 6];
        for obs in observations {
            let (r_ra, r_dec) = residual(&state, epoch_jd, obs)?;
            let mut jac_ra = [0.0; 6];
            let mut jac_dec = [0.0; 6];
            for (k, step) in steps.iter().enumerate() {
                let up = perturb(&state, k, *step);
                let down = perturb(&state, k, -*step);
                let (ra_up, dec_up) = residual(&up, epoch_jd, obs)?;
                let (ra_down, dec_down) = residual(&down, epoch_jd, obs)?;
                // Residual falls as the model improves, hence the sign.
                jac_ra[k] = -(ra_up - ra_down) / (2.0 * step);
                jac_dec[k] = -(dec_up - dec_down) / (2.0 * step);
            }
            for i in 0..6 {
                atb[i] += jac_ra[i] * r_ra + jac_dec[i] * r_dec;
                for j in 0..6 {
                    ata[i][j] += jac_ra[i] * jac_ra[j] + jac_dec[i] * jac_dec[j];
                }
            }
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
        for (k, d) in delta.iter().enumerate() {
            if !d.is_finite() {
                return None;
            }
            candidate = perturb(&candidate, k, *d);
        }

        match rms_arcsec(&candidate, epoch_jd, observations) {
            Some(trial) if trial < best => {
                let improvement = best - trial;
                state = candidate;
                best = trial;
                lambda = (lambda * 0.5).max(1e-9);
                // Converged once the fit stops moving.
                if improvement < 1e-4 {
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
        rms_arcsec: best,
        iterations,
        n_obs: observations.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::sso_geometry::heliocentric_position;

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
                let (ra, dec) = predict_radec(&state, epoch, jd).expect("ephemeris");
                Observation { jd, ra, dec }
            })
            .collect()
    }

    const EPOCH: f64 = 2460015.0;
    const NIGHTS: [f64; 6] = [
        2460010.0, 2460010.05, 2460013.0, 2460013.05, 2460020.0, 2460020.05,
    ];

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
