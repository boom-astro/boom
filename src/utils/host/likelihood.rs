//! Likelihood functions for host galaxy association.
//!
//! The primary likelihood is based on the fractional offset (separation / DLR),
//! which for true hosts follows a Gamma(a = 0.75) distribution.

/// Γ(0.75) — the gamma function evaluated at 0.75.
const GAMMA_0_75: f64 = 1.2254167024651776;

/// Value returned at a fractional offset of exactly zero, where the
/// Gamma(a < 1) density diverges. Large but finite so that a transient sitting
/// precisely on a galaxy centroid dominates the posterior without producing a
/// non-finite total.
const OFFSET_LIKELIHOOD_AT_ZERO: f64 = 1e6;

/// Compute the offset likelihood using a Gamma(a=0.75) distribution.
///
/// PDF: f(x; a=0.75) = x^(a-1) * exp(-x) / Γ(a)
///                   = x^(-0.25) * exp(-x) / Γ(0.75)
///
/// This models the distribution of fractional offsets (separation/DLR)
/// for true host galaxies.
pub fn offset_likelihood(fractional_offset: f64) -> f64 {
    if !fractional_offset.is_finite() || fractional_offset < 0.0 {
        return 0.0;
    }
    if fractional_offset == 0.0 {
        return OFFSET_LIKELIHOOD_AT_ZERO;
    }
    fractional_offset.powf(-0.25) * (-fractional_offset).exp() / GAMMA_0_75
}

/// Compute the redshift likelihood.
///
/// If both transient and galaxy have redshifts, use a Gaussian centred on the
/// transient's redshift. If either is unknown the term is uninformative and
/// returns 1.0, leaving the offset term to carry the posterior.
pub fn redshift_likelihood(
    galaxy_z: Option<f64>,
    galaxy_z_err: Option<f64>,
    transient_z: Option<f64>,
    transient_z_err: Option<f64>,
) -> f64 {
    match (galaxy_z, transient_z) {
        (Some(gz), Some(tz)) => {
            // Guard the fallbacks: a catalog may carry a null or zero
            // uncertainty, which would otherwise divide by zero.
            let gz_err = galaxy_z_err
                .filter(|e| e.is_finite() && *e > 0.0)
                .unwrap_or(0.01);
            let tz_err = transient_z_err
                .filter(|e| e.is_finite() && *e > 0.0)
                .unwrap_or(0.01);
            let sigma2 = gz_err * gz_err + tz_err * tz_err;
            let dz = gz - tz;
            (-0.5 * dz * dz / sigma2).exp()
        }
        _ => 1.0,
    }
}

/// Compute the absolute magnitude likelihood.
///
/// Not yet implemented; returns 1.0 (uninformative), so enabling it via
/// `use_absmag` currently has no effect on ranking. Implementing it means
/// evaluating a Schechter luminosity function:
///   M = m - 5*log10(d_L/10pc) - K(z)
///   L(M) ∝ 10^(0.4*(M*-M)*(α+1)) * exp(-10^(0.4*(M*-M)))
pub fn absmag_likelihood(_mag: Option<f64>, _mag_err: Option<f64>, _redshift: Option<f64>) -> f64 {
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_likelihood_zero() {
        assert_close!(offset_likelihood(0.0), OFFSET_LIKELIHOOD_AT_ZERO);
    }

    #[test]
    fn test_offset_likelihood_decreasing() {
        // Since a < 1 the density diverges as x → 0+, so it is monotonically
        // decreasing over the range we care about.
        let l1 = offset_likelihood(0.1);
        let l2 = offset_likelihood(1.0);
        let l3 = offset_likelihood(5.0);
        assert!(l1 > l2);
        assert!(l2 > l3);
    }

    #[test]
    fn test_offset_likelihood_at_one() {
        // f(1) = 1^(-0.25) * exp(-1) / Γ(0.75)
        let expected = (-1.0_f64).exp() / GAMMA_0_75;
        assert_close!(offset_likelihood(1.0), expected, epsilon = 1e-10);
    }

    #[test]
    fn test_offset_likelihood_rejects_bad_input() {
        assert_close!(offset_likelihood(-1.0), 0.0);
        assert_close!(offset_likelihood(f64::NAN), 0.0);
        assert_close!(offset_likelihood(f64::INFINITY), 0.0);
    }

    #[test]
    fn test_redshift_likelihood_matching() {
        let l = redshift_likelihood(Some(0.05), Some(0.001), Some(0.05), Some(0.001));
        assert_close!(l, 1.0, epsilon = 1e-10);
    }

    #[test]
    fn test_redshift_likelihood_discrepant() {
        let l = redshift_likelihood(Some(0.05), Some(0.001), Some(0.5), Some(0.001));
        assert!(l < 1e-10);
    }

    #[test]
    fn test_redshift_likelihood_no_info() {
        assert_close!(redshift_likelihood(None, None, None, None), 1.0);
        // Galaxy redshift alone is uninformative without a transient redshift.
        assert_close!(
            redshift_likelihood(Some(0.05), Some(0.001), None, None),
            1.0
        );
    }

    #[test]
    fn test_redshift_likelihood_zero_uncertainty() {
        // A zero uncertainty must fall back to the default rather than
        // dividing by zero and producing NaN.
        let l = redshift_likelihood(Some(0.05), Some(0.0), Some(0.05), Some(0.0));
        assert!(l.is_finite());
        assert_close!(l, 1.0, epsilon = 1e-10);
    }
}
