/// Gamma(0.75), the normalisation of the offset likelihood.
const GAMMA_0_75: f64 = 1.2254167024651776;

/// Value at a fractional offset of exactly zero, where the Gamma(a < 1) density
/// diverges. Large but finite, so the posterior total stays finite.
const OFFSET_LIKELIHOOD_AT_ZERO: f64 = 1e6;

/// Gamma(a = 0.75) density of the fractional offset: x^(-0.25) exp(-x) / Γ(0.75).
pub fn offset_likelihood(fractional_offset: f64) -> f64 {
    if !fractional_offset.is_finite() || fractional_offset < 0.0 {
        return 0.0;
    }
    if fractional_offset == 0.0 {
        return OFFSET_LIKELIHOOD_AT_ZERO;
    }
    fractional_offset.powf(-0.25) * (-fractional_offset).exp() / GAMMA_0_75
}

/// Gaussian on the redshift difference, uninformative unless both are known.
pub fn redshift_likelihood(
    galaxy_z: Option<f64>,
    galaxy_z_err: Option<f64>,
    transient_z: Option<f64>,
    transient_z_err: Option<f64>,
) -> f64 {
    let (Some(gz), Some(tz)) = (galaxy_z, transient_z) else {
        return 1.0;
    };
    // A catalog may carry a null or zero uncertainty, which would divide by zero.
    let err = |e: Option<f64>| e.filter(|e| e.is_finite() && *e > 0.0).unwrap_or(0.01);
    let gz_err = err(galaxy_z_err);
    let tz_err = err(transient_z_err);
    let dz = gz - tz;
    (-0.5 * dz * dz / (gz_err * gz_err + tz_err * tz_err)).exp()
}

/// Not implemented: returns 1.0, so `use_absmag` does not affect ranking.
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
        let l1 = offset_likelihood(0.1);
        let l2 = offset_likelihood(1.0);
        let l3 = offset_likelihood(5.0);
        assert!(l1 > l2);
        assert!(l2 > l3);
    }

    #[test]
    fn test_offset_likelihood_at_one() {
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
        assert_close!(
            redshift_likelihood(Some(0.05), Some(0.001), None, None),
            1.0
        );
    }

    #[test]
    fn test_redshift_likelihood_zero_uncertainty() {
        let l = redshift_likelihood(Some(0.05), Some(0.0), Some(0.05), Some(0.0));
        assert!(l.is_finite());
        assert_close!(l, 1.0, epsilon = 1e-10);
    }
}
