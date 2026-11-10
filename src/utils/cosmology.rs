use flare::cosmo::Cosmo;

/// Redshift to Planck18 luminosity distance, in Mpc.
pub fn luminosity_distance_mpc(z: f64) -> f64 {
    if z <= 0.0 {
        return 0.0;
    }
    Cosmo::planck18().luminosity_distance(z)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reference values from astropy Planck18 (H0=67.66, Om=0.3111, flat ΛCDM).
    const TOL: f64 = 0.005;

    fn check(z: f64, expected_mpc: f64) {
        let got = luminosity_distance_mpc(z);
        let err = (got - expected_mpc).abs() / expected_mpc;
        assert!(
            err < TOL,
            "z={z}: got {got:.2} Mpc, expected {expected_mpc:.2} Mpc, err={:.4}%",
            err * 100.0
        );
    }

    #[test]
    fn test_z_zero() {
        assert_eq!(luminosity_distance_mpc(0.0), 0.0);
    }

    #[test]
    fn test_planck18_reference_points() {
        check(0.01, 44.65);
        check(0.1, 475.83);
        check(0.5, 2919.72);
        check(1.0, 6791.73);
        check(2.0, 15926.59);
    }
}
