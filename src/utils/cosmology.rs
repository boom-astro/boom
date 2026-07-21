/// Planck18 flat ΛCDM parameters (Aghanim et al. 2020, Table 2, TT,TE,EE+lowE+lensing)
const H0: f64 = 67.66; // km/s/Mpc
const OMEGA_M: f64 = 0.3111;
const OMEGA_L: f64 = 1.0 - OMEGA_M;
const C_KM_S: f64 = 299792.458; // speed of light in km/s

/// E(z) = H(z)/H0 for flat ΛCDM
#[inline]
fn e_z(z: f64) -> f64 {
    (OMEGA_M * (1.0 + z).powi(3) + OMEGA_L).sqrt()
}

/// Convert redshift z to luminosity distance in Mpc using Planck18 flat ΛCDM.
/// Uses Simpson's rule with 200 steps; accurate to <0.1% for z ∈ [0, 5].
pub fn luminosity_distance_mpc(z: f64) -> f64 {
    if z <= 0.0 {
        return 0.0;
    }

    let n = 200usize;
    let dz = z / n as f64;

    // Simpson's rule: ∫₀ᶻ dz'/E(z')
    let mut sum = 1.0 / e_z(0.0) + 1.0 / e_z(z);
    for i in 1..n {
        let zi = i as f64 * dz;
        let weight = if i % 2 == 0 { 2.0 } else { 4.0 };
        sum += weight / e_z(zi);
    }
    let comoving_mpc = (C_KM_S / H0) * (dz / 3.0) * sum;

    (1.0 + z) * comoving_mpc
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reference values verified against Planck18 parameters (H0=67.66, Om=0.3111, flat ΛCDM).
    // Tolerance 0.5% — well above the <0.1% numerical error of 200-step Simpson's rule.
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
