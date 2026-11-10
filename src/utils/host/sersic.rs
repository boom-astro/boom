//! Converting a Tractor half-light radius to a 25 mag/arcsec^2 isophotal size,
//! so Legacy Survey and NED-LVS D25 sizes rank on one scale.
//!
//! Follows A. Gregory, "The Identification of Host Galaxies of Fast
//! Transients", section 2.2.

use std::f64::consts::{LN_10, PI};

/// Surface brightness defining the isophote, mag/arcsec^2.
pub const MU_25: f64 = 25.0;
/// Legacy Survey fluxes are nanomaggies: m = 22.5 - 2.5 log10(flux).
const NANOMAGGY_ZP: f64 = 22.5;

/// Sersic index implied by a Legacy Survey morphological type.
///
/// Only SER carries a fitted index; the others are fixed by definition, so a
/// null `sersic` on those types is not a missing value.
pub fn sersic_index_for_type(objtype: &str, fitted: Option<f64>) -> Option<f64> {
    match objtype {
        "REX" | "EXP" => Some(1.0),
        "DEV" => Some(4.0),
        "SER" => fitted.filter(|n| n.is_finite() && *n > 0.0),
        _ => None,
    }
}

pub fn total_mag(flux_nanomaggy: f64) -> Option<f64> {
    (flux_nanomaggy > 0.0 && flux_nanomaggy.is_finite())
        .then(|| NANOMAGGY_ZP - 2.5 * flux_nanomaggy.log10())
}

/// Log of the gamma function, Lanczos approximation with g = 7, n = 9.
fn ln_gamma(x: f64) -> f64 {
    #[allow(clippy::excessive_precision)]
    const C: [f64; 9] = [
        0.999_999_999_999_809_93,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_13,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    if x < 0.5 {
        // Reflection: Gamma(x) Gamma(1-x) = pi / sin(pi x).
        return (PI / (PI * x).sin()).ln() - ln_gamma(1.0 - x);
    }
    let x = x - 1.0;
    let mut a = C[0];
    let t = x + 7.5;
    for (i, c) in C.iter().enumerate().skip(1) {
        a += c / (x + i as f64);
    }
    0.5 * (2.0 * PI).ln() + (x + 0.5) * t.ln() - t + a.ln()
}

/// Regularised lower incomplete gamma P(a, x), by series below the transition
/// and continued fraction above it.
fn gamma_p(a: f64, x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    let scale = (-x + a * x.ln() - ln_gamma(a)).exp();
    if x < a + 1.0 {
        let mut ap = a;
        let mut sum = 1.0 / a;
        let mut del = sum;
        for _ in 0..500 {
            ap += 1.0;
            del *= x / ap;
            sum += del;
            if del.abs() < sum.abs() * 1e-15 {
                break;
            }
        }
        return sum * scale;
    }
    // Continued fraction for Q(a, x), then P = 1 - Q.
    let tiny = 1e-300;
    let mut b = x + 1.0 - a;
    let mut c = 1.0 / tiny;
    let mut d = 1.0 / b;
    let mut h = d;
    for i in 1..500 {
        let an = -(i as f64) * (i as f64 - a);
        b += 2.0;
        d = an * d + b;
        if d.abs() < tiny {
            d = tiny;
        }
        c = b + an / c;
        if c.abs() < tiny {
            c = tiny;
        }
        d = 1.0 / d;
        let del = d * c;
        h *= del;
        if (del - 1.0).abs() < 1e-15 {
            break;
        }
    }
    1.0 - scale * h
}

/// Sersic constant b_n, the median of a Gamma(2n) distribution, by bisection.
pub fn sersic_b(n: f64) -> Option<f64> {
    if !n.is_finite() || n <= 0.0 {
        return None;
    }
    let a = 2.0 * n;
    // b_n ~ 2n - 1/3, well inside this bracket for the 0 < n <= 10 Tractor fits.
    let (mut lo, mut hi) = (1e-8, 4.0 * a + 20.0);
    if gamma_p(a, hi) < 0.5 {
        return None;
    }
    for _ in 0..200 {
        let mid = 0.5 * (lo + hi);
        if gamma_p(a, mid) < 0.5 {
            lo = mid;
        } else {
            hi = mid;
        }
        if hi - lo < 1e-12 * hi.max(1.0) {
            break;
        }
    }
    Some(0.5 * (lo + hi))
}

/// `None` when the model never reaches the isophote: a galaxy already fainter
/// than `mu_target` at its centre gets no size rather than a fabricated one.
pub fn isophotal_semi_major(r_e: f64, q: f64, n: f64, m_tot: f64, mu_target: f64) -> Option<f64> {
    if !(r_e.is_finite() && r_e > 0.0 && q.is_finite() && q > 0.0 && m_tot.is_finite()) {
        return None;
    }
    let b_n = sersic_b(n)?;

    let mu_bar_e = m_tot + 2.5 * (2.0 * PI * r_e * r_e * q).log10();
    let mu_e =
        mu_bar_e + 2.5 * (n * b_n.exp() * b_n.powf(-2.0 * n) * ln_gamma(2.0 * n).exp()).log10();
    let bracket = 1.0 + (LN_10 / (2.5 * b_n)) * (mu_target - mu_e);

    (bracket > 0.0)
        .then(|| r_e * bracket.powf(n))
        .filter(|a25| a25.is_finite() && *a25 > 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    // n = 1/2 is exact: a = 2n = 1 makes P(1, x) = 1 - exp(-x), so b = ln 2.
    #[test]
    fn test_sersic_b_matches_published_values() {
        for (n, expected) in [
            (1.0, 1.678_346_99),
            (4.0, 7.669_249_6),
            (0.5, std::f64::consts::LN_2),
        ] {
            let b = sersic_b(n).expect("solvable");
            assert!(
                (b - expected).abs() < 1e-5,
                "b_{n} = {b}, expected {expected}"
            );
        }
    }

    #[test]
    fn test_sersic_b_approaches_the_asymptotic_form() {
        for n in [2.0, 4.0, 8.0] {
            let b = sersic_b(n).unwrap();
            assert!((b - (2.0 * n - 1.0 / 3.0)).abs() < 0.01, "n={n} b={b}");
        }
    }

    #[test]
    fn test_gamma_p_is_a_distribution_function() {
        assert!(gamma_p(2.0, 0.0).abs() < 1e-12);
        assert!((gamma_p(2.0, 1e6) - 1.0).abs() < 1e-9);
        let (mut prev, mut x) = (0.0, 0.0);
        while x < 20.0 {
            let p = gamma_p(3.0, x);
            assert!(p >= prev - 1e-12, "not monotonic at x={x}");
            prev = p;
            x += 0.25;
        }
    }

    #[test]
    fn test_ln_gamma_matches_factorials() {
        for (x, fact) in [(1.0, 1.0), (2.0, 1.0), (5.0, 24.0), (7.0, 720.0)] {
            assert!(
                (ln_gamma(x).exp() - fact).abs() < 1e-6 * fact.max(1.0),
                "x={x}"
            );
        }
    }

    #[test]
    fn test_isophotal_size_exceeds_half_light_radius() {
        let a25 = isophotal_semi_major(2.0, 0.7, 1.0, 18.0, MU_25).expect("isophote exists");
        assert!(a25 > 2.0, "a25 = {a25} should exceed R_e = 2");
    }

    #[test]
    fn test_low_surface_brightness_source_has_no_isophote() {
        assert!(isophotal_semi_major(10.0, 1.0, 1.0, 26.0, MU_25).is_none());
    }

    #[test]
    fn test_brighter_galaxy_has_a_larger_isophote() {
        let bright = isophotal_semi_major(2.0, 0.8, 1.0, 16.0, MU_25).unwrap();
        let faint = isophotal_semi_major(2.0, 0.8, 1.0, 19.0, MU_25).unwrap();
        assert!(
            bright > faint,
            "bright {bright} should exceed faint {faint}"
        );
    }

    #[test]
    fn test_sersic_index_by_type() {
        assert_eq!(sersic_index_for_type("EXP", None), Some(1.0));
        assert_eq!(sersic_index_for_type("REX", None), Some(1.0));
        assert_eq!(sersic_index_for_type("DEV", None), Some(4.0));
        assert_eq!(sersic_index_for_type("SER", Some(2.5)), Some(2.5));
        assert_eq!(sersic_index_for_type("SER", None), None);
        assert_eq!(sersic_index_for_type("PSF", Some(1.0)), None);
        assert_eq!(sersic_index_for_type("DUP", None), None);
    }

    #[test]
    fn test_total_mag_rejects_non_positive_flux() {
        assert!(total_mag(-5.0).is_none());
        assert!(total_mag(0.0).is_none());
        assert!((total_mag(1.0).unwrap() - 22.5).abs() < 1e-12);
    }
}
