use super::ellipse::Ellipse;

/// Result of a directional light radius computation.
#[derive(Debug, Clone)]
pub struct DlrResult {
    /// Angular separation between transient and galaxy center (arcsec)
    pub separation_arcsec: f64,
    /// Directional light radius: galaxy effective radius along the
    /// direction toward the transient (arcsec)
    pub directional_radius: f64,
    /// Fractional offset = separation / directional_radius
    pub fractional_offset: f64,
}

/// Compute the directional light radius (DLR) for a transient-galaxy pair.
///
/// Uses tangent-plane projection centered on the galaxy to compute offsets,
/// then rotates into the galaxy's ellipse frame to find the effective radius
/// along the direction toward the transient.
pub fn compute_dlr(
    transient_ra: f64,
    transient_dec: f64,
    galaxy_ra: f64,
    galaxy_dec: f64,
    ellipse: &Ellipse,
) -> DlrResult {
    let dec_g_rad = galaxy_dec.to_radians();
    let cos_dec = dec_g_rad.cos();

    // Tangent-plane offsets in arcsec. Wrap the RA difference into
    // (-180, 180] deg *before* scaling, so a pair straddling RA=0 does not
    // pick up a ~1.3e6 arcsec separation.
    let mut dra_deg = transient_ra - galaxy_ra;
    if dra_deg > 180.0 {
        dra_deg -= 360.0;
    } else if dra_deg < -180.0 {
        dra_deg += 360.0;
    }
    let dra = dra_deg * cos_dec * 3600.0;
    let ddec = (transient_dec - galaxy_dec) * 3600.0;

    let separation = dra.hypot(ddec);

    if separation < 1e-15 {
        return DlrResult {
            separation_arcsec: 0.0,
            directional_radius: ellipse.a,
            fractional_offset: 0.0,
        };
    }

    // Rotate into galaxy ellipse frame
    let (sin_pa, cos_pa) = ellipse.pa_rad.sin_cos();
    let x_maj = dra * cos_pa + ddec * sin_pa;
    let y_min = -dra * sin_pa + ddec * cos_pa;

    // Angle of transient in the ellipse frame
    let theta = y_min.atan2(x_maj);
    let (sin_t, cos_t) = theta.sin_cos();

    // Directional radius from the ellipse equation:
    // r(θ) = a*b / sqrt((b*cosθ)² + (a*sinθ)²)
    let denom = (ellipse.b * cos_t).hypot(ellipse.a * sin_t);
    let directional_radius = if denom > 1e-15 {
        ellipse.a * ellipse.b / denom
    } else {
        ellipse.a
    };

    let fractional_offset = separation / directional_radius;

    DlrResult {
        separation_arcsec: separation,
        directional_radius,
        fractional_offset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dlr_on_center() {
        let e = Ellipse::new(2.0, 1.0, 0.0).unwrap();
        let result = compute_dlr(180.0, 45.0, 180.0, 45.0, &e);
        assert_close!(result.separation_arcsec, 0.0);
        assert_close!(result.fractional_offset, 0.0);
    }

    #[test]
    fn test_dlr_along_minor_axis() {
        // Galaxy with PA=0: the major axis lies along RA, the minor along Dec.
        // A transient offset purely in Dec therefore probes the *minor* axis,
        // so the directional radius should come back as b.
        let e = Ellipse::new(4.0, 2.0, 0.0).unwrap();
        let galaxy_dec = 0.0;
        let transient_dec = galaxy_dec + 2.0 / 3600.0;
        let result = compute_dlr(0.0, transient_dec, 0.0, galaxy_dec, &e);
        assert_close!(result.separation_arcsec, 2.0, epsilon = 0.01);
        assert_close!(result.directional_radius, 2.0, epsilon = 0.01);
        assert_close!(result.fractional_offset, 1.0, epsilon = 0.01);
    }

    #[test]
    fn test_dlr_along_major_axis() {
        // Same galaxy, but offset in RA → probes the major axis → radius = a.
        let e = Ellipse::new(4.0, 2.0, 0.0).unwrap();
        let result = compute_dlr(2.0 / 3600.0, 0.0, 0.0, 0.0, &e);
        assert_close!(result.separation_arcsec, 2.0, epsilon = 0.01);
        assert_close!(result.directional_radius, 4.0, epsilon = 0.01);
        assert_close!(result.fractional_offset, 0.5, epsilon = 0.01);
    }

    #[test]
    fn test_dlr_circular_galaxy() {
        let e = Ellipse::new(3.0, 3.0, 0.0).unwrap();
        let result = compute_dlr(10.001, 45.0, 10.0, 45.0, &e);
        assert_close!(result.directional_radius, 3.0, epsilon = 0.01);
    }

    #[test]
    fn test_dlr_ra_wraparound() {
        let e = Ellipse::new(3.0, 3.0, 0.0).unwrap();
        // Galaxy near RA=0, transient near RA=360
        let r1 = compute_dlr(359.999, 0.0, 0.001, 0.0, &e);
        assert!(r1.separation_arcsec < 10.0); // ~7.2 arcsec, not ~1.3M
        assert_close!(r1.separation_arcsec, 7.2, epsilon = 0.01);
    }

    #[test]
    fn test_dlr_ra_wraparound_at_high_dec() {
        // The wrap has to happen in degrees, before scaling by cos(dec)*3600.
        // Wrapping afterwards against a fixed arcsec threshold fails at high
        // declination: here the scaled difference is ~647996 arcsec, just under
        // a 648000 threshold, so the wrap would never fire and the separation
        // would come back as ~648000 arcsec instead of ~3.6 -- silently pushing
        // a real host out of every candidate list.
        let e = Ellipse::new(3.0, 3.0, 0.0).unwrap();
        let r = compute_dlr(359.999, 60.0, 0.001, 60.0, &e);
        assert!(
            r.separation_arcsec < 10.0,
            "separation was {} arcsec",
            r.separation_arcsec
        );
        assert_close!(r.separation_arcsec, 3.6, epsilon = 0.01);
    }

    #[test]
    fn test_dlr_symmetric_across_ra_zero() {
        // The wrap must be sign-correct, not just magnitude-correct: the same
        // pair evaluated from either side has to give the same separation.
        let e = Ellipse::new(5.0, 2.0, 30.0).unwrap();
        let a = compute_dlr(359.999, 0.0, 0.001, 0.0, &e);
        let b = compute_dlr(0.001, 0.0, 359.999, 0.0, &e);
        assert_close!(a.separation_arcsec, b.separation_arcsec, epsilon = 1e-9);
        assert_close!(a.directional_radius, b.directional_radius, epsilon = 1e-9);
    }

    #[test]
    fn test_dlr_scales_with_cos_dec() {
        // At high declination a given RA offset subtends a smaller angle.
        let e = Ellipse::new(3.0, 3.0, 0.0).unwrap();
        let equator = compute_dlr(0.001, 0.0, 0.0, 0.0, &e);
        let high_dec = compute_dlr(0.001, 60.0, 0.0, 60.0, &e);
        assert_close!(
            high_dec.separation_arcsec,
            equator.separation_arcsec * 0.5,
            epsilon = 1e-6
        );
    }
}
