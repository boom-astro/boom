use super::ellipse::Ellipse;

#[derive(Debug, Clone)]
pub struct DlrResult {
    /// Angular separation between transient and galaxy centre, arcsec.
    pub separation_arcsec: f64,
    /// Galaxy light radius toward the transient, arcsec.
    pub directional_radius: f64,
    pub fractional_offset: f64,
}

/// By tangent-plane projection onto the galaxy's ellipse frame.
pub fn compute_dlr(
    transient_ra: f64,
    transient_dec: f64,
    galaxy_ra: f64,
    galaxy_dec: f64,
    ellipse: &Ellipse,
) -> DlrResult {
    // Wrap in degrees, before the cos(dec) scaling: wrapping after fails at high dec.
    let mut dra_deg = transient_ra - galaxy_ra;
    if dra_deg > 180.0 {
        dra_deg -= 360.0;
    } else if dra_deg < -180.0 {
        dra_deg += 360.0;
    }
    let dra = dra_deg * galaxy_dec.to_radians().cos() * 3600.0;
    let ddec = (transient_dec - galaxy_dec) * 3600.0;

    let separation = dra.hypot(ddec);
    if separation < 1e-15 {
        return DlrResult {
            separation_arcsec: 0.0,
            directional_radius: ellipse.a,
            fractional_offset: 0.0,
        };
    }

    // PA is east of north, so the major axis is (sin PA, cos PA) in (east, north).
    let (sin_pa, cos_pa) = ellipse.pa_rad.sin_cos();
    let x_maj = dra * sin_pa + ddec * cos_pa;
    let y_min = dra * cos_pa - ddec * sin_pa;

    // r(t) = a*b / hypot(b*cos t, a*sin t), t the angle in the ellipse frame.
    let (sin_t, cos_t) = y_min.atan2(x_maj).sin_cos();
    let denom = (ellipse.b * cos_t).hypot(ellipse.a * sin_t);
    let directional_radius = if denom > 1e-15 {
        ellipse.a * ellipse.b / denom
    } else {
        ellipse.a
    };

    DlrResult {
        separation_arcsec: separation,
        directional_radius,
        fractional_offset: separation / directional_radius,
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
    fn test_dlr_along_major_axis_at_pa_zero() {
        let e = Ellipse::new(4.0, 2.0, 0.0).unwrap();
        let result = compute_dlr(0.0, 2.0 / 3600.0, 0.0, 0.0, &e);
        assert_close!(result.separation_arcsec, 2.0, epsilon = 0.01);
        assert_close!(result.directional_radius, 4.0, epsilon = 0.01);
        assert_close!(result.fractional_offset, 0.5, epsilon = 0.01);
    }

    #[test]
    fn test_dlr_along_minor_axis_at_pa_zero() {
        let e = Ellipse::new(4.0, 2.0, 0.0).unwrap();
        let result = compute_dlr(2.0 / 3600.0, 0.0, 0.0, 0.0, &e);
        assert_close!(result.separation_arcsec, 2.0, epsilon = 0.01);
        assert_close!(result.directional_radius, 2.0, epsilon = 0.01);
        assert_close!(result.fractional_offset, 1.0, epsilon = 0.01);
    }

    #[test]
    fn test_dlr_convention_at_pa_ninety() {
        // At PA=90 the major axis lies east-west.
        let e = Ellipse::new(4.0, 2.0, 90.0).unwrap();
        let east = compute_dlr(2.0 / 3600.0, 0.0, 0.0, 0.0, &e);
        assert_close!(east.directional_radius, 4.0, epsilon = 0.01);
        let north = compute_dlr(0.0, 2.0 / 3600.0, 0.0, 0.0, &e);
        assert_close!(north.directional_radius, 2.0, epsilon = 0.01);
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
        let r1 = compute_dlr(359.999, 0.0, 0.001, 0.0, &e);
        assert_close!(r1.separation_arcsec, 7.2, epsilon = 0.01);
    }

    #[test]
    fn test_dlr_ra_wraparound_at_high_dec() {
        // Wrapping after the cos(dec)*3600 scaling gives ~648000 arcsec here.
        let e = Ellipse::new(3.0, 3.0, 0.0).unwrap();
        let r = compute_dlr(359.999, 60.0, 0.001, 60.0, &e);
        assert_close!(r.separation_arcsec, 3.6, epsilon = 0.01);
    }

    #[test]
    fn test_dlr_symmetric_across_ra_zero() {
        let e = Ellipse::new(5.0, 2.0, 30.0).unwrap();
        let a = compute_dlr(359.999, 0.0, 0.001, 0.0, &e);
        let b = compute_dlr(0.001, 0.0, 359.999, 0.0, &e);
        assert_close!(a.separation_arcsec, b.separation_arcsec, epsilon = 1e-9);
        assert_close!(a.directional_radius, b.directional_radius, epsilon = 1e-9);
    }

    #[test]
    fn test_dlr_scales_with_cos_dec() {
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
