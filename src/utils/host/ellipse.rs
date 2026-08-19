use super::error::HostError;
use super::types::GalaxyCandidate;

/// Galaxy ellipse parameters derived from catalog shape measurements.
#[derive(Debug, Clone)]
pub struct Ellipse {
    /// Semi-major axis in arcsec
    pub a: f64,
    /// Semi-minor axis in arcsec
    pub b: f64,
    /// Position angle in radians (N through E)
    pub pa_rad: f64,
    /// Axis ratio b/a
    pub axis_ratio: f64,
}

impl Ellipse {
    /// Build an ellipse from explicit semi-axes and PA.
    pub fn new(a_arcsec: f64, b_arcsec: f64, pa_deg: f64) -> Result<Self, HostError> {
        if !(a_arcsec.is_finite() && b_arcsec.is_finite() && pa_deg.is_finite()) {
            return Err(HostError::InvalidShape(format!(
                "non-finite shape: a={a_arcsec}, b={b_arcsec}, pa={pa_deg}"
            )));
        }
        if a_arcsec <= 0.0 || b_arcsec <= 0.0 {
            return Err(HostError::InvalidShape(format!(
                "semi-axes must be positive: a={a_arcsec}, b={b_arcsec}"
            )));
        }
        let (a, b) = if a_arcsec >= b_arcsec {
            (a_arcsec, b_arcsec)
        } else {
            (b_arcsec, a_arcsec)
        };
        Ok(Self {
            a,
            b,
            pa_rad: pa_deg.to_radians(),
            axis_ratio: b / a,
        })
    }

    /// Build an ellipse from Tractor shape parameters (Legacy Survey).
    ///
    /// - `shape_r`: effective radius in arcsec
    /// - `shape_e1`, `shape_e2`: ellipticity components
    /// - `min_b`: minimum semi-minor axis floor in arcsec
    pub fn from_tractor(
        shape_r: f64,
        shape_e1: f64,
        shape_e2: f64,
        min_b: f64,
    ) -> Result<Self, HostError> {
        if shape_r <= 0.0 {
            return Err(HostError::InvalidShape(format!(
                "shape_r must be positive: {shape_r}"
            )));
        }
        if !shape_e1.is_finite() || !shape_e2.is_finite() {
            return Err(HostError::InvalidShape(format!(
                "non-finite ellipticity: e1={shape_e1}, e2={shape_e2}"
            )));
        }

        let e = shape_e1.hypot(shape_e2).min(0.999);
        let q = (1.0 - e) / (1.0 + e);
        let pa_rad = 0.5 * shape_e2.atan2(shape_e1);

        let a = shape_r;
        let b = (a * q).max(min_b);

        Ok(Self {
            a,
            b,
            pa_rad,
            axis_ratio: b / a,
        })
    }

    /// Build an ellipse from a [`GalaxyCandidate`], flooring the semi-minor
    /// axis at `min_b` so that catalog rows with a degenerate (or zero) minor
    /// axis still yield a usable directional radius.
    pub fn from_candidate(candidate: &GalaxyCandidate, min_b: f64) -> Result<Self, HostError> {
        let a = candidate.a_arcsec;
        let b = candidate.b_arcsec.max(min_b);
        Self::new(a, b, candidate.pa_deg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ellipse_new() {
        let e = Ellipse::new(2.0, 1.0, 45.0).unwrap();
        assert_close!(e.a, 2.0);
        assert_close!(e.b, 1.0);
        assert_close!(e.axis_ratio, 0.5);
        assert_close!(e.pa_rad, std::f64::consts::FRAC_PI_4);
    }

    #[test]
    fn test_ellipse_swaps_axes() {
        let e = Ellipse::new(1.0, 3.0, 0.0).unwrap();
        assert_close!(e.a, 3.0);
        assert_close!(e.b, 1.0);
    }

    #[test]
    fn test_ellipse_invalid() {
        assert!(Ellipse::new(-1.0, 1.0, 0.0).is_err());
        assert!(Ellipse::new(1.0, 0.0, 0.0).is_err());
    }

    #[test]
    fn test_ellipse_non_finite() {
        // NED-LVS stores absent diameters as null; a NaN must not slip through
        // into the DLR as a silently-poisoned radius.
        assert!(Ellipse::new(f64::NAN, 1.0, 0.0).is_err());
        assert!(Ellipse::new(2.0, 1.0, f64::NAN).is_err());
    }

    #[test]
    fn test_from_tractor_round() {
        // Circular source: e1=0, e2=0 → e=0, q=1, a=b=shape_r
        let e = Ellipse::from_tractor(1.5, 0.0, 0.0, 0.05).unwrap();
        assert_close!(e.a, 1.5);
        assert_close!(e.b, 1.5);
        assert_close!(e.axis_ratio, 1.0);
    }

    #[test]
    fn test_from_tractor_elongated() {
        let e = Ellipse::from_tractor(2.0, 0.5, 0.0, 0.05).unwrap();
        let expected_q = (1.0 - 0.5) / (1.0 + 0.5); // 1/3
        assert_close!(e.b, 2.0 * expected_q, epsilon = 1e-10);
    }

    #[test]
    fn test_from_tractor_min_b_floor() {
        // Very elongated → b gets floored
        let e = Ellipse::from_tractor(0.1, 0.99, 0.0, 0.05).unwrap();
        assert_close!(e.b, 0.05);
    }

    #[test]
    fn test_from_tractor_invalid() {
        assert!(Ellipse::from_tractor(0.0, 0.0, 0.0, 0.05).is_err());
        assert!(Ellipse::from_tractor(1.0, f64::NAN, 0.0, 0.05).is_err());
    }
}
