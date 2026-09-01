//! Outburst statistic for solar system objects.
//!
//! Every point in a recent window is scaled to the observing geometry and colour
//! of the point under test; the sigma-distance of each from the test point is the
//! statistic. A large positive median means the test point is brighter than the
//! object's recent trend.
//!
//! Follows M. S. P. Kelley's reference implementation. The phase function is the
//! IAU HG1G2 basis (Muinonen et al. 2010) with the Penttila et al. 2016 G12
//! parameterisation, matching `sbpy.photometry.HG12_Pen16`.

/// Power law index on heliocentric distance. Comets differ (-4).
pub const RH_SLOPE: f64 = -2.0;
/// Power law index on observer-target distance. Comets differ (-1).
pub const DELTA_SLOPE: f64 = -2.0;
/// Middle value across asteroids, used until per-object fits exist.
pub const DEFAULT_G12: f64 = 0.5;

/// One photometric point in the window.
#[derive(Debug, Clone, Copy)]
pub struct Point {
    /// Heliocentric distance, au.
    pub rh: f64,
    /// Observer-target distance, au.
    pub delta: f64,
    /// Sun-target-observer angle, degrees.
    pub phase: f64,
    /// Apparent magnitude.
    pub mag: f64,
    /// One-sigma uncertainty on `mag`.
    pub mag_err: f64,
    /// Bandpass, compared by equality only.
    pub band: u8,
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum OutburstError {
    #[error("need at least two points, got {0}")]
    TooFewPoints(usize),
    #[error("no earlier observation in the test point's band")]
    NoColorReference,
    #[error("a point carries a non-finite value")]
    NotFinite,
}

/// A clamped cubic spline through `(x, y)` with fixed end slopes.
///
/// The HG1G2 bases are defined this way; outside the knots the spline continues
/// linearly along the end slope, which is what keeps the basis monotonic well
/// past the last knot.
struct Spline {
    x: Vec<f64>,
    y: Vec<f64>,
    /// Second derivatives at the knots, solved once on first use.
    m: Vec<f64>,
    dy0: f64,
    dyn_: f64,
}

impl Spline {
    /// Knots are given in degrees for legibility and converted here; the end
    /// slopes are per radian, as the basis definitions give them.
    fn new(x_deg: &[f64], y: &[f64], dy0: f64, dyn_: f64) -> Self {
        let x: Vec<f64> = x_deg.iter().map(|d| d.to_radians()).collect();
        let y = y.to_vec();
        let n = x.len();
        // Clamped cubic spline: tridiagonal solve for the second derivatives.
        let mut h = vec![0.0; n - 1];
        for i in 0..n - 1 {
            h[i] = x[i + 1] - x[i];
        }
        let (mut a, mut b, mut c, mut d) = (vec![0.0; n], vec![0.0; n], vec![0.0; n], vec![0.0; n]);
        b[0] = 2.0 * h[0];
        c[0] = h[0];
        d[0] = 6.0 * ((y[1] - y[0]) / h[0] - dy0);
        for i in 1..n - 1 {
            a[i] = h[i - 1];
            b[i] = 2.0 * (h[i - 1] + h[i]);
            c[i] = h[i];
            d[i] = 6.0 * ((y[i + 1] - y[i]) / h[i] - (y[i] - y[i - 1]) / h[i - 1]);
        }
        a[n - 1] = h[n - 2];
        b[n - 1] = 2.0 * h[n - 2];
        d[n - 1] = 6.0 * (dyn_ - (y[n - 1] - y[n - 2]) / h[n - 2]);

        for i in 1..n {
            let w = a[i] / b[i - 1];
            b[i] -= w * c[i - 1];
            d[i] -= w * d[i - 1];
        }
        let mut m = vec![0.0; n];
        m[n - 1] = d[n - 1] / b[n - 1];
        for i in (0..n - 1).rev() {
            m[i] = (d[i] - c[i] * m[i + 1]) / b[i];
        }
        Self { x, y, m, dy0, dyn_ }
    }

    /// Never negative: a basis function is a reflectance, and the linear
    /// extrapolation beyond the last knot would otherwise cross zero.
    fn eval(&self, t: f64) -> f64 {
        let n = self.x.len();
        if t <= self.x[0] {
            return (self.y[0] + self.dy0 * (t - self.x[0])).max(0.0);
        }
        if t >= self.x[n - 1] {
            return (self.y[n - 1] + self.dyn_ * (t - self.x[n - 1])).max(0.0);
        }
        let mut i = 0;
        while i + 1 < n - 1 && t > self.x[i + 1] {
            i += 1;
        }
        let h = self.x[i + 1] - self.x[i];
        let a = (self.x[i + 1] - t) / h;
        let b = (t - self.x[i]) / h;
        (a * self.y[i]
            + b * self.y[i + 1]
            + ((a * a * a - a) * self.m[i] + (b * b * b - b) * self.m[i + 1]) * h * h / 6.0)
            .max(0.0)
    }
}

// Knots, values and end slopes of the three HG1G2 basis functions. Knots are in
// degrees; the end slopes are per radian.
const PHI12_X_DEG: [f64; 6] = [7.5, 30.0, 60.0, 90.0, 120.0, 150.0];
const PHI1_Y: [f64; 6] = [
    0.75,
    0.334_860_16,
    0.134_105_6,
    0.051_104_756,
    0.021_465_687,
    0.003_639_698_9,
];
const PHI2_Y: [f64; 6] = [
    0.925,
    0.628_841_69,
    0.317_554_95,
    0.127_163_67,
    0.022_373_903,
    0.000_165_056_89,
];
const PHI3_X_DEG: [f64; 9] = [0.0, 0.3, 1.0, 2.0, 4.0, 8.0, 12.0, 20.0, 30.0];
const PHI3_Y: [f64; 9] = [
    1.0,
    0.833_811_85,
    0.577_354_24,
    0.421_447_72,
    0.231_742_3,
    0.103_481_78,
    0.061_733_473,
    0.016_107_006,
    0.0,
];

fn bases() -> &'static (Spline, Spline, Spline) {
    static BASES: std::sync::OnceLock<(Spline, Spline, Spline)> = std::sync::OnceLock::new();
    BASES.get_or_init(|| {
        (
            Spline::new(&PHI12_X_DEG, &PHI1_Y, -1.909_859_3, -0.091_328_612),
            Spline::new(&PHI12_X_DEG, &PHI2_Y, -0.572_957_8, -8.657_313_8e-8),
            Spline::new(&PHI3_X_DEG, &PHI3_Y, -1.063_009_7, 0.0),
        )
    })
}

/// HG12 phase function as a relative magnitude, zero at zero phase.
pub fn hg12(phase_deg: f64, g12: f64) -> f64 {
    // Penttila et al. 2016 mapping from G12 to the HG1G2 pair.
    let g1 = 0.842_936_49 * g12;
    let g2 = 0.535_133_50 * (1.0 - g12);
    let (p1, p2, p3) = bases();
    let a = phase_deg.to_radians();
    // The bases are reflectances; the magnitude is relative to zero phase, where
    // every basis is 1 by construction.
    let r = g1 * p1.eval(a) + g2 * p2.eval(a) + (1.0 - g1 - g2) * p3.eval(a);
    let r0 = g1 * p1.eval(0.0) + g2 * p2.eval(0.0) + (1.0 - g1 - g2) * p3.eval(0.0);
    -2.5 * (r / r0).log10()
}

/// Magnitude offsets bringing every point to the last point's geometry.
pub fn scale_by_geometry(points: &[Point], g12: f64) -> Vec<f64> {
    let last = points[points.len() - 1];
    points
        .iter()
        .map(|p| {
            -2.5 * ((last.rh / p.rh).powf(RH_SLOPE) * (last.delta / p.delta).powf(DELTA_SLOPE))
                .log10()
                + hg12(last.phase, g12)
                - hg12(p.phase, g12)
        })
        .collect()
}

fn weighted_mean(values: &[f64], errs: &[f64]) -> Option<f64> {
    let mut sum_w = 0.0;
    let mut sum_wx = 0.0;
    for (v, e) in values.iter().zip(errs) {
        if !v.is_finite() || !e.is_finite() || *e <= 0.0 {
            continue;
        }
        let w = 1.0 / (e * e);
        sum_w += w;
        sum_wx += w * v;
    }
    (sum_w > 0.0).then(|| sum_wx / sum_w)
}

/// The statistic for each earlier point, and the median across them.
///
/// The last point is the one under test. Every earlier point is brought to its
/// geometry and colour, then differenced and divided by the combined
/// uncertainty, so the result is in sigma.
pub fn outburst_statistic(points: &[Point], g12: f64) -> Result<(Vec<f64>, f64), OutburstError> {
    if points.len() < 2 {
        return Err(OutburstError::TooFewPoints(points.len()));
    }
    if points.iter().any(|p| {
        !(p.rh.is_finite() && p.delta.is_finite() && p.phase.is_finite() && p.mag.is_finite())
    }) {
        return Err(OutburstError::NotFinite);
    }

    let geom = scale_by_geometry(points, g12);
    let scaled: Vec<f64> = points.iter().zip(&geom).map(|(p, g)| p.mag + g).collect();
    let errs: Vec<f64> = points.iter().map(|p| p.mag_err).collect();
    let last = points.len() - 1;
    let target_band = points[last].band;

    // Band averages exclude the test point, so it is never compared to itself.
    let mut colors: std::collections::HashMap<u8, f64> = std::collections::HashMap::new();
    let band_mean = |band: u8| -> Option<f64> {
        let (v, e): (Vec<f64>, Vec<f64>) = points[..last]
            .iter()
            .enumerate()
            .filter(|(_, p)| p.band == band)
            .map(|(i, _)| (scaled[i], errs[i]))
            .unzip();
        weighted_mean(&v, &e)
    };
    let target_mean = band_mean(target_band).ok_or(OutburstError::NoColorReference)?;
    for p in &points[..last] {
        if let std::collections::hash_map::Entry::Vacant(slot) = colors.entry(p.band) {
            let mean = band_mean(p.band).unwrap_or(target_mean);
            slot.insert(mean - target_mean);
        }
    }

    let mut stats: Vec<f64> = Vec::with_capacity(last);
    for (i, p) in points[..last].iter().enumerate() {
        let color = colors.get(&p.band).copied().unwrap_or(0.0);
        let numerator = scaled[i] - color - points[last].mag;
        let denominator = (errs[last] * errs[last] + p.mag_err * p.mag_err).sqrt();
        stats.push(numerator / denominator);
    }

    let mut sorted = stats.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = if sorted.len() % 2 == 1 {
        sorted[sorted.len() / 2]
    } else {
        0.5 * (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2])
    };
    Ok((stats, median))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(rh: f64, delta: f64, phase: f64, mag: f64, mag_err: f64, band: u8) -> Point {
        Point {
            rh,
            delta,
            phase,
            mag,
            mag_err,
            band,
        }
    }

    // (phase_deg, Phi) from sbpy HG12_Pen16 with G12 = 0.5.
    const SBPY: [(f64, f64); 21] = [
        (0.0, 0.000000000839),
        (0.5, 0.109749085132),
        (1.0, 0.174095558939),
        (2.0, 0.260558183407),
        (3.0, 0.333948886900),
        (5.0, 0.449966330470),
        (7.5, 0.558215059221),
        (10.0, 0.651920593745),
        (15.0, 0.826162147554),
        (20.0, 0.987385048745),
        (25.0, 1.135767410041),
        (30.0, 1.273734139104),
        (40.0, 1.547053721924),
        (50.0, 1.829441342475),
        (60.0, 2.123197928271),
        (75.0, 2.598430354758),
        (90.0, 3.138020117802),
        (105.0, 3.796035249255),
        (120.0, 4.557341227593),
        (140.0, 5.460566342543),
        (160.0, 10.887434893003),
    ];

    /// The phase function is the only part with published reference values, and
    /// an error in it biases every scaled point.
    #[test]
    fn test_phase_function_matches_sbpy() {
        for (phase, expected) in SBPY {
            let got = hg12(phase, DEFAULT_G12);
            assert!(
                (got - expected).abs() < 1e-6,
                "phase {phase}: got {got}, sbpy {expected}"
            );
        }
    }

    #[test]
    fn test_phase_function_is_zero_at_opposition() {
        assert!(hg12(0.0, DEFAULT_G12).abs() < 1e-6);
    }

    #[test]
    fn test_phase_function_dims_with_phase() {
        let mut previous = f64::NEG_INFINITY;
        let mut phase = 0.0;
        while phase <= 120.0 {
            let phi = hg12(phase, DEFAULT_G12);
            assert!(phi >= previous - 1e-9, "not monotonic at {phase}");
            previous = phi;
            phase += 1.0;
        }
    }

    // From the reference implementation's own tests: a point one magnitude
    // brighter than its predecessor, same geometry, is that far above the trend.
    #[test]
    fn test_brightening_gives_a_positive_statistic() {
        let points = [
            point(1.0, 1.0, 0.0, 0.0, 0.1, b'r'),
            point(1.0, 1.0, 0.0, -1.0, 0.1, b'r'),
        ];
        let (stats, median) = outburst_statistic(&points, DEFAULT_G12).unwrap();
        let expected = 1.0 / (2.0_f64 * 0.1 * 0.1).sqrt();
        assert!((stats[0] - expected).abs() < 1e-9, "got {}", stats[0]);
        assert!((median - expected).abs() < 1e-9);
    }

    // Also from the reference: a phase difference that exactly accounts for the
    // magnitude difference must leave the statistic unchanged.
    #[test]
    fn test_phase_effect_is_corrected() {
        let points = [
            point(1.0, 1.0, 30.0, hg12(30.0, DEFAULT_G12), 0.1, b'r'),
            point(1.0, 1.0, 0.0, -1.0, 0.1, b'r'),
        ];
        let (stats, _) = outburst_statistic(&points, DEFAULT_G12).unwrap();
        let expected = 1.0 / (2.0_f64 * 0.1 * 0.1).sqrt();
        assert!((stats[0] - expected).abs() < 1e-9, "got {}", stats[0]);
    }

    /// Distance scaling: a point twice as far in both rh and delta is dimmer by
    /// 2.5*log10(2^2 * 2^2), and correcting for it must cancel exactly.
    #[test]
    fn test_distance_effect_is_corrected() {
        let dimming = 2.5 * (16.0_f64).log10();
        let points = [
            point(2.0, 2.0, 0.0, dimming, 0.1, b'r'),
            point(1.0, 1.0, 0.0, 0.0, 0.1, b'r'),
        ];
        let (stats, _) = outburst_statistic(&points, DEFAULT_G12).unwrap();
        assert!(
            stats[0].abs() < 1e-9,
            "steady object should give ~0, got {}",
            stats[0]
        );
    }

    #[test]
    fn test_steady_object_gives_no_signal() {
        let points: Vec<Point> = (0..6)
            .map(|i| point(2.5, 1.6, 10.0 + i as f64 * 0.1, 18.0, 0.05, b'r'))
            .collect();
        let (_, median) = outburst_statistic(&points, DEFAULT_G12).unwrap();
        assert!(median.abs() < 0.5, "median {median} should be near zero");
    }

    /// Colour offsets are measured excluding the test point, so a different band
    /// with a genuine colour term does not read as an outburst.
    #[test]
    fn test_color_offset_does_not_read_as_outburst() {
        let points = [
            point(2.5, 1.6, 10.0, 18.5, 0.05, b'g'),
            point(2.5, 1.6, 10.0, 18.5, 0.05, b'g'),
            point(2.5, 1.6, 10.0, 18.0, 0.05, b'r'),
            point(2.5, 1.6, 10.0, 18.0, 0.05, b'r'),
        ];
        let (_, median) = outburst_statistic(&points, DEFAULT_G12).unwrap();
        assert!(
            median.abs() < 0.5,
            "colour alone should not signal, got {median}"
        );
    }

    /// Without an earlier point in the test band there is no colour reference,
    /// which is a different outcome from "no outburst".
    #[test]
    fn test_missing_color_reference_is_an_error() {
        let points = [
            point(2.5, 1.6, 10.0, 18.5, 0.05, b'g'),
            point(2.5, 1.6, 10.0, 18.0, 0.05, b'r'),
        ];
        assert_eq!(
            outburst_statistic(&points, DEFAULT_G12).unwrap_err(),
            OutburstError::NoColorReference
        );
    }

    #[test]
    fn test_too_few_points_is_an_error() {
        let points = [point(2.5, 1.6, 10.0, 18.0, 0.05, b'r')];
        assert_eq!(
            outburst_statistic(&points, DEFAULT_G12).unwrap_err(),
            OutburstError::TooFewPoints(1)
        );
    }
}
