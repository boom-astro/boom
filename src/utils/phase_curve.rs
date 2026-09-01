//! Per-object phase curve: the brightness a moving object is expected to have
//! at a given observing geometry.
//!
//! Fitted once over an object's whole archive, this is the baseline the trailing
//! window in [`crate::utils::outburst`] cannot provide. A window compares an
//! object to its own recent past, so activity that lasts longer than the window
//! becomes the object's new normal and stops registering; a phase curve fitted
//! across years keeps registering it for as long as it lasts.

use crate::utils::outburst::{hg12, Point, DEFAULT_G12};
use mongodb::bson::{doc, Bson, Document};
use std::collections::HashMap;

/// Fewer points than this cannot support a fit that is worth storing.
pub const MIN_POINTS: usize = 8;

/// Phase angle span, degrees, below which `g12` is left at its default.
///
/// The slope is what the curve does *between* phase angles, so a short span
/// gives almost no leverage on it and the scan settles wherever the noise is
/// smallest -- usually against one end of the interval. A fitted `g12` pinned at
/// 0 or 1 is that failure, and it biases the predicted magnitude for any later
/// detection outside the range that was fitted.
pub const MIN_PHASE_SPAN: f64 = 15.0;

/// Points below which `g12` is left at its default, for the same reason.
pub const MIN_POINTS_FOR_SLOPE: usize = 20;

/// Floor on the reported scatter, magnitudes. A handful of points can agree by
/// chance, and a zero here would divide a later comparison by the photometric
/// error alone.
pub const MIN_SCATTER: f64 = 0.02;

/// Residual, in units of the first pass's scatter, beyond which a point is
/// dropped before refitting.
pub const CLIP_SIGMA: f64 = 3.0;

/// An object's brightness model in one band.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PhaseCurve {
    /// Absolute magnitude: the reduced magnitude extrapolated to zero phase.
    pub h: f64,
    /// IAU HG12 slope parameter.
    pub g12: f64,
    /// Robust spread of the residuals, magnitudes.
    ///
    /// This is the quantity that makes a deviation meaningful. It absorbs
    /// rotation, which for a bright object is many times the photometric error,
    /// so a sigma scaled by error alone reports rotation as a detection.
    pub scatter: f64,
    /// Points the fit used.
    pub n: usize,
}

/// Distance-corrected magnitude: what the object would show at 1 au from both
/// the Sun and the observer, still carrying the phase angle's effect.
fn reduced_magnitude(p: &Point) -> f64 {
    p.mag - 5.0 * (p.rh * p.delta).log10()
}

/// Median of `values`, which must be non-empty. Sorts a copy.
fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        0.5 * (sorted[mid - 1] + sorted[mid])
    }
}

/// Median absolute deviation, scaled to match a standard deviation on normal
/// data. Robust so that a genuinely active object still yields the baseline it
/// is active against, as long as it is quiet for most of the archive.
fn robust_scatter(residuals: &[f64], center: f64) -> f64 {
    let deviations: Vec<f64> = residuals.iter().map(|r| (r - center).abs()).collect();
    1.4826 * median(&deviations)
}

/// Fit `h`, `g12` and the residual scatter to one band's photometry.
///
/// `g12` is chosen by scanning the interval it is defined on rather than by an
/// optimiser: the residual is not smooth in it, the range is bounded, and a scan
/// cannot land in a local minimum. `h` follows in closed form as the median
/// residual at each trial value, which is what keeps a minority of active epochs
/// from dragging the baseline up to meet them.
pub fn fit(points: &[Point]) -> Option<PhaseCurve> {
    let usable: Vec<&Point> = points
        .iter()
        .filter(|p| {
            p.rh.is_finite()
                && p.delta.is_finite()
                && p.phase.is_finite()
                && p.mag.is_finite()
                && p.rh > 0.0
                && p.delta > 0.0
        })
        .collect();
    if usable.len() < MIN_POINTS {
        return None;
    }

    let reduced: Vec<f64> = usable.iter().map(|p| reduced_magnitude(p)).collect();
    let phases: Vec<f64> = usable.iter().map(|p| p.phase).collect();
    let span = phases.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
        - phases.iter().cloned().fold(f64::INFINITY, f64::min);

    let candidates: Vec<f64> = if span >= MIN_PHASE_SPAN && usable.len() >= MIN_POINTS_FOR_SLOPE {
        (0..=100).map(|i| i as f64 / 100.0).collect()
    } else {
        vec![DEFAULT_G12]
    };

    let mut kept: Vec<usize> = (0..usable.len()).collect();
    let mut best: Option<PhaseCurve> = None;

    // Two passes. The first is fitted on everything, so an object active for a
    // large share of its archive inflates its own scatter and hides behind it;
    // the second refits on the survivors, which is what recovers the quiescent
    // baseline such an object should be measured against.
    for pass in 0..2 {
        best = scan(&reduced, &phases, &candidates, &kept, usable.len());
        let Some(curve) = best else { break };
        if pass == 1 {
            break;
        }
        let surviving: Vec<usize> = kept
            .iter()
            .copied()
            .filter(|i| {
                let residual = reduced[*i] - hg12(phases[*i], curve.g12);
                (residual - curve.h).abs() <= CLIP_SIGMA * curve.scatter
            })
            .collect();
        if surviving.len() < MIN_POINTS || surviving.len() == kept.len() {
            break;
        }
        kept = surviving;
    }

    best.map(|curve| PhaseCurve {
        scatter: curve.scatter.max(MIN_SCATTER),
        ..curve
    })
}

/// Best `g12` over `candidates`, judged by the scatter it leaves behind.
///
/// `n` is reported as the object's total point count rather than the count that
/// survived clipping, so a caller can see how well observed the object is.
fn scan(
    reduced: &[f64],
    phases: &[f64],
    candidates: &[f64],
    kept: &[usize],
    n: usize,
) -> Option<PhaseCurve> {
    let mut best: Option<PhaseCurve> = None;
    for g12 in candidates.iter().copied() {
        let residuals: Vec<f64> = kept
            .iter()
            .map(|i| reduced[*i] - hg12(phases[*i], g12))
            .collect();
        if residuals.iter().any(|r| !r.is_finite()) {
            continue;
        }
        let h = median(&residuals);
        let scatter = robust_scatter(&residuals, h);
        if !scatter.is_finite() {
            continue;
        }
        if best.is_none_or(|b| scatter < b.scatter) {
            best = Some(PhaseCurve { h, g12, scatter, n });
        }
    }
    best
}

/// How far above its own baseline a point sits, in sigma.
///
/// Positive means brighter than the phase curve predicts. The denominator adds
/// the fitted scatter to the photometric error, so the result asks whether the
/// point is unusual for this object rather than merely well measured.
pub fn deviation(curve: &PhaseCurve, p: &Point) -> Option<f64> {
    if !(p.rh.is_finite() && p.delta.is_finite() && p.phase.is_finite() && p.mag.is_finite()) {
        return None;
    }
    if p.rh <= 0.0 || p.delta <= 0.0 {
        return None;
    }
    let expected = curve.h + hg12(p.phase, curve.g12);
    let brighter_by = expected - reduced_magnitude(p);
    let denominator = (p.mag_err * p.mag_err + curve.scatter * curve.scatter).sqrt();
    (denominator > 0.0 && brighter_by.is_finite()).then(|| brighter_by / denominator)
}

/// Where fitted curves live, keyed by the designation as `ssnamenr` carries it.
pub const BASELINES_COLLECTION: &str = "ZTF_sso_baselines";

/// Read the per-band curves out of a stored baseline document.
///
/// Kept beside [`baseline_document`] so the two cannot drift apart.
pub fn curves_from_document(doc: &Document) -> HashMap<u8, PhaseCurve> {
    let Ok(bands) = doc.get_document("bands") else {
        return HashMap::new();
    };
    bands
        .iter()
        .filter_map(|(band, value)| {
            let entry = value.as_document()?;
            let number = |key: &str| entry.get(key).and_then(crate::utils::bson_number);
            Some((
                band.parse::<u8>().ok()?,
                PhaseCurve {
                    h: number("h")?,
                    g12: number("g12")?,
                    scatter: number("scatter")?,
                    n: number("n")? as usize,
                },
            ))
        })
        .collect()
}

/// The stored form of one object's curves.
pub fn baseline_document(
    designation: &str,
    curves: &HashMap<u8, PhaseCurve>,
    now: f64,
) -> Document {
    let bands: Document = curves
        .iter()
        .map(|(band, c)| {
            (
                band.to_string(),
                Bson::Document(doc! {
                    "h": c.h, "g12": c.g12, "scatter": c.scatter, "n": c.n as i64,
                }),
            )
        })
        .collect();
    doc! { "_id": designation, "bands": bands, "updated_at": now }
}

#[cfg(test)]
mod document_tests {
    use super::*;

    #[test]
    fn test_a_document_round_trips() {
        let curves = HashMap::from([
            (
                1u8,
                PhaseCurve {
                    h: 15.5,
                    g12: 0.3,
                    scatter: 0.08,
                    n: 120,
                },
            ),
            (
                2u8,
                PhaseCurve {
                    h: 15.1,
                    g12: 0.4,
                    scatter: 0.06,
                    n: 200,
                },
            ),
        ]);
        let doc = baseline_document("6478", &curves, 2_460_000.0);
        assert_eq!(doc.get_str("_id").expect("id"), "6478");
        assert_eq!(curves_from_document(&doc), curves);
    }

    #[test]
    fn test_a_document_without_bands_reads_as_empty() {
        assert!(curves_from_document(&doc! { "_id": "6478" }).is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(rh: f64, delta: f64, phase: f64, mag: f64, band: u8) -> Point {
        Point {
            rh,
            delta,
            phase,
            mag,
            mag_err: 0.03,
            band,
        }
    }

    /// Photometry generated from a known curve must fit back to it.
    fn synthetic(h: f64, g12: f64, phases: &[f64]) -> Vec<Point> {
        phases
            .iter()
            .map(|phase| {
                let (rh, delta): (f64, f64) = (2.5, 1.7);
                let mag = h + hg12(*phase, g12) + 5.0 * (rh * delta).log10();
                point(rh, delta, *phase, mag, 1)
            })
            .collect()
    }

    #[test]
    fn test_fit_recovers_a_known_curve() {
        let phases: Vec<f64> = (0..24).map(|i| 2.0 + 1.7 * i as f64).collect();
        let curve = fit(&synthetic(15.5, 0.3, &phases)).expect("fit");
        assert!((curve.h - 15.5).abs() < 0.01, "h was {}", curve.h);
        assert!((curve.g12 - 0.3).abs() < 0.05, "g12 was {}", curve.g12);
        assert_eq!(
            curve.scatter, MIN_SCATTER,
            "a perfect fit floors the scatter"
        );
    }

    #[test]
    fn test_too_few_points_is_no_fit() {
        assert!(fit(&synthetic(15.5, 0.3, &[2.0, 5.0, 8.0])).is_none());
    }

    /// With every point at one geometry the slope cannot be measured, so it is
    /// left alone rather than fitted to noise.
    #[test]
    fn test_narrow_phase_span_keeps_the_default_slope() {
        let phases = [10.0, 10.2, 10.4, 10.6, 10.8, 11.0, 11.2, 11.4];
        let curve = fit(&synthetic(15.5, 0.9, &phases)).expect("fit");
        assert_eq!(curve.g12, DEFAULT_G12);
    }

    /// The point of a median fit: a minority of bright epochs must not pull the
    /// baseline up to meet them, or an active object stops looking active.
    #[test]
    fn test_a_minority_of_bright_points_does_not_move_the_baseline() {
        let phases: Vec<f64> = (0..30).map(|i| 2.0 + i as f64).collect();
        let mut points = synthetic(15.5, 0.3, &phases);
        for p in points.iter_mut().take(8) {
            p.mag -= 2.0;
        }
        let curve = fit(&points).expect("fit");
        assert!(
            (curve.h - 15.5).abs() < 0.05,
            "baseline moved to {}",
            curve.h
        );
    }

    #[test]
    fn test_deviation_is_positive_when_brighter_than_the_curve() {
        let phases: Vec<f64> = (0..24).map(|i| 2.0 + 1.7 * i as f64).collect();
        let curve = fit(&synthetic(15.5, 0.3, &phases)).expect("fit");

        let on_curve = synthetic(15.5, 0.3, &[15.0])[0];
        assert!(deviation(&curve, &on_curve).expect("deviation").abs() < 0.5);

        let mut brighter = on_curve;
        brighter.mag -= 1.0;
        let sigma = deviation(&curve, &brighter).expect("deviation");
        assert!(sigma > 10.0, "a magnitude of brightening gave {}", sigma);
    }

    /// Rotation is what separates a scatter-aware sigma from an error-only one:
    /// the same deviation must read as ordinary for a variable object.
    #[test]
    fn test_scatter_absorbs_rotation() {
        let phases: Vec<f64> = (0..40).map(|i| 2.0 + i as f64).collect();
        let mut points = synthetic(15.5, 0.3, &phases);
        for (i, p) in points.iter_mut().enumerate() {
            p.mag += if i % 2 == 0 { 0.15 } else { -0.15 };
        }
        let curve = fit(&points).expect("fit");
        assert!(
            curve.scatter > 0.1,
            "rotation should show up as scatter, got {}",
            curve.scatter
        );

        let mut wobble = synthetic(15.5, 0.3, &[15.0])[0];
        wobble.mag -= 0.15;
        let sigma = deviation(&curve, &wobble).expect("deviation");
        assert!(sigma < 2.0, "a rotation-sized deviation gave {}", sigma);
    }
}
