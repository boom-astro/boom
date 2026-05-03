//! Light-curve binning math.
//!
//! Aggregates per-band photometric points into per-bin summary records used
//! by the per-source `binned_lightcurve` representation on the
//! `<survey>_alerts_aux` collection. See `docs/binned-lightcurves.md` for
//! the schema and design rationale.
//!
//! Pure module: no MongoDB, no Redis, no async, no config. The
//! `lightcurve_binner` binary wires these functions into nightly
//! aggregation; this module supplies the math.
//!
//! # Flux convention
//!
//! Inputs and outputs are in whatever flux unit the caller chose to store
//! on the upstream documents. ZTF's `psf_flux` field is in `1e9 ×
//! (internal ZTF_ZP unit)`; LSST's is in genuine AB nJy. Binning is linear
//! in flux so the unit choice doesn't affect bin math; downstream consumers
//! convert to magnitude using the survey-appropriate zero point.

use crate::utils::lightcurves::Band;
use serde::{Deserialize, Serialize};

/// Provenance of a flux measurement contributing to a bin.
#[derive(Debug, PartialEq, Eq, Clone, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SrcKind {
    /// Detection-pipeline-triggered measurement (from `prv_candidates`).
    Psf,
    /// Forced-photometry measurement at a known position (from `fp_hists`).
    Fp,
    /// Upper limit at a non-detection time (from `prv_nondetections`).
    Upper,
}

/// One input flux measurement at a single epoch.
///
/// `flux` is `None` only for `SrcKind::Upper` points; for those, `flux_err`
/// carries the equivalent flux limit (e.g., `diffmaglim` converted via
/// `5 * flux_err`).
#[derive(Debug, Clone)]
pub struct FluxPoint {
    pub jd: f64,
    pub flux: Option<f64>,
    pub flux_err: f64,
    pub kind: SrcKind,
}

/// One per-band bin produced by the binner. One entry per element of
/// `binned_lightcurve.<band>` on the per-objectId aux document.
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct BinnedPoint {
    /// Median JD of contributing flux points (or weighted mean for n=2);
    /// median JD of upper limits if the bin is upper-limit-only.
    pub jd: f64,
    /// Number of finite-flux points in the bin (excluding upper limits).
    pub n: i32,
    /// Inverse-variance-weighted central flux. `None` when the bin is
    /// upper-limit-only.
    pub flux_med: Option<f64>,
    /// Robust standard error of the central flux: `max(weighted-mean error,
    /// 1.4826 · MAD / √n)`. For an upper-limit-only bin, this is the deepest
    /// limit (smallest flux_err) in the window.
    pub flux_err: f64,
    /// MAD of contributing fluxes. Zero when `n < 2`.
    pub flux_mad: f64,
    /// Whether the window contained any upper-limit points.
    pub has_nondet: bool,
    /// Distinct provenance kinds that contributed (sorted: Psf, Fp, Upper).
    pub src_kinds: Vec<SrcKind>,
    /// Start JD of the bin's source window. Together with `band` this is the
    /// bin's stable identity key: re-runs of the binner over the same window
    /// replace the bin in place rather than appending a near-duplicate when
    /// late-arriving photometry shifts the median `jd`.
    pub window_start_jd: f64,
    /// Width of the bin window in days. Lets consumers reason about the
    /// cadence at which each bin was computed (cadence can change across
    /// bins under the tier state machine).
    pub window_days: f64,
    pub band: Band,
}

/// A single half-open time window `[start_jd, end_jd)` for binning.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BinWindow {
    pub start_jd: f64,
    pub end_jd: f64,
}

impl BinWindow {
    pub fn width_days(&self) -> f64 {
        self.end_jd - self.start_jd
    }
    pub fn center_jd(&self) -> f64 {
        0.5 * (self.start_jd + self.end_jd)
    }
    pub fn contains(&self, jd: f64) -> bool {
        jd >= self.start_jd && jd < self.end_jd
    }
}

/// Bin a single band's points into a single window.
///
/// `points` is filtered by the window and by validity (finite, positive
/// `flux_err`; finite `flux` for non-upper points). Out-of-window points are
/// silently skipped — not an error. Returns `None` only when the window
/// contains no valid points at all (no flux measurements *and* no upper
/// limits).
///
/// Bin math:
/// - `n` counts finite-flux Psf and Fp points; Upper points don't add to `n`
///   but set `has_nondet`.
/// - Central flux: `n == 1` keeps the single point; `n == 2` uses the
///   inverse-variance-weighted mean (a 2-point median is degenerate); `n >= 3`
///   uses the inverse-variance-weighted median (robust to outliers).
/// - `flux_err`: `max(weighted-mean error of inputs, 1.4826 · MAD / √n)`.
///   The lower bound from the inputs prevents catastrophically small errors
///   when the spread happens to be small by chance.
/// - Upper-limit-only bins emit `flux_med = None`, `flux_err = deepest input
///   limit`, `flux_mad = 0`, and `has_nondet = true`.
///
/// All arithmetic is in `f64` for numerical stability even when the input is
/// stored as `f32` upstream.
pub fn bin_band(points: &[FluxPoint], window: BinWindow, band: Band) -> Option<BinnedPoint> {
    let mut flux_pts: Vec<&FluxPoint> = Vec::new();
    let mut upper_pts: Vec<&FluxPoint> = Vec::new();

    for p in points {
        if !window.contains(p.jd) {
            continue;
        }
        if !p.flux_err.is_finite() || p.flux_err <= 0.0 {
            continue;
        }
        match (&p.kind, p.flux) {
            (SrcKind::Upper, _) => upper_pts.push(p),
            (_, Some(f)) if f.is_finite() => flux_pts.push(p),
            _ => {}
        }
    }

    let has_nondet = !upper_pts.is_empty();
    let n = flux_pts.len() as i32;

    if n == 0 && !has_nondet {
        return None;
    }

    // Track distinct provenance kinds in canonical order so equivalent bins
    // serialize identically regardless of input order.
    let mut src_kinds: Vec<SrcKind> = Vec::new();
    for kind in [SrcKind::Psf, SrcKind::Fp] {
        if flux_pts.iter().any(|p| p.kind == kind) {
            src_kinds.push(kind);
        }
    }
    if has_nondet {
        src_kinds.push(SrcKind::Upper);
    }

    let (jd, flux_med, flux_err, flux_mad) = if n == 0 {
        // Upper-limit-only bin.
        let deepest = upper_pts
            .iter()
            .map(|p| p.flux_err)
            .fold(f64::INFINITY, f64::min);
        let upper_jds: Vec<f64> = upper_pts.iter().map(|p| p.jd).collect();
        (median_f64(&upper_jds), None, deepest, 0.0)
    } else {
        let fluxes: Vec<f64> = flux_pts.iter().map(|p| p.flux.unwrap()).collect();
        let errs: Vec<f64> = flux_pts.iter().map(|p| p.flux_err).collect();
        let jds: Vec<f64> = flux_pts.iter().map(|p| p.jd).collect();

        let weights: Vec<f64> = errs.iter().map(|e| 1.0 / (e * e)).collect();
        let sum_w: f64 = weights.iter().sum();
        let weighted_mean_err = (1.0 / sum_w).sqrt();

        let central = match n {
            1 => fluxes[0],
            2 => weighted_mean(&fluxes, &weights),
            _ => weighted_median(&fluxes, &weights),
        };

        let mad = if n < 2 {
            0.0
        } else {
            median_absolute_deviation(&fluxes, central)
        };
        let robust_sem = if n >= 2 {
            1.4826 * mad / (n as f64).sqrt()
        } else {
            errs[0]
        };
        let err = robust_sem.max(weighted_mean_err);

        let central_jd = match n {
            1 => jds[0],
            2 => weighted_mean(&jds, &weights),
            _ => median_f64(&jds),
        };

        (central_jd, Some(central), err, mad)
    };

    Some(BinnedPoint {
        jd,
        n,
        flux_med,
        flux_err,
        flux_mad,
        has_nondet,
        src_kinds,
        window_start_jd: window.start_jd,
        window_days: window.width_days(),
        band,
    })
}

/// Bin a band's points across multiple windows. Equivalent to calling
/// `bin_band` for each window; returns one bin per non-empty window.
///
/// Windows do not need to be contiguous or non-overlapping — the binner
/// treats them independently, so the caller can implement variable-width
/// (e.g., per-tier) cadence by emitting a custom window list.
pub fn bin_band_windows(
    points: &[FluxPoint],
    windows: &[BinWindow],
    band: Band,
) -> Vec<BinnedPoint> {
    windows
        .iter()
        .filter_map(|w| bin_band(points, *w, band.clone()))
        .collect()
}

// ─── pure helpers ────────────────────────────────────────────────────────────

fn weighted_mean(values: &[f64], weights: &[f64]) -> f64 {
    let sum_w: f64 = weights.iter().sum();
    let sum_wv: f64 = values.iter().zip(weights).map(|(v, w)| v * w).sum();
    sum_wv / sum_w
}

/// Inverse-variance weighted median: the smallest `v` whose cumulative weight
/// (over points with value ≤ v after sorting) reaches half the total weight.
///
/// Ties on weight broken by taking the smaller value, which matches the
/// behavior of standard weighted-median definitions.
fn weighted_median(values: &[f64], weights: &[f64]) -> f64 {
    debug_assert_eq!(values.len(), weights.len());
    debug_assert!(!values.is_empty());

    let mut pairs: Vec<(f64, f64)> = values
        .iter()
        .zip(weights.iter())
        .map(|(v, w)| (*v, *w))
        .collect();
    pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    let total: f64 = weights.iter().sum();
    let half = 0.5 * total;
    let mut cum = 0.0;
    for (v, w) in &pairs {
        cum += w;
        if cum >= half {
            return *v;
        }
    }
    pairs.last().unwrap().0
}

/// Plain-median; for an even-sized input returns the average of the two
/// central values. NaN-free input assumed (callers filter beforehand).
/// Exposed for downstream modules (e.g., cadence outburst detection).
pub fn median_f64(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = sorted.len();
    if n == 0 {
        return f64::NAN;
    }
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        0.5 * (sorted[n / 2 - 1] + sorted[n / 2])
    }
}

/// Median absolute deviation around a given center.
/// Exposed for downstream modules (e.g., cadence outburst detection).
pub fn median_absolute_deviation(values: &[f64], center: f64) -> f64 {
    let abs_devs: Vec<f64> = values.iter().map(|v| (*v - center).abs()).collect();
    median_f64(&abs_devs)
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn psf(jd: f64, flux: f64, flux_err: f64) -> FluxPoint {
        FluxPoint {
            jd,
            flux: Some(flux),
            flux_err,
            kind: SrcKind::Psf,
        }
    }

    fn fp(jd: f64, flux: f64, flux_err: f64) -> FluxPoint {
        FluxPoint {
            jd,
            flux: Some(flux),
            flux_err,
            kind: SrcKind::Fp,
        }
    }

    fn upper(jd: f64, flux_err: f64) -> FluxPoint {
        FluxPoint {
            jd,
            flux: None,
            flux_err,
            kind: SrcKind::Upper,
        }
    }

    fn full_window() -> BinWindow {
        BinWindow {
            start_jd: 0.0,
            end_jd: 100.0,
        }
    }

    #[test]
    fn empty_input_returns_none() {
        let bin = bin_band(&[], full_window(), Band::R);
        assert!(bin.is_none());
    }

    #[test]
    fn out_of_window_points_only_returns_none() {
        let pts = vec![psf(200.0, 100.0, 5.0), psf(201.0, 110.0, 5.0)];
        let bin = bin_band(&pts, full_window(), Band::R);
        assert!(bin.is_none());
    }

    #[test]
    fn single_psf_point_returns_that_point() {
        let pts = vec![psf(50.0, 1234.5, 12.3)];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        assert_eq!(bin.n, 1);
        assert_eq!(bin.flux_med, Some(1234.5));
        assert!((bin.flux_err - 12.3).abs() < 1e-9);
        assert_eq!(bin.flux_mad, 0.0);
        assert_eq!(bin.has_nondet, false);
        assert_eq!(bin.src_kinds, vec![SrcKind::Psf]);
        assert_eq!(bin.jd, 50.0);
        assert_eq!(bin.band, Band::R);
        assert_eq!(bin.window_start_jd, 0.0);
        assert_eq!(bin.window_days, 100.0);
    }

    #[test]
    fn two_psf_points_equal_err_yield_arithmetic_mean() {
        let pts = vec![psf(50.0, 100.0, 10.0), psf(51.0, 200.0, 10.0)];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        assert_eq!(bin.n, 2);
        // weighted_mean with equal weights == arithmetic mean
        assert!((bin.flux_med.unwrap() - 150.0).abs() < 1e-9);
        // flux_err = max(weighted_mean_err, 1.4826*MAD/sqrt(2))
        // weighted_mean_err = sqrt(1 / (1/100 + 1/100)) = sqrt(50) ~ 7.071
        // MAD of {100, 200} around mean 150 = median(|100-150|, |200-150|) = 50
        // robust_sem = 1.4826 * 50 / sqrt(2) ~ 52.42
        assert!((bin.flux_err - 52.42).abs() < 0.01);
        assert!((bin.flux_mad - 50.0).abs() < 1e-9);
        // jd is weighted mean for n=2
        assert!((bin.jd - 50.5).abs() < 1e-9);
    }

    #[test]
    fn two_psf_points_unequal_err_weighted_toward_smaller_err() {
        // Point with smaller error (1.0) should dominate point with larger (10.0).
        let pts = vec![psf(50.0, 100.0, 1.0), psf(51.0, 200.0, 10.0)];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        // weights: 1/1 = 1, 1/100 = 0.01; weighted mean ~ 100.99
        let expected = (100.0 * 1.0 + 200.0 * 0.01) / 1.01;
        assert!((bin.flux_med.unwrap() - expected).abs() < 1e-6);
        // weighted-mean error sqrt(1/(1+0.01)) ~ 0.995
        assert!(bin.flux_err > 0.99);
    }

    #[test]
    fn three_psf_points_use_weighted_median() {
        // Equal weights — weighted median == regular median.
        let pts = vec![
            psf(50.0, 100.0, 5.0),
            psf(51.0, 110.0, 5.0),
            psf(52.0, 120.0, 5.0),
        ];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        assert_eq!(bin.n, 3);
        assert_eq!(bin.flux_med, Some(110.0)); // middle of {100, 110, 120}
        // jd is plain median for n>=3
        assert_eq!(bin.jd, 51.0);
        // MAD: median(|100-110|, |110-110|, |120-110|) = median(10, 0, 10) = 10
        assert!((bin.flux_mad - 10.0).abs() < 1e-9);
    }

    #[test]
    fn weighted_median_robust_to_high_error_outlier() {
        // Outlier at 1000 with huge error should not pull the central value.
        // Three equal-weight points around 100 + one heavy-error point at 1000.
        let pts = vec![
            psf(50.0, 100.0, 5.0),
            psf(51.0, 110.0, 5.0),
            psf(52.0, 120.0, 5.0),
            psf(53.0, 1000.0, 500.0),
        ];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        // With cumulative weight {0.04, 0.04, 0.04, 4e-6}, half-total ~ 0.06,
        // walking sorted ascending {100, 110, 120, 1000}: cum hits 0.04 at v=100,
        // 0.08 at v=110 → returns 110. Outlier at 1000 has negligible weight.
        assert_eq!(bin.flux_med, Some(110.0));
    }

    #[test]
    fn mixed_psf_and_fp_share_one_bin() {
        let pts = vec![psf(50.0, 100.0, 5.0), fp(50.5, 105.0, 5.0)];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        assert_eq!(bin.n, 2);
        assert_eq!(bin.has_nondet, false);
        assert!(bin.src_kinds.contains(&SrcKind::Psf));
        assert!(bin.src_kinds.contains(&SrcKind::Fp));
        // canonical order: Psf, Fp
        assert_eq!(bin.src_kinds, vec![SrcKind::Psf, SrcKind::Fp]);
    }

    #[test]
    fn upper_limits_only_returns_upper_only_bin() {
        let pts = vec![upper(50.0, 50.0), upper(51.0, 30.0), upper(52.0, 40.0)];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        assert_eq!(bin.n, 0);
        assert_eq!(bin.flux_med, None);
        // deepest = smallest flux_err = 30.0
        assert_eq!(bin.flux_err, 30.0);
        assert_eq!(bin.flux_mad, 0.0);
        assert_eq!(bin.has_nondet, true);
        assert_eq!(bin.src_kinds, vec![SrcKind::Upper]);
        assert_eq!(bin.jd, 51.0); // median of upper-limit jds
    }

    #[test]
    fn psf_plus_upper_keeps_psf_bin_with_nondet_flag() {
        let pts = vec![psf(50.0, 100.0, 5.0), upper(50.5, 200.0)];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        assert_eq!(bin.n, 1);
        assert_eq!(bin.flux_med, Some(100.0));
        assert_eq!(bin.has_nondet, true);
        assert_eq!(bin.src_kinds, vec![SrcKind::Psf, SrcKind::Upper]);
    }

    #[test]
    fn negative_flux_difference_photometry_bins_normally() {
        // ZTF stores negative psf_flux when isdiffpos is false.
        let pts = vec![psf(50.0, -100.0, 5.0), psf(51.0, -110.0, 5.0)];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        assert_eq!(bin.n, 2);
        assert!((bin.flux_med.unwrap() - (-105.0)).abs() < 1e-9);
    }

    #[test]
    fn nan_flux_dropped_from_input() {
        let pts = vec![
            psf(50.0, f64::NAN, 5.0),
            psf(51.0, 100.0, 5.0),
            psf(52.0, 110.0, 5.0),
        ];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        assert_eq!(bin.n, 2);
        assert!((bin.flux_med.unwrap() - 105.0).abs() < 1e-9);
    }

    #[test]
    fn nonpositive_flux_err_dropped() {
        let pts = vec![
            psf(50.0, 100.0, -1.0),
            psf(51.0, 110.0, 0.0),
            psf(52.0, 120.0, 5.0),
        ];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        assert_eq!(bin.n, 1);
        assert_eq!(bin.flux_med, Some(120.0));
    }

    #[test]
    fn nonfinite_flux_err_dropped() {
        let pts = vec![
            psf(50.0, 100.0, f64::NAN),
            psf(51.0, 110.0, f64::INFINITY),
            psf(52.0, 120.0, 5.0),
        ];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        assert_eq!(bin.n, 1);
        assert_eq!(bin.flux_med, Some(120.0));
    }

    #[test]
    fn out_of_window_points_partition_correctly() {
        let pts = vec![
            psf(0.0, 50.0, 5.0),    // out (window starts at 10)
            psf(15.0, 100.0, 5.0),  // in
            psf(16.0, 110.0, 5.0),  // in
            psf(20.0, 200.0, 5.0),  // out (window ends at 20, half-open)
        ];
        let win = BinWindow {
            start_jd: 10.0,
            end_jd: 20.0,
        };
        let bin = bin_band(&pts, win, Band::R).unwrap();
        assert_eq!(bin.n, 2);
        // arithmetic mean of 100 and 110 = 105
        assert!((bin.flux_med.unwrap() - 105.0).abs() < 1e-9);
        assert_eq!(bin.window_start_jd, 10.0);
        assert_eq!(bin.window_days, 10.0);
    }

    #[test]
    fn flux_err_lower_bound_kicks_in_when_spread_is_small() {
        // Two points with the same flux: MAD = 0, so robust_sem = 0;
        // weighted-mean error must take over.
        let pts = vec![psf(50.0, 100.0, 5.0), psf(51.0, 100.0, 5.0)];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        // weighted-mean error = sqrt(1/(1/25 + 1/25)) = sqrt(12.5) ~ 3.536
        assert_eq!(bin.flux_mad, 0.0);
        assert!((bin.flux_err - 3.535533).abs() < 1e-5);
    }

    #[test]
    fn window_start_jd_is_stable_under_late_arriving_points() {
        // The `(band, window_start_jd)` pair is the bin's identity key. Two
        // bins computed against the same window must report the same
        // `window_start_jd` even when their median `jd` shifts as more
        // points arrive — this is what makes the binner's aggregation-pipeline
        // dedup robust to late-arriving photometry.
        let win = BinWindow { start_jd: 10.0, end_jd: 20.0 };
        let early = vec![psf(15.0, 100.0, 5.0)];
        let late = vec![psf(15.0, 100.0, 5.0), psf(11.0, 110.0, 5.0)];
        let bin_early = bin_band(&early, win, Band::R).unwrap();
        let bin_late = bin_band(&late, win, Band::R).unwrap();
        assert_ne!(bin_early.jd, bin_late.jd, "median jd should shift");
        assert_eq!(bin_early.window_start_jd, 10.0);
        assert_eq!(bin_late.window_start_jd, 10.0);
    }

    #[test]
    fn bin_band_windows_returns_one_bin_per_nonempty_window() {
        let pts = vec![
            psf(5.0, 100.0, 5.0),
            psf(15.0, 200.0, 5.0),
            psf(35.0, 300.0, 5.0),
        ];
        let windows = vec![
            BinWindow { start_jd: 0.0, end_jd: 10.0 },     // → bin
            BinWindow { start_jd: 10.0, end_jd: 20.0 },    // → bin
            BinWindow { start_jd: 20.0, end_jd: 30.0 },    // → empty, skipped
            BinWindow { start_jd: 30.0, end_jd: 40.0 },    // → bin
        ];
        let bins = bin_band_windows(&pts, &windows, Band::R);
        assert_eq!(bins.len(), 3);
        assert_eq!(bins[0].flux_med, Some(100.0));
        assert_eq!(bins[1].flux_med, Some(200.0));
        assert_eq!(bins[2].flux_med, Some(300.0));
    }

    #[test]
    fn bin_band_windows_handles_overlapping_windows_independently() {
        let pts = vec![psf(5.0, 100.0, 5.0), psf(15.0, 200.0, 5.0)];
        let windows = vec![
            BinWindow { start_jd: 0.0, end_jd: 10.0 },
            BinWindow { start_jd: 0.0, end_jd: 20.0 },
        ];
        let bins = bin_band_windows(&pts, &windows, Band::R);
        assert_eq!(bins.len(), 2);
        assert_eq!(bins[0].n, 1);
        assert_eq!(bins[1].n, 2);
    }

    #[test]
    fn band_is_passed_through_to_output() {
        let pts = vec![psf(50.0, 100.0, 5.0)];
        for band in [Band::G, Band::R, Band::I, Band::Z, Band::U, Band::Y] {
            let bin = bin_band(&pts, full_window(), band.clone()).unwrap();
            assert_eq!(bin.band, band);
        }
    }

    #[test]
    fn bin_serializes_round_trip_through_bson() {
        // Sanity check: BinnedPoint must round-trip through bson because that's
        // how it lives on the aux doc.
        let pts = vec![
            psf(50.0, 100.0, 5.0),
            fp(50.5, 105.0, 4.0),
            upper(51.0, 200.0),
        ];
        let bin = bin_band(&pts, full_window(), Band::R).unwrap();
        let doc = mongodb::bson::to_document(&bin).unwrap();
        let round_tripped: BinnedPoint = mongodb::bson::from_document(doc).unwrap();
        assert_eq!(bin, round_tripped);
    }

    // ─── pure-helper unit tests ──────────────────────────────────────────────

    #[test]
    fn weighted_mean_basics() {
        assert!((weighted_mean(&[1.0, 3.0], &[1.0, 1.0]) - 2.0).abs() < 1e-9);
        // weight 3:1 toward 1.0
        assert!((weighted_mean(&[1.0, 3.0], &[3.0, 1.0]) - 1.5).abs() < 1e-9);
    }

    #[test]
    fn weighted_median_basics() {
        // Equal weights, odd count → middle value.
        assert_eq!(weighted_median(&[1.0, 2.0, 3.0], &[1.0, 1.0, 1.0]), 2.0);
        // Equal weights, even count → upper-of-middle (cumulative weight ≥ half).
        // {1,2,3,4} total 4, half 2; cum: 1,2,3 → 2 hits at v=2.
        assert_eq!(
            weighted_median(&[1.0, 2.0, 3.0, 4.0], &[1.0, 1.0, 1.0, 1.0]),
            2.0
        );
        // Unsorted input.
        assert_eq!(weighted_median(&[3.0, 1.0, 2.0], &[1.0, 1.0, 1.0]), 2.0);
        // Heavily-weighted single point dominates.
        assert_eq!(weighted_median(&[1.0, 2.0, 3.0], &[1.0, 100.0, 1.0]), 2.0);
    }

    #[test]
    fn median_f64_basics() {
        assert_eq!(median_f64(&[1.0, 2.0, 3.0]), 2.0);
        assert_eq!(median_f64(&[1.0, 2.0, 3.0, 4.0]), 2.5);
        assert_eq!(median_f64(&[5.0]), 5.0);
        assert!(median_f64(&[]).is_nan());
        // Unsorted input.
        assert_eq!(median_f64(&[3.0, 1.0, 2.0]), 2.0);
    }

    #[test]
    fn mad_basics() {
        // values {1, 2, 3} around center 2 → |1-2|, |2-2|, |3-2| = {1, 0, 1} → MAD 1
        assert_eq!(median_absolute_deviation(&[1.0, 2.0, 3.0], 2.0), 1.0);
        // values {1, 100} around center 50 → {49, 50} → MAD 49.5
        assert_eq!(median_absolute_deviation(&[1.0, 100.0], 50.0), 49.5);
    }
}
