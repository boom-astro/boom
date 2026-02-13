use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::utils::lightcurves::{mag2flux, Band, PhotometryMag};

use super::nonparametric::NonparametricBandResult;
use super::parametric::ParametricBandResult;

/// Per-band time/value/error triplet for fitting.
#[derive(Debug, Clone)]
pub struct BandData {
    pub times: Vec<f64>,
    pub values: Vec<f64>,
    pub errors: Vec<f64>,
}

/// Combined result of nonparametric + parametric lightcurve fitting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightcurveFittingResult {
    pub nonparametric: Vec<NonparametricBandResult>,
    pub parametric: Vec<ParametricBandResult>,
}

/// Convert `PhotometryMag` to per-band magnitude data.
///
/// Groups by band, converts JD to relative time (days from minimum JD).
pub fn photometry_to_mag_bands(photometry: &[PhotometryMag]) -> HashMap<String, BandData> {
    if photometry.is_empty() {
        return HashMap::new();
    }

    let jd_min = photometry
        .iter()
        .map(|p| p.time)
        .fold(f64::INFINITY, f64::min);

    let mut bands: HashMap<String, BandData> = HashMap::new();
    for p in photometry {
        let mag = p.mag as f64;
        let mag_err = p.mag_err as f64;
        if !mag.is_finite() || !mag_err.is_finite() {
            continue;
        }
        let band_name = band_to_string(&p.band);
        let entry = bands.entry(band_name).or_insert_with(|| BandData {
            times: Vec::new(),
            values: Vec::new(),
            errors: Vec::new(),
        });
        entry.times.push(p.time - jd_min);
        entry.values.push(mag);
        entry.errors.push(mag_err);
    }

    bands
}

/// Convert `PhotometryMag` to per-band flux data.
///
/// Groups by band, converts JD to relative time, and converts mag to flux
/// using `mag2flux()` with ZP = 23.9.
pub fn photometry_to_flux_bands(photometry: &[PhotometryMag]) -> HashMap<String, BandData> {
    if photometry.is_empty() {
        return HashMap::new();
    }

    let jd_min = photometry
        .iter()
        .map(|p| p.time)
        .fold(f64::INFINITY, f64::min);

    let zp: f32 = 23.9;
    let mut bands: HashMap<String, BandData> = HashMap::new();
    for p in photometry {
        let (flux, flux_err) = mag2flux(p.mag, p.mag_err, zp);
        if !flux.is_finite() || !flux_err.is_finite() || flux <= 0.0 || flux_err <= 0.0 {
            continue;
        }
        let band_name = band_to_string(&p.band);
        let entry = bands.entry(band_name).or_insert_with(|| BandData {
            times: Vec::new(),
            values: Vec::new(),
            errors: Vec::new(),
        });
        entry.times.push(p.time - jd_min);
        entry.values.push(flux as f64);
        entry.errors.push(flux_err as f64);
    }

    bands
}

fn band_to_string(band: &Band) -> String {
    match band {
        Band::G => "g".to_string(),
        Band::R => "r".to_string(),
        Band::I => "i".to_string(),
        Band::Z => "z".to_string(),
        Band::Y => "y".to_string(),
        Band::U => "u".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Math utilities
// ---------------------------------------------------------------------------

pub fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        Some((values[mid - 1] + values[mid]) / 2.0)
    } else {
        Some(values[mid])
    }
}

pub fn extract_rise_timescale(times: &[f64], mags: &[f64], peak_idx: usize) -> f64 {
    if peak_idx == 0 || peak_idx >= mags.len() {
        return f64::NAN;
    }
    let peak_mag = mags[peak_idx];
    let n = peak_idx.min(3);
    let baseline = if n > 0 {
        mags[..n].iter().sum::<f64>() / n as f64
    } else {
        peak_mag + 0.5
    };
    let target_amp = baseline + (peak_mag - baseline) * (1.0 - (-1.0_f64).exp());
    let mut closest_t_before = f64::NAN;
    let mut closest_diff = f64::INFINITY;
    for i in 0..peak_idx {
        let diff = (mags[i] - target_amp).abs();
        if diff < closest_diff {
            closest_diff = diff;
            closest_t_before = times[i];
        }
    }
    if closest_t_before.is_nan() {
        return f64::NAN;
    }
    times[peak_idx] - closest_t_before
}

pub fn extract_decay_timescale(times: &[f64], mags: &[f64], peak_idx: usize) -> f64 {
    if peak_idx >= mags.len() - 1 {
        return f64::NAN;
    }
    let peak_mag = mags[peak_idx];
    let baseline = if peak_idx < mags.len() - 1 {
        let end_idx = ((peak_idx + mags.len()) / 2).min(mags.len() - 1);
        let count = (end_idx - peak_idx).max(1);
        mags[peak_idx + 1..=end_idx].iter().sum::<f64>() / count as f64
    } else {
        peak_mag + 0.5
    };
    let target_amp = baseline + (peak_mag - baseline) * (-1.0_f64).exp();
    let mut closest_t_after = f64::NAN;
    let mut closest_diff = f64::INFINITY;
    for i in (peak_idx + 1)..mags.len() {
        let diff = (mags[i] - target_amp).abs();
        if diff < closest_diff {
            closest_diff = diff;
            closest_t_after = times[i];
        }
    }
    if closest_t_after.is_nan() {
        return f64::NAN;
    }
    closest_t_after - times[peak_idx]
}

pub fn compute_fwhm(times: &[f64], mags: &[f64], peak_idx: usize) -> (f64, f64, f64) {
    if peak_idx >= mags.len() {
        return (f64::NAN, f64::NAN, f64::NAN);
    }
    let peak_mag = mags[peak_idx];
    let half_max_mag = peak_mag + 0.75;
    let mut t_before = f64::NAN;
    for i in (0..peak_idx).rev() {
        if mags[i] >= half_max_mag {
            t_before = times[i];
            break;
        }
    }
    let mut t_after = f64::NAN;
    for i in (peak_idx + 1)..mags.len() {
        if mags[i] >= half_max_mag {
            t_after = times[i];
            break;
        }
    }
    if t_before.is_nan() || t_after.is_nan() {
        return (f64::NAN, f64::NAN, f64::NAN);
    }
    (t_after - t_before, t_before, t_after)
}

pub fn compute_rise_rate(times: &[f64], mags: &[f64]) -> f64 {
    if times.len() < 2 {
        return f64::NAN;
    }
    let n_early = (times.len() as f64 * 0.25).ceil() as usize;
    let n_early = n_early.max(2).min(times.len() - 1);
    let early_times = &times[0..n_early];
    let early_mags = &mags[0..n_early];
    let n = early_times.len() as f64;
    let sum_t: f64 = early_times.iter().sum();
    let sum_m: f64 = early_mags.iter().sum();
    let sum_tt: f64 = early_times.iter().map(|t| t * t).sum();
    let sum_tm: f64 = early_times
        .iter()
        .zip(early_mags.iter())
        .map(|(t, m)| t * m)
        .sum();
    let denominator = n * sum_tt - sum_t * sum_t;
    if denominator.abs() < 1e-10 {
        return f64::NAN;
    }
    (n * sum_tm - sum_t * sum_m) / denominator
}

pub fn compute_decay_rate(times: &[f64], mags: &[f64]) -> f64 {
    if times.len() < 2 {
        return f64::NAN;
    }
    let n_late = (times.len() as f64 * 0.25).ceil() as usize;
    let n_late = n_late.max(2).min(times.len());
    let start_idx = times.len() - n_late;
    let late_times = &times[start_idx..];
    let late_mags = &mags[start_idx..];
    let n = late_times.len() as f64;
    let sum_t: f64 = late_times.iter().sum();
    let sum_m: f64 = late_mags.iter().sum();
    let sum_tt: f64 = late_times.iter().map(|t| t * t).sum();
    let sum_tm: f64 = late_times
        .iter()
        .zip(late_mags.iter())
        .map(|(t, m)| t * m)
        .sum();
    let denominator = n * sum_tt - sum_t * sum_t;
    if denominator.abs() < 1e-10 {
        return f64::NAN;
    }
    (n * sum_tm - sum_t * sum_m) / denominator
}

/// Convert NaN/Inf to None for BSON safety.
pub fn finite_or_none(v: f64) -> Option<f64> {
    if v.is_finite() {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::lightcurves::{Band, PhotometryMag};

    // -----------------------------------------------------------------------
    // median
    // -----------------------------------------------------------------------

    #[test]
    fn test_median_empty() {
        assert_eq!(median(&mut []), None);
    }

    #[test]
    fn test_median_single() {
        assert_eq!(median(&mut [5.0]), Some(5.0));
    }

    #[test]
    fn test_median_odd() {
        let mut vals = vec![3.0, 1.0, 2.0];
        assert_eq!(median(&mut vals), Some(2.0));
    }

    #[test]
    fn test_median_even() {
        let mut vals = vec![4.0, 1.0, 3.0, 2.0];
        assert_eq!(median(&mut vals), Some(2.5));
    }

    #[test]
    fn test_median_with_nan() {
        // NaN should be sorted to the end by total_cmp
        let mut vals = vec![1.0, f64::NAN, 3.0, 2.0];
        let result = median(&mut vals);
        assert!(result.is_some());
        // After sorting with total_cmp: [1.0, 2.0, 3.0, NaN]
        // median of even length = (2.0 + 3.0) / 2 = 2.5
        assert_eq!(result, Some(2.5));
    }

    // -----------------------------------------------------------------------
    // finite_or_none
    // -----------------------------------------------------------------------

    #[test]
    fn test_finite_or_none_finite() {
        assert_eq!(finite_or_none(1.5), Some(1.5));
        assert_eq!(finite_or_none(0.0), Some(0.0));
        assert_eq!(finite_or_none(-42.0), Some(-42.0));
    }

    #[test]
    fn test_finite_or_none_nan() {
        assert_eq!(finite_or_none(f64::NAN), None);
    }

    #[test]
    fn test_finite_or_none_inf() {
        assert_eq!(finite_or_none(f64::INFINITY), None);
        assert_eq!(finite_or_none(f64::NEG_INFINITY), None);
    }

    // -----------------------------------------------------------------------
    // extract_rise_timescale
    // -----------------------------------------------------------------------

    #[test]
    fn test_rise_timescale_peak_at_zero() {
        let times = vec![0.0, 1.0, 2.0];
        let mags = vec![18.0, 19.0, 20.0];
        // peak_idx == 0 → NaN
        assert!(extract_rise_timescale(&times, &mags, 0).is_nan());
    }

    #[test]
    fn test_rise_timescale_peak_out_of_bounds() {
        let times = vec![0.0, 1.0];
        let mags = vec![20.0, 19.0];
        assert!(extract_rise_timescale(&times, &mags, 5).is_nan());
    }

    #[test]
    fn test_rise_timescale_normal() {
        // Simulated brightening: magnitudes decrease toward peak
        let times = vec![0.0, 5.0, 10.0, 15.0, 20.0];
        let mags = vec![21.0, 20.5, 20.0, 19.5, 19.0];
        let result = extract_rise_timescale(&times, &mags, 4);
        assert!(result.is_finite());
        assert!(result > 0.0);
    }

    // -----------------------------------------------------------------------
    // extract_decay_timescale
    // -----------------------------------------------------------------------

    #[test]
    fn test_decay_timescale_peak_at_end() {
        let times = vec![0.0, 1.0, 2.0];
        let mags = vec![20.0, 19.0, 18.0];
        // peak at last element → NaN
        assert!(extract_decay_timescale(&times, &mags, 2).is_nan());
    }

    #[test]
    fn test_decay_timescale_normal() {
        // Peak in the middle, fading after
        let times = vec![0.0, 5.0, 10.0, 15.0, 20.0];
        let mags = vec![20.0, 19.0, 18.5, 19.0, 20.0];
        let result = extract_decay_timescale(&times, &mags, 2);
        assert!(result.is_finite());
        assert!(result > 0.0);
    }

    // -----------------------------------------------------------------------
    // compute_fwhm
    // -----------------------------------------------------------------------

    #[test]
    fn test_fwhm_out_of_bounds() {
        let times = vec![0.0, 1.0];
        let mags = vec![19.0, 20.0];
        let (fwhm, tb, ta) = compute_fwhm(&times, &mags, 5);
        assert!(fwhm.is_nan());
        assert!(tb.is_nan());
        assert!(ta.is_nan());
    }

    #[test]
    fn test_fwhm_symmetric() {
        // Build a symmetric lightcurve with peak at center
        // Peak mag = 18.0, half-max threshold = 18.0 + 0.75 = 18.75
        let times = vec![0.0, 5.0, 10.0, 15.0, 20.0];
        let mags = vec![20.0, 19.0, 18.0, 19.0, 20.0];
        let (fwhm, t_before, t_after) = compute_fwhm(&times, &mags, 2);
        assert!(fwhm.is_finite());
        assert!(t_before.is_finite());
        assert!(t_after.is_finite());
        // t_before=5.0, t_after=15.0 → fwhm=10.0
        assert!((fwhm - 10.0).abs() < 1e-10);
        assert!((t_before - 5.0).abs() < 1e-10);
        assert!((t_after - 15.0).abs() < 1e-10);
    }

    #[test]
    fn test_fwhm_no_crossing() {
        // All mags brighter than half-max threshold → no crossing found
        let times = vec![0.0, 1.0, 2.0];
        let mags = vec![18.0, 17.5, 18.0];
        // half_max_mag = 17.5 + 0.75 = 18.25
        // mags[0] = 18.0 < 18.25, mags[2] = 18.0 < 18.25 → no crossing after peak
        let (fwhm, _, _) = compute_fwhm(&times, &mags, 1);
        assert!(fwhm.is_nan());
    }

    // -----------------------------------------------------------------------
    // compute_rise_rate / compute_decay_rate
    // -----------------------------------------------------------------------

    #[test]
    fn test_rise_rate_insufficient_data() {
        assert!(compute_rise_rate(&[1.0], &[20.0]).is_nan());
        assert!(compute_rise_rate(&[], &[]).is_nan());
    }

    #[test]
    fn test_rise_rate_linear() {
        // Perfect linear data: mag = 20 - 0.5*t
        let times: Vec<f64> = (0..10).map(|i| i as f64).collect();
        let mags: Vec<f64> = times.iter().map(|t| 20.0 - 0.5 * t).collect();
        let rate = compute_rise_rate(&times, &mags);
        assert!(rate.is_finite());
        // Rise rate uses early 25% of data, slope should be ~-0.5
        assert!((rate - (-0.5)).abs() < 0.1);
    }

    #[test]
    fn test_decay_rate_insufficient_data() {
        assert!(compute_decay_rate(&[1.0], &[20.0]).is_nan());
    }

    #[test]
    fn test_decay_rate_linear() {
        // Perfect linear data: mag = 18 + 0.3*t
        let times: Vec<f64> = (0..10).map(|i| i as f64).collect();
        let mags: Vec<f64> = times.iter().map(|t| 18.0 + 0.3 * t).collect();
        let rate = compute_decay_rate(&times, &mags);
        assert!(rate.is_finite());
        // Decay rate uses last 25% of data, slope should be ~0.3
        assert!((rate - 0.3).abs() < 0.1);
    }

    // -----------------------------------------------------------------------
    // photometry_to_mag_bands
    // -----------------------------------------------------------------------

    #[test]
    fn test_photometry_to_mag_bands_empty() {
        let result = photometry_to_mag_bands(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_photometry_to_mag_bands_single_band() {
        let photometry = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.5,
                mag_err: 0.12,
                band: Band::R,
            },
        ];
        let bands = photometry_to_mag_bands(&photometry);
        assert_eq!(bands.len(), 1);
        let r = bands.get("r").unwrap();
        assert_eq!(r.times.len(), 2);
        assert_eq!(r.values.len(), 2);
        // Times should be relative to jd_min
        assert!((r.times[0] - 0.0).abs() < 1e-10);
        assert!((r.times[1] - 1.0).abs() < 1e-10);
        assert!((r.values[0] - 20.0).abs() < 1e-10);
        assert!((r.values[1] - 19.5).abs() < 1e-10);
    }

    #[test]
    fn test_photometry_to_mag_bands_multiple_bands() {
        let photometry = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459000.5,
                mag: 20.5,
                mag_err: 0.15,
                band: Band::G,
            },
        ];
        let bands = photometry_to_mag_bands(&photometry);
        assert_eq!(bands.len(), 2);
        assert!(bands.contains_key("r"));
        assert!(bands.contains_key("g"));
    }

    #[test]
    fn test_photometry_to_mag_bands_filters_nan() {
        let photometry = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: f32::NAN,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 20.0,
                mag_err: f32::NAN,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459002.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        let bands = photometry_to_mag_bands(&photometry);
        // Only the third point should survive
        let r = bands.get("r").unwrap();
        assert_eq!(r.times.len(), 1);
        assert!((r.values[0] - 19.0).abs() < 1e-10);
    }

    // -----------------------------------------------------------------------
    // photometry_to_flux_bands
    // -----------------------------------------------------------------------

    #[test]
    fn test_photometry_to_flux_bands_empty() {
        let result = photometry_to_flux_bands(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_photometry_to_flux_bands_converts_correctly() {
        let photometry = vec![PhotometryMag {
            time: 2459000.5,
            mag: 20.0,
            mag_err: 0.1,
            band: Band::R,
        }];
        let bands = photometry_to_flux_bands(&photometry);
        let r = bands.get("r").unwrap();
        assert_eq!(r.times.len(), 1);
        assert!((r.times[0] - 0.0).abs() < 1e-10);
        // flux should be positive and finite
        assert!(r.values[0] > 0.0);
        assert!(r.values[0].is_finite());
        assert!(r.errors[0] > 0.0);
        assert!(r.errors[0].is_finite());
    }
}
