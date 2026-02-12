use std::collections::HashMap;

use scirs2_core::ndarray::{Array1, Array2, Axis};
use serde::{Deserialize, Serialize};
use sklears_core::traits::{Fit, Predict, Untrained};
use sklears_gaussian_process::{
    kernels::{ConstantKernel, ProductKernel, SumKernel, WhiteKernel, RBF},
    GaussianProcessRegressor, GprTrained, Kernel,
};

use super::common::{
    compute_decay_rate, compute_fwhm, compute_rise_rate, extract_decay_timescale,
    extract_rise_timescale, finite_or_none, BandData,
};

/// Result of nonparametric GP fitting for a single band.
/// All f64 fields are `Option<f64>` for BSON safety (NaN → None).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonparametricBandResult {
    pub band: String,
    pub rise_time: Option<f64>,
    pub decay_time: Option<f64>,
    pub t0: Option<f64>,
    pub peak_mag: Option<f64>,
    pub chi2: Option<f64>,
    pub baseline_chi2: Option<f64>,
    pub n_obs: usize,
    pub fwhm: Option<f64>,
    pub rise_rate: Option<f64>,
    pub decay_rate: Option<f64>,
    pub gp_dfdt_now: Option<f64>,
    pub gp_dfdt_next: Option<f64>,
    pub gp_d2fdt2_now: Option<f64>,
    pub gp_predicted_mag_1d: Option<f64>,
    pub gp_predicted_mag_2d: Option<f64>,
    pub gp_time_to_peak: Option<f64>,
    pub gp_extrap_slope: Option<f64>,
    pub gp_sigma_f: Option<f64>,
    pub gp_peak_to_peak: Option<f64>,
    pub gp_snr_max: Option<f64>,
    pub gp_dfdt_max: Option<f64>,
    pub gp_dfdt_min: Option<f64>,
    pub gp_frac_of_peak: Option<f64>,
    pub gp_post_var_mean: Option<f64>,
    pub gp_post_var_max: Option<f64>,
    pub gp_skewness: Option<f64>,
    pub gp_kurtosis: Option<f64>,
    pub gp_n_inflections: Option<f64>,
}

// ---------------------------------------------------------------------------
// FastGP fallback with early-time weighting
// ---------------------------------------------------------------------------

struct FastGP {
    base: GaussianProcessRegressor<Untrained>,
}

impl FastGP {
    fn new(t_max: f64) -> Self {
        let amp = 0.2;
        let cst: Box<dyn Kernel> = Box::new(ConstantKernel::new(amp));
        let lengthscale = (t_max / 16.0).max(0.3).min(12.0);
        let rbf: Box<dyn Kernel> = Box::new(RBF::new(lengthscale));
        let prod = Box::new(ProductKernel::new(vec![cst, rbf]));
        let white = Box::new(WhiteKernel::new(1e-10));
        let kernel = SumKernel::new(vec![prod, white]);

        let base = GaussianProcessRegressor::new()
            .kernel(Box::new(kernel))
            .alpha(1e-10)
            .normalize_y(true);

        Self { base }
    }

    fn fit(
        &self,
        times: &Array1<f64>,
        values: &Array1<f64>,
        errors: &[f64],
    ) -> Option<GaussianProcessRegressor<GprTrained>> {
        let t_min = times.iter().cloned().fold(f64::INFINITY, f64::min);
        let t_max = times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let t_range = t_max - t_min;
        let early_time_cutoff = t_min + 0.2 * t_range;

        let weighted_errors: Vec<f64> = times
            .iter()
            .zip(errors.iter())
            .map(|(t, e)| {
                if t <= &early_time_cutoff {
                    *e * 0.7
                } else {
                    *e
                }
            })
            .collect();

        let avg_error_var = if !weighted_errors.is_empty() {
            weighted_errors.iter().map(|e| e * e).sum::<f64>() / weighted_errors.len() as f64
        } else {
            1e-4
        };

        let alpha_with_errors = avg_error_var.max(1e-5);
        let gp_with_alpha = self.base.clone().alpha(alpha_with_errors);
        let xt = times.view().insert_axis(Axis(1)).to_owned();
        gp_with_alpha.fit(&xt, values).ok()
    }
}

// ---------------------------------------------------------------------------
// GP fitting helper
// ---------------------------------------------------------------------------

fn fit_sklears_gp(
    times: &Array1<f64>,
    values: &Array1<f64>,
    amp: f64,
    lengthscale: f64,
    alpha: f64,
) -> Option<GaussianProcessRegressor<GprTrained>> {
    let cst: Box<dyn Kernel> = Box::new(ConstantKernel::new(amp));
    let rbf: Box<dyn Kernel> = Box::new(RBF::new(lengthscale));
    let prod = Box::new(ProductKernel::new(vec![cst, rbf]));
    let white = Box::new(WhiteKernel::new(1e-10));
    let kernel = SumKernel::new(vec![prod, white]);

    let gp = GaussianProcessRegressor::new()
        .kernel(Box::new(kernel))
        .alpha(alpha)
        .normalize_y(true);

    let xt = times.view().insert_axis(Axis(1)).to_owned();
    gp.fit(&xt, values).ok()
}

// ---------------------------------------------------------------------------
// Subsampling
// ---------------------------------------------------------------------------

fn subsample_data(
    times: &[f64],
    mags: &[f64],
    errors: &[f64],
    max_points: usize,
) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    if times.len() <= max_points {
        return (times.to_vec(), mags.to_vec(), errors.to_vec());
    }

    let step = times.len() as f64 / max_points as f64;
    let mut indices = Vec::with_capacity(max_points);
    for i in 0..max_points {
        let idx = ((i as f64 + 0.5) * step).floor() as usize;
        indices.push(idx.min(times.len() - 1));
    }

    let times_sub: Vec<f64> = indices.iter().map(|&i| times[i]).collect();
    let mags_sub: Vec<f64> = indices.iter().map(|&i| mags[i]).collect();
    let errors_sub: Vec<f64> = indices.iter().map(|&i| errors[i]).collect();

    (times_sub, mags_sub, errors_sub)
}

// ---------------------------------------------------------------------------
// Predictive features
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PredictiveFeatures {
    gp_dfdt_now: f64,
    gp_dfdt_next: f64,
    gp_d2fdt2_now: f64,
    gp_predicted_mag_1d: f64,
    gp_predicted_mag_2d: f64,
    gp_time_to_peak: f64,
    gp_extrap_slope: f64,
    gp_sigma_f: f64,
    gp_peak_to_peak: f64,
    gp_snr_max: f64,
    gp_dfdt_max: f64,
    gp_dfdt_min: f64,
    gp_frac_of_peak: f64,
    gp_post_var_mean: f64,
    gp_post_var_max: f64,
    gp_skewness: f64,
    gp_kurtosis: f64,
    gp_n_inflections: f64,
}

fn compute_predictive_features(
    gp: &GaussianProcessRegressor<GprTrained>,
    t_last: f64,
    t0: f64,
    times_pred: &[f64],
    pred: &[f64],
    std: &[f64],
    obs_mags: &[f64],
    obs_errors: &[f64],
) -> PredictiveFeatures {
    let dt = 1.0;
    let tq = vec![
        t_last - dt,
        t_last,
        t_last + dt,
        t_last + 2.0 * dt,
        t_last + 3.0 * dt,
    ];
    let xq = Array2::from_shape_fn((tq.len(), 1), |(i, _)| tq[i]);
    let y = gp.predict(&xq).unwrap().to_vec();

    let f_m1 = y[0];
    let f_0 = y[1];
    let f_p1 = y[2];
    let f_p2 = y[3];
    let f_p3 = y[4];

    // Variability strength
    let gp_sigma_f = if !pred.is_empty() {
        let mean = pred.iter().sum::<f64>() / pred.len() as f64;
        (pred.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / pred.len() as f64).sqrt()
    } else {
        f64::NAN
    };

    let gp_peak_to_peak = if !pred.is_empty() {
        let max_mag = pred.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let min_mag = pred.iter().cloned().fold(f64::INFINITY, f64::min);
        max_mag - min_mag
    } else {
        f64::NAN
    };

    let gp_snr_max = if !obs_mags.is_empty() && !obs_errors.is_empty() {
        obs_mags
            .iter()
            .zip(obs_errors.iter())
            .map(|(mag, err)| mag.abs() / err)
            .fold(f64::NEG_INFINITY, f64::max)
    } else {
        f64::NAN
    };

    // Derivative features
    let gp_dfdt_max = if pred.len() > 1 && times_pred.len() > 1 {
        let dt_grid = times_pred[1] - times_pred[0];
        (0..pred.len() - 1)
            .map(|i| (pred[i + 1] - pred[i]) / dt_grid)
            .fold(f64::NEG_INFINITY, f64::max)
    } else {
        f64::NAN
    };

    let gp_dfdt_min = if pred.len() > 1 && times_pred.len() > 1 {
        let dt_grid = times_pred[1] - times_pred[0];
        (0..pred.len() - 1)
            .map(|i| (pred[i + 1] - pred[i]) / dt_grid)
            .fold(f64::INFINITY, f64::min)
    } else {
        f64::NAN
    };

    // Phase feature
    let gp_frac_of_peak = if !pred.is_empty() {
        let peak_mag = pred.iter().cloned().fold(f64::INFINITY, f64::min);
        let last_mag = pred.last().copied().unwrap_or(f64::NAN);
        last_mag / peak_mag
    } else {
        f64::NAN
    };

    // Uncertainty quantification
    let gp_post_var_mean = if !std.is_empty() {
        std.iter().map(|s| s * s).sum::<f64>() / std.len() as f64
    } else {
        f64::NAN
    };

    let gp_post_var_max = if !std.is_empty() {
        std.iter().map(|s| s * s).fold(f64::NEG_INFINITY, f64::max)
    } else {
        f64::NAN
    };

    // Statistical shape features
    let (gp_skewness, gp_kurtosis) = if !pred.is_empty() && pred.len() > 3 {
        let mean = pred.iter().sum::<f64>() / pred.len() as f64;
        let variance = pred.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / pred.len() as f64;
        let std_dev = variance.sqrt();

        if std_dev > 1e-10 {
            let skew = pred
                .iter()
                .map(|&x| ((x - mean) / std_dev).powi(3))
                .sum::<f64>()
                / pred.len() as f64;
            let kurt = pred
                .iter()
                .map(|&x| ((x - mean) / std_dev).powi(4))
                .sum::<f64>()
                / pred.len() as f64
                - 3.0;
            (skew, kurt)
        } else {
            (f64::NAN, f64::NAN)
        }
    } else {
        (f64::NAN, f64::NAN)
    };

    // Inflection points
    let gp_n_inflections = if pred.len() > 2 && times_pred.len() > 2 {
        let dt_grid = times_pred[1] - times_pred[0];
        let mut d2: Vec<f64> = Vec::with_capacity(pred.len().saturating_sub(2));
        for i in 1..(pred.len() - 1) {
            let v = (pred[i + 1] - 2.0 * pred[i] + pred[i - 1]) / (dt_grid * dt_grid);
            d2.push(v);
        }
        let eps = 1e-6_f64;
        let mut count = 0usize;
        for i in 0..(d2.len().saturating_sub(1)) {
            let a = d2[i];
            let b = d2[i + 1];
            if a.is_finite() && b.is_finite() && a.abs() > eps && b.abs() > eps && (a * b) < 0.0 {
                count += 1;
            }
        }
        count as f64
    } else {
        f64::NAN
    };

    PredictiveFeatures {
        gp_dfdt_now: (f_0 - f_m1) / dt,
        gp_dfdt_next: (f_p1 - f_0) / dt,
        gp_d2fdt2_now: (f_p1 - 2.0 * f_0 + f_m1) / (dt * dt),
        gp_predicted_mag_1d: f_p1,
        gp_predicted_mag_2d: f_p2,
        gp_time_to_peak: t0 - t_last,
        gp_extrap_slope: (f_p3 - f_p2) / dt,
        gp_sigma_f,
        gp_peak_to_peak,
        gp_snr_max,
        gp_dfdt_max,
        gp_dfdt_min,
        gp_frac_of_peak,
        gp_post_var_mean,
        gp_post_var_max,
        gp_skewness,
        gp_kurtosis,
        gp_n_inflections,
    }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/// Fit nonparametric GP models to all bands.
///
/// `bands` maps band names to `BandData` containing magnitude values.
pub fn fit_nonparametric(bands: &HashMap<String, BandData>) -> Vec<NonparametricBandResult> {
    if bands.is_empty() {
        return Vec::new();
    }

    let mut t_min = f64::INFINITY;
    let mut t_max = f64::NEG_INFINITY;
    for band_data in bands.values() {
        for &t in &band_data.times {
            t_min = t_min.min(t);
            t_max = t_max.max(t);
        }
    }
    let duration = t_max - t_min;
    if duration <= 0.0 {
        return Vec::new();
    }

    let n_pred = 50;
    let times_pred: Vec<f64> = (0..n_pred)
        .map(|i| t_min + (i as f64) * duration / (n_pred - 1) as f64)
        .collect();
    let times_pred_arr = Array1::from_vec(times_pred.clone());
    let times_pred_2d = times_pred_arr.view().insert_axis(Axis(1)).to_owned();

    let min_points_for_independent_fit = 5;
    let amp_candidates = vec![0.05, 0.1, 0.2, 0.4];
    let ls_factors = vec![4.0, 6.0, 8.0, 12.0, 16.0, 24.0];

    let mut results = Vec::new();

    for (band_name, band_data) in bands {
        if band_data.times.len() < min_points_for_independent_fit {
            continue;
        }

        let max_subsample = if band_data.times.len() <= 30 {
            band_data.times.len()
        } else {
            25
        };
        let (times_sub, mags_sub, errors_sub) = subsample_data(
            &band_data.times,
            &band_data.values,
            &band_data.errors,
            max_subsample,
        );

        let times_arr = Array1::from_vec(times_sub);
        let mags_arr = Array1::from_vec(mags_sub);

        // Compute average error variance for alpha candidates
        let avg_error_var = if !errors_sub.is_empty() {
            errors_sub.iter().map(|e| e * e).sum::<f64>() / errors_sub.len() as f64
        } else {
            1e-4
        };
        let alpha_candidates = vec![avg_error_var.max(1e-6), avg_error_var.max(1e-4)];

        // Compute minimum lengthscale from data sampling
        let mut dt_vec: Vec<f64> = Vec::new();
        for w in 1..times_arr.len() {
            dt_vec.push(times_arr[w] - times_arr[w - 1]);
        }
        dt_vec.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median_dt = if !dt_vec.is_empty() {
            dt_vec[dt_vec.len() / 2]
        } else {
            1.0
        };
        let min_lengthscale = (median_dt * 2.0).max(0.1);

        let xt_sub = times_arr.view().insert_axis(Axis(1)).to_owned();
        let times_orig_2d = Array1::from_vec(band_data.times.clone())
            .view()
            .insert_axis(Axis(1))
            .to_owned();

        // Grid search over amplitude, lengthscale, and alpha
        let mut best_gp: Option<GaussianProcessRegressor<GprTrained>> = None;
        let mut best_score = f64::INFINITY;

        for &amp in &amp_candidates {
            for &factor in &ls_factors {
                let lengthscale = (duration / factor).max(0.1);
                if lengthscale < min_lengthscale {
                    continue;
                }

                for &alpha in &alpha_candidates {
                    if let Some(trained) =
                        fit_sklears_gp(&times_arr, &mags_arr, amp, lengthscale, alpha)
                    {
                        if let Ok(pred_at_obs) = trained.predict(&xt_sub) {
                            let mut residuals_sq = 0.0f64;
                            for i in 0..mags_arr.len() {
                                let residual = mags_arr[i] - pred_at_obs[i];
                                residuals_sq += residual * residual;
                            }
                            let rms = (residuals_sq / mags_arr.len() as f64).sqrt();

                            // Compute mean predictive std to penalize overconfident fits
                            let mut mean_pred_std = 0.0f64;
                            if let Ok((pred_std_obs, _)) = trained.predict_with_std(&xt_sub) {
                                let v = pred_std_obs.to_vec();
                                let (ssum, scnt) = v
                                    .iter()
                                    .filter(|s| s.is_finite())
                                    .fold((0.0f64, 0usize), |(s, c), &val| (s + val, c + 1));
                                if scnt > 0 {
                                    mean_pred_std = ssum / scnt as f64;
                                }
                            }

                            // Reject candidates with absurd extrapolated peak magnitudes
                            if let Ok(pred_grid) = trained.predict(&times_pred_2d) {
                                let pred_grid_min =
                                    pred_grid.iter().cloned().fold(f64::INFINITY, f64::min);
                                let obs_min =
                                    mags_arr.iter().cloned().fold(f64::INFINITY, f64::min);
                                if pred_grid_min.is_finite()
                                    && (pred_grid_min - obs_min).abs() > 6.0
                                {
                                    continue;
                                }
                            }

                            // Combined score: fit quality + uncertainty penalty
                            let penalty_coef = 0.6_f64;
                            let score = rms + penalty_coef * mean_pred_std;
                            if score.is_finite() && score < best_score {
                                best_score = score;
                                best_gp = Some(trained);
                            }
                        }
                    }
                }
            }
        }

        // Fallback to FastGP with early-time weighting if grid search failed
        if best_gp.is_none() {
            let fallback = FastGP::new(duration);
            best_gp = fallback.fit(&times_arr, &mags_arr, &errors_sub);
        }

        if let Some(gp_fit) = best_gp {
            // Get spatially-varying uncertainty from predict_with_std
            let (pred, mut std_vec) =
                if let Ok((pred_arr, pred_std_arr)) = gp_fit.predict_with_std(&times_pred_2d) {
                    (pred_arr.to_vec(), pred_std_arr.to_vec())
                } else if let Ok(pred_arr) = gp_fit.predict(&times_pred_2d) {
                    // Fallback: if predict_with_std fails, use flat uncertainty
                    let p = pred_arr.to_vec();
                    let s = vec![0.1; p.len()];
                    (p, s)
                } else {
                    continue;
                };

            // Compute RMS residual at observed points for uncertainty rescaling
            if let Ok(pred_at_obs_arr) = gp_fit.predict(&times_orig_2d) {
                let pred_at_obs = pred_at_obs_arr.to_vec();

                let mut residuals_sq = 0.0;
                for i in 0..band_data.values.len() {
                    let residual = band_data.values[i] - pred_at_obs[i];
                    residuals_sq += residual * residual;
                }
                let rms_residual = (residuals_sq / band_data.values.len() as f64).sqrt();

                // Scale GP std to match observed RMS with conservative shrinkage
                if let Ok((pred_std_obs, _)) = gp_fit.predict_with_std(&times_orig_2d) {
                    let pred_std_obs_vec = pred_std_obs.to_vec();
                    let (sum, cnt) = pred_std_obs_vec
                        .iter()
                        .filter(|s| s.is_finite())
                        .fold((0.0f64, 0usize), |(s, c), &val| (s + val, c + 1));
                    if cnt > 0 {
                        let mean_pred_std_obs = sum / cnt as f64;
                        if mean_pred_std_obs > 1e-12 && rms_residual.is_finite() {
                            let mut scale = (rms_residual / mean_pred_std_obs).max(0.05).min(5.0);
                            scale *= 0.6; // conservative shrinkage
                            for v in std_vec.iter_mut() {
                                *v *= scale;
                            }
                        }
                    }
                }

                // Compute chi2 and features
                let mut chi2 = 0.0;
                let mut baseline_var = 0.0;
                let mean_mag = band_data.values.iter().sum::<f64>() / band_data.values.len() as f64;
                for i in 0..band_data.values.len() {
                    let residual = band_data.values[i] - pred_at_obs[i];
                    let err_sq = band_data.errors[i] * band_data.errors[i] + 1e-10;
                    chi2 += residual * residual / err_sq;
                    baseline_var += (band_data.values[i] - mean_mag).powi(2) / err_sq;
                }
                let chi2_reduced = chi2 / band_data.values.len().max(1) as f64;
                let baseline_chi2 = baseline_var / band_data.values.len().max(1) as f64;

                let peak_idx = pred
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                let t0 = times_pred[peak_idx];
                let peak_mag = pred[peak_idx];

                let rise_time = extract_rise_timescale(&times_pred, &pred, peak_idx);
                let decay_time = extract_decay_timescale(&times_pred, &pred, peak_idx);
                let (fwhm_calc, t_before, t_after) = compute_fwhm(&times_pred, &pred, peak_idx);
                let fwhm = if !t_before.is_nan() && !t_after.is_nan() {
                    t_after - t_before
                } else {
                    fwhm_calc
                };
                let rise_rate = compute_rise_rate(&times_pred, &pred);
                let decay_rate = compute_decay_rate(&times_pred, &pred);

                let t_last = *band_data.times.last().unwrap();
                let predictive = compute_predictive_features(
                    &gp_fit,
                    t_last,
                    t0,
                    &times_pred,
                    &pred,
                    &std_vec,
                    &band_data.values,
                    &band_data.errors,
                );

                results.push(NonparametricBandResult {
                    band: band_name.clone(),
                    rise_time: finite_or_none(rise_time),
                    decay_time: finite_or_none(decay_time),
                    t0: finite_or_none(t0),
                    peak_mag: finite_or_none(peak_mag),
                    chi2: finite_or_none(chi2_reduced),
                    baseline_chi2: finite_or_none(baseline_chi2),
                    n_obs: band_data.values.len(),
                    fwhm: finite_or_none(fwhm),
                    rise_rate: finite_or_none(rise_rate),
                    decay_rate: finite_or_none(decay_rate),
                    gp_dfdt_now: finite_or_none(predictive.gp_dfdt_now),
                    gp_dfdt_next: finite_or_none(predictive.gp_dfdt_next),
                    gp_d2fdt2_now: finite_or_none(predictive.gp_d2fdt2_now),
                    gp_predicted_mag_1d: finite_or_none(predictive.gp_predicted_mag_1d),
                    gp_predicted_mag_2d: finite_or_none(predictive.gp_predicted_mag_2d),
                    gp_time_to_peak: finite_or_none(predictive.gp_time_to_peak),
                    gp_extrap_slope: finite_or_none(predictive.gp_extrap_slope),
                    gp_sigma_f: finite_or_none(predictive.gp_sigma_f),
                    gp_peak_to_peak: finite_or_none(predictive.gp_peak_to_peak),
                    gp_snr_max: finite_or_none(predictive.gp_snr_max),
                    gp_dfdt_max: finite_or_none(predictive.gp_dfdt_max),
                    gp_dfdt_min: finite_or_none(predictive.gp_dfdt_min),
                    gp_frac_of_peak: finite_or_none(predictive.gp_frac_of_peak),
                    gp_post_var_mean: finite_or_none(predictive.gp_post_var_mean),
                    gp_post_var_max: finite_or_none(predictive.gp_post_var_max),
                    gp_skewness: finite_or_none(predictive.gp_skewness),
                    gp_kurtosis: finite_or_none(predictive.gp_kurtosis),
                    gp_n_inflections: finite_or_none(predictive.gp_n_inflections),
                });
            }
        }
    }

    results
}
