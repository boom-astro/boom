use std::collections::HashMap;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BandData {
    pub times: Vec<f64>,
    pub mags: Vec<f64>,
    pub errors: Vec<f64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TimescaleParams {
    pub object: String,
    pub band: String,
    pub method: String,
    pub rise_time: f64,
    pub decay_time: f64,
    pub t0: f64,
    pub peak_mag: f64,
    pub chi2: f64,
    pub baseline_chi2: f64,
    pub n_obs: usize,
    pub fwhm: f64,
    pub rise_rate: f64,
    pub decay_rate: f64,
    pub gp_dfdt_now: f64,
    pub gp_dfdt_next: f64,
    pub gp_d2fdt2_now: f64,
    pub gp_predicted_mag_1d: f64,
    pub gp_predicted_mag_2d: f64,
    pub gp_time_to_peak: f64,
    pub gp_extrap_slope: f64,
    #[serde(rename = "gp_T_peak")]
    pub gp_t_peak: f64,
    #[serde(rename = "gp_T_now")]
    pub gp_t_now: f64,
    #[serde(rename = "gp_dTdt_peak")]
    pub gp_dtdt_peak: f64,
    #[serde(rename = "gp_dTdt_now")]
    pub gp_dtdt_now: f64,
    pub gp_sigma_f: f64,
    pub gp_peak_to_peak: f64,
    pub gp_snr_max: f64,
    pub gp_dfdt_max: f64,
    pub gp_dfdt_min: f64,
    pub gp_frac_of_peak: f64,
    pub gp_post_var_mean: f64,
    pub gp_post_var_max: f64,
    pub gp_skewness: f64,
    pub gp_kurtosis: f64,
    pub gp_n_inflections: f64,
}

/// Convert flux values to magnitudes (AB system).
/// Returns (mag, mag_err) tuples for each point. Skips points with flux <= 0.
pub fn flux_to_mag_bands(
    bands_flux: HashMap<String, (Vec<f64>, Vec<f64>, Vec<f64>)>,
) -> HashMap<String, BandData> {
    let zp = 23.9;
    let mut result = HashMap::new();
    for (name, (times, fluxes, flux_errs)) in bands_flux {
        let mut bd = BandData {
            times: Vec::new(),
            mags: Vec::new(),
            errors: Vec::new(),
        };
        for i in 0..times.len() {
            let flux = fluxes[i];
            if flux > 0.0 {
                let mag = -2.5 * flux.log10() + zp;
                let mag_err = 1.0857 * flux_errs[i] / flux;
                bd.times.push(times[i]);
                bd.mags.push(mag);
                bd.errors.push(mag_err);
            }
        }
        if !bd.times.is_empty() {
            result.insert(name, bd);
        }
    }
    result
}

pub fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
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
    let baseline = if peak_idx > 1 {
        mags[..peak_idx.min(3)].iter().sum::<f64>() / mags[..peak_idx.min(3)].len() as f64
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
        let end_idx = (peak_idx + mags.len()) / 2;
        let end_idx = end_idx.min(mags.len() - 1);
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
    let sum_t = early_times.iter().sum::<f64>();
    let sum_m = early_mags.iter().sum::<f64>();
    let sum_tt = early_times.iter().map(|t| t * t).sum::<f64>();
    let sum_tm = early_times
        .iter()
        .zip(early_mags.iter())
        .map(|(t, m)| t * m)
        .sum::<f64>();
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
    let sum_t = late_times.iter().sum::<f64>();
    let sum_m = late_mags.iter().sum::<f64>();
    let sum_tt = late_times.iter().map(|t| t * t).sum::<f64>();
    let sum_tm = late_times
        .iter()
        .zip(late_mags.iter())
        .map(|(t, m)| t * m)
        .sum::<f64>();
    let denominator = n * sum_tt - sum_t * sum_t;
    if denominator.abs() < 1e-10 {
        return f64::NAN;
    }
    (n * sum_tm - sum_t * sum_m) / denominator
}
