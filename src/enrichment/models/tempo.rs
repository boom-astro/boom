/// TEMPO Evidential Classifier model for BOOM enrichment.
///
/// Takes a ZTF alert's photometry history and produces:
///   - Per-class evidence values and probabilities for 5 transient classes
///     (SNI, SNII, TDE, AGN, CV)
///   - Uses Evidential Deep Learning (Dirichlet) for uncertainty-aware
///     classification
///
/// The model was trained in PyTorch by Felipe Fontenele-Nunes and exported
/// to ONNX via `export_onnx_tempo.py`.
///
/// Input tensors:
///   x:               (1, 257, 5) — [log1p(dt), log1p(dt_prev), logflux, logflux_err, band_id]
///   pad_mask:         (1, 257)    — true = padding
///   global_features:  (1, 24)     — physics summary features
///
/// The 4 continuous channels are normalized with mean/std from
/// `feature_stats_day100.npz`. Band is an integer index (g=0, r=1).
/// i-band observations are dropped.
use crate::enrichment::{ZtfAlertForEnrichment, models::ModelError};
use crate::utils::lightcurves::Band;
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;

/// Maximum sequence length the ONNX model accepts
const MAX_LEN: usize = 257;

/// Number of input channels per timestep (embed mode: 4 continuous + 1 band_id)
const IN_CHANNELS: usize = 5;

/// Number of output classes (transient_variable_5c taxonomy)
const N_CLASSES: usize = 5;

/// Number of global physics features
const N_GLOBAL: usize = 24;

/// Default uncertainty threshold for confident classifications.
/// Objects with uncertainty > this value should be flagged for human review.
///
/// Derived from validation on 2,737 labeled test samples:
///   - Correct predictions: mean uncertainty = 0.192
///   - Incorrect predictions: mean uncertainty = 0.260
///   - At this threshold, rejecting the most uncertain ~20% of predictions
///     pushes accuracy to near 100%.
const DEFAULT_UNCERTAINTY_THRESHOLD: f32 = 0.25;

/// Class names in output order
const CLASS_NAMES: [&str; N_CLASSES] = ["SNI", "SNII", "TDE", "AGN", "CV"];

/// Feature normalization: mean of [log1p(dt_first), log1p(dt_prev), log_flux, log_flux_err]
/// From feature_stats_day100.npz
const FEAT_MEAN: [f32; 4] = [3.2246506, 0.75406283, 1.8746188, 0.05986891];

/// Feature normalization: std
const FEAT_STD: [f32; 4] = [1.1197281, 0.72683305, 0.4150701, 0.03053664];

pub struct TempoModel {
    session: Session,
}

/// Output from TEMPO model inference
#[derive(Debug, Clone, serde::Serialize)]
pub struct TempoOutput {
    /// Evidence values for each class (5 values, non-negative)
    pub evidence: Vec<f32>,
    /// Class probabilities from Dirichlet mean (evidence + 1 → normalize)
    pub class_probs: Vec<f32>,
    /// Class names in output order
    pub class_names: Vec<String>,
    /// Index of the most probable class
    pub predicted_class: usize,
    /// Name of the most probable class
    pub predicted_label: String,
    /// Uncertainty: K / sum(alpha), lower = more confident
    pub uncertainty: f32,
    /// Whether the prediction passes the confidence threshold.
    /// `true` = classification is trustworthy.
    /// `false` = too uncertain, should be flagged for human review.
    pub passes_threshold: bool,
    /// The uncertainty threshold used for this prediction
    pub uncertainty_threshold: f32,
}

impl TempoModel {
    #[instrument(err)]
    pub fn new(model_path: &str) -> Result<Self, ModelError> {
        Ok(Self {
            session: crate::enrichment::models::load_model(model_path)?,
        })
    }

    /// Build the input tensors from an alert.
    ///
    /// Returns:
    ///   - x: (1, MAX_LEN, 5) normalized event tensor
    ///   - pad_mask: (1, MAX_LEN) boolean mask
    ///   - global_features: (1, 24) physics summary
    #[instrument(skip_all, err)]
    pub fn build_input(
        &self,
        alert: &ZtfAlertForEnrichment,
    ) -> Result<
        (
            Array<f32, Dim<[usize; 3]>>,
            Array<bool, Dim<[usize; 2]>>,
            Array<f32, Dim<[usize; 2]>>,
        ),
        ModelError,
    > {
        let candidate = &alert.candidate.candidate;

        // ---------------------------------------------------------------
        // 1. Collect all valid detections (must have magpsf + sigmapsf)
        //    Each entry: (jd, mag, sigmag, band_idx)
        // ---------------------------------------------------------------
        let mut detections: Vec<(f64, f32, f32, usize)> = Vec::new();

        // Current candidate
        let band_idx = band_to_idx(&alert.candidate.band);
        // Drop i-band (band_idx == 2) per config drop_i_band=true
        if band_idx != 2 {
            detections.push((candidate.jd, candidate.magpsf, candidate.sigmapsf, band_idx));
        }

        // Previous candidates
        for phot in &alert.prv_candidates {
            if let (Some(mag), Some(sig)) = (phot.magpsf, phot.sigmapsf) {
                let idx = band_to_idx(&phot.band);
                if idx != 2 {
                    detections.push((phot.jd, mag as f32, sig as f32, idx));
                }
            }
        }

        // Forced photometry
        for phot in &alert.fp_hists {
            if let (Some(mag), Some(sig)) = (phot.magpsf, phot.sigmapsf) {
                let idx = band_to_idx(&phot.band);
                if idx != 2 {
                    detections.push((phot.jd, mag as f32, sig as f32, idx));
                }
            }
        }

        // ---------------------------------------------------------------
        // 2. Sort by JD ascending, truncate to most recent MAX_LEN
        // ---------------------------------------------------------------
        detections.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        if detections.len() > MAX_LEN {
            let start = detections.len() - MAX_LEN;
            detections.drain(..start);
        }

        let n_obs = detections.len();

        // ---------------------------------------------------------------
        // 3. Build raw features (before normalization)
        //    Needed for global feature computation
        // ---------------------------------------------------------------
        let jd0 = if !detections.is_empty() {
            detections[0].0
        } else {
            0.0
        };

        // raw_features: Vec of (log1p_dt, log1p_dt_prev, logflux, logflux_err, band_idx)
        let mut raw_features: Vec<[f32; 5]> = Vec::with_capacity(n_obs);

        for (i, (jd, mag, sigmag, bidx)) in detections.iter().enumerate() {
            let dt = (*jd - jd0) as f32;
            let dt_prev = if i > 0 {
                (*jd - detections[i - 1].0) as f32
            } else {
                0.0
            };

            let log1p_dt = (1.0 + dt).ln();
            let log1p_dt_prev = (1.0 + dt_prev).ln();
            let logflux = -0.4 * mag;
            let logflux_err = 0.4 * sigmag;

            raw_features.push([log1p_dt, log1p_dt_prev, logflux, logflux_err, *bidx as f32]);
        }

        // ---------------------------------------------------------------
        // 4. Compute 24 global physics features from raw (unnormalized) data
        // ---------------------------------------------------------------
        let global_features = compute_global_features(&raw_features);

        // ---------------------------------------------------------------
        // 5. Build normalized input tensor + pad mask
        // ---------------------------------------------------------------
        let mut x = Array::zeros((1, MAX_LEN, IN_CHANNELS));
        let mut pad_mask = Array::from_elem((1, MAX_LEN), true);

        for (i, feat) in raw_features.iter().enumerate() {
            pad_mask[[0, i]] = false;

            // Normalize continuous features: (val - mean) / std
            for c in 0..4 {
                x[[0, i, c]] = (feat[c] - FEAT_MEAN[c]) / (FEAT_STD[c] + 1e-8);
            }
            // Band index (not normalized, used as embedding index)
            x[[0, i, 4]] = feat[4];
        }

        // ---------------------------------------------------------------
        // 6. Wrap global features as (1, 24) array
        // ---------------------------------------------------------------
        let mut g = Array::zeros((1, N_GLOBAL));
        for (j, val) in global_features.iter().enumerate() {
            g[[0, j]] = *val;
        }

        Ok((x, pad_mask, g))
    }

    /// Run inference: alert → evidence → class probabilities.
    #[instrument(skip_all, err)]
    pub fn predict_alert(&self, alert: &ZtfAlertForEnrichment) -> Result<TempoOutput, ModelError> {
        let (x, pad_mask, global_features) = self.build_input(alert)?;

        let model_inputs = inputs! {
            "x" => TensorRef::from_array_view(&x)?,
            "pad_mask" => TensorRef::from_array_view(&pad_mask)?,
            "global_features" => TensorRef::from_array_view(&global_features)?,
        };

        let outputs = self.session.run(model_inputs)?;

        let evidence: Vec<f32> = match outputs["evidence"].try_extract_tensor::<f32>() {
            Ok((_, e)) => e.to_vec(),
            Err(_) => return Err(ModelError::ModelOutputToVecError),
        };

        // Convert evidence to class probabilities via Dirichlet mean:
        //   alpha = evidence + 1
        //   probs = alpha / sum(alpha)
        let alpha: Vec<f32> = evidence.iter().map(|e| e + 1.0).collect();
        let alpha_sum: f32 = alpha.iter().sum();
        let class_probs: Vec<f32> = alpha.iter().map(|a| a / alpha_sum).collect();

        // Find predicted class
        let predicted_class = class_probs
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| i)
            .unwrap_or(0);

        // Uncertainty: K / S where K = num_classes, S = sum(alpha)
        let uncertainty = N_CLASSES as f32 / alpha_sum;

        Ok(TempoOutput {
            evidence,
            class_probs,
            class_names: CLASS_NAMES.iter().map(|s| s.to_string()).collect(),
            predicted_class,
            predicted_label: CLASS_NAMES[predicted_class].to_string(),
            uncertainty,
            passes_threshold: uncertainty <= DEFAULT_UNCERTAINTY_THRESHOLD,
            uncertainty_threshold: DEFAULT_UNCERTAINTY_THRESHOLD,
        })
    }

    /// Run inference with a custom uncertainty threshold.
    ///
    /// Same as `predict_alert`, but allows overriding the default threshold
    /// (e.g., for stricter filtering on high-value science programs).
    #[instrument(skip_all, err)]
    pub fn predict_alert_with_threshold(
        &self,
        alert: &ZtfAlertForEnrichment,
        threshold: f32,
    ) -> Result<TempoOutput, ModelError> {
        let mut output = self.predict_alert(alert)?;
        output.passes_threshold = output.uncertainty <= threshold;
        output.uncertainty_threshold = threshold;
        Ok(output)
    }
}

/// Convert band enum to index: g=0, r=1, i=2
fn band_to_idx(band: &Band) -> usize {
    match band {
        Band::G => 0,
        Band::R => 1,
        Band::I => 2,
        _ => 1,
    }
}

// ===========================================================================
// Global physics features (24-dim)
//
// Matches `_global_features_from_sequence()` in photometry_edl/data.py
// with feature_set="physics".
//
// Input: raw (unnormalized) token array where each row is:
//   [log1p(dt_first), log1p(dt_prev), log_flux, log_flux_err, band_id]
// ===========================================================================

/// Compute 24 global physics features from the raw token sequence.
fn compute_global_features(raw: &[[f32; 5]]) -> [f32; N_GLOBAL] {
    let mut out = [0.0f32; N_GLOBAL];

    if raw.is_empty() {
        return out;
    }

    let n_obs = raw.len() as f32;

    // Un-log the dt values for physics computations
    // dt_first_days[i] = expm1(log1p_dt) = dt in days
    let dt_first_days: Vec<f32> = raw.iter().map(|r| r[0].exp() - 1.0).collect();
    let dt_prev_days: Vec<f32> = raw.iter().map(|r| r[1].exp() - 1.0).collect();
    let logf: Vec<f32> = raw.iter().map(|r| r[2]).collect();
    let band_id: Vec<usize> = raw.iter().map(|r| r[4] as usize).collect();

    // Duration
    let duration = dt_first_days
        .iter()
        .cloned()
        .fold(0.0f32, f32::max)
        .max(0.0);

    // Per-band counts
    let mut counts = [0.0f32; 3];
    for &b in &band_id {
        if b < 3 {
            counts[b] += 1.0;
        }
    }

    // Amplitude
    let logf_min = logf.iter().cloned().fold(f32::INFINITY, f32::min);
    let logf_max = logf.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
    let amplitude = logf_max - logf_min;

    // Color proxies: mean logflux per band
    let mut band_means = [0.0f32; 3];
    for b in 0..3 {
        let sum: f32 = logf
            .iter()
            .zip(&band_id)
            .filter(|(_, &bi)| bi == b)
            .map(|(f, _)| f)
            .sum();
        band_means[b] = if counts[b] > 0.0 {
            sum / counts[b]
        } else {
            0.0
        };
    }
    let color_gr = band_means[0] - band_means[1];
    let color_ri = band_means[1] - band_means[2];

    // ----- Basic (8) -----
    out[0] = duration;
    out[1] = n_obs;
    out[2] = counts[0]; // count_g
    out[3] = counts[1]; // count_r
    out[4] = counts[2]; // count_i
    out[5] = amplitude;
    out[6] = color_gr;
    out[7] = color_ri;

    // ----- Enhanced (8 more) -----
    // Peak index (argmax of logflux)
    let idx_peak = logf
        .iter()
        .enumerate()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
        .unwrap_or(0);

    let peak_t = dt_first_days[idx_peak];
    let peak_frac_h = peak_t / duration.max(1e-6);
    let peak_flux = logf[idx_peak];

    // Median dt_prev
    let mut dt_prev_sorted = dt_prev_days.clone();
    dt_prev_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let med_dt_prev = if dt_prev_sorted.is_empty() {
        0.0
    } else {
        let mid = dt_prev_sorted.len() / 2;
        if dt_prev_sorted.len() % 2 == 0 && dt_prev_sorted.len() > 1 {
            (dt_prev_sorted[mid - 1] + dt_prev_sorted[mid]) / 2.0
        } else {
            dt_prev_sorted[mid]
        }
    };

    // Std of logflux (unbiased=False, i.e. population std)
    let logf_mean: f32 = logf.iter().sum::<f32>() / n_obs;
    let std_flux = if n_obs > 1.0 {
        (logf.iter().map(|f| (f - logf_mean).powi(2)).sum::<f32>() / n_obs).sqrt()
    } else {
        0.0
    };

    // Percentiles (90th, 10th)
    let mut logf_sorted = logf.clone();
    logf_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p90 = percentile(&logf_sorted, 0.90);
    let p10 = percentile(&logf_sorted, 0.10);

    // Rise and fall slopes
    let rise_slope = safe_slope(&dt_first_days, &logf, 0, idx_peak + 1);
    let fall_slope = safe_slope(&dt_first_days, &logf, idx_peak, raw.len());
    let rise_fall_ratio = rise_slope / fall_slope.abs().max(1e-6);

    out[8] = peak_frac_h;
    out[9] = peak_flux;
    out[10] = med_dt_prev;
    out[11] = std_flux;
    out[12] = p90 - p10;
    out[13] = rise_slope;
    out[14] = fall_slope;
    out[15] = rise_fall_ratio;

    // ----- Physics (8 more) -----
    let n_safe = n_obs.max(1.0);
    out[16] = counts[0] / n_safe; // frac_g
    out[17] = counts[1] / n_safe; // frac_r
    out[18] = counts[2] / n_safe; // frac_i

    // Per-band slopes
    let slope_g = safe_slope_band(&dt_first_days, &logf, &band_id, 0);
    let slope_r = safe_slope_band(&dt_first_days, &logf, &band_id, 1);
    let slope_i = safe_slope_band(&dt_first_days, &logf, &band_id, 2);
    out[19] = slope_g;
    out[20] = slope_r;
    out[21] = slope_i;
    out[22] = slope_g - slope_r; // color_gr_slope
    out[23] = slope_r - slope_i; // color_ri_slope

    out
}

/// Compute the q-th percentile of a sorted slice using linear interpolation.
fn percentile(sorted: &[f32], q: f32) -> f32 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let pos = q * (sorted.len() - 1) as f32;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    let frac = pos - lo as f32;
    sorted[lo] * (1.0 - frac) + sorted[hi.min(sorted.len() - 1)] * frac
}

/// Simple linear regression slope over a contiguous range [start, end).
/// Returns 0.0 if fewer than 2 points or near-zero variance.
fn safe_slope(x: &[f32], y: &[f32], start: usize, end: usize) -> f32 {
    let n = end.saturating_sub(start);
    if n < 2 {
        return 0.0;
    }
    let xm: f32 = x[start..end].iter().sum::<f32>() / n as f32;
    let ym: f32 = y[start..end].iter().sum::<f32>() / n as f32;
    let mut sxx = 0.0f32;
    let mut sxy = 0.0f32;
    for i in start..end {
        let dx = x[i] - xm;
        sxx += dx * dx;
        sxy += dx * (y[i] - ym);
    }
    if sxx.abs() <= 1e-12 {
        return 0.0;
    }
    sxy / sxx
}

/// Linear regression slope for a specific band only.
fn safe_slope_band(x: &[f32], y: &[f32], bands: &[usize], target_band: usize) -> f32 {
    let xf: Vec<f32> = x
        .iter()
        .zip(bands)
        .filter(|(_, &b)| b == target_band)
        .map(|(v, _)| *v)
        .collect();
    let yf: Vec<f32> = y
        .iter()
        .zip(bands)
        .filter(|(_, &b)| b == target_band)
        .map(|(v, _)| *v)
        .collect();
    if xf.len() < 2 {
        return 0.0;
    }
    let n = xf.len() as f32;
    let xm: f32 = xf.iter().sum::<f32>() / n;
    let ym: f32 = yf.iter().sum::<f32>() / n;
    let mut sxx = 0.0f32;
    let mut sxy = 0.0f32;
    for i in 0..xf.len() {
        let dx = xf[i] - xm;
        sxx += dx * dx;
        sxy += dx * (yf[i] - ym);
    }
    if sxx.abs() <= 1e-12 {
        return 0.0;
    }
    sxy / sxx
}
