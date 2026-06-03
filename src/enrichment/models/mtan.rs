//! mTAN (multi-Time Attention Network) encoder model for BOOM deployment.
//!
//! Loads `mtan_embed.onnx` and produces 2-dimensional latent embeddings
//! from ZTF alert photometry light curves (g, r bands only).
//!
//! The mTAN encoder processes irregularly-sampled time series using
//! learned time embeddings and multi-head attention, outputting
//! (qz0_mean, qz0_logvar) at each query time point.
//!
//! For the vector database, we use the mean of qz0_mean across
//! valid query times to produce a single 2D embedding per source.
//!
//! ONNX inputs:
//!   - x:           (B, T, 4)   [observed_g, observed_r, mask_g, mask_r]
//!   - time_steps:  (B, T)      observation timestamps (normalized to [0,1])
//!   - query_times: (B, Q)      query time grid (normalized to [0,1])
//!
//! ONNX output:
//!   - output:      (B, Q, 4)   [qz0_mean(2), qz0_logvar(2)] per query time

use crate::enrichment::models::{load_model, load_model_on_device, ModelError};
use crate::enrichment::ztf::ZtfAlertForEnrichment;
use crate::utils::lightcurves::Band;
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;

/// mTAN model constants matching the Python training configuration.
pub const MTAN_DIM: usize = 2;
pub const MTAN_LATENT_DIM: usize = 2;

/// Maximum observation sequence length
const MAX_SEQ_LEN: usize = 200;
/// Number of query time points
const MAX_QUERY_LEN: usize = 50;
/// Minimum number of g+r observations required for mTAN inference
const MIN_OBS: usize = 3;
/// Merge tolerance in days (~1 minute) for nearby observations
const MERGE_TOL_DAYS: f64 = 1.0 / (24.0 * 60.0);

pub struct MtanModel {
    model: Session,
}

impl MtanModel {
    /// Load mTAN ONNX model on CPU.
    #[instrument(err)]
    pub fn new(path: &str) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model(path)?,
        })
    }

    /// Load mTAN ONNX model on a specific CUDA device.
    pub fn new_on_device(path: &str, device_id: i32) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model_on_device(path, Some(device_id))?,
        })
    }

    /// Run the mTAN encoder to produce raw query-level outputs.
    ///
    /// # Arguments
    /// * `x` - Observation tensor of shape (B, T, 4).
    ///         Channels: [observed_g, observed_r, mask_g, mask_r]
    /// * `time_steps` - Observation timestamps of shape (B, T), normalized to [0,1].
    /// * `query_times` - Query time grid of shape (B, Q), normalized to [0,1].
    ///
    /// # Returns
    /// Vec<f32> of length B * Q * 4, containing [qz0_mean, qz0_logvar] at each query time.
    #[instrument(skip_all, err)]
    pub fn embed_raw(
        &mut self,
        x: &Array<f32, Dim<[usize; 3]>>,
        time_steps: &Array<f32, Dim<[usize; 2]>>,
        query_times: &Array<f32, Dim<[usize; 2]>>,
    ) -> Result<Vec<f32>, ModelError> {
        let model_inputs = inputs! {
            "x" => TensorRef::from_array_view(x)?,
            "time_steps" => TensorRef::from_array_view(time_steps)?,
            "query_times" => TensorRef::from_array_view(query_times)?,
        };

        let outputs = self.model.run(model_inputs)?;

        match outputs["output"].try_extract_tensor::<f32>() {
            Ok((_, raw)) => Ok(raw.to_vec()),
            Err(_) => Err(ModelError::ModelOutputToVecError),
        }
    }

    /// Prepare mTAN input tensors from a single alert's photometry.
    ///
    /// Extracts g-band and r-band photometry, merges observations at similar
    /// times, normalizes magnitudes and timestamps, and builds the input tensors.
    ///
    /// Returns `Err` if the alert has fewer than 3 g+r observations.
    pub fn prepare_features(
        alert: &ZtfAlertForEnrichment,
    ) -> Result<
        (
            Array<f32, Dim<[usize; 3]>>, // x: (1, MAX_SEQ_LEN, 4)
            Array<f32, Dim<[usize; 2]>>, // time_steps: (1, MAX_SEQ_LEN)
            Array<f32, Dim<[usize; 2]>>, // query_times: (1, MAX_QUERY_LEN)
        ),
        ModelError,
    > {
        let current_jd = alert.candidate.candidate.jd;

        // Step 1: Collect g and r band photometry with valid magnitudes
        struct PhotoPoint {
            jd: f64,
            mag: f64,
            band_idx: usize, // 0 = g, 1 = r
        }

        let mut points: Vec<PhotoPoint> = Vec::new();

        for p in alert.prv_candidates.iter().chain(alert.fp_hists.iter()) {
            if p.jd > current_jd {
                continue;
            }
            if let Some(mag) = p.magpsf {
                let band_idx = match p.band {
                    Band::G => 0,
                    Band::R => 1,
                    _ => continue, // mTAN only uses g and r
                };
                points.push(PhotoPoint {
                    jd: p.jd,
                    mag,
                    band_idx,
                });
            }
        }

        if points.len() < MIN_OBS {
            return Err(ModelError::MissingFeature(
                "mTAN requires at least 3 g+r observations",
            ));
        }

        // Step 2: Sort by JD
        points.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap_or(std::cmp::Ordering::Equal));

        // Step 3: Merge observations at similar times (within ~1 minute)
        // Each merged time step has [mag_g, mag_r, mask_g, mask_r]
        struct MergedPoint {
            jd: f64,
            mag: [f32; 2],  // [g, r]
            mask: [f32; 2], // [g, r]
        }

        let mut merged: Vec<MergedPoint> = Vec::new();
        let mut i = 0;
        while i < points.len() {
            let t = points[i].jd;
            let mut mp = MergedPoint {
                jd: t,
                mag: [0.0, 0.0],
                mask: [0.0, 0.0],
            };

            let mut j = i;
            while j < points.len() && (points[j].jd - t).abs() <= MERGE_TOL_DAYS {
                let band = points[j].band_idx;
                mp.mag[band] = points[j].mag as f32;
                mp.mask[band] = 1.0;
                j += 1;
            }

            merged.push(mp);
            i = j;
        }

        if merged.len() < MIN_OBS {
            return Err(ModelError::MissingFeature(
                "mTAN requires at least 3 merged time steps",
            ));
        }

        // Step 4: Truncate to MAX_SEQ_LEN
        if merged.len() > MAX_SEQ_LEN {
            merged.truncate(MAX_SEQ_LEN);
        }

        let n_steps = merged.len();

        // Step 5: Normalize magnitudes — (mag - min_mag) / 2.5
        // Find min magnitude across all observed values
        let mut min_mag = f32::INFINITY;
        for mp in &merged {
            for band in 0..2 {
                if mp.mask[band] > 0.0 && mp.mag[band] < min_mag {
                    min_mag = mp.mag[band];
                }
            }
        }

        // Apply normalization, zero out unobserved positions
        for mp in merged.iter_mut() {
            for band in 0..2 {
                if mp.mask[band] > 0.0 {
                    mp.mag[band] = (mp.mag[band] - min_mag) / 2.5;
                } else {
                    mp.mag[band] = 0.0;
                }
            }
        }

        // Step 6: Convert JD to hours from first detection, then normalize to [0, 1]
        let first_jd = merged[0].jd;
        let mut times_hours: Vec<f32> = merged
            .iter()
            .map(|mp| ((mp.jd - first_jd) * 24.0) as f32)
            .collect();

        let max_time = times_hours
            .iter()
            .copied()
            .fold(0.0f32, f32::max);

        if max_time > 0.0 {
            for t in times_hours.iter_mut() {
                *t /= max_time;
            }
        }

        // Step 7: Build output tensors
        // x: (1, MAX_SEQ_LEN, 4) — [mag_g, mag_r, mask_g, mask_r]
        let mut x = Array::<f32, _>::zeros((1, MAX_SEQ_LEN, 4));
        let mut time_steps = Array::<f32, _>::zeros((1, MAX_SEQ_LEN));

        for (i, mp) in merged.iter().enumerate() {
            x[[0, i, 0]] = mp.mag[0];
            x[[0, i, 1]] = mp.mag[1];
            x[[0, i, 2]] = mp.mask[0];
            x[[0, i, 3]] = mp.mask[1];
            time_steps[[0, i]] = times_hours[i];
        }
        // Remaining positions stay zero-padded

        // query_times: (1, MAX_QUERY_LEN) — uniform grid in [0, 1]
        let mut query_times = Array::<f32, _>::zeros((1, MAX_QUERY_LEN));
        for i in 0..MAX_QUERY_LEN {
            query_times[[0, i]] = i as f32 / (MAX_QUERY_LEN - 1) as f32;
        }

        Ok((x, time_steps, query_times))
    }

    /// Mean-pool qz0_mean from raw mTAN output to produce a 2D embedding.
    ///
    /// The raw output is (B, Q, 4) where channels are [qz0_mean_0, qz0_mean_1, qz0_logvar_0, qz0_logvar_1].
    /// We average qz0_mean (first 2 channels) across all Q query times.
    pub fn pool_embedding(raw_output: &[f32], n_query: usize) -> Vec<f32> {
        let mut sum = [0.0f32; 2];
        for qi in 0..n_query {
            sum[0] += raw_output[qi * 4];
            sum[1] += raw_output[qi * 4 + 1];
        }
        vec![sum[0] / n_query as f32, sum[1] / n_query as f32]
    }
}
