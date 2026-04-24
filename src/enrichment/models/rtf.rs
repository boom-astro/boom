/// RTF (Real-Time Filter) Autoencoder model for BOOM enrichment.
///
/// Takes a ZTF alert's full photometry history and candidate metadata and produces:
///   - A 128-dimensional embedding vector (for downstream anomaly detection)
///   - A scalar reconstruction error (anomaly score)
///
/// The model was trained in PyTorch and exported to ONNX via `export_onnx.py`.
/// This loader replicates the exact preprocessing from `preprocess_alerts.py`
/// and `dataset.py` in the RTF repository.
///
/// Input tensor format: (1, MAX_LEN, 37) where each timestep has:
///   [0:4]  = log1p(dt), log1p(dt_prev), logflux, logflux_err
///   [4:7]  = one-hot band (g, r, i)
///   [7:37] = 30 alert metadata fields (ALERT_META_KEYS)
use crate::enrichment::{
    ZtfAlertForEnrichment,
    models::{ModelError, load_model},
};
use crate::utils::lightcurves::Band;
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;

/// Maximum sequence length the ONNX model accepts (must match export_onnx.py)
const MAX_LEN: usize = 257;

/// Number of input channels per timestep
const IN_CHANNELS: usize = 37;

/// Number of base photometry features (dt, dt_prev, logflux, logflux_err)
const N_BASE: usize = 4;

/// Number of band one-hot features (g, r, i)
const N_BAND: usize = 3;

/// Number of alert metadata features.
/// Matches ALERT_META_KEYS in dataset.py.
const N_META: usize = 30;

pub struct RtfModel {
    embed_model: Session,
    recon_model: Session,
}

/// Output from RTF model inference
#[derive(Debug, Clone, serde::Serialize)]
pub struct RtfOutput {
    /// 128-dimensional embedding vector
    pub embedding: Vec<f32>,
    /// Scalar reconstruction error (higher = more anomalous)
    pub recon_error: f32,
}

impl RtfModel {
    #[instrument(err)]
    pub fn new(embed_path: &str, recon_path: &str) -> Result<Self, ModelError> {
        Ok(Self {
            embed_model: load_model(embed_path)?,
            recon_model: load_model(recon_path)?,
        })
    }

    /// Build the (1, MAX_LEN, 37) input tensor and (1, MAX_LEN) pad mask from an alert.
    ///
    /// Replicates the exact preprocessing from `preprocess_alerts.py`:
    ///   1. Collect all detections from prv_candidates + fp_hists + current candidate
    ///   2. Sort by JD, take the most recent MAX_LEN
    ///   3. Compute dt = jd - jd[0], dt_prev = jd[i] - jd[i-1]
    ///   4. Compute logflux = -0.4 * magpsf, logflux_err = 0.4 * sigmapsf
    ///   5. One-hot encode band (g=0, r=1, i=2)
    ///   6. Broadcast candidate metadata across all timesteps
    ///   7. Pad to MAX_LEN, build pad_mask (true = padding)
    #[instrument(skip_all, err)]
    pub fn build_input(
        &self,
        alert: &ZtfAlertForEnrichment,
    ) -> Result<(Array<f32, Dim<[usize; 3]>>, Array<bool, Dim<[usize; 2]>>), ModelError> {
        let candidate = &alert.candidate.candidate;

        // Collect all valid detections (must have magpsf + sigmapsf)
        let mut detections: Vec<(f64, f32, f32, usize)> = Vec::new(); // (jd, mag, sigmag, band_idx)

        // Current candidate
        let band_idx = band_to_idx(&alert.candidate.band);
        detections.push((candidate.jd, candidate.magpsf, candidate.sigmapsf, band_idx));

        // Previous candidates
        for phot in &alert.prv_candidates {
            if let (Some(mag), Some(sig)) = (phot.magpsf, phot.sigmapsf) {
                let mag = mag as f32;
                let sig = sig as f32;
                let idx = band_to_idx(&phot.band);
                detections.push((phot.jd, mag, sig, idx));
            }
        }

        // Forced photometry
        for phot in &alert.fp_hists {
            if let (Some(mag), Some(sig)) = (phot.magpsf, phot.sigmapsf) {
                let mag = mag as f32;
                let sig = sig as f32;
                let idx = band_to_idx(&phot.band);
                detections.push((phot.jd, mag, sig, idx));
            }
        }

        // Sort by JD ascending
        detections.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        // Truncate to most recent MAX_LEN detections
        if detections.len() > MAX_LEN {
            let start = detections.len() - MAX_LEN;
            detections.drain(..start);
        }

        // Build the tensor
        let mut x = Array::zeros((1, MAX_LEN, IN_CHANNELS));
        let mut pad_mask = Array::from_elem((1, MAX_LEN), true);

        let jd0 = if !detections.is_empty() {
            detections[0].0
        } else {
            0.0
        };

        // Extract the 30 metadata values from the candidate (broadcast to all timesteps)
        let meta = extract_candidate_metadata(candidate);

        for (i, (jd, mag, sigmag, bidx)) in detections.iter().enumerate() {
            pad_mask[[0, i]] = false;

            // dt = time since first detection
            let dt = (*jd - jd0) as f32;
            // dt_prev = time since previous detection
            let dt_prev = if i > 0 {
                (*jd - detections[i - 1].0) as f32
            } else {
                0.0
            };

            // log1p(dt) and log1p(dt_prev) as in Python preprocessing
            x[[0, i, 0]] = (1.0 + dt).ln();
            x[[0, i, 1]] = (1.0 + dt_prev).ln();

            // logflux = -0.4 * magpsf (log10 flux in ZP=23.9 system)
            x[[0, i, 2]] = -0.4 * mag;
            // logflux_err = 0.4 * sigmapsf
            x[[0, i, 3]] = 0.4 * sigmag;

            // One-hot band encoding
            x[[0, i, N_BASE + bidx]] = 1.0;

            // Metadata (same for all timesteps)
            for (j, val) in meta.iter().enumerate() {
                x[[0, i, N_BASE + N_BAND + j]] = *val;
            }
        }

        Ok((x, pad_mask))
    }

    /// Run the embedding model: x, pad_mask → 128-dim embedding.
    ///
    /// Note: The ONNX graph only accepts (x, pad_mask). The images input was
    /// folded away during ONNX tracing (image features are baked into the
    /// transformer's CLS token during training but not needed at inference
    /// in the current exported graph).
    #[instrument(skip_all, err)]
    pub fn predict_embed(
        &self,
        x: &Array<f32, Dim<[usize; 3]>>,
        pad_mask: &Array<bool, Dim<[usize; 2]>>,
    ) -> Result<Vec<f32>, ModelError> {
        let model_inputs = inputs! {
            "x" => TensorRef::from_array_view(x)?,
            "pad_mask" => TensorRef::from_array_view(pad_mask)?,
        };

        let outputs = self.embed_model.run(model_inputs)?;

        match outputs["output"].try_extract_tensor::<f32>() {
            Ok((_, emb)) => Ok(emb.to_vec()),
            Err(_) => Err(ModelError::ModelOutputToVecError),
        }
    }

    /// Run the reconstruction model: x, pad_mask → scalar error.
    #[instrument(skip_all, err)]
    pub fn predict_recon(
        &self,
        x: &Array<f32, Dim<[usize; 3]>>,
        pad_mask: &Array<bool, Dim<[usize; 2]>>,
    ) -> Result<f32, ModelError> {
        let model_inputs = inputs! {
            "x" => TensorRef::from_array_view(x)?,
            "pad_mask" => TensorRef::from_array_view(pad_mask)?,
        };

        let outputs = self.recon_model.run(model_inputs)?;

        match outputs["output"].try_extract_tensor::<f32>() {
            Ok((_, err)) => {
                if err.is_empty() {
                    Err(ModelError::ModelOutputToVecError)
                } else {
                    Ok(err[0])
                }
            }
            Err(_) => Err(ModelError::ModelOutputToVecError),
        }
    }

    /// Full inference: build input tensor from alert, run both models.
    #[instrument(skip_all, err)]
    pub fn predict_alert(&self, alert: &ZtfAlertForEnrichment) -> Result<RtfOutput, ModelError> {
        let (x, pad_mask) = self.build_input(alert)?;

        let embedding = self.predict_embed(&x, &pad_mask)?;
        let recon_error = self.predict_recon(&x, &pad_mask)?;

        Ok(RtfOutput {
            embedding,
            recon_error,
        })
    }
}

/// Convert band enum to index: g=0, r=1, i=2
fn band_to_idx(band: &Band) -> usize {
    match band {
        Band::G => 0,
        Band::R => 1,
        Band::I => 2,
        // For non-ZTF bands, default to r-band
        _ => 1,
    }
}

/// Extract the 30 metadata values from the ZTF candidate.
///
/// Order MUST match ALERT_META_KEYS in dataset.py exactly:
///   sgscore1, sgscore2, distpsnr1, distpsnr2, nmtchps, sharpnr, scorr,
///   diffmaglim, sky, ndethist, ncovhist, sigmapsf, chinr, classtar, rb,
///   chipsf, distnr, magnr, fwhm, srmag1, sgmag1, simag1, szmag1,
///   srmag2, sgmag2, simag2, szmag2, clrcoeff, clrcounc, zpclrcov
fn extract_candidate_metadata(candidate: &crate::alert::ztf::Candidate) -> [f32; N_META] {
    [
        candidate.sgscore1.unwrap_or(0.0),
        candidate.sgscore2.unwrap_or(0.0),
        candidate.distpsnr1.unwrap_or(0.0),
        candidate.distpsnr2.unwrap_or(0.0),
        candidate.nmtchps as f32,
        candidate.sharpnr.unwrap_or(0.0),
        candidate.scorr.unwrap_or(0.0) as f32,
        candidate.diffmaglim.unwrap_or(0.0),
        candidate.sky.unwrap_or(0.0),
        candidate.ndethist as f32,
        candidate.ncovhist as f32,
        candidate.sigmapsf,
        candidate.chinr.unwrap_or(0.0),
        candidate.classtar.unwrap_or(0.0),
        candidate.rb.unwrap_or(0.0),
        candidate.chipsf.unwrap_or(0.0),
        candidate.distnr.unwrap_or(0.0),
        candidate.magnr.unwrap_or(0.0),
        candidate.fwhm.unwrap_or(0.0),
        candidate.srmag1.unwrap_or(0.0),
        candidate.sgmag1.unwrap_or(0.0),
        candidate.simag1.unwrap_or(0.0),
        candidate.szmag1.unwrap_or(0.0),
        candidate.srmag2.unwrap_or(0.0),
        candidate.sgmag2.unwrap_or(0.0),
        candidate.simag2.unwrap_or(0.0),
        candidate.szmag2.unwrap_or(0.0),
        candidate.clrcoeff.unwrap_or(0.0),
        candidate.clrcounc.unwrap_or(0.0),
        0.0, // zpclrcov: not present in Candidate struct, default to 0.0
    ]
}
