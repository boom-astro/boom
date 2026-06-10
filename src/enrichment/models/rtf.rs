//! RTF (Radio Transient Finder) encoder model for BOOM deployment.
//!
//! Loads `rtf_embed.onnx` and produces 128-dimensional latent embeddings
//! from ZTF alert photometry sequences + cutout images.
//!
//! ONNX inputs:
//!   - x:        (B, 257, 37)  padded photometry tensor
//!   - pad_mask: (B, 257)      bool padding mask (true = padded)
//!   - images:   (B, 3, 63, 63) science/template/difference cutout stamps
//!
//! ONNX output:
//!   - output:   (B, 128)      latent embedding

use crate::utils::cutouts::AlertCutout;
use crate::enrichment::models::{load_model, load_model_on_device, ModelError};
use crate::enrichment::ztf::ZtfAlertForEnrichment;
use crate::utils::fits::prepare_triplet;
use crate::utils::lightcurves::Band;
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;

/// RTF model constants matching the Python export configuration.
pub const RTF_MAX_LEN: usize = 257;
pub const RTF_IN_CHANNELS: usize = 37;
pub const RTF_LATENT_DIM: usize = 128;

/// Number of continuous base channels: log1p(dt), log1p(dt_prev), logflux, logflux_err
const N_CONT_BASE: usize = 4;
/// Number of one-hot band channels: g, r, i
const N_BAND: usize = 3;
/// Number of alert metadata channels
const N_META: usize = 30;
/// Cutout stamp size
const STAMP_SIZE: usize = 63;

pub struct RtfModel {
    model: Session,
}

impl RtfModel {
    /// Load RTF ONNX model on CPU.
    #[instrument(err)]
    pub fn new(path: &str) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model(path)?,
        })
    }

    /// Load RTF ONNX model on a specific CUDA device.
    pub fn new_on_device(path: &str, device_id: i32) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model_on_device(path, Some(device_id))?,
        })
    }

    /// Run the RTF encoder to produce 128D embeddings.
    ///
    /// # Arguments
    /// * `x` - Padded photometry tensor of shape (B, 257, 37).
    ///         Channels: [log1p(dt), log1p(dt_prev), logflux, logflux_err,
    ///                    band_g, band_r, band_i, + 30 metadata channels]
    /// * `pad_mask` - Boolean padding mask of shape (B, 257).
    ///               `true` = padded position, `false` = valid observation.
    /// * `images` - Cutout stamp tensor of shape (B, 3, 63, 63).
    ///             Channels: [science, template, difference].
    ///
    /// # Returns
    /// Vec<f32> of length B * 128, containing the flattened embeddings.
    #[instrument(skip_all, err)]
    pub fn embed(
        &mut self,
        x: &Array<f32, Dim<[usize; 3]>>,
        pad_mask: &Array<bool, Dim<[usize; 2]>>,
        images: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError> {
        let model_inputs = inputs! {
            "x" => TensorRef::from_array_view(x)?,
            "pad_mask" => TensorRef::from_array_view(pad_mask)?,
            "images" => TensorRef::from_array_view(images)?,
        };

        let outputs = self.model.run(model_inputs)?;

        match outputs["output"].try_extract_tensor::<f32>() {
            Ok((_, embeddings)) => Ok(embeddings.to_vec()),
            Err(_) => Err(ModelError::ModelOutputToVecError),
        }
    }

    /// Prepare RTF input tensors from a single alert and its cutout.
    ///
    /// Builds the (1, 257, 37) photometry tensor, (1, 257) padding mask,
    /// and (1, 3, 63, 63) cutout image tensor from the raw alert data.
    ///
    /// The 37 channels per observation are:
    ///   [0]  log1p(dt)       — log of days since first observation
    ///   [1]  log1p(dt_prev)  — log of days since previous observation
    ///   [2]  logflux         — -0.4 * magpsf
    ///   [3]  logflux_err     — 0.4 * sigmapsf
    ///   [4]  band_g          — 1.0 if g-band, else 0.0
    ///   [5]  band_r          — 1.0 if r-band, else 0.0
    ///   [6]  band_i          — 1.0 if i-band, else 0.0
    ///   [7..37] 30 alert metadata fields (broadcast from current candidate)
    pub fn prepare_features(
        alert: &ZtfAlertForEnrichment,
        cutout: &AlertCutout,
    ) -> Result<
        (
            Array<f32, Dim<[usize; 3]>>,  // x: (1, 257, 37)
            Array<bool, Dim<[usize; 2]>>, // pad_mask: (1, 257)
            Array<f32, Dim<[usize; 4]>>,  // images: (1, 3, 63, 63)
        ),
        ModelError,
    > {
        let candidate = &alert.candidate.candidate;
        let current_jd = candidate.jd;

        // Collect valid photometry points (must have magpsf and sigmapsf)
        // from prv_candidates and fp_hists, filtered to jd <= current jd
        struct PhotoPoint {
            jd: f64,
            magpsf: f64,
            sigmapsf: f64,
            band: Band,
        }

        let mut points: Vec<PhotoPoint> = Vec::new();

        for p in alert.prv_candidates.iter().chain(alert.fp_hists.iter()) {
            if p.jd > current_jd {
                continue;
            }
            if let (Some(mag), Some(sig)) = (p.magpsf, p.sigmapsf) {
                points.push(PhotoPoint {
                    jd: p.jd,
                    magpsf: mag,
                    sigmapsf: sig,
                    band: p.band.clone(),
                });
            }
        }

        // Sort by JD ascending
        points.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap_or(std::cmp::Ordering::Equal));

        // Truncate to max 257 observations
        if points.len() > RTF_MAX_LEN {
            points.truncate(RTF_MAX_LEN);
        }

        let n_obs = points.len();

        // Build the 30-element metadata vector from the current candidate.
        // These values are broadcast across all time steps (same candidate metadata
        // for every observation), matching the training pipeline behavior.
        let meta = Self::extract_metadata(candidate);

        // Initialize output tensors
        let mut x = Array::<f32, _>::zeros((1, RTF_MAX_LEN, RTF_IN_CHANNELS));
        let mut pad_mask = Array::<bool, _>::from_elem((1, RTF_MAX_LEN), true);

        let first_jd = if n_obs > 0 { points[0].jd } else { 0.0 };

        for (i, point) in points.iter().enumerate() {
            // Mark as valid (not padded)
            pad_mask[[0, i]] = false;

            // Time features
            let dt = (point.jd - first_jd) as f32;
            let dt_prev = if i > 0 {
                (point.jd - points[i - 1].jd) as f32
            } else {
                0.0f32
            };

            x[[0, i, 0]] = (1.0 + dt).ln();
            x[[0, i, 1]] = (1.0 + dt_prev).ln();

            // Photometry features
            x[[0, i, 2]] = -0.4 * point.magpsf as f32;
            x[[0, i, 3]] = 0.4 * point.sigmapsf as f32;

            // One-hot band encoding
            match point.band {
                Band::G => x[[0, i, N_CONT_BASE]] = 1.0,
                Band::R => x[[0, i, N_CONT_BASE + 1]] = 1.0,
                Band::I => x[[0, i, N_CONT_BASE + 2]] = 1.0,
                _ => {} // ZTF only has g, r, i
            }

            // Metadata (same for all time steps)
            for (j, &val) in meta.iter().enumerate() {
                x[[0, i, N_CONT_BASE + N_BAND + j]] = val;
            }
        }

        // Prepare cutout image in CHW format (1, 3, 63, 63)
        let (science, template, difference) = prepare_triplet(cutout)?;
        let mut images = Array::<f32, _>::zeros((1, 3, STAMP_SIZE, STAMP_SIZE));

        // Each cutout is a flattened Vec<f32> of size 63*63 in row-major order.
        // We need to place them as separate channels in CHW format.
        for (ch, cutout_flat) in [&science, &template, &difference].iter().enumerate() {
            for row in 0..STAMP_SIZE {
                for col in 0..STAMP_SIZE {
                    images[[0, ch, row, col]] = cutout_flat[row * STAMP_SIZE + col];
                }
            }
        }

        Ok((x, pad_mask, images))
    }

    /// Extract the 30 metadata values from a ZTF candidate in the exact order
    /// matching ALERT_META_KEYS from the RTF Python training pipeline.
    fn extract_metadata(candidate: &crate::alert::Candidate) -> [f32; N_META] {
        let opt_f32 = |v: Option<f32>| v.unwrap_or(0.0);
        let opt_f64_as_f32 = |v: Option<f64>| v.unwrap_or(0.0) as f32;

        [
            opt_f32(candidate.sgscore1),     // 0: sgscore1
            opt_f32(candidate.sgscore2),     // 1: sgscore2
            opt_f32(candidate.distpsnr1),    // 2: distpsnr1
            opt_f32(candidate.distpsnr2),    // 3: distpsnr2
            candidate.nmtchps as f32,        // 4: nmtchps
            opt_f32(candidate.sharpnr),      // 5: sharpnr
            opt_f64_as_f32(candidate.scorr), // 6: scorr
            opt_f32(candidate.diffmaglim),   // 7: diffmaglim
            opt_f32(candidate.sky),          // 8: sky
            candidate.ndethist as f32,       // 9: ndethist
            candidate.ncovhist as f32,       // 10: ncovhist
            candidate.sigmapsf,              // 11: sigmapsf
            opt_f32(candidate.chinr),        // 12: chinr
            opt_f32(candidate.classtar),     // 13: classtar
            opt_f32(candidate.rb),           // 14: rb
            opt_f32(candidate.chipsf),       // 15: chipsf
            opt_f32(candidate.distnr),       // 16: distnr
            opt_f32(candidate.magnr),        // 17: magnr
            opt_f32(candidate.fwhm),         // 18: fwhm
            opt_f32(candidate.srmag1),       // 19: srmag1
            opt_f32(candidate.sgmag1),       // 20: sgmag1
            opt_f32(candidate.simag1),       // 21: simag1
            opt_f32(candidate.szmag1),       // 22: szmag1
            opt_f32(candidate.srmag2),       // 23: srmag2
            opt_f32(candidate.sgmag2),       // 24: sgmag2
            opt_f32(candidate.simag2),       // 25: simag2
            opt_f32(candidate.szmag2),       // 26: szmag2
            opt_f32(candidate.clrcoeff),     // 27: clrcoeff
            opt_f32(candidate.clrcounc),     // 28: clrcounc
            0.0,                             // 29: zpclrcov (not in Candidate struct)
        ]
    }
}
