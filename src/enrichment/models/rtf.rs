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

use crate::enrichment::models::{load_model, load_model_on_device, ModelError};
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;

/// RTF model constants matching the Python export configuration.
pub const RTF_MAX_LEN: usize = 257;
pub const RTF_IN_CHANNELS: usize = 37;
pub const RTF_LATENT_DIM: usize = 128;

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
}
