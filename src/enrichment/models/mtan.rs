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
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;

/// mTAN model constants matching the Python training configuration.
pub const MTAN_DIM: usize = 2;
pub const MTAN_LATENT_DIM: usize = 2;

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
}
