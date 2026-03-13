mod acai;
mod base;
mod btsbot;

pub use acai::AcaiModel;
pub use base::{load_model, load_model_on_device, Model, ModelError};
pub use btsbot::BtsBotModel;

use std::sync::{Arc, Mutex};
use tracing::info;

/// ONNX models shared across all enrichment worker threads via `Arc`.
///
/// Each model is wrapped in a `Mutex` because `Session::run` requires `&mut self`.
/// Concurrent workers will serialize on the mutex, but model weights are loaded
/// only once in memory (and on GPU VRAM if using CUDA).
pub struct SharedModels {
    pub acai_h: Mutex<AcaiModel>,
    pub acai_n: Mutex<AcaiModel>,
    pub acai_v: Mutex<AcaiModel>,
    pub acai_o: Mutex<AcaiModel>,
    pub acai_b: Mutex<AcaiModel>,
    pub btsbot: Mutex<BtsBotModel>,
}

impl std::fmt::Debug for SharedModels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedModels").finish_non_exhaustive()
    }
}

impl SharedModels {
    /// Load all ONNX models, optionally on a specific CUDA device.
    /// Returns an `Arc` for sharing across threads.
    pub fn load(device_id: Option<i32>) -> Result<Arc<Self>, ModelError> {
        info!(?device_id, "loading shared ONNX models");
        let models = match device_id {
            Some(id) => Self {
                acai_h: Mutex::new(AcaiModel::new_on_device(
                    "data/models/acai_h.d1_dnn_20201130.onnx",
                    id,
                )?),
                acai_n: Mutex::new(AcaiModel::new_on_device(
                    "data/models/acai_n.d1_dnn_20201130.onnx",
                    id,
                )?),
                acai_v: Mutex::new(AcaiModel::new_on_device(
                    "data/models/acai_v.d1_dnn_20201130.onnx",
                    id,
                )?),
                acai_o: Mutex::new(AcaiModel::new_on_device(
                    "data/models/acai_o.d1_dnn_20201130.onnx",
                    id,
                )?),
                acai_b: Mutex::new(AcaiModel::new_on_device(
                    "data/models/acai_b.d1_dnn_20201130.onnx",
                    id,
                )?),
                btsbot: Mutex::new(BtsBotModel::new_on_device(
                    "data/models/btsbot-v1.0.1.onnx",
                    id,
                )?),
            },
            None => Self {
                acai_h: Mutex::new(AcaiModel::new("data/models/acai_h.d1_dnn_20201130.onnx")?),
                acai_n: Mutex::new(AcaiModel::new("data/models/acai_n.d1_dnn_20201130.onnx")?),
                acai_v: Mutex::new(AcaiModel::new("data/models/acai_v.d1_dnn_20201130.onnx")?),
                acai_o: Mutex::new(AcaiModel::new("data/models/acai_o.d1_dnn_20201130.onnx")?),
                acai_b: Mutex::new(AcaiModel::new("data/models/acai_b.d1_dnn_20201130.onnx")?),
                btsbot: Mutex::new(BtsBotModel::new("data/models/btsbot-v1.0.1.onnx")?),
            },
        };
        info!("all ONNX models loaded successfully");
        Ok(Arc::new(models))
    }
}
