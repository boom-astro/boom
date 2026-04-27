mod acai;
mod base;
mod btsbot;

pub use acai::AcaiModel;
pub use base::{load_model, load_model_on_device, Model, ModelError};
pub use btsbot::BtsBotModel;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::info;

const SESSIONS_PER_DEVICE: usize = 1;

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

/// Pool of model sets across multiple GPU devices (or a single CPU set).
///
/// Each device gets its own complete set of ONNX models. Workers are assigned
/// a model set via round-robin so that mutex contention is spread across devices.
/// With N devices and M workers, each device serves at most ceil(M/N) workers.
pub struct SharedModelPool {
    model_sets: Vec<Arc<SharedModels>>,
    next: AtomicUsize,
}

impl std::fmt::Debug for SharedModelPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedModelPool")
            .field("n_devices", &self.model_sets.len())
            .finish()
    }
}

impl SharedModelPool {
    /// Load models on all specified CUDA devices (one full model set per device).
    /// If `device_ids` is empty, loads a single CPU model set.
    pub fn load(device_ids: &[i32]) -> Result<Arc<Self>, ModelError> {
        let model_sets = if device_ids.is_empty() {
            info!("loading ONNX models on CPU");
            vec![SharedModels::load(None)?]
        } else {
            let mut sets = Vec::with_capacity(device_ids.len() * SESSIONS_PER_DEVICE);
            for &id in device_ids {
                for session_idx in 0..SESSIONS_PER_DEVICE {
                    info!(
                        device_id = id,
                        session_idx, "loading ONNX models on GPU device"
                    );
                    sets.push(SharedModels::load(Some(id))?);
                }
            }
            info!(
                n_devices = sets.len(),
                "all GPU model sets loaded successfully"
            );
            sets
        };

        Ok(Arc::new(Self {
            model_sets,
            next: AtomicUsize::new(0),
        }))
    }

    /// Get the next model set via round-robin. Call this once per worker at
    /// init time to assign each worker a device.
    pub fn next_model_set(&self) -> Arc<SharedModels> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.model_sets.len();
        Arc::clone(&self.model_sets[idx])
    }

    /// Number of device-specific model sets in the pool.
    pub fn len(&self) -> usize {
        self.model_sets.len()
    }

    /// Returns true if the pool has no model sets (should never happen after construction).
    pub fn is_empty(&self) -> bool {
        self.model_sets.is_empty()
    }
}
