mod acai;
mod base;
mod btsbot;
mod cider;

pub use acai::AcaiModel;
pub use base::{
    load_model, load_model_on_device, load_model_on_device_with_cpu_fallback, FusionModel, Model,
    ModelError,
};
pub use btsbot::BtsBotModel;
pub use cider::CiderFusionModel;

#[cfg(all(feature = "gpu", target_os = "linux"))]
use villar_pso::gpu::{GpuContext, Stream};
#[cfg(all(feature = "gpu", target_os = "macos"))]
use villar_pso::gpu_metal::GpuContext;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::info;

const SESSIONS_PER_DEVICE: usize = 1;

/// ONNX models shared across all enrichment worker threads via `Arc`.
///
/// Each model is wrapped in a `Mutex` because `Session::run` requires `&mut self`.
/// Concurrent workers will serialize on the mutex, but model weights are loaded
/// only once in memory (and on GPU VRAM if using CUDA).
///
/// On Linux+GPU, all sessions share a single CUDA stream with the villar-pso
/// `GpuContext` so PSO and ONNX inference run on the same stream and avoid
/// the implicit cross-stream barriers of the legacy default stream.
///
/// **Field ordering matters**: Rust drops struct fields in declaration order,
/// so the stream must be declared AFTER everything that uses it (the ORT
/// sessions and the `GpuContext`) to ensure `cudaStreamDestroy` fires last.
pub struct SharedModels {
    pub acai_h: Mutex<AcaiModel>,
    pub acai_n: Mutex<AcaiModel>,
    pub acai_v: Mutex<AcaiModel>,
    pub acai_o: Mutex<AcaiModel>,
    pub acai_b: Mutex<AcaiModel>,
    pub btsbot: Mutex<BtsBotModel>,
    pub cider: Mutex<cider::CiderFusionModel>,
    /// Villar-PSO GPU context bound to this device. `None` when running
    /// without the `gpu` feature.
    #[cfg(feature = "gpu")]
    pub gpu_ctx: Option<GpuContext>,
    /// CUDA stream shared with the ORT sessions above. Must be dropped last
    /// — see struct-level docstring.
    #[cfg(all(feature = "gpu", target_os = "linux"))]
    _stream: Option<Stream>,
}

impl std::fmt::Debug for SharedModels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedModels").finish_non_exhaustive()
    }
}

impl SharedModels {
    /// Load all ONNX models, optionally on a specific CUDA device.
    /// Returns an `Arc` for sharing across threads.
    ///
    /// On Linux+`gpu`, this creates a CUDA stream for `device_id` and binds
    /// every ORT session to it via `with_compute_stream`. The same stream is
    /// also handed to the villar-pso `GpuContext`, so all GPU work for this
    /// device runs on one stream.
    pub fn load(device_id: Option<i32>) -> Result<Arc<Self>, ModelError> {
        info!(?device_id, "loading shared ONNX models");

        // Create the per-device CUDA stream first (Linux+gpu only). The raw
        // pointer is shared between ORT sessions and villar-pso.
        #[cfg(all(feature = "gpu", target_os = "linux"))]
        let stream: Option<Stream> = match device_id {
            Some(id) => Some(Stream::new_on_device(id).map_err(|e| {
                ModelError::Ort(ort::Error::new(format!(
                    "failed to create CUDA stream on device {}: {}",
                    id, e
                )))
            })?),
            None => None,
        };
        #[cfg(all(feature = "gpu", target_os = "linux"))]
        let stream_ptr: *mut std::ffi::c_void = stream
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null_mut());
        #[cfg(not(all(feature = "gpu", target_os = "linux")))]
        let stream_ptr: *mut std::ffi::c_void = std::ptr::null_mut();

        let (acai_h, acai_n, acai_v, acai_o, acai_b, btsbot, cider) = match device_id {
            Some(id) => (
                AcaiModel::new_on_device(
                    "data/models/acai_h.d1_dnn_20201130.onnx",
                    id,
                    stream_ptr,
                )?,
                AcaiModel::new_on_device(
                    "data/models/acai_n.d1_dnn_20201130.onnx",
                    id,
                    stream_ptr,
                )?,
                AcaiModel::new_on_device(
                    "data/models/acai_v.d1_dnn_20201130.onnx",
                    id,
                    stream_ptr,
                )?,
                AcaiModel::new_on_device(
                    "data/models/acai_o.d1_dnn_20201130.onnx",
                    id,
                    stream_ptr,
                )?,
                AcaiModel::new_on_device(
                    "data/models/acai_b.d1_dnn_20201130.onnx",
                    id,
                    stream_ptr,
                )?,
                BtsBotModel::new_on_device("data/models/btsbot-v1.0.1.onnx", id, stream_ptr)?,
                cider::CiderFusionModel::new_on_device(
                    "data/models/cider_fusion_plus_embedding.onnx",
                    id,
                )?,
            ),
            None => (
                AcaiModel::new("data/models/acai_h.d1_dnn_20201130.onnx")?,
                AcaiModel::new("data/models/acai_n.d1_dnn_20201130.onnx")?,
                AcaiModel::new("data/models/acai_v.d1_dnn_20201130.onnx")?,
                AcaiModel::new("data/models/acai_o.d1_dnn_20201130.onnx")?,
                AcaiModel::new("data/models/acai_b.d1_dnn_20201130.onnx")?,
                BtsBotModel::new("data/models/btsbot-v1.0.1.onnx")?,
                cider::CiderFusionModel::new("data/models/cider_fusion_plus_embedding.onnx")?,
            ),
        };

        // Build the villar-pso GpuContext on the same device + stream.
        #[cfg(feature = "gpu")]
        let gpu_ctx: Option<GpuContext> = match device_id {
            #[cfg(target_os = "linux")]
            Some(id) => Some(GpuContext::new(id, stream_ptr).map_err(|e| {
                ModelError::Ort(ort::Error::new(format!(
                    "villar-pso GPU init failed for device {}: {}",
                    id, e
                )))
            })?),
            #[cfg(target_os = "macos")]
            Some(id) => Some(GpuContext::new(id).map_err(|e| {
                ModelError::Ort(ort::Error::new(format!(
                    "villar-pso GPU init failed for device {}: {}",
                    id, e
                )))
            })?),
            None => None,
        };

        let models = Self {
            acai_h: Mutex::new(acai_h),
            acai_n: Mutex::new(acai_n),
            acai_v: Mutex::new(acai_v),
            acai_o: Mutex::new(acai_o),
            acai_b: Mutex::new(acai_b),
            btsbot: Mutex::new(btsbot),
            cider: Mutex::new(cider),
            #[cfg(feature = "gpu")]
            gpu_ctx,
            #[cfg(all(feature = "gpu", target_os = "linux"))]
            _stream: stream,
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
