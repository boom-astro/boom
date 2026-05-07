use crate::{
    utils::cutouts::AlertCutout,
    utils::fits::{prepare_triplet, CutoutError},
};
use ndarray::{Array, Dim};
use ort::session::{builder::GraphOptimizationLevel, Session};
use std::env;
use tracing::instrument;

#[derive(thiserror::Error, Debug)]
pub enum ModelError {
    #[error("failed to access document field")]
    MissingDocumentField(#[from] mongodb::bson::document::ValueAccessError),
    #[error("shape error from ndarray")]
    NdarrayShape(#[from] ndarray::ShapeError),
    #[error("error from ort")]
    Ort(#[from] ort::Error),
    #[error("error from ort session builder")]
    OrtSessionBuilder(#[from] ort::Error<ort::session::builder::SessionBuilder>),
    #[error("error preparing cutout data")]
    PrepareCutoutError(#[from] CutoutError),
    #[error("error converting predictions to vec")]
    ModelOutputToVecError,
    #[error("missing feature in alert: {0}")]
    MissingFeature(&'static str),
    #[error("ORT_DYLIB_PATH is not set on Linux; ONNX Runtime cannot be loaded. Please set ORT_DYLIB_PATH to the path of your libonnxruntime.so.")]
    MissingOrtDylibPath,
}

/// Load an ONNX model, optionally on a specific CUDA device.
///
/// GPU usage is controlled by the `BOOM_GPU__ENABLED` environment variable (default: `"true"`).
/// When `device_id` is `Some(id)`, that CUDA device is used; otherwise device 0.
pub fn load_model(path: &str) -> Result<Session, ModelError> {
    load_model_on_device(path, None, std::ptr::null_mut())
}

pub fn load_model_on_device_with_cpu_fallback(
    path: &str,
    device_id: Option<i32>,
) -> Result<Session, ModelError> {
    load_model_on_device_inner(path, device_id, std::ptr::null_mut(), true)
}

fn env_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Load an ONNX model on a specific device. On Linux+CUDA, `cuda_stream` (a
/// `cudaStream_t` cast to `*mut c_void`) lets the session share its compute
/// stream with other CUDA work — pass `std::ptr::null_mut()` to let ORT
/// allocate its own stream. The stream argument is ignored on macOS.
///
/// # Safety
/// When non-null, `cuda_stream` must be a valid `cudaStream_t` belonging to
/// `device_id`'s device, and must outlive the returned [`Session`].
pub fn load_model_on_device(
    path: &str,
    device_id: Option<i32>,
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    cuda_stream: *mut std::ffi::c_void,
) -> Result<Session, ModelError> {
    load_model_on_device_inner(path, device_id, cuda_stream, false)
}

fn load_model_on_device_inner(
    path: &str,
    device_id: Option<i32>,
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    cuda_stream: *mut std::ffi::c_void,
    allow_cpu_fallback: bool,
) -> Result<Session, ModelError> {
    let mut builder = Session::builder()?;

    let use_gpu = env::var("BOOM_GPU__ENABLED")
        .map(|v| env_truthy(&v))
        .unwrap_or(true);

    // #[cfg(target_os = "linux")]
    // if env::var_os("ORT_DYLIB_PATH").is_none() {
    //     return Err(ModelError::MissingOrtDylibPath);
    // }

    // Pin execution providers explicitly so CPU mode never initializes GPU EPs.
    if use_gpu {
        // Disable CPU fallback to make sure we only use the GPUs as instructed.
        // We only do this on Linux as Apple's CoreML EP does need to fallback
        // to the CPU for some operators of the ONNX runtime.
        #[cfg(target_os = "linux")]
        if !allow_cpu_fallback {
            builder = builder.with_disable_cpu_fallback()?;
        }

        #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
        let dev = device_id.unwrap_or(0);

        #[cfg(target_os = "linux")]
        let cuda_ep = {
            let mut ep = ort::ep::CUDAExecutionProvider::default().with_device_id(dev);
            if !cuda_stream.is_null() {
                // Safety: caller guarantees the stream is valid for `dev`
                // and outlives the session (see fn-level safety comment).
                ep = unsafe { ep.with_compute_stream(cuda_stream as *mut ()) };
            }
            ep.build()
        };

        builder = builder.with_execution_providers([
            #[cfg(target_os = "linux")]
            cuda_ep,
            #[cfg(target_os = "macos")]
            ort::ep::CoreMLExecutionProvider::default().build(),
        ])?;
    } else {
        builder =
            builder.with_execution_providers([ort::ep::CPUExecutionProvider::default().build()])?;
    }

    let model = builder
        .with_optimization_level(GraphOptimizationLevel::Level3)?
        .with_intra_threads(1)?
        .commit_from_file(path)?;

    Ok(model)
}

pub trait Model {
    fn new(path: &str) -> Result<Self, ModelError>
    where
        Self: Sized;
    #[instrument(skip_all, err)]
    fn get_triplet(
        alert_cutouts: &[&AlertCutout],
    ) -> Result<Array<f32, Dim<[usize; 4]>>, ModelError> {
        let mut triplets = Array::zeros((alert_cutouts.len(), 63, 63, 3));
        for i in 0..alert_cutouts.len() {
            let (cutout_science, cutout_template, cutout_difference) =
                prepare_triplet(alert_cutouts[i])?;
            for (j, cutout) in [cutout_science, cutout_template, cutout_difference]
                .iter()
                .enumerate()
            {
                let mut slice = triplets.slice_mut(ndarray::s![i, .., .., j]);
                let cutout_array = Array::from_shape_vec((63, 63), cutout.to_vec())?;
                slice.assign(&cutout_array);
            }
        }
        Ok(triplets)
    }

    /// Build triplets for all valid cutouts and return the original indices kept.
    /// Invalid cutouts are skipped.
    fn get_triplet_indexed(
        alert_cutouts: &[&AlertCutout],
    ) -> Result<(Vec<usize>, Array<f32, Dim<[usize; 4]>>), ModelError> {
        let mut kept_indices: Vec<usize> = Vec::new();
        let mut kept_triplets: Vec<(Vec<f32>, Vec<f32>, Vec<f32>)> = Vec::new();

        for (idx, cutout) in alert_cutouts.iter().enumerate() {
            if let Ok((science, template, difference)) = prepare_triplet(cutout) {
                kept_indices.push(idx);
                kept_triplets.push((science.to_vec(), template.to_vec(), difference.to_vec()));
            }
        }

        if kept_indices.is_empty() {
            return Ok((kept_indices, Array::zeros((0, 63, 63, 3))));
        }

        let mut triplets = Array::zeros((kept_indices.len(), 63, 63, 3));
        for (i, (science, template, difference)) in kept_triplets.into_iter().enumerate() {
            for (j, cutout) in [science, template, difference].iter().enumerate() {
                let mut slice = triplets.slice_mut(ndarray::s![i, .., .., j]);
                let cutout_array = Array::from_shape_vec((63, 63), cutout.clone())?;
                slice.assign(&cutout_array);
            }
        }

        Ok((kept_indices, triplets))
    }

    fn predict(
        &mut self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError>;
}

pub trait FusionModel {
    /// Returns `(probs, fusion_embedding)`.
    fn predict(
        &mut self,
        tempo_x: &ndarray::Array3<f32>,
        tempo_pad_mask: &ndarray::Array2<bool>,
        tempo_global: &ndarray::Array2<f32>,
        metadata: &Array<f32, Dim<[usize; 2]>>,
        image: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<(Vec<f32>, Vec<f32>), ModelError>;
}
