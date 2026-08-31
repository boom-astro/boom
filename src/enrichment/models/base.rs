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
}

pub fn load_model(path: &str) -> Result<Session, ModelError> {
    load_model_on_device(path, None, std::ptr::null_mut())
}

/// Load an ONNX model on a specific device. On Linux+CUDA, `cuda_stream` (a
/// `cudaStream_t` cast to `*mut c_void`) lets the session share its compute
/// stream with other CUDA work: pass `std::ptr::null_mut()` to let ORT
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

fn load_model_on_device_inner(
    path: &str,
    device_id: Option<i32>,
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    cuda_stream: *mut std::ffi::c_void,
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] allow_cpu_fallback: bool,
) -> Result<Session, ModelError> {
    let mut builder = Session::builder()?;

    let use_gpu = env::var("BOOM_GPU__ENABLED")
        .map(|v| env_truthy(&v))
        .unwrap_or(true);

    // Pin execution providers explicitly so CPU mode never initializes GPU EPs.
    if use_gpu {
        // Linux only: Apple's CoreML EP does need to fall back to the CPU for
        // some ONNX Runtime operators.
        #[cfg(target_os = "linux")]
        if !allow_cpu_fallback {
            builder = builder.with_disable_cpu_fallback()?;
        }

        #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
        let dev = device_id.unwrap_or(0);

        #[cfg(target_os = "linux")]
        let cuda_ep = {
            // `with_conv_max_workspace(false)` caps the cuDNN algorithm-search
            // workspace at 32 MB. Do not add `arena_extend_strategy =
            // SameAsRequested`: with dynamic-batch models its exact-sized
            // extensions wreck BFC arena reuse and VRAM climbs until OOM.
            let mut ep = ort::ep::CUDAExecutionProvider::default()
                .with_device_id(dev)
                .with_conv_max_workspace(false);
            if !cuda_stream.is_null() {
                // Safety: guaranteed by this function's caller, see above.
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

    Ok(builder
        .with_optimization_level(GraphOptimizationLevel::Level3)?
        .with_intra_threads(1)?
        .commit_from_file(path)?)
}

fn assign_triplet(
    triplets: &mut Array<f32, Dim<[usize; 4]>>,
    row: usize,
    cutouts: (Vec<f32>, Vec<f32>, Vec<f32>),
) -> Result<(), ModelError> {
    for (j, cutout) in [cutouts.0, cutouts.1, cutouts.2].into_iter().enumerate() {
        triplets
            .slice_mut(ndarray::s![row, .., .., j])
            .assign(&Array::from_shape_vec((63, 63), cutout)?);
    }
    Ok(())
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
        for (i, cutout) in alert_cutouts.iter().enumerate() {
            assign_triplet(&mut triplets, i, prepare_triplet(cutout)?)?;
        }
        Ok(triplets)
    }

    /// Build triplets for the valid cutouts only, with the original indices kept.
    fn get_triplet_indexed(
        alert_cutouts: &[&AlertCutout],
    ) -> Result<(Vec<usize>, Array<f32, Dim<[usize; 4]>>), ModelError> {
        let kept: Vec<_> = alert_cutouts
            .iter()
            .enumerate()
            .filter_map(|(i, c)| prepare_triplet(c).ok().map(|t| (i, t)))
            .collect();

        let mut indices = Vec::with_capacity(kept.len());
        let mut triplets = Array::zeros((kept.len(), 63, 63, 3));
        for (row, (index, cutouts)) in kept.into_iter().enumerate() {
            indices.push(index);
            assign_triplet(&mut triplets, row, cutouts)?;
        }

        Ok((indices, triplets))
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
