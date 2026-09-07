use crate::utils::{
    cutouts::AlertCutout,
    fits::{prepare_triplet, CutoutError},
};
use ndarray::{Array, Dim};
use ort::session::{builder::GraphOptimizationLevel, Session};
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

pub fn load_model(path: &str) -> Result<Session, ModelError> {
    load_model_on_device(path, None, std::ptr::null_mut())
}

/// Load an ONNX model on `device_id`'s CUDA device, or on CPU when `None`.
/// On Linux+CUDA, `cuda_stream` (a `cudaStream_t` cast to `*mut c_void`) lets
/// the session share its compute stream with other CUDA work — pass
/// `std::ptr::null_mut()` to let ORT allocate its own stream. The stream
/// argument is ignored on macOS.
///
/// # Safety
/// When non-null, `cuda_stream` must be a valid `cudaStream_t` belonging to
/// `device_id`'s device, and must outlive the returned [`Session`].
#[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
pub fn load_model_on_device(
    path: &str,
    device_id: Option<i32>,
    cuda_stream: *mut std::ffi::c_void,
) -> Result<Session, ModelError> {
    let mut builder = Session::builder()?;

    #[cfg(target_os = "linux")]
    if std::env::var_os("ORT_DYLIB_PATH").is_none() {
        return Err(ModelError::MissingOrtDylibPath);
    }

    // Pin execution providers explicitly so CPU mode never initializes GPU EPs.
    if let Some(dev) = device_id {
        // Linux only: CoreML needs CPU fallback for some ONNX operators.
        #[cfg(target_os = "linux")]
        {
            builder = builder.with_disable_cpu_fallback()?;
        }

        #[cfg(target_os = "linux")]
        let cuda_ep = {
            // Tried and reverted: with dynamic batch it kills BFC arena reuse and OOMs.
            // .with_arena_extend_strategy(ort::ep::ArenaExtendStrategy::SameAsRequested)
            let mut ep = ort::ep::CUDAExecutionProvider::default()
                .with_device_id(dev)
                .with_conv_max_workspace(false);
            if !cuda_stream.is_null() {
                // Safety: guaranteed by this function's own safety contract.
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

/// Batch of 63x63x3 science/template/difference cutouts, one per alert.
pub type Triplets = Array<f32, Dim<[usize; 4]>>;

fn stack_triplets(cutouts: Vec<(Vec<f32>, Vec<f32>, Vec<f32>)>) -> Result<Triplets, ModelError> {
    let mut triplets = Array::zeros((cutouts.len(), 63, 63, 3));
    for (i, (science, template, difference)) in cutouts.into_iter().enumerate() {
        for (j, cutout) in [science, template, difference].into_iter().enumerate() {
            let mut slice = triplets.slice_mut(ndarray::s![i, .., .., j]);
            slice.assign(&Array::from_shape_vec((63, 63), cutout)?);
        }
    }
    Ok(triplets)
}

pub trait Model {
    fn new(path: &str) -> Result<Self, ModelError>
    where
        Self: Sized;
    #[instrument(skip_all, err)]
    fn get_triplet(alert_cutouts: &[&AlertCutout]) -> Result<Triplets, ModelError> {
        let cutouts = alert_cutouts
            .iter()
            .copied()
            .map(prepare_triplet)
            .collect::<Result<Vec<_>, _>>()?;
        stack_triplets(cutouts)
    }

    /// Like [`Model::get_triplet`], but skips invalid cutouts and returns the indices kept.
    fn get_triplet_indexed(
        alert_cutouts: &[&AlertCutout],
    ) -> Result<(Vec<usize>, Triplets), ModelError> {
        let (kept_indices, cutouts): (Vec<usize>, Vec<_>) = alert_cutouts
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(idx, cutout)| prepare_triplet(cutout).ok().map(|t| (idx, t)))
            .unzip();
        Ok((kept_indices, stack_triplets(cutouts)?))
    }

    fn predict(
        &mut self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError>;
}
