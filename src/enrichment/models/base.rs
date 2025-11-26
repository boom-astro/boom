use crate::{
    alert::AlertCutout,
    utils::fits::{prepare_triplet, CutoutError},
};
use ndarray::{Array, Array2, Axis, Dim};
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
    #[error("error preparing cutout data")]
    PrepareCutoutError(#[from] CutoutError),
    #[error("error converting predictions to vec")]
    ModelOutputToVecError,
    #[error("error converting array to tensor")]
    TensorConversionError,
    #[error("missing feature in alert")]
    MissingFeature(&'static str),
}

pub fn load_model(path: &str) -> Result<Session, ModelError> {
    let mut builder = Session::builder()?;

    let use_gpu = env::var("USE_GPU").map(|v| v == "true").unwrap_or(true);
    // Only attempt to load CUDA/CoreML when USE_GPU=true
    if use_gpu {
        // if CUDA or Apple's CoreML aren't available,
        // it will fall back to CPU execution provider
        builder = builder.with_execution_providers([
            #[cfg(target_os = "linux")]
            ort::execution_providers::CUDAExecutionProvider::default().build(),
            #[cfg(target_os = "macos")]
            ort::execution_providers::CoreMLExecutionProvider::default().build(),
        ])?;
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
        &self,
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
                let mut slice = triplets.slice_mut(ndarray::s![i, .., .., j as usize]);
                let cutout_array = Array::from_shape_vec((63, 63), cutout.to_vec())?;
                slice.assign(&cutout_array);
            }
        }
        Ok(triplets)
    }

    #[instrument(skip_all)]
    fn softmax(input: Array2<f32>) -> Array2<f32> {
        let mut output = Array2::zeros(input.raw_dim());

        for (i, row) in input.axis_iter(Axis(0)).enumerate() {
            let max_val = row.iter().fold(f32::NEG_INFINITY, |acc, &x| acc.max(x));
            let exp_values: Vec<f32> = row.iter().map(|&x| (x - max_val).exp()).collect();
            let sum_exp: f32 = exp_values.iter().sum();

            for (j, &exp_val) in exp_values.iter().enumerate() {
                output[[i, j]] = exp_val / sum_exp;
            }
        }
        output
    }
}
