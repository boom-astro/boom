use crate::utils::fits::{prepare_triplet, CutoutError};
use mongodb::bson::Document;
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
    #[error("error preparing cutout data")]
    PrepareCutoutError(#[from] CutoutError),
    #[error("error converting predictions to vec")]
    ModelOutputToVecError,
}

pub fn load_model(path: &str) -> Result<Session, ModelError> {
    let builder = Session::builder()?;

    // if CUDA or Apple's CoreML aren't available,
    // it will fall back to CPU execution provider
    let model = builder
        .with_execution_providers([
            #[cfg(target_os = "linux")]
            ort::execution_providers::CUDAExecutionProvider::default().build(),
            #[cfg(target_os = "macos")]
            ort::execution_providers::CoreMLExecutionProvider::default().build(),
        ])?
        .with_optimization_level(GraphOptimizationLevel::Level3)?
        .with_intra_threads(1)?
        .commit_from_file(path)?;

    Ok(model)
}

pub trait Model {
    fn new(path: &str) -> Result<Self, ModelError>
    where
        Self: Sized;
    fn get_metadata(&self, alerts: &[Document]) -> Result<Array<f32, Dim<[usize; 2]>>, ModelError>;
    #[instrument(skip_all, err)]
    fn get_triplet(&self, alerts: &[Document]) -> Result<Array<f32, Dim<[usize; 4]>>, ModelError> {
        let mut triplets = Array::zeros((alerts.len(), 63, 63, 3));
        for i in 0..alerts.len() {
            // TODO: remove unwrap once the crate::utils::fits dedicated error type is merged to main
            let (cutout_science, cutout_template, cutout_difference) = prepare_triplet(&alerts[i])?;
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
    fn predict(
        &mut self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError>;
}
