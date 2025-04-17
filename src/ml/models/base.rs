use ndarray::{Array, Dim};
use ort::{
    execution_providers::{
        CUDAExecutionProvider,
        // CoreMLExecutionProvider
    },
    session::{builder::GraphOptimizationLevel, Session},
};

use crate::utils::fits::{prepare_triplet, CutoutError};
use mongodb::bson::Document;

#[derive(thiserror::Error, Debug)]
pub enum ModelError {
    #[error("failed to access document field")]
    MissingDocumentField(#[from] mongodb::bson::document::ValueAccessError),
    #[error("failed to load model")]
    LoadModelError(#[source] ort::Error),
    #[error("error creating new array")]
    NewArrayError(#[source] ndarray::ShapeError),
    #[error("failed to create model inputs")]
    InputError(#[source] ort::Error),
    #[error("error running model")]
    RunModelError(#[source] ort::Error),
    #[error("error retrieving predictions")]
    ModelOutputError(#[source] ort::Error),
    #[error("error converting predictions to vec")]
    ModelOutputToVecError,
    #[error("error preparing cutout data")]
    PrepareCutoutError(#[from] CutoutError),
}

pub fn load_model(path: &str) -> Result<Session, ModelError> {
    let builder = Session::builder().map_err(ModelError::LoadModelError)?;

    // if CUDA or Apple's CoreML aren't available,
    // it will fall back to CPU execution provider
    let model = builder
        .with_execution_providers([
            CUDAExecutionProvider::default().build(),
            // adding the coreml feature in Cargo.toml is creating some issues
            // at compile time. Needs to be fixed so we can add this back
            // CoreMLExecutionProvider::default().build(),
        ])
        .map_err(ModelError::LoadModelError)?
        .with_optimization_level(GraphOptimizationLevel::Level3)
        .map_err(ModelError::LoadModelError)?
        .with_intra_threads(1)
        .map_err(ModelError::LoadModelError)?
        .commit_from_file(path)
        .map_err(ModelError::LoadModelError)?;

    Ok(model)
}

pub trait Model {
    fn new(path: &str) -> Result<Self, ModelError>
    where
        Self: Sized;
    fn get_metadata(&self, alerts: &[Document]) -> Result<Array<f32, Dim<[usize; 2]>>, ModelError>;
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
                let cutout_array = Array::from_shape_vec((63, 63), cutout.to_vec())
                    .map_err(ModelError::NewArrayError)?;
                slice.assign(&cutout_array);
            }
        }
        Ok(triplets)
    }
    fn predict(
        &self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError>;
}
