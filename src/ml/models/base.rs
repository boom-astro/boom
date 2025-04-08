use ndarray::{Array, Dim};
use ort::{
    execution_providers::CUDAExecutionProvider,
    session::{builder::GraphOptimizationLevel, Session},
};

use crate::utils::fits::prepare_triplet;
use mongodb::bson::Document;

pub fn load_model(path: &str) -> Session {
    let builder = Session::builder().unwrap();

    builder
        .with_execution_providers([CUDAExecutionProvider::default().build()])
        .unwrap()
        .with_optimization_level(GraphOptimizationLevel::Level3)
        .unwrap()
        .with_intra_threads(1)
        .unwrap()
        .commit_from_file(path)
        .unwrap()
}

pub trait Model {
    fn new(path: &str) -> Self;
    fn get_metadata(&self, alerts: &[Document]) -> Array<f32, Dim<[usize; 2]>>;
    fn get_triplet(&self, alerts: &[Document]) -> Array<f32, Dim<[usize; 4]>> {
        let mut triplets = Array::zeros((alerts.len(), 63, 63, 3));
        for i in 0..alerts.len() {
            let (cutout_science, cutout_template, cutout_difference) =
                prepare_triplet(&alerts[i]).unwrap();
            for (j, cutout) in [cutout_science, cutout_template, cutout_difference]
                .iter()
                .enumerate()
            {
                let mut slice = triplets.slice_mut(ndarray::s![i, .., .., j as usize]);
                let cutout_array = Array::from_shape_vec((63, 63), cutout.to_vec()).unwrap();
                slice.assign(&cutout_array);
            }
        }
        triplets
    }
    fn predict(
        &self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Vec<f32>;
}
