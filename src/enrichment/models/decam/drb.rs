use crate::enrichment::models::{load_model, Model, ModelError};
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;

pub struct DecamDrbModel {
    model: Session,
}

impl Model for DecamDrbModel {
    #[instrument(err)]
    fn new(path: &str) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model(&path)?,
        })
    }
}

impl DecamDrbModel {
    #[instrument(skip_all, err)]
    pub fn predict(
        &mut self,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError> {
        let model_inputs = inputs! {
            "input" => TensorRef::from_array_view(image_features)?,
        };

        let outputs = self.model.run(model_inputs)?;

        match outputs["rb_score"].try_extract_tensor::<f32>() {
            Ok((_, scores)) => Ok(scores.to_vec()),
            Err(_) => Err(ModelError::ModelOutputToVecError),
        }
    }
}
