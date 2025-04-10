use ndarray::{Array, Dim};
use ort::{inputs, session::Session};

use crate::ml::models::{load_model, Model, ModelError};
use mongodb::bson::Document;

pub struct AcaiModel {
    model: Session,
}

impl Model for AcaiModel {
    fn new(path: &str) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model(&path)?,
        })
    }

    fn get_metadata(&self, alerts: &[Document]) -> Result<Array<f32, Dim<[usize; 2]>>, ModelError> {
        let mut features_batch: Vec<f32> = Vec::with_capacity(alerts.len() * 25);

        for alert in alerts {
            let candidate = alert.get_document("candidate")?;

            let drb = candidate.get_f64("drb")? as f32;
            let diffmaglim = candidate.get_f64("diffmaglim")? as f32;
            let ra = candidate.get_f64("ra")? as f32;
            let dec = candidate.get_f64("dec")? as f32;
            let magpsf = candidate.get_f64("magpsf")? as f32;
            let sigmapsf = candidate.get_f64("sigmapsf")? as f32;
            let chipsf = candidate.get_f64("chipsf")? as f32;
            let fwhm = candidate.get_f64("fwhm")? as f32;
            let sky = candidate.get_f64("sky")? as f32;
            let chinr = candidate.get_f64("chinr")? as f32;
            let sharpnr = candidate.get_f64("sharpnr")? as f32;

            // TODO: handle missing sgscore and distpsnr values
            // to use sensible defaults if missing
            let sgscore1 = candidate.get_f64("sgscore1")? as f32;
            let distpsnr1 = candidate.get_f64("distpsnr1")? as f32;
            let sgscore2 = candidate.get_f64("sgscore2")? as f32;
            let distpsnr2 = candidate.get_f64("distpsnr2")? as f32;
            let sgscore3 = candidate.get_f64("sgscore3")? as f32;
            let distpsnr3 = candidate.get_f64("distpsnr3")? as f32;

            let ndethist = candidate.get_i32("ndethist")? as f32;
            let ncovhist = candidate.get_i32("ncovhist")? as f32;
            let scorr = candidate.get_f64("scorr")? as f32;
            let nmtchps = candidate.get_i32("nmtchps")? as f32;
            let clrcoeff = candidate.get_f64("clrcoeff")? as f32;
            let clrcounc = candidate.get_f64("clrcounc")? as f32;

            // TODO: handle missing neargaia and neargaiabright values
            let neargaia = candidate.get_f64("neargaia")? as f32;
            let neargaiabright = candidate.get_f64("neargaiabright")? as f32;

            // the vec is nested because we need it to be of shape (1, N), not just a flat array
            let alert_features = [
                drb,
                diffmaglim,
                ra,
                dec,
                magpsf,
                sigmapsf,
                chipsf,
                fwhm,
                sky,
                chinr,
                sharpnr,
                sgscore1,
                distpsnr1,
                sgscore2,
                distpsnr2,
                sgscore3,
                distpsnr3,
                ndethist,
                ncovhist,
                scorr,
                nmtchps,
                clrcoeff,
                clrcounc,
                neargaia,
                neargaiabright,
            ];

            features_batch.extend(alert_features);
        }

        let features_array = Array::from_shape_vec((alerts.len(), 25), features_batch)
            .map_err(ModelError::NewArrayError)?;
        Ok(features_array)
    }

    fn predict(
        &self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError> {
        let model_inputs = inputs! {
            "features" =>  metadata_features.clone(),
            "triplets" => image_features.clone(),
        }
        .map_err(ModelError::InputError)?;

        let outputs = self
            .model
            .run(model_inputs)
            .map_err(ModelError::RunModelError)?;

        match outputs["score"]
            .try_extract_tensor::<f32>()
            .map_err(ModelError::ModelOutputError)?
            .as_slice()
        {
            Some(scores) => Ok(scores.to_vec()),
            None => Err(ModelError::ModelOutputToVecError),
        }
    }
}
