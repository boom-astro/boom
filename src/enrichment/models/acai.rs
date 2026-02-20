use crate::enrichment::{
    models::{load_model, Model, ModelError},
    ZtfAlertForEnrichment,
};
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;

pub struct AcaiModel {
    model: Session,
}

impl Model for AcaiModel {
    #[instrument(skip(path), fields(model_path = %path), err)]
    fn new(path: &str) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model(&path)?,
        })
    }

    #[instrument(skip_all, err)]
    fn predict(
        &mut self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError> {
        let model_inputs = inputs! {
            "features" =>  TensorRef::from_array_view(metadata_features)?,
            "triplets" => TensorRef::from_array_view(image_features)?,
        };

        let outputs = self.model.run(model_inputs)?;

        match outputs["score"].try_extract_tensor::<f32>() {
            Ok((_, scores)) => Ok(scores.to_vec()),
            Err(_) => Err(ModelError::ModelOutputToVecError),
        }
    }
}

impl AcaiModel {
    #[instrument(skip_all, fields(batch_size = alerts.len()), err)]
    pub fn get_metadata(
        &self,
        alerts: &[&ZtfAlertForEnrichment],
    ) -> Result<Array<f32, Dim<[usize; 2]>>, ModelError> {
        let mut features_batch: Vec<f32> = Vec::with_capacity(alerts.len() * 25);

        for alert in alerts {
            let candidate = &alert.candidate.candidate;

            let drb = candidate.drb.ok_or(ModelError::MissingFeature("drb"))? as f32;
            let diffmaglim = candidate
                .diffmaglim
                .ok_or(ModelError::MissingFeature("diffmaglim"))?
                as f32;
            let ra = candidate.ra as f32;
            let dec = candidate.dec as f32;
            let magpsf = candidate.magpsf;
            let sigmapsf = candidate.sigmapsf;
            let chipsf = candidate
                .chipsf
                .ok_or(ModelError::MissingFeature("chipsf"))? as f32;
            let fwhm = candidate.fwhm.ok_or(ModelError::MissingFeature("fwhm"))? as f32;
            let sky = candidate.sky.ok_or(ModelError::MissingFeature("sky"))? as f32;
            let chinr = candidate.chinr.ok_or(ModelError::MissingFeature("chinr"))? as f32;
            let sharpnr = candidate
                .sharpnr
                .ok_or(ModelError::MissingFeature("sharpnr"))? as f32;
            let sgscore1 = candidate
                .sgscore1
                .ok_or(ModelError::MissingFeature("sgscore1"))? as f32;
            let distpsnr1 = candidate
                .distpsnr1
                .ok_or(ModelError::MissingFeature("distpsnr1"))? as f32;
            let sgscore2 = candidate
                .sgscore2
                .ok_or(ModelError::MissingFeature("sgscore2"))? as f32;
            let distpsnr2 = candidate
                .distpsnr2
                .ok_or(ModelError::MissingFeature("distpsnr2"))? as f32;
            let sgscore3 = candidate
                .sgscore3
                .ok_or(ModelError::MissingFeature("sgscore3"))? as f32;
            let distpsnr3 = candidate
                .distpsnr3
                .ok_or(ModelError::MissingFeature("distpsnr3"))? as f32;
            let ndethist = candidate.ndethist as f32;
            let ncovhist = candidate.ncovhist as f32;
            let scorr = candidate.scorr.ok_or(ModelError::MissingFeature("scorr"))? as f32;
            let nmtchps = candidate.nmtchps as f32;
            let clrcoeff = candidate
                .clrcoeff
                .ok_or(ModelError::MissingFeature("clrcoeff"))? as f32;
            let clrcounc = candidate
                .clrcounc
                .ok_or(ModelError::MissingFeature("clrcounc"))? as f32;
            let neargaia = candidate
                .neargaia
                .ok_or(ModelError::MissingFeature("neargaia"))? as f32;
            let neargaiabright = candidate
                .neargaiabright
                .ok_or(ModelError::MissingFeature("neargaiabright"))?
                as f32;

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

        let features_array = Array::from_shape_vec((alerts.len(), 25), features_batch)?;
        Ok(features_array)
    }
}
