use ndarray::{Array, Dim};
use ort::{inputs, session::Session};

use crate::ml::models::{load_model, Model};
use mongodb::bson::Document;

pub struct AcaiModel {
    model: Session,
}

impl Model for AcaiModel {
    fn new(path: &str) -> Self {
        Self {
            model: load_model(&path),
        }
    }

    fn get_metadata(&self, alerts: &[Document]) -> Array<f32, Dim<[usize; 2]>> {
        let mut features_batch: Vec<[f32; 25]> = Vec::new();

        for alert in alerts {
            let candidate = alert.get_document("candidate").unwrap();

            let drb = candidate.get_f64("drb").unwrap() as f32;
            let diffmaglim = candidate.get_f64("diffmaglim").unwrap() as f32;
            let ra = candidate.get_f64("ra").unwrap() as f32;
            let dec = candidate.get_f64("dec").unwrap() as f32;
            let magpsf = candidate.get_f64("magpsf").unwrap() as f32;
            let sigmapsf = candidate.get_f64("sigmapsf").unwrap() as f32;
            let chipsf = candidate.get_f64("chipsf").unwrap() as f32;
            let fwhm = candidate.get_f64("fwhm").unwrap() as f32;
            let sky = candidate.get_f64("sky").unwrap() as f32;
            let chinr = candidate.get_f64("chinr").unwrap() as f32;
            let sharpnr = candidate.get_f64("sharpnr").unwrap() as f32;
            let sgscore1 = candidate.get_f64("sgscore1").unwrap() as f32;
            let distpsnr1 = candidate.get_f64("distpsnr1").unwrap() as f32;
            let sgscore2 = candidate.get_f64("sgscore2").unwrap() as f32;
            let distpsnr2 = candidate.get_f64("distpsnr2").unwrap() as f32;
            let sgscore3 = candidate.get_f64("sgscore3").unwrap() as f32;
            let distpsnr3 = candidate.get_f64("distpsnr3").unwrap() as f32;
            let ndethist = candidate.get_i32("ndethist").unwrap() as f32;
            let ncovhist = candidate.get_i32("ncovhist").unwrap() as f32;
            let scorr = candidate.get_f64("scorr").unwrap() as f32;
            let nmtchps = candidate.get_i32("nmtchps").unwrap() as f32;
            let clrcoeff = candidate.get_f64("clrcoeff").unwrap() as f32;
            let clrcounc = candidate.get_f64("clrcounc").unwrap() as f32;
            let neargaia = candidate.get_f64("neargaia").unwrap() as f32;
            let neargaiabright = candidate.get_f64("neargaiabright").unwrap() as f32;

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

            features_batch.push(alert_features);
        }

        Array::from(features_batch)
    }

    fn predict(
        &self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Vec<f32> {
        let model_inputs = inputs! {
            "features" =>  metadata_features.clone(),
            "triplets" => image_features.clone(),
        }
        .unwrap();

        let outputs = self.model.run(model_inputs).unwrap();
        let scores = outputs["score"].try_extract_tensor::<f32>().unwrap();

        return scores.as_slice().unwrap().to_vec();
    }
}
