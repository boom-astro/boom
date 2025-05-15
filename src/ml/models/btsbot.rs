use ndarray::{Array, Dim};
use ort::{inputs, session::Session};

use crate::ml::models::{load_model, Model, ModelError};
use mongodb::bson::Document;

pub struct BtsBotModel {
    model: Session,
}

impl Model for BtsBotModel {
    fn new(path: &str) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model(&path)?,
        })
    }

    fn get_metadata(&self, alerts: &[Document]) -> Result<Array<f32, Dim<[usize; 2]>>, ModelError> {
        let mut features_batch: Vec<f32> = Vec::with_capacity(alerts.len() * 25);

        for alert in alerts {
            let candidate = alert.get_document("candidate")?;

            // TODO: handle missing sgscore and distpsnr values
            // to use sensible defaults if missing
            let sgscore1 = candidate.get_f64("sgscore1")? as f32;
            let distpsnr1 = candidate.get_f64("distpsnr1")? as f32;
            let sgscore2 = candidate.get_f64("sgscore2")? as f32;
            let distpsnr2 = candidate.get_f64("distpsnr2")? as f32;

            let fwhm = candidate.get_f64("fwhm")? as f32;
            let magpsf = candidate.get_f64("magpsf")?; // we convert to f32 later
            let sigmapsf = candidate.get_f64("sigmapsf")? as f32;
            let chipsf = candidate.get_f64("chipsf")? as f32;
            let ra = candidate.get_f64("ra")? as f32;
            let dec = candidate.get_f64("dec")? as f32;
            let diffmaglim = candidate.get_f64("diffmaglim")? as f32;
            let ndethist = candidate.get_i32("ndethist")? as f32;
            let nmtchps = candidate.get_i32("nmtchps")? as f32;

            let drb = candidate.get_f64("drb")? as f32;
            let ncovhist = candidate.get_i32("ncovhist")? as f32;

            let chinr = candidate.get_f64("chinr")? as f32;
            let sharpnr = candidate.get_f64("sharpnr")? as f32;
            let scorr = candidate.get_f64("scorr")? as f32;
            let sky = candidate.get_f64("sky")? as f32;

            // next, we compute some custom features based on the lightcurve
            let jd = candidate.get_f64("jd")?;
            let mut firstdet_jd = jd;
            let mut peakmag_jd = jd;
            let mut peakmag = magpsf;
            let mut maxmag = magpsf;

            match alert.get_array("prv_candidates") {
                Ok(prv_candidates) => {
                    for prv_cand in prv_candidates {
                        match prv_cand.as_document() {
                            Some(prv_cand) => {
                                let prv_cand_magpsf = prv_cand.get_f64("magpsf")?;
                                let prv_cand_jd = prv_cand.get_f64("jd")?;
                                if prv_cand_magpsf < peakmag {
                                    peakmag = prv_cand_magpsf;
                                    peakmag_jd = prv_cand_jd;
                                }
                                if prv_cand_magpsf > maxmag {
                                    maxmag = prv_cand_magpsf;
                                }
                                if prv_cand_jd < firstdet_jd {
                                    firstdet_jd = prv_cand_jd;
                                }
                            }
                            None => {}
                        }
                    }
                }
                Err(_) => {}
            }

            let days_since_peak = (jd - peakmag_jd) as f32;
            let days_to_peak = (peakmag_jd - firstdet_jd) as f32;
            let age = (firstdet_jd - jd) as f32;

            let nnondet = ncovhist - ndethist;

            let alert_features = [
                sgscore1,
                distpsnr1,
                sgscore2,
                distpsnr2,
                fwhm,
                magpsf as f32,
                sigmapsf,
                chipsf,
                ra,
                dec,
                diffmaglim,
                ndethist,
                nmtchps,
                age,
                days_since_peak,
                days_to_peak,
                peakmag as f32,
                drb,
                ncovhist,
                nnondet,
                chinr,
                sharpnr,
                scorr,
                sky,
                maxmag as f32,
            ];

            features_batch.extend(alert_features);
        }

        let features_array = Array::from_shape_vec((alerts.len(), 25), features_batch)?;
        Ok(features_array)
    }

    fn predict(
        &self,
        metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError> {
        let model_inputs = inputs! {
            "triplet" => image_features.clone(),
            "metadata" =>  metadata_features.clone(),
        }?;

        let outputs = self.model.run(model_inputs)?;

        match outputs["fc_out"].try_extract_tensor::<f32>()?.as_slice() {
            Some(scores) => Ok(scores.to_vec()),
            None => Err(ModelError::ModelOutputToVecError),
        }
    }
}
