use ndarray::{Array, Dim};
use ort::{inputs, session::Session};

use crate::ml::models::{load_model, Model};
use mongodb::bson::Document;

pub struct BtsBotModel {
    model: Session,
}

impl Model for BtsBotModel {
    fn new(path: &str) -> Self {
        Self {
            model: load_model(&path),
        }
    }

    fn get_metadata(&self, alerts: &[Document]) -> Array<f32, Dim<[usize; 2]>> {
        let mut features_batch: Vec<[f32; 25]> = Vec::new();

        for alert in alerts {
            let candidate = alert.get_document("candidate").unwrap();

            let sgscore1 = candidate.get_f64("sgscore1").unwrap() as f32;
            let distpsnr1 = candidate.get_f64("distpsnr1").unwrap() as f32;
            let sgscore2 = candidate.get_f64("sgscore2").unwrap() as f32;
            let distpsnr2 = candidate.get_f64("distpsnr2").unwrap() as f32;
            let fwhm = candidate.get_f64("fwhm").unwrap() as f32;
            let magpsf = candidate.get_f64("magpsf").unwrap(); // we convert to f32 later
            let sigmapsf = candidate.get_f64("sigmapsf").unwrap() as f32;
            let chipsf = candidate.get_f64("chipsf").unwrap() as f32;
            let ra = candidate.get_f64("ra").unwrap() as f32;
            let dec = candidate.get_f64("dec").unwrap() as f32;
            let diffmaglim = candidate.get_f64("diffmaglim").unwrap() as f32;
            let ndethist = candidate.get_i32("ndethist").unwrap() as f32;
            let nmtchps = candidate.get_i32("nmtchps").unwrap() as f32;

            let drb = candidate.get_f64("drb").unwrap() as f32;
            let ncovhist = candidate.get_i32("ncovhist").unwrap() as f32;

            let chinr = candidate.get_f64("chinr").unwrap() as f32;
            let sharpnr = candidate.get_f64("sharpnr").unwrap() as f32;
            let scorr = candidate.get_f64("scorr").unwrap() as f32;
            let sky = candidate.get_f64("sky").unwrap() as f32;

            // next, we compute some custom features based on the lightcurve
            let jd = candidate.get_f64("jd").unwrap();
            let mut firstdet_jd = jd.clone();
            let mut peakmag_jd = jd.clone();
            let mut peakmag = magpsf.clone();
            let mut maxmag = magpsf.clone();

            for prv_cand in alert.get_array("prv_candidates").unwrap() {
                let prv_cand = prv_cand.as_document().unwrap();
                let prv_cand_magpsf = prv_cand.get_f64("magpsf").unwrap();
                let prv_cand_jd = prv_cand.get_f64("jd").unwrap();
                if prv_cand_magpsf < peakmag {
                    peakmag = prv_cand_magpsf;
                    peakmag_jd = prv_cand_jd;
                }
                if prv_cand_magpsf > maxmag {
                    maxmag = prv_cand_magpsf
                }
                if prv_cand_jd < firstdet_jd {
                    firstdet_jd = prv_cand_jd
                }
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
            "triplet" => image_features.clone(),
            "metadata" =>  metadata_features.clone(),
        }
        .unwrap();

        let outputs = self.model.run(model_inputs).unwrap();

        let scores = outputs["fc_out"].try_extract_tensor::<f32>().unwrap();

        return scores.as_slice().unwrap().to_vec();
    }
}
