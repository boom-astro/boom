use crate::enrichment::models::{load_model, Model, ModelError};
use mongodb::bson::Document;
use ndarray::{Array, Array2, Axis, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;


// This is mostly taken from BTSBot model since
// both algorithms are close enough
pub struct CiderImagesModel {
    model: Session,
}

impl Model for CiderImagesModel {
    #[instrument(err)]
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
            "metadata" => TensorRef::from_array_view(metadata_features)?,
            "images" => TensorRef::from_array_view(image_features)?,
        };
        
        let first_dim_size = image_features.shape()[0];
        let outputs = self.model.run(model_inputs)?;
        let (_, scores) = outputs["logits"]
            .try_extract_tensor::<f32>()
            .map_err(|_| ModelError::ModelOutputToVecError)?;
        
        let vec_scores = scores.to_vec();
        let temp_array = Array::from_shape_vec((first_dim_size, 4), vec_scores)?;
        let output = Self::softmax(temp_array);
        Ok(output.row(0).to_vec())
    }
}

impl CiderImagesModel {
    #[instrument(skip_all, err)]
    pub fn get_cider_metadata(
        &self,
        alerts: &[Document],
        alert_properties: &[Document],
    ) -> Result<Array<f32, Dim<[usize; 2]>>, ModelError> {
        let mut features_batch: Vec<f32> = Vec::with_capacity(alerts.len() * 25);

        for alert in alerts {
            let candidate = alert.get_document("candidate")?;

            // TODO: handle missing sgscore and distpsnr values
            // to use sensible defaults if missing
            let sgscore1 = candidate.get_f64("sgscore1")? as f32;  
            let distpsnr1 = candidate.get_f64("distpsnr1")? as f32; 
            let sgscore2 = candidate.get_f64("sgscore2")? as f32; 
            let distpsnr2 = candidate.get_f64("distpsnr2")? as f32; 

            let magpsf = candidate.get_f64("magpsf")?; // we convert to f32 later 
            let sigmapsf = candidate.get_f64("sigmapsf")? as f32;
            let ra = candidate.get_f64("ra")? as f32; 
            let dec = candidate.get_f64("dec")? as f32; 
            let diffmaglim = candidate.get_f64("diffmaglim")? as f32; 
            let ndethist = candidate.get_i32("ndethist")? as f32;
            let nmtchps = candidate.get_i32("nmtchps")? as f32; 
            let ncovhist = candidate.get_i32("ncovhist")? as f32; 

            let chinr = candidate.get_f64("chinr")? as f32; 
            let sharpnr = candidate.get_f64("sharpnr")? as f32; 
            let scorr = candidate.get_f64("scorr")? as f32; 
            let sky = candidate.get_f64("sky")? as f32; 
            let classtar = candidate.get_f64("classtar")? as f32; 
            let band = candidate.get_str("band")?; 
            let filter_id = match band {
                "g" => 1,
                "r" => 2,
                "i" => 3,
                _ => 0, // default value if field is missing or has other value
            };
            // alert properties already computed from lightcurve analysis
            let peakmag_so_far = alert_properties[0].get_f64("peak_mag").unwrap();
            let peakjd = alert_properties[0].get_f64("peak_jd").unwrap();
            let maxmag_so_far = alert_properties[0].get_f64("faintest_mag").unwrap();
            let firstjd = alert_properties[0].get_f64("first_jd").unwrap();
            let lastjd = alert_properties[0].get_f64("last_jd").unwrap();

            let days_since_peak = (lastjd - peakjd) as f32;
            let days_to_peak = (peakjd - firstjd) as f32;
            let age = (firstjd - lastjd) as f32;

            let nnondet = ncovhist - ndethist; 

            let alert_features = [
                sgscore1,
                sgscore2,
                distpsnr1,
                distpsnr2,
                nmtchps,
                sharpnr,
                scorr,
                ra,
                dec,
                diffmaglim,
                sky,
                ndethist,
                ncovhist,
                sigmapsf,
                chinr,
                magpsf as f32,
                nnondet,
                classtar,
                filter_id as f32,
                days_since_peak,
                days_to_peak,
                age,
                peakmag_so_far as f32,
                maxmag_so_far as f32
            ];

            features_batch.extend(alert_features);
        }

        let features_array = Array::from_shape_vec((alerts.len(), 24), features_batch)?;
        Ok(features_array)
    }
}

impl CiderImagesModel {
    #[instrument(skip_all)]
    fn softmax(input: Array2<f32>) -> Array2<f32> {
        let mut output = Array2::zeros(input.raw_dim());
        
        for (i, row) in input.axis_iter(Axis(0)).enumerate() {
            let max_val = row.iter()
                .fold(f32::NEG_INFINITY, |acc, &x| acc.max(x));
            
            let exp_values: Vec<f32> = row.iter()
                .map(|&x| (x - max_val).exp())
                .collect();
            
            let sum_exp: f32 = exp_values.iter().sum();
            
            for (j, &exp_val) in exp_values.iter().enumerate() {
                output[[i, j]] = exp_val / sum_exp;
            }
        }
        output
    }
}