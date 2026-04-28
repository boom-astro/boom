use crate::{
    enrichment::{
        models::{load_model, load_model_on_device, Model, ModelError},
        ZtfAlertForEnrichment,
    },
    utils::lightcurves::AllBandsProperties,
};
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;

pub struct BtsBotModel {
    model: Session,
}

impl Model for BtsBotModel {
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
            "triplet" => TensorRef::from_array_view(image_features)?,
            "metadata" => TensorRef::from_array_view(metadata_features)?,
        };

        let outputs = self.model.run(model_inputs)?;

        match outputs["fc_out"].try_extract_tensor::<f32>() {
            Ok((_, scores)) => Ok(scores.to_vec()),
            Err(_) => Err(ModelError::ModelOutputToVecError),
        }
    }
}

impl BtsBotModel {
    /// Load on a specific CUDA device.
    pub fn new_on_device(path: &str, device_id: i32) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model_on_device(path, Some(device_id))?,
        })
    }

    #[instrument(skip_all, err)]
    pub fn get_metadata(
        alerts: &[&ZtfAlertForEnrichment],
        alert_properties: &[AllBandsProperties],
    ) -> Result<Array<f32, Dim<[usize; 2]>>, ModelError> {
        let mut features_batch: Vec<f32> = Vec::with_capacity(alerts.len() * 25);

        for i in 0..alerts.len() {
            let alert_features = Self::metadata_for_alert(&alerts[i], &alert_properties[i])?;

            features_batch.extend(alert_features);
        }

        let features_array = Array::from_shape_vec((alerts.len(), 25), features_batch)?;
        Ok(features_array)
    }

    /// Build metadata for all valid alerts and return the original indices kept.
    pub fn get_metadata_indexed(
        alerts: &[&ZtfAlertForEnrichment],
        alert_properties: &[AllBandsProperties],
    ) -> Result<(Vec<usize>, Array<f32, Dim<[usize; 2]>>), ModelError> {
        let mut kept_indices: Vec<usize> = Vec::new();
        let mut features_batch: Vec<f32> = Vec::new();

        for i in 0..alerts.len() {
            if let Ok(features) = Self::metadata_for_alert(&alerts[i], &alert_properties[i]) {
                kept_indices.push(i);
                features_batch.extend(features);
            }
        }

        if kept_indices.is_empty() {
            return Ok((kept_indices, Array::zeros((0, 25))));
        }

        let features_array = Array::from_shape_vec((kept_indices.len(), 25), features_batch)?;
        Ok((kept_indices, features_array))
    }

    fn metadata_for_alert(
        alert: &ZtfAlertForEnrichment,
        alert_properties: &AllBandsProperties,
    ) -> Result<[f32; 25], ModelError> {
        let candidate = &alert.candidate.candidate;

        let drb = candidate.drb.ok_or(ModelError::MissingFeature("drb"))? as f32;
        let diffmaglim = candidate
            .diffmaglim
            .ok_or(ModelError::MissingFeature("diffmaglim"))? as f32;
        let ra = candidate.ra as f32;
        let dec = candidate.dec as f32;
        let fwhm = candidate.fwhm.ok_or(ModelError::MissingFeature("fwhm"))? as f32;
        let magpsf = candidate.magpsf;
        let sigmapsf = candidate.sigmapsf;
        let chipsf = candidate
            .chipsf
            .ok_or(ModelError::MissingFeature("chipsf"))? as f32;
        let ndethist = candidate.ndethist as f32;
        let nmtchps = candidate.nmtchps as f32;
        let ncovhist = candidate.ncovhist as f32;
        let chinr = candidate.chinr.ok_or(ModelError::MissingFeature("chinr"))? as f32;
        let sharpnr = candidate
            .sharpnr
            .ok_or(ModelError::MissingFeature("sharpnr"))? as f32;
        let scorr = candidate.scorr.ok_or(ModelError::MissingFeature("scorr"))? as f32;
        let sky = candidate.sky.ok_or(ModelError::MissingFeature("sky"))? as f32;
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

        let peakmag = alert_properties.peak_mag;
        let peakjd = alert_properties.peak_jd;
        let faintestmag = alert_properties.faintest_mag;
        let firstjd = alert_properties.first_jd;
        let lastjd = alert_properties.last_jd;

        let days_since_peak = (lastjd - peakjd) as f32;
        let days_to_peak = (peakjd - firstjd) as f32;
        let age = (firstjd - lastjd) as f32;

        let nnondet = ncovhist - ndethist;

        Ok([
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
            faintestmag as f32,
        ])
    }
}
