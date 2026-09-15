use crate::enrichment::{
    models::{load_model, load_model_on_device, Model, ModelError},
    ZtfAlertForEnrichment,
};
use crate::utils::lightcurves::{analyze_photometry, prepare_photometry, PhotometryMag};
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
        // ACAI reads channels-last; BTSbot is a PyTorch export and converts.
        let channels_first = image_features
            .view()
            .permuted_axes([0, 3, 1, 2])
            .as_standard_layout()
            .to_owned();

        let model_inputs = inputs! {
            "image" => TensorRef::from_array_view(&channels_first)?,
            "metadata" => TensorRef::from_array_view(metadata_features)?,
        };

        let outputs = self.model.run(model_inputs)?;

        // v2 emits logits; filters cut on a probability, so squash it back.
        match outputs["logits"].try_extract_tensor::<f32>() {
            Ok((_, logits)) => Ok(logits.iter().map(|x| 1.0 / (1.0 + (-x).exp())).collect()),
            Err(_) => Err(ModelError::ModelOutputToVecError),
        }
    }
}

/// SNR a detection must reach to count for BTSbot. ZTF's historical default.
pub const MIN_SNR: f64 = 5.0;

/// Detections up to the alert epoch that clear [`MIN_SNR`], band-agnostic.
fn btsbot_lightcurve(alert: &ZtfAlertForEnrichment) -> Vec<PhotometryMag> {
    let mut points: Vec<_> = alert
        .prv_candidates
        .iter()
        .filter(|p| p.jd <= alert.candidate.candidate.jd)
        .filter_map(|p| p.to_photometry_mag(Some(MIN_SNR)))
        .collect();
    prepare_photometry(&mut points);
    points
}

/// When the object was first seen, by whichever record reaches back further.
fn first_seen_jd(jdstarthist: Option<f64>, first_jd: f64) -> f64 {
    jdstarthist.map_or(first_jd, |j| j.min(first_jd))
}

impl BtsBotModel {
    /// Load on a specific CUDA device, optionally sharing a compute stream.
    /// `cuda_stream` is a `cudaStream_t` (or null) — see [`load_model_on_device`].
    pub fn new_on_device(
        path: &str,
        device_id: i32,
        cuda_stream: *mut std::ffi::c_void,
    ) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model_on_device(path, Some(device_id), cuda_stream)?,
        })
    }

    #[instrument(skip_all, err)]
    pub fn get_metadata(
        alerts: &[&ZtfAlertForEnrichment],
    ) -> Result<Array<f32, Dim<[usize; 2]>>, ModelError> {
        let mut features_batch: Vec<f32> = Vec::with_capacity(alerts.len() * 25);

        for i in 0..alerts.len() {
            let alert_features = Self::metadata_for_alert(alerts[i])?;

            features_batch.extend(alert_features);
        }

        let features_array = Array::from_shape_vec((alerts.len(), 25), features_batch)?;
        Ok(features_array)
    }

    /// Build metadata for all valid alerts and return the original indices kept.
    pub fn get_metadata_indexed(
        alerts: &[&ZtfAlertForEnrichment],
    ) -> Result<(Vec<usize>, Array<f32, Dim<[usize; 2]>>), ModelError> {
        let mut kept_indices: Vec<usize> = Vec::new();
        let mut features_batch: Vec<f32> = Vec::new();

        for i in 0..alerts.len() {
            if let Ok(features) = Self::metadata_for_alert(alerts[i]) {
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

    fn metadata_for_alert(alert: &ZtfAlertForEnrichment) -> Result<[f32; 25], ModelError> {
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

        // BTSbot's own view, so the SNR floor does not move `photstats`.
        let lightcurve = btsbot_lightcurve(alert);
        if lightcurve.is_empty() {
            return Err(ModelError::MissingFeature("lightcurve"));
        }
        let (_, properties, _) = analyze_photometry(&lightcurve);
        let peakmag = properties.peak_mag;
        let peakjd = properties.peak_jd;
        let faintestmag = properties.faintest_mag;
        let firstjd = properties.first_jd;

        // Anchored so that age == days_since_peak + days_to_peak.
        let start_jd = first_seen_jd(candidate.jdstarthist, firstjd);
        let age = (candidate.jd - start_jd) as f32;
        let days_since_peak = (candidate.jd - peakjd) as f32;
        let days_to_peak = (peakjd - start_jd) as f32;

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

#[cfg(test)]
mod tests {

    /// The first sighting is whichever record reaches back further.
    #[test]
    fn test_first_seen_is_the_earlier_record() {
        let jd = 2_460_000.0;
        assert_eq!(first_seen_jd(Some(jd - 900.0), jd - 40.0), jd - 900.0);
        assert_eq!(first_seen_jd(Some(jd - 10.0), jd - 40.0), jd - 40.0);
        assert_eq!(first_seen_jd(None, jd - 40.0), jd - 40.0);
    }

    use super::*;

    const MODEL: &str = "data/models/btsbot-v2.0.0.onnx";

    /// Loads the shipped model and runs it the way enrichment does, with a
    /// channels-last triplet.
    ///
    /// Pins the three things that changed between v1 and v2 and that a type
    /// check cannot catch: the tensor names, the channel order, and that the
    /// score is a probability rather than the logit the model emits.
    #[test]
    fn test_the_shipped_model_scores_a_channels_last_triplet() {
        let mut model = BtsBotModel::new(MODEL).expect("the shipped model loads");

        let batch = 3;
        let metadata = Array::from_shape_vec((batch, 25), vec![0.5; batch * 25]).expect("metadata");
        let triplet = Array::from_shape_vec(
            (batch, 63, 63, 3),
            (0..batch * 63 * 63 * 3)
                .map(|i| (i % 17) as f32 / 17.0)
                .collect(),
        )
        .expect("triplet");

        let scores = model.predict(&metadata, &triplet).expect("inference runs");

        assert_eq!(scores.len(), batch);
        for score in &scores {
            assert!(
                (0.0..=1.0).contains(score),
                "score {score} is outside [0, 1], so the logit was not squashed"
            );
            assert!(score.is_finite(), "score {score} is not finite");
        }
    }

    /// The channel axis is the one that moves, so a triplet whose three planes
    /// differ must score differently from one where they do not -- a transpose
    /// that silently reinterpreted rows as channels would not.
    #[test]
    fn test_channel_order_reaches_the_model() {
        let mut model = BtsBotModel::new(MODEL).expect("the shipped model loads");
        let metadata = Array::from_shape_vec((1, 25), vec![0.5; 25]).expect("metadata");

        let uniform = Array::from_shape_vec((1, 63, 63, 3), vec![0.5; 63 * 63 * 3]).expect("flat");
        let per_channel = Array::from_shape_vec(
            (1, 63, 63, 3),
            (0..63 * 63 * 3)
                .map(|i| match i % 3 {
                    0 => 0.1,
                    1 => 0.5,
                    _ => 0.9,
                })
                .collect(),
        )
        .expect("per channel");

        let a = model.predict(&metadata, &uniform).expect("uniform")[0];
        let b = model.predict(&metadata, &per_channel).expect("per channel")[0];
        assert!(
            (a - b).abs() > 1e-6,
            "channel content did not change the score ({a} vs {b})"
        );
    }
}
