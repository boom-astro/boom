use crate::alert::AlertCutout;
use crate::enrichment::models::{load_model, Model, ModelError};
use crate::enrichment::ZtfAlertForEnrichment;
use crate::utils::fits::prepare_triplet;
use crate::utils::lightcurves::{AllBandsProperties, Band, PhotometryMag};
use ndarray::{Array, Array2, Array3, ArrayBase, Dim, OwnedRepr};
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
}

impl CiderImagesModel {
    #[instrument(skip_all, err)]
    pub fn get_metadata(
        &self,
        alerts: &[ZtfAlertForEnrichment],
        alert_properties: &[&AllBandsProperties],
    ) -> Result<Array<f32, Dim<[usize; 2]>>, ModelError> {
        let mut features_batch: Vec<f32> = Vec::with_capacity(alerts.len() * 25);

        for i in 0..alerts.len() {
            let candidate = &alerts[i].candidate.candidate;

            // TODO: handle missing sgscore and distpsnr values
            // to use sensible defaults if missing
            let diffmaglim = candidate
                .diffmaglim
                .ok_or(ModelError::MissingFeature("diffmaglim"))?
                as f32;
            let ra = candidate.ra as f32;
            let dec = candidate.dec as f32;
            let magpsf = candidate.magpsf;
            let sigmapsf = candidate.sigmapsf;
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

            // Specific to CIDER model
            let classtar = candidate
                .classtar
                .ok_or(ModelError::MissingFeature("classtar"))? as f32;
            let fid = candidate.fid as f32;

            // alert properties already computed from lightcurve analysis
            let peakmag = alert_properties[i].peak_mag;
            let peakjd = alert_properties[i].peak_jd;
            let faintestmag = alert_properties[i].faintest_mag;
            let firstjd = alert_properties[i].first_jd;
            let lastjd = alert_properties[i].last_jd;

            let days_since_peak = (lastjd - peakjd) as f32;
            let days_to_peak = (peakjd - firstjd) as f32;
            let age = (firstjd - lastjd) as f32;

            let nnondet = ncovhist - ndethist;
            // Normalized values based on images and metadata training model
            let alert_features = [
                (sgscore1 - 0.236) / 0.266,
                (sgscore2 - 0.401) / 0.328,
                (distpsnr1 - 3.151) / 3.757,
                (distpsnr2 - 9.269) / 6.323,
                (nmtchps - 9.231) / 8.089,
                (sharpnr - 0.253) / 0.256,
                (scorr - 22.089) / 16.757,
                ra,
                dec,
                (diffmaglim - 20.17) / 0.535,
                (sky - 0.0901) / 2.58,
                (ndethist - 18.7) / 27.83,
                (ncovhist - 1144.9) / 1141.27,
                (sigmapsf - 0.996) / 0.0448,
                (chinr - 3.257) / 21.5,
                (magpsf - 18.64) / 0.936,
                (nnondet - 1126.0) / 1140.0,
                (classtar - 0.95) / 0.08,
                (fid - 1.62) / 0.57,
                (days_since_peak - 359.59) / 627.91,
                (days_to_peak - 90.93) / 327.51,
                (age - 450.5) / 734.29,
                (peakmag - 17.945) / 0.896,
                (faintestmag - 20.37) / 0.6325,
            ];

            features_batch.extend(alert_features);
        }

        let features_array = Array::from_shape_vec((alerts.len(), 24), features_batch)?;
        Ok(features_array)
    }

    #[instrument(skip_all, err)]
    pub fn get_triplet(
        &self,
        alert_cutouts: &[&AlertCutout],
    ) -> Result<Array<f32, Dim<[usize; 4]>>, ModelError> {
        let mut triplets = Array::zeros((alert_cutouts.len(), 3, 49, 49));
        for i in 0..alert_cutouts.len() {
            let (cutout_science, cutout_template, cutout_difference) =
                prepare_triplet(&alert_cutouts[i])?;
            for (j, cutout) in [cutout_science, cutout_template, cutout_difference]
                .iter()
                .enumerate()
            {
                let mut slice = triplets.slice_mut(ndarray::s![i, j, .., ..]);
                let full_array = Array::from_shape_vec((63, 63), cutout.to_vec())?;
                let cutout_array = full_array.slice(ndarray::s![7..56, 7..56]).to_owned();
                slice.assign(&cutout_array);
            }
        }
        Ok(triplets)
    }

    #[instrument(skip_all, err)]
    pub fn predict(
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

pub struct CiderPhotometryModel {
    model: Session,
}

impl Model for CiderPhotometryModel {
    #[instrument(err)]
    fn new(path: &str) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model(&path)?,
        })
    }
}

impl CiderPhotometryModel {
    #[instrument(skip_all, err)]
    pub fn predict(
        &mut self,
        data_array: &ArrayBase<OwnedRepr<f32>, Dim<[usize; 3]>>,
        mask_array: &ArrayBase<OwnedRepr<bool>, Dim<[usize; 2]>>,
    ) -> Result<Vec<f32>, ModelError> {
        let model_inputs = inputs! {
            "x" => TensorRef::from_array_view(data_array.view())?,
            "pad_mask" => TensorRef::from_array_view(mask_array.view())?
        };

        // let first_dim_size = image_features.shape()[0];
        let outputs = self.model.run(model_inputs)?;
        let (_, scores) = outputs["logits"]
            .try_extract_tensor::<f32>()
            .map_err(|_| ModelError::ModelOutputToVecError)?;

        let vec_scores = scores.to_vec();
        let temp_array = Array::from_shape_vec((1, 5), vec_scores)?;
        let output = Self::softmax(temp_array);
        Ok(output.row(0).to_vec())
    }

    #[instrument(skip_all, err)]
    pub fn photometry_inputs(
        &self,
        photometry: Vec<PhotometryMag>,
    ) -> Result<
        (
            ArrayBase<OwnedRepr<f32>, Dim<[usize; 3]>>,
            ArrayBase<OwnedRepr<bool>, Dim<[usize; 2]>>,
        ),
        ModelError,
    > {
        let mut sorted_photometry = Vec::from(photometry);
        sorted_photometry.sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap());
        sorted_photometry.dedup_by(|a, b| a.time == b.time && a.band == b.band);
        let mut result = Vec::new();

        let mut bands: std::collections::HashMap<Band, Vec<PhotometryMag>> =
            std::collections::HashMap::new();
        for point in sorted_photometry {
            bands
                .entry(point.band.clone())
                .or_insert_with(Vec::new)
                .push(point);
        }
        // Process each band separately
        for (_band, mut points) in bands {
            // Sort by time
            points.sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap());
            let mut i = 0;
            while i < points.len() {
                let mut group = vec![&points[i]];
                let mut j = i + 1;

                // Collect all points within time_threshold of the first point in group
                while j < points.len() && (points[j].time - points[i].time) <= 0.5 {
                    group.push(&points[j]);
                    j += 1;
                }

                // Combine the group
                let combined = Self::combine_group(&group);
                result.push(combined);

                i = j;
            }
        }
        // Sort final result by time
        result.sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap());

        let initial_t = result[0].time;
        let mut dt = Vec::new();
        let mut delta_t = Vec::new();
        let mut temp = result[0].time;
        let mut log_fluxes = Vec::new();
        let mut sigma_fluxes = Vec::new();
        let mut tail1: Vec<f32> = Vec::new();
        let mut tail2: Vec<f32> = Vec::new();
        let mut tail3: Vec<f32> = Vec::new();

        for point in &result {
            let diff_delta_t = point.time - initial_t;
            let diff_dt = point.time - temp;
            temp = point.time;
            dt.push(diff_dt as f32);
            delta_t.push(diff_delta_t as f32);
            log_fluxes.push(point.mag.max(1e-6).log10() as f32);
            sigma_fluxes.push(point.mag_err / (point.mag * std::f32::consts::LN_10) as f32);
            match point.band {
                Band::G => {
                    tail1.push(1.0);
                    tail2.push(0.0);
                    tail3.push(0.0);
                }
                Band::R => {
                    tail1.push(0.0);
                    tail2.push(1.0);
                    tail3.push(0.0);
                }
                _ => {
                    tail1.push(0.0);
                    tail2.push(0.0);
                    tail3.push(1.0);
                }
            }
        }
        // normalizing constants
        let mean = [3.2246544, 0.7540643, 1.8746203, 0.05986896];
        let std = [1.1197147, 0.7268315, 0.415058, 0.03053633];

        let norm_delta_t: Vec<f32> = delta_t
            .iter()
            .map(|&dt| (dt - mean[0]) / (std[0] + 1e-8))
            .collect();

        let norm_dt: Vec<f32> = dt
            .iter()
            .map(|&t| (t - mean[1]) / (std[1] + 1e-8))
            .collect();

        let norm_log_fluxes: Vec<f32> = log_fluxes
            .iter()
            .map(|&lf| (lf - mean[2]) / (std[2] + 1e-8))
            .collect();

        let norm_sigma_fluxes: Vec<f32> = sigma_fluxes
            .iter()
            .map(|&sf| (sf - mean[3]) / (std[3] + 1e-8))
            .collect();

        let n = delta_t.len();
        let mut no_pad_input: Vec<Vec<f32>> = Vec::new();

        for i in 0..n {
            no_pad_input.push(vec![
                norm_delta_t[i],
                norm_dt[i],
                norm_log_fluxes[i],
                norm_sigma_fluxes[i],
                tail1[i],
                tail2[i],
                tail3[i],
            ]);
        }

        let (final_input, mask) = Self::pad_or_truncate(&no_pad_input, 257);
        let rows = final_input.len();
        let cols = if rows > 0 { final_input[0].len() } else { 0 };

        // Flatten the 2D vec into 1D
        let flat_data: Vec<f32> = final_input.into_iter().flatten().collect();

        // Create Array3 with shape [1, rows, cols]
        let data_array: ndarray::ArrayBase<ndarray::OwnedRepr<f32>, ndarray::Dim<[usize; 3]>> =
            Array3::from_shape_vec((1, rows, cols), flat_data)
                .expect("Shape mismatch when creating Array3");

        // // Convert Vec<bool> to Array2 with shape [1, 257]
        // // First convert bool to f32 or i32 if needed
        // let mask_flat: Vec<i32> = mask.iter().map(|&b| if b { 1 } else { 0 }).collect();
        let mask_array: ndarray::ArrayBase<ndarray::OwnedRepr<bool>, ndarray::Dim<[usize; 2]>> =
            Array2::from_shape_vec((1, mask.len()), mask).unwrap();

        Ok((data_array, mask_array))
    }

    fn combine_group(group: &[&PhotometryMag]) -> PhotometryMag {
        if group.len() == 1 {
            return (*group[0]).clone();
        }

        let eps = 1e-8;
        let mut weighted_time_sum = 0.0;
        let mut weighted_mag_sum = 0.0;
        let mut weighted_err_sum = 0.0;
        let mut weight_sum = 0.0;

        for point in group {
            let weight = 1.0 / (point.mag_err + eps);

            weighted_time_sum += point.time * weight as f64;
            weighted_mag_sum += point.mag * weight;
            weighted_err_sum += point.mag_err * weight;
            weight_sum += weight;
        }

        let combined_time = weighted_time_sum / weight_sum as f64;
        let combined_mag = weighted_mag_sum / weight_sum;
        let combined_err = weighted_err_sum / weight_sum;

        PhotometryMag {
            time: combined_time,
            mag: combined_mag,
            mag_err: combined_err,
            band: group[0].band.clone(),
        }
    }

    fn pad_or_truncate(arr: &Vec<Vec<f32>>, max_len: usize) -> (Vec<Vec<f32>>, Vec<bool>) {
        let n = arr.len();
        let d = if n > 0 { arr[0].len() } else { 0 };

        if n >= max_len {
            // Truncate to max_len
            let data: Vec<Vec<f32>> = arr[0..max_len]
                .iter()
                .map(|row| row.iter().map(|&x| x as f32).collect())
                .collect();
            let mask = vec![false; max_len];
            (data, mask)
        } else {
            // Pad with zeros
            let mut data: Vec<Vec<f32>> = arr
                .iter()
                .map(|row| row.iter().map(|&x| x as f32).collect())
                .collect();

            // Add padding rows
            for _ in 0..(max_len - n) {
                data.push(vec![0.0; d]);
            }

            // Create mask: false for real data, true for padding
            let mut mask = vec![false; n];
            mask.extend(vec![true; max_len - n]);

            (data, mask)
        }
    }
}
