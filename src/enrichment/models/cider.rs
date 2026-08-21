use crate::enrichment::models::{
    load_model, load_model_on_device_with_cpu_fallback, FusionModel, ModelError,
};
use crate::enrichment::ZtfAlertForEnrichment;
use crate::utils::cutouts::AlertCutout;
use crate::utils::fits::prepare_triplet;
use crate::utils::lightcurves::{AllBandsProperties, Band, PhotometryMag};
use ndarray::{Array, Array2, Array3, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use tracing::instrument;

// metadata missing value
const METADATA_SENTINEL: f32 = -999.0;

// This is mostly taken from BTSBot model since
// both algorithms are close enough
pub struct CiderFusionModel {
    model: Session,
}

impl CiderFusionModel {
    #[instrument(err)]
    pub fn new(path: &str) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model(path)?,
        })
    }

    pub fn new_on_device(path: &str, device_id: i32) -> Result<Self, ModelError> {
        Ok(Self {
            model: load_model_on_device_with_cpu_fallback(path, Some(device_id))?,
        })
    }
}

impl FusionModel for CiderFusionModel {
    #[instrument(skip_all, err)]
    fn predict(
        &mut self,
        tempo_x: &Array3<f32>,
        tempo_pad_mask: &Array2<bool>,
        tempo_global: &Array2<f32>,
        metadata: &Array<f32, Dim<[usize; 2]>>,
        image: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<(Vec<f32>, Vec<f32>), ModelError> {
        let outputs = self.model.run(inputs! {
            "tempo_x"        => TensorRef::from_array_view(tempo_x.view())?,
            "tempo_pad_mask" => TensorRef::from_array_view(tempo_pad_mask.view())?,
            "tempo_global"   => TensorRef::from_array_view(tempo_global.view())?,
            "metadata"       => TensorRef::from_array_view(metadata.view())?,
            "image"          => TensorRef::from_array_view(image.view())?,
        })?;

        let (_, probs) = outputs["probs"]
            .try_extract_tensor::<f32>()
            .map_err(|_| ModelError::ModelOutputToVecError)?;
        let (_, embedding) = outputs["fusion_embedding"]
            .try_extract_tensor::<f32>()
            .map_err(|_| ModelError::ModelOutputToVecError)?;
        Ok((probs.to_vec(), embedding.to_vec()))
    }
}

fn safe_slope(ts: &[f32], ys: &[f32]) -> f32 {
    if ts.len() < 2 {
        return 0.0;
    }
    let n = ts.len() as f32;
    let xm = ts.iter().sum::<f32>() / n;
    let ym = ys.iter().sum::<f32>() / n;
    let denom: f32 = ts.iter().map(|x| (x - xm).powi(2)).sum();
    if denom <= 1e-12 {
        return 0.0;
    }
    ts.iter()
        .zip(ys.iter())
        .map(|(x, y)| (x - xm) * (y - ym))
        .sum::<f32>()
        / denom
}

/// Compute the 24-dim physics global feature vector from the encoded sequence.
/// Mirrors `compute_global_features` in test.rs.
fn compute_global_features(
    log1p_dt: &[f32],
    log1p_dt_prev: &[f32],
    log_flux: &[f32],
    band_ids: &[usize],
) -> [f32; 24] {
    let n = log1p_dt.len();
    if n == 0 {
        return [0.0f32; 24];
    }

    let dt_first: Vec<f32> = log1p_dt.iter().map(|&v| v.exp_m1().max(0.0)).collect();
    let dt_prev: Vec<f32> = log1p_dt_prev.iter().map(|&v| v.exp_m1().max(0.0)).collect();

    let duration = dt_first.iter().cloned().fold(0.0_f32, f32::max);
    let n_obs = n as f32;
    let amp = log_flux.iter().cloned().fold(f32::NEG_INFINITY, f32::max)
        - log_flux.iter().cloned().fold(f32::INFINITY, f32::min);

    let mut counts = [0.0f32; 3];
    for &b in band_ids {
        if b < 3 {
            counts[b] += 1.0;
        }
    }

    let band_mean = |k: usize| -> f32 {
        let vals: Vec<f32> = log_flux
            .iter()
            .zip(band_ids.iter())
            .filter(|(_, &b)| b == k)
            .map(|(&f, _)| f)
            .collect();
        if vals.is_empty() {
            0.0
        } else {
            vals.iter().sum::<f32>() / vals.len() as f32
        }
    };
    let color_gr = band_mean(0) - band_mean(1);
    let color_ri = band_mean(1) - band_mean(2);
    let basic = [
        duration, n_obs, counts[0], counts[1], counts[2], amp, color_gr, color_ri,
    ];

    let idx_peak = log_flux
        .iter()
        .enumerate()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .map(|(i, _)| i)
        .unwrap_or(0);
    let peak_t = dt_first[idx_peak];
    let peak_frac_h = peak_t / duration.max(1e-6);
    let peak_flux = log_flux[idx_peak];

    let mut sorted_dt = dt_prev.clone();
    sorted_dt.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med_dt_prev = sorted_dt[sorted_dt.len() / 2];

    let mean_f: f32 = log_flux.iter().sum::<f32>() / n as f32;
    let std_flux = if n > 1 {
        (log_flux.iter().map(|f| (f - mean_f).powi(2)).sum::<f32>() / n as f32).sqrt()
    } else {
        0.0
    };

    let mut sorted_f = log_flux.to_vec();
    sorted_f.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p10 = sorted_f[(n as f32 * 0.10) as usize];
    let p90 = sorted_f[((n as f32 * 0.90) as usize).min(n - 1)];

    let peak_dt = dt_first[idx_peak];
    let rise_t: Vec<f32> = dt_first
        .iter()
        .zip(log_flux.iter())
        .filter(|(t, _)| **t <= peak_dt)
        .map(|(t, _)| *t)
        .collect();
    let rise_f: Vec<f32> = dt_first
        .iter()
        .zip(log_flux.iter())
        .filter(|(t, _)| **t <= peak_dt)
        .map(|(_, f)| *f)
        .collect();
    let fall_t: Vec<f32> = dt_first
        .iter()
        .zip(log_flux.iter())
        .filter(|(t, _)| **t >= peak_dt)
        .map(|(t, _)| *t)
        .collect();
    let fall_f: Vec<f32> = dt_first
        .iter()
        .zip(log_flux.iter())
        .filter(|(t, _)| **t >= peak_dt)
        .map(|(_, f)| *f)
        .collect();
    let rise_slope = safe_slope(&rise_t, &rise_f);
    let fall_slope = safe_slope(&fall_t, &fall_f);
    let rise_fall_ratio = rise_slope / fall_slope.abs().max(1e-6);
    let enhanced = [
        peak_frac_h,
        peak_flux,
        med_dt_prev,
        std_flux,
        p90 - p10,
        rise_slope,
        fall_slope,
        rise_fall_ratio,
    ];

    let n_safe = n_obs.max(1.0);
    let frac = [counts[0] / n_safe, counts[1] / n_safe, counts[2] / n_safe];
    let band_slope = |k: usize| -> f32 {
        let (ts, fs): (Vec<f32>, Vec<f32>) = dt_first
            .iter()
            .zip(log_flux.iter())
            .zip(band_ids.iter())
            .filter(|(_, &b)| b == k)
            .map(|((t, f), _)| (*t, *f))
            .unzip();
        safe_slope(&ts, &fs)
    };
    let (sg, sr, si) = (band_slope(0), band_slope(1), band_slope(2));
    let physics = [frac[0], frac[1], frac[2], sg, sr, si, sg - sr, sr - si];

    let mut out = [0.0f32; 24];
    out[..8].copy_from_slice(&basic);
    out[8..16].copy_from_slice(&enhanced);
    out[16..].copy_from_slice(&physics);
    out
}

impl CiderFusionModel {
    #[instrument(skip_all, err)]
    pub fn get_metadata(
        &self,
        alerts: &[&ZtfAlertForEnrichment],
        alert_properties: &[&AllBandsProperties],
    ) -> Result<Array<f32, Dim<[usize; 2]>>, ModelError> {
        // mean for the data
        const META_MEAN: [f32; 55] = [
            0.24254899,
            0.41347787,
            3.50880599,
            9.02469254,
            9.75798225,
            0.25894931,
            22.80080223,
            0.0,
            0.0,
            20.14443016,
            0.11146311,
            21.33818626,
            1112.40771484,
            0.10090445,
            3.71102977,
            18.64438820,
            1091.06958008,
            0.95796055,
            1.61305416,
            2.52790070,
            5.52801895,
            0.0,
            18.46061707,
            19.60049057,
            1.06290066,
            3.29015160,
            5.43312645,
            18.58586693,
            19.50944901,
            2.82571507,
            5.67974806,
            18.51563644,
            19.36137772,
            17.47620964,
            0.81434739,
            11.28922939,
            2.39916945,
            19.49070549,
            191.52610779,
            28.24017715,
            2.56472850,
            19.17591095,
            19.48890495,
            18.93409157,
            18.59346771,
            19.90670776,
            20.05836296,
            19.73174858,
            19.32674408,
            49.78094482,
            18.07178497,
            30.03812218,
            0.03397351,
            0.000030636,
            -0.000020390,
        ];
        // standard deviation for the data
        const META_STD: [f32; 55] = [
            0.27388790,
            0.33104500,
            3.76759219,
            6.11158562,
            8.12946415,
            0.26185125,
            17.62609673,
            1.0,
            1.0,
            0.53922176,
            1.49864447,
            32.75554276,
            1116.99694824,
            0.04500616,
            2.32353473,
            0.91892433,
            1116.43896484,
            0.08389983,
            0.57251018,
            6.40846777,
            4.88665485,
            1.0,
            0.95565951,
            0.98277485,
            0.04837016,
            7.15657187,
            4.51992846,
            0.96032465,
            0.99783283,
            6.50551176,
            5.03694773,
            0.94555891,
            0.96880257,
            27.34542084,
            0.16761307,
            102.93301392,
            2.45365310,
            1.72538114,
            99.01464081,
            24.47701263,
            0.89686346,
            1.61337090,
            1.49749708,
            1.64117110,
            1.53313792,
            1.66218638,
            1.62950861,
            1.64028490,
            1.55078745,
            75.86976624,
            27.72556686,
            60.27622223,
            0.09014013,
            0.00117264,
            0.00066503,
        ];

        let mut features_batch: Vec<f32> = Vec::with_capacity(alerts.len() * 55);

        for i in 0..alerts.len() {
            let c = &alerts[i].candidate.candidate;
            let p = alert_properties[i];

            let ndethist = c.ndethist as f32;
            let ncovhist = c.ncovhist as f32;
            let peak_mag = p.peak_mag;
            let faintest_mag = p.faintest_mag;

            let mut arr = [METADATA_SENTINEL; 55];

            // [0–5] host_environment
            arr[0] = c.sgscore1.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[1] = c.sgscore2.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[2] = c.distpsnr1.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[3] = c.distpsnr2.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[4] = c.nmtchps as f32;
            arr[5] = c.sharpnr.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [6] scorr
            arr[6] = c.scorr.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [7, 8] ra, dec
            arr[7] = c.ra as f32;
            arr[8] = c.dec as f32;
            // [9] diffmaglim
            arr[9] = c.diffmaglim.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [10] sky
            arr[10] = c.sky.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [11, 12] history counts
            arr[11] = ndethist;
            arr[12] = ncovhist;
            // [13] sigmapsf
            arr[13] = c.sigmapsf;
            // [14] chinr
            arr[14] = c.chinr.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [15] magpsf
            arr[15] = c.magpsf;
            // [16] nnondet
            arr[16] = ncovhist - ndethist;
            // [17] classtar
            arr[17] = c.classtar.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [18] fid
            arr[18] = c.fid as f32;
            // [19–24] temporal_shape
            arr[19] = (p.last_jd - p.peak_jd) as f32; // days_since_peak
            arr[20] = (p.peak_jd - p.first_jd) as f32; // days_to_peak
            arr[21] = (p.last_jd - p.first_jd) as f32; // age_sum_days
            arr[22] = peak_mag; // peakmag_so_far
            arr[23] = faintest_mag; // maxmag_so_far
            arr[24] = if peak_mag > 0.0 {
                faintest_mag / peak_mag
            } else {
                METADATA_SENTINEL
            }; // max_over_peak_mag
               // [25–28] per_filter_g
            arr[25] = p.days_since_peak_g.unwrap_or(METADATA_SENTINEL);
            arr[26] = p.days_to_peak_g.unwrap_or(METADATA_SENTINEL);
            arr[27] = p.peakmag_g.unwrap_or(METADATA_SENTINEL);
            arr[28] = p.maxmag_g.unwrap_or(METADATA_SENTINEL);
            // [29–32] per_filter_r
            arr[29] = p.days_since_peak_r.unwrap_or(METADATA_SENTINEL);
            arr[30] = p.days_to_peak_r.unwrap_or(METADATA_SENTINEL);
            arr[31] = p.peakmag_r.unwrap_or(METADATA_SENTINEL);
            arr[32] = p.maxmag_r.unwrap_or(METADATA_SENTINEL);
            // [33] alert_idx
            arr[33] = ndethist - 1.0;
            // [34] rb
            arr[34] = c.rb.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [35] chipsf
            arr[35] = c.chipsf.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [36, 37] distnr, magnr
            arr[36] = c.distnr.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[37] = c.magnr.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [38, 39] ranr, decnr
            arr[38] = c.ranr as f32;
            arr[39] = c.decnr as f32;
            // [40] fwhm
            arr[40] = c.fwhm.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [41–48] PS1 reference magnitudes
            arr[41] = c.srmag1.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[42] = c.sgmag1.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[43] = c.simag1.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[44] = c.szmag1.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[45] = c.srmag2.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[46] = c.sgmag2.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[47] = c.simag2.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[48] = c.szmag2.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            // [49–51] photometry counts
            arr[49] = p.n_photometry_total;
            arr[50] = p.n_photometry_g;
            arr[51] = p.n_photometry_r;
            // [52–54] color calibration
            arr[52] = c.clrcoeff.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[53] = c.clrcounc.map(|v| v as f32).unwrap_or(METADATA_SENTINEL);
            arr[54] = METADATA_SENTINEL; // zpclrcov not present in alert packet

            // z-score normalize; leave sentinel values unchanged
            for j in 0..55 {
                if arr[j] != METADATA_SENTINEL {
                    arr[j] = (arr[j] - META_MEAN[j]) / META_STD[j].max(1e-8);
                }
            }

            features_batch.extend_from_slice(&arr);
        }

        let features_array = Array::from_shape_vec((alerts.len(), 55), features_batch)?;
        Ok(features_array)
    }

    #[instrument(skip_all, err)]
    pub fn get_triplet(
        &self,
        alert_cutouts: &[&AlertCutout],
    ) -> Result<Array<f32, Dim<[usize; 4]>>, ModelError> {
        // let mut triplets = Array::zeros((alert_cutouts.len(), 3, 49, 49));
        let mut triplets = Array::zeros((alert_cutouts.len(), 3, 63, 63));
        for i in 0..alert_cutouts.len() {
            let (cutout_science, cutout_template, cutout_difference) =
                prepare_triplet(&alert_cutouts[i])?;
            for (j, cutout) in [cutout_science, cutout_template, cutout_difference]
                .iter()
                .enumerate()
            {
                let mut slice = triplets.slice_mut(ndarray::s![i, j, .., ..]);
                let full_array = Array::from_shape_vec((63, 63), cutout.to_vec())?;
                slice.assign(&full_array);
            }
        }
        Ok(triplets)
    }

    #[instrument(skip_all, err)]
    pub fn photometry_inputs(
        &self,
        mut photometry: Vec<PhotometryMag>,
    ) -> Result<
        (
            Array3<f32>,  // tempo_x       (1, 512, 5)
            Array2<bool>, // tempo_pad_mask (1, 512)
            Array2<f32>,  // tempo_global   (1, 24)
        ),
        ModelError,
    > {
        const ZTF_ZP: f32 = 23.9;
        const SEQ_LEN: usize = 512;
        const NORM_MEAN: [f32; 4] = [3.2246506, 0.75406283, 1.8746188, 0.05986891];
        const NORM_STD: [f32; 4] = [1.1197281, 0.72683305, 0.41507009, 0.03053664];

        // Sort, dedup, horizon-trim (100 days), cap at 256 events (keep most recent)
        photometry.sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap());
        photometry.dedup_by(|a, b| a.time == b.time && a.band == b.band);
        if !photometry.is_empty() {
            let t0 = photometry[0].time;
            photometry.retain(|p| (p.time - t0) as f32 <= 100.0);
        }
        if photometry.len() > 256 {
            photometry = photometry[photometry.len() - 256..].to_vec();
        }

        let n = photometry.len();
        let t0 = if n > 0 { photometry[0].time } else { 0.0 };

        let mut log1p_dt: Vec<f32> = Vec::with_capacity(n);
        let mut log1p_dt_prev: Vec<f32> = Vec::with_capacity(n);
        let mut log10_flux: Vec<f32> = Vec::with_capacity(n);
        let mut log10_flux_err: Vec<f32> = Vec::with_capacity(n);
        let mut band_ids: Vec<usize> = Vec::with_capacity(n);

        for (i, p) in photometry.iter().enumerate() {
            let dt = (p.time - t0) as f32;
            let dt_prev = if i == 0 {
                0.0
            } else {
                (p.time - photometry[i - 1].time) as f32
            };
            log1p_dt.push(dt.ln_1p());
            log1p_dt_prev.push(dt_prev.ln_1p());
            log10_flux.push((ZTF_ZP - p.mag) / 2.5);
            log10_flux_err.push(p.mag_err / 2.5);
            band_ids.push(match p.band {
                Band::G => 0,
                Band::R => 1,
                _ => 2,
            });
        }

        // 24-dim physics global features
        let global = compute_global_features(&log1p_dt, &log1p_dt_prev, &log10_flux, &band_ids);

        // tempo_x: normalize first 4 channels, append raw band_id as 5th, zero-pad to SEQ_LEN
        let n_real = n.min(SEQ_LEN);
        let mut tempo_flat: Vec<f32> = Vec::with_capacity(SEQ_LEN * 5);
        for i in 0..n_real {
            let raw = [
                log1p_dt[i],
                log1p_dt_prev[i],
                log10_flux[i],
                log10_flux_err[i],
            ];
            let norm: [f32; 4] =
                std::array::from_fn(|j| (raw[j] - NORM_MEAN[j]) / (NORM_STD[j] + 1e-8));
            tempo_flat.extend_from_slice(&[norm[0], norm[1], norm[2], norm[3], band_ids[i] as f32]);
        }
        tempo_flat.resize(SEQ_LEN * 5, 0.0);

        // tempo_pad_mask: false = real token, true = padding
        let mut pad_mask: Vec<bool> = vec![false; n_real];
        pad_mask.resize(SEQ_LEN, true);

        let tempo_x = Array3::from_shape_vec((1, SEQ_LEN, 5), tempo_flat)?;
        let pad_mask_arr = Array2::from_shape_vec((1, SEQ_LEN), pad_mask)?;
        let tempo_global = Array2::from_shape_vec((1, 24), global.to_vec())?;

        Ok((tempo_x, pad_mask_arr, tempo_global))
    }
}
