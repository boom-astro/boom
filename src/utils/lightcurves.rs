use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use utoipa::ToSchema;

pub const ZP_AB: f32 = 8.90; // Zero point for AB magnitude
pub const SNT: f32 = 3.0; // Signal-to-noise threshold for detection
const FACTOR: f32 = 1.0857362047581294; // where 1.0857362047581294 = 2.5 / np.log(10)

// now survey-specific values:
pub const ZTF_ZP: f32 = 23.9;
pub const LSST_ZP_AB_NJY: f32 = ZP_AB + 22.5; // ZP + nJy to Jy conversion factor, as 2.5 * log10(1e9) = 22.5

pub fn flux2mag(flux: f32, flux_err: f32, zp: f32) -> (f32, f32) {
    let mag = -2.5 * (flux).log10() + zp;
    let sigma = (2.5 / 10.0_f32.ln()) * (flux_err / flux);

    (mag, sigma)
}

pub fn fluxerr2diffmaglim(flux_err: f32, zp: f32) -> f32 {
    -2.5 * (5.0 * flux_err).log10() + zp
}

pub fn mag2flux(mag: f32, mag_err: f32, zp: f32) -> (f32, f32) {
    let flux = 10.0_f32.powf(-0.4 * (mag - zp));
    let fluxerr = mag_err / FACTOR * flux;
    (flux, fluxerr)
}

pub fn diffmaglim2fluxerr(diffmaglim: f32, zp: f32) -> f32 {
    10.0_f32.powf((diffmaglim - zp) / -2.5) / 5.0
}

#[apache_avro_macros::serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Eq, Hash, ToSchema)]
pub enum Band {
    #[serde(rename = "g")]
    G,
    #[serde(rename = "r")]
    R,
    #[serde(rename = "i")]
    I,
    #[serde(rename = "z")]
    Z,
    #[serde(rename = "y")]
    Y,
    #[serde(rename = "u")]
    U,
}

impl std::fmt::Display for Band {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Band::G => write!(f, "g"),
            Band::R => write!(f, "r"),
            Band::I => write!(f, "i"),
            Band::Z => write!(f, "z"),
            Band::Y => write!(f, "y"),
            Band::U => write!(f, "u"),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema)]
pub struct PhotometryMag {
    #[serde(alias = "jd")]
    pub time: f64,
    #[serde(alias = "magpsf")]
    pub mag: f32,
    #[serde(alias = "sigmapsf")]
    pub mag_err: f32,
    pub band: Band,
}

/// Convert a `PhotometryMag` lightcurve to parallel arrays for the `lightcurve-fitting` crate.
pub fn photometry_to_lc_arrays(
    lc: &[PhotometryMag],
) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<String>) {
    let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
    let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
    let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
    let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();
    (times, mags, mag_errs, bands)
}

/// Run GPU-accelerated batch lightcurve fitting for a batch of sources.
///
/// Acquires a GPU device from the pool, runs batch 1D GP (nonparametric),
/// batch PSO + SVI (parametric), and CPU thermal fitting.
/// Returns one `Option<LightcurveFittingResult>` per source.
#[cfg(feature = "cuda")]
pub async fn run_fitting_gpu_batch(
    lightcurves: &[Vec<PhotometryMag>],
    do_np: bool,
    do_param: bool,
    gpu_pool: std::sync::Arc<crate::gpu::GpuPool>,
) -> Vec<Option<lightcurve_fitting::LightcurveFittingResult>> {
    use lightcurve_fitting::gpu::{BatchSource, GpuBatchData};
    use std::time::Duration;
    use tracing::warn;

    let n_sources = lightcurves.len();

    // Pre-build per-source band data on the calling thread (cheap)
    let mut all_mag_bands: Vec<std::collections::HashMap<String, lightcurve_fitting::BandData>> =
        Vec::with_capacity(n_sources);
    let mut all_flux_bands: Vec<std::collections::HashMap<String, lightcurve_fitting::BandData>> =
        Vec::with_capacity(n_sources);

    for lc in lightcurves {
        let (times, mags, mag_errs, bands) = photometry_to_lc_arrays(lc);
        let mag_bands = lightcurve_fitting::build_mag_bands(&times, &mags, &mag_errs, &bands);
        let flux_bands = if do_param {
            lightcurve_fitting::build_flux_bands(&times, &mags, &mag_errs, &bands)
        } else {
            std::collections::HashMap::new()
        };
        all_mag_bands.push(mag_bands);
        all_flux_bands.push(flux_bands);
    }

    let mag_bands_clone = all_mag_bands;
    let flux_bands_clone = all_flux_bands;

    let result = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::task::spawn_blocking(move || {
            let device = gpu_pool.acquire();

            // ---- Step 1: GPU nonparametric batch (1D GP) ----
            let np_results = if do_np {
                lightcurve_fitting::fit_nonparametric_batch_gpu(
                    &device.context.lc_gpu,
                    &mag_bands_clone,
                )
            } else {
                vec![(vec![], std::collections::HashMap::new()); n_sources]
            };

            // Thermal fitting uses trained GPs (CPU, fast)
            let thermals: Vec<Option<lightcurve_fitting::ThermalResult>> = if do_np {
                np_results
                    .iter()
                    .zip(mag_bands_clone.iter())
                    .map(|((_, gps), mb)| lightcurve_fitting::fit_thermal(mb, Some(gps)))
                    .collect()
            } else {
                vec![None; n_sources]
            };

            // ---- Step 2: GPU parametric (model select PSO + SVI) ----
            let parametrics: Vec<Vec<lightcurve_fitting::ParametricBandResult>> = if do_param {
                let mut batch_sources: Vec<BatchSource> = Vec::new();
                let mut band_map: Vec<(usize, String)> = Vec::new();

                for (si, flux_bands) in flux_bands_clone.iter().enumerate() {
                    let mut sorted: Vec<(&String, &lightcurve_fitting::BandData)> =
                        flux_bands.iter().collect();
                    sorted.sort_by(|a, b| b.1.times.len().cmp(&a.1.times.len()));

                    for (band_name, bd) in &sorted {
                        if bd.values.is_empty() {
                            continue;
                        }
                        let peak_flux = bd.values.iter().cloned().fold(f64::MIN, f64::max);
                        if peak_flux <= 0.0 {
                            continue;
                        }
                        let snr_threshold = 3.0;
                        let norm_flux: Vec<f64> = bd.values.iter().map(|f| f / peak_flux).collect();
                        let norm_err: Vec<f64> = bd.errors.iter().map(|e| e / peak_flux).collect();
                        let obs_var: Vec<f64> = norm_err.iter().map(|e| e * e + 1e-10).collect();
                        let is_upper: Vec<bool> = bd
                            .values
                            .iter()
                            .zip(bd.errors.iter())
                            .map(|(f, e)| *e > 0.0 && (*f / *e) < snr_threshold)
                            .collect();
                        let upper_flux: Vec<f64> = bd
                            .errors
                            .iter()
                            .map(|e| snr_threshold * e / peak_flux)
                            .collect();

                        batch_sources.push(BatchSource {
                            times: bd.times.clone(),
                            flux: norm_flux,
                            obs_var,
                            is_upper,
                            upper_flux,
                        });
                        band_map.push((si, (*band_name).clone()));
                    }
                }

                if batch_sources.is_empty() {
                    vec![vec![]; n_sources]
                } else {
                    let gpu_data = match GpuBatchData::new(&batch_sources) {
                        Ok(d) => d,
                        Err(e) => {
                            warn!("GPU batch data upload failed: {e}");
                            return (np_results, thermals, vec![vec![]; n_sources]);
                        }
                    };
                    let pso_results = device
                        .context
                        .lc_gpu
                        .batch_model_select(&gpu_data, 30, 60, 12, 0.1)
                        .unwrap_or_default();

                    // Build SVI inputs from PSO results
                    let mut svi_inputs: Vec<lightcurve_fitting::SviBatchInput> = Vec::new();
                    let mut svi_obs_sources: Vec<BatchSource> = Vec::new();
                    let mut svi_map: Vec<(usize, usize)> = Vec::new();
                    let mut per_source_pso: Vec<Vec<lightcurve_fitting::GpuPsoBandResult>> =
                        vec![Vec::new(); n_sources];
                    let mut per_source_band_names: Vec<Vec<String>> = vec![Vec::new(); n_sources];

                    for (bi, ((model, pso_result), (si, band_name))) in
                        pso_results.iter().zip(band_map.iter()).enumerate()
                    {
                        let svi_name = model.to_svi_name();
                        let gpu_res_idx = per_source_pso[*si].len();

                        per_source_pso[*si].push(lightcurve_fitting::GpuPsoBandResult {
                            model: svi_name.clone(),
                            pso_params: pso_result.params.clone(),
                            pso_cost: pso_result.cost,
                            per_model_chi2: std::collections::HashMap::new(),
                            per_model_params: std::collections::HashMap::new(),
                            multi_bazin: None,
                        });
                        per_source_band_names[*si].push(band_name.clone());

                        if !pso_result.params.is_empty() {
                            let (model_id, _np, se_idx) =
                                lightcurve_fitting::svi_model_meta(&svi_name);
                            let (centers, widths) = lightcurve_fitting::svi_prior_for_model(
                                &svi_name,
                                &pso_result.params,
                            );
                            svi_inputs.push(lightcurve_fitting::SviBatchInput {
                                model_id,
                                pso_params: pso_result.params.clone(),
                                se_idx,
                                prior_centers: centers,
                                prior_widths: widths,
                            });
                            svi_obs_sources.push(batch_sources[bi].clone());
                            svi_map.push((*si, gpu_res_idx));
                        }
                    }

                    // GPU SVI
                    let svi_outputs = if !svi_inputs.is_empty() {
                        match GpuBatchData::new(&svi_obs_sources) {
                            Ok(svi_data) => device
                                .context
                                .lc_gpu
                                .batch_svi_fit(&svi_data, &svi_inputs, 150, 2, 0.01)
                                .unwrap_or_default(),
                            Err(_) => Vec::new(),
                        }
                    } else {
                        Vec::new()
                    };

                    // Distribute SVI outputs back to per-source arrays
                    let mut per_source_svi: Vec<Vec<(Vec<f64>, Vec<f64>, f64)>> =
                        vec![Vec::new(); n_sources];
                    for si in 0..n_sources {
                        per_source_svi[si] =
                            vec![(Vec::new(), Vec::new(), f64::NAN); per_source_pso[si].len()];
                    }
                    for (idx, out) in svi_outputs.iter().enumerate() {
                        if idx < svi_map.len() {
                            let (si, res_idx) = svi_map[idx];
                            per_source_svi[si][res_idx] =
                                (out.mu.clone(), out.log_sigma.clone(), out.elbo);
                        }
                    }

                    // Finalize on CPU (t0 refine + mag chi2)
                    (0..n_sources)
                        .map(|si| {
                            if per_source_pso[si].is_empty() {
                                return vec![];
                            }
                            lightcurve_fitting::finalize_parametric_with_gpu_svi(
                                &flux_bands_clone[si],
                                &per_source_pso[si],
                                &per_source_svi[si],
                            )
                        })
                        .collect()
                }
            } else {
                vec![vec![]; n_sources]
            };

            (np_results, thermals, parametrics)
        }),
    )
    .await;

    match result {
        Ok(Ok((np_results, thermals, parametrics))) => np_results
            .into_iter()
            .zip(thermals)
            .zip(parametrics)
            .map(|(((nonparametric, _trained_gps), thermal), parametric)| {
                Some(lightcurve_fitting::LightcurveFittingResult {
                    nonparametric,
                    parametric,
                    thermal,
                })
            })
            .collect(),
        Ok(Err(e)) => {
            warn!("GPU batch LC fitting panicked: {e}");
            vec![None; n_sources]
        }
        Err(_) => {
            warn!("GPU batch LC fitting timed out");
            vec![None; n_sources]
        }
    }
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, AvroSchema, ToSchema)]
pub struct BandRateProperties {
    pub rate: f32,
    pub rate_error: f32,
    pub red_chi2: f32,
    pub nb_data: i32,
    pub dt: f32,
}

// TODO: avro serialization fail when we use skip_serializing_none,
// since the optional fields are not just None but simply missing
// (this needs to be fixed in the apache_avro-related crates)
// #[serde_as]
// #[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, AvroSchema, ToSchema)]
pub struct BandProperties {
    pub peak_jd: f64,
    pub peak_mag: f32,
    pub peak_mag_err: f32,
    pub dt: f32,
    pub rising: Option<BandRateProperties>,
    pub fading: Option<BandRateProperties>,
}

// TODO: avro serialization fail when we use skip_serializing_none,
// since the optional fields are not just None but simply missing
// (this needs to be fixed in the apache_avro-related crates)
// #[serde_as]
// #[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, ToSchema)]
pub struct PerBandProperties {
    pub g: Option<BandProperties>,
    pub r: Option<BandProperties>,
    pub i: Option<BandProperties>,
    pub z: Option<BandProperties>,
    pub y: Option<BandProperties>,
    pub u: Option<BandProperties>,
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, AvroSchema)]
pub struct AllBandsProperties {
    pub peak_jd: f64,
    pub peak_mag: f32,
    pub peak_mag_err: f32,
    pub peak_band: Band,
    pub faintest_jd: f64,
    pub faintest_mag: f32,
    pub faintest_mag_err: f32,
    pub faintest_band: Band,
    pub first_jd: f64,
    pub last_jd: f64,
}

/// Performs weighted least squares fit for y = a*x + b (centered for numerical stability)
/// Returns None if the fit cannot be performed (e.g., not enough data points)
/// or if the matrix is singular
fn weighted_least_squares_centered(
    x: &[f32],
    y: &[f32],
    sigma: &[f32],
) -> Option<BandRateProperties> {
    let n = x.len();

    if n < 2 || y.len() != n || sigma.len() != n {
        return None;
    }

    let mut sum_w = 0.0;
    let mut sum_wx = 0.0;
    let mut sum_wy = 0.0;

    for i in 0..n {
        if sigma[i] <= 0.0 || !sigma[i].is_finite() {
            return None;
        }
        let w = 1.0 / (sigma[i] * sigma[i]);
        if !w.is_finite() {
            return None;
        }

        sum_w += w;
        sum_wx += w * x[i];
        sum_wy += w * y[i];
    }

    let x_mean = sum_wx / sum_w;
    let y_mean = sum_wy / sum_w;

    let mut sxx = 0.0;
    let mut sxy = 0.0;

    for i in 0..n {
        let w = 1.0 / (sigma[i] * sigma[i]);
        let dx = x[i] - x_mean;
        let dy = y[i] - y_mean;

        sxx += w * dx * dx;
        sxy += w * dx * dy;
    }

    if sxx.abs() < 1e-10 {
        return None;
    }

    let a = sxy / sxx;
    let b = y_mean - a * x_mean;
    let a_err = (1.0 / sxx).sqrt();

    let mut chi2 = 0.0;
    for i in 0..n {
        let residual = y[i] - (a * x[i] + b);
        chi2 += (residual / sigma[i]).powi(2);
    }

    let reduced_chi2 = if n > 2 {
        chi2 / (n - 2) as f32
    } else {
        f32::NAN
    };

    Some(BandRateProperties {
        rate: a,
        rate_error: a_err,
        red_chi2: reduced_chi2,
        nb_data: n as i32,
        dt: x[n - 1] - x[0],
    })
}

/// Prepares photometry data by sorting and removing duplicates
pub fn prepare_photometry(photometry: &mut Vec<PhotometryMag>) {
    // sort by time
    photometry.sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap());

    // remove duplicates (same time and band)
    photometry.dedup_by(|a, b| a.time == b.time && a.band == b.band);
}

// we want a function that takes a Vec of PhotometryMag and:
// - sort by time (ascending)
// - divide it by band
// - identifies the index of the peak (minimum magnitude) for each band
// - for each band, do a linear fit of the data before the peak and after the peak independently
// - return a vec of PhotometryProperties
pub fn analyze_photometry(
    sorted_photometry: &[PhotometryMag],
) -> (PerBandProperties, AllBandsProperties, bool) {
    let stationary = sorted_photometry.len() > 0
        && (sorted_photometry.last().unwrap().time - sorted_photometry[0].time) > 0.01;

    let mut global_peak_jd = sorted_photometry[0].time;
    let mut global_peak_mag = sorted_photometry[0].mag;
    let mut global_peak_mag_err = sorted_photometry[0].mag_err;
    let mut global_peak_band = sorted_photometry[0].band.clone();
    let mut global_faintest_jd = sorted_photometry[0].time;
    let mut global_faintest_mag = sorted_photometry[0].mag;
    let mut global_faintest_mag_err = sorted_photometry[0].mag_err;
    let mut global_faintest_band = sorted_photometry[0].band.clone();
    let first_jd = sorted_photometry[0].time;
    let last_jd = sorted_photometry.last().unwrap().time;

    // group by band
    let mut bands: std::collections::HashMap<Band, Vec<&PhotometryMag>> =
        std::collections::HashMap::new();
    for mag in sorted_photometry {
        bands
            .entry(mag.band.clone())
            .or_insert_with(Vec::new)
            .push(mag);
    }

    // let mut results = HashMap::new();
    let mut results: PerBandProperties = PerBandProperties {
        g: None,
        r: None,
        i: None,
        z: None,
        y: None,
        u: None,
    };
    for (band, mags) in bands {
        if mags.is_empty() {
            continue;
        }
        // find the peak index (minimum magnitude) and faintest index (maximum magnitude)
        let (peak_index, faintest_index) =
            mags.iter()
                .enumerate()
                .fold((0, 0), |(peak, faintest), (i, mag)| {
                    if mag.mag < mags[peak].mag {
                        (i, faintest)
                    } else if mag.mag > mags[faintest].mag {
                        (peak, i)
                    } else {
                        (peak, faintest)
                    }
                });

        let peak_jd = mags[peak_index].time;
        let peak_mag = mags[peak_index].mag;
        let peak_mag_err = mags[peak_index].mag_err;

        if peak_mag < global_peak_mag {
            global_peak_jd = peak_jd;
            global_peak_mag = peak_mag;
            global_peak_mag_err = peak_mag_err;
            global_peak_band = band.clone();
        }

        let faintest_jd = mags[faintest_index].time;
        let faintest_mag = mags[faintest_index].mag;
        let faintest_mag_err = mags[faintest_index].mag_err;

        if faintest_mag > global_faintest_mag {
            global_faintest_jd = faintest_jd;
            global_faintest_mag = faintest_mag;
            global_faintest_mag_err = faintest_mag_err;
            global_faintest_band = band.clone();
        }

        let mut rising_properties = None;
        let mut fading_properties = None;

        // before is from 0 to peak_index inclusive, after is from peak_index inclusive to the end
        let before = &mags[0..=peak_index];
        let after = &mags[peak_index..];

        // if there isn't enough time separation between the first and last point, we cannot do a linear fit
        // so we will just return empty arrays
        if before.len() > 1 && before.last().unwrap().time - before.first().unwrap().time > 0.01 {
            let mut before_time = Vec::new();
            let mut before_mag = Vec::new();
            let mut before_mag_err = Vec::new();
            let mut min_mag = f32::INFINITY;
            let mut min_mag_err = f32::INFINITY;
            let mut max_mag = f32::NEG_INFINITY;
            let mut max_mag_err = f32::NEG_INFINITY;

            // get the min time for the before and after segments
            let before_min_time = before[0].time;

            for m in before {
                before_time.push((m.time - before_min_time) as f32);
                before_mag.push(m.mag);
                before_mag_err.push(m.mag_err);
                if m.mag < min_mag {
                    min_mag = m.mag;
                    min_mag_err = m.mag_err;
                }
                if m.mag > max_mag {
                    max_mag = m.mag;
                    max_mag_err = m.mag_err;
                }
            }
            // if min_mag + min_mag_err >= max_mag - max_mag_err, then we cannot do a linear fit
            // as the data is just within the noise
            // so when that happens, empty the before arrays
            if min_mag + min_mag_err >= max_mag - max_mag_err {
                before_time.clear();
                before_mag.clear();
                before_mag_err.clear();
            }

            rising_properties =
                weighted_least_squares_centered(&before_time, &before_mag, &before_mag_err);
        }

        // same for after
        if after.len() > 1 && after.last().unwrap().time - after.first().unwrap().time > 0.01 {
            let mut after_time = Vec::new();
            let mut after_mag = Vec::new();
            let mut after_mag_err = Vec::new();
            let mut min_mag = f32::INFINITY;
            let mut min_mag_err = f32::INFINITY;
            let mut max_mag = f32::NEG_INFINITY;
            let mut max_mag_err = f32::NEG_INFINITY;

            let after_min_time = after[0].time;

            for m in after {
                after_time.push((m.time - after_min_time) as f32);
                after_mag.push(m.mag);
                after_mag_err.push(m.mag_err);
                if m.mag < min_mag {
                    min_mag = m.mag;
                    min_mag_err = m.mag_err;
                }
                if m.mag > max_mag {
                    max_mag = m.mag;
                    max_mag_err = m.mag_err;
                }
            }
            // if min_mag + min_mag_err >= max_mag - max_mag_err, then we cannot do a linear fit
            // as the data is just within the noise
            // so when that happens, empty the after arrays
            if min_mag + min_mag_err >= max_mag - max_mag_err {
                after_time.clear();
                after_mag.clear();
                after_mag_err.clear();
            }

            fading_properties =
                weighted_least_squares_centered(&after_time, &after_mag, &after_mag_err);
        }

        let dt = (mags.last().unwrap().time - mags.first().unwrap().time) as f32;
        let band_properties = BandProperties {
            peak_jd,
            peak_mag,
            peak_mag_err,
            dt,
            rising: rising_properties,
            fading: fading_properties,
        };
        match band {
            Band::G => results.g = Some(band_properties),
            Band::R => results.r = Some(band_properties),
            Band::I => results.i = Some(band_properties),
            Band::Z => results.z = Some(band_properties),
            Band::Y => results.y = Some(band_properties),
            Band::U => results.u = Some(band_properties),
        }
    }

    let all_bands_properties = AllBandsProperties {
        peak_jd: global_peak_jd,
        peak_mag: global_peak_mag,
        peak_mag_err: global_peak_mag_err,
        peak_band: global_peak_band,
        faintest_jd: global_faintest_jd,
        faintest_mag: global_faintest_mag,
        faintest_mag_err: global_faintest_mag_err,
        faintest_band: global_faintest_band,
        first_jd,
        last_jd,
    };

    (results, all_bands_properties, stationary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flux2mag() {
        let flux = 200.0;
        let flux_err = 20.0;
        let zp = 23.9;
        let (mag, mag_err) = flux2mag(flux, flux_err, zp);
        assert!((mag - 18.147425).abs() < 1e-5);
        assert!((mag_err - 0.108574).abs() < 1e-5);

        // let's change the zp to make sure that it is being used correctly
        let zp = 25.0;
        let (mag, mag_err) = flux2mag(flux, flux_err, zp);
        assert!((mag - 19.247425).abs() < 1e-5);
        assert!((mag_err - 0.108574).abs() < 1e-5);

        // test with flux = 0, should return inf
        let flux = 0.0;
        let (mag, mag_err) = flux2mag(flux, flux_err, zp);
        assert!(mag.is_infinite());
        assert!(mag_err.is_infinite());
    }

    #[test]
    fn test_fluxerr2diffmaglim() {
        let zp = 23.9;
        let flux_err = 20.0;
        let diffmaglim = fluxerr2diffmaglim(flux_err, zp);
        assert!((diffmaglim - 18.9).abs() < 1e-5);
    }

    #[test]
    fn test_weighted_least_squares_centered() {
        // Test case 1: simple linear data (approximately y = 2x) with small errors
        // => expecting a good fit
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![2.0, 4.1, 6.0, 8.1, 10.2];
        let sigma = vec![0.1, 0.1, 0.1, 0.1, 0.1];

        let result = weighted_least_squares_centered(&x, &y, &sigma).unwrap();
        assert!((result.rate - 2.04).abs() < 1e-2);
        assert!((result.rate_error - 0.031623).abs() < 1e-6);
        assert!((result.red_chi2 - 0.400001).abs() < 1e-6);
        assert_eq!(result.nb_data, 5);
        assert!((result.dt - 4.0).abs() < 1e-5);

        // Test case 2: insufficient data points
        // => should return None
        let x = vec![1.0];
        let y = vec![2.0];
        let sigma = vec![0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 3: singular matrix (all x values are the same)
        // => should return None
        let x = vec![1.0, 1.0, 1.0];
        let y = vec![2.0, 2.1, 1.9];
        let sigma = vec![0.1, 0.1, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 4: mismatched input lengths
        // => should return None
        let x = vec![1.0, 2.0];
        let y = vec![2.0, 4.0, 6.0];
        let sigma = vec![0.1, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 5: zero or negative sigma values
        // => should return None
        let x = vec![1.0, 2.0, 3.0];
        let y = vec![2.0, 4.0, 6.0];
        let sigma = vec![0.1, 0.0, -0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 6: non-finite sigma values
        let x = vec![1.0, 2.0, 3.0];
        let y = vec![2.0, 4.0, 6.0];
        let sigma = vec![0.1, f32::NAN, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 7: non-finite weights due to very small sigma
        let x = vec![1.0, 2.0, 3.0];
        let y = vec![2.0, 4.0, 6.0];
        let sigma = vec![1e-40, 1e-40, 1e-40];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 8: bad fit due to large scatter
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![2.0, 8.0, 1.0, 7.0, 3.0];
        let sigma = vec![0.1, 0.1, 0.1, 0.1, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma).unwrap();
        assert!((result.red_chi2 - 1290.0).abs() < 1e-1);

        // Test case 9: exactly two data points
        let x = vec![1.0, 2.0];
        let y = vec![2.0, 4.0];
        let sigma = vec![0.1, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma).unwrap();
        assert!((result.rate - 2.0).abs() < 1e-6);
        assert!((result.rate_error - 0.141421).abs() < 1e-6);
        assert!(result.red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(result.nb_data, 2);
        assert!((result.dt - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_prepare_photometry() {
        let mut photometry = vec![
            PhotometryMag {
                time: 2459001.5,
                mag: 19.5,
                mag_err: 0.1,
                band: Band::R,
            }, // later point that should be sorted down
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            }, // earlier point that should be sorted up
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            }, // duplicate that should be removed
        ];
        prepare_photometry(&mut photometry);
        assert_eq!(photometry.len(), 2);
        assert_eq!(photometry[0].time, 2459000.5);
        assert_eq!(photometry[1].time, 2459001.5);
    }

    #[test]
    fn test_analyze_photometry() {
        // Test case 1: only one data point
        let mut data = vec![PhotometryMag {
            time: 2459000.5,
            mag: 20.0,
            mag_err: 0.1,
            band: Band::R,
        }];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, false);
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        assert!((data[0].time - r_peak_jd).abs() < 1e-6);
        assert!((data[0].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[0].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 0.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_some(), false);
        assert_eq!(r_stats.fading.is_some(), false);

        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.peak_jd;
        let peak_mag = all_bands_props.peak_mag;
        let peak_mag_err = all_bands_props.peak_mag_err;
        let peak_band = all_bands_props.peak_band;
        assert!((data[0].time - peak_jd).abs() < 1e-6);
        assert!((data[0].mag - peak_mag).abs() < 1e-6);
        assert!((data[0].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[0].band, peak_band);

        // Test case 2: 2 data points in the same band, rising
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        assert!((data[1].time - r_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 1.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_some(), true);
        assert_eq!(r_stats.fading.is_none(), true);

        let rising_stats = r_stats.rising.clone().unwrap();
        let rising_rate = rising_stats.rate;
        let rising_red_chi2 = rising_stats.red_chi2;
        let rising_nb_data = rising_stats.nb_data;
        let rising_dt = rising_stats.dt;
        assert!((rising_rate + 1.0).abs() < 1e-6); // should be -1 mag/day
        assert!(rising_red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(rising_nb_data, 2);
        assert!((rising_dt - 1.0).abs() < 1e-6);

        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.peak_jd;
        let peak_mag = all_bands_props.peak_mag;
        let peak_mag_err = all_bands_props.peak_mag_err;
        let peak_band = all_bands_props.peak_band;
        assert!((data[1].time - peak_jd).abs() < 1e-6);
        assert!((data[1].mag - peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[1].band, peak_band);

        // Test case 3: 2 data points in the same band, fading
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        assert!((data[0].time - r_peak_jd).abs() < 1e-6);
        assert!((data[0].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[0].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 1.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_none(), true);
        assert_eq!(r_stats.fading.is_some(), true);
        let fading_stats = r_stats.fading.clone().unwrap();
        let fading_rate = fading_stats.rate;
        let red_chi2 = fading_stats.red_chi2;
        let fading_nb_data = fading_stats.nb_data;
        let fading_dt = fading_stats.dt;
        assert!((fading_rate - 1.0).abs() < 1e-6); // should be 1 mag/day
        assert!(red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(fading_nb_data, 2);
        assert!((fading_dt - 1.0).abs() < 1e-6);
        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.peak_jd;
        let peak_mag = all_bands_props.peak_mag;
        let peak_mag_err = all_bands_props.peak_mag_err;
        let peak_band = all_bands_props.peak_band;
        assert!((data[0].time - peak_jd).abs() < 1e-6);
        assert!((data[0].mag - peak_mag).abs() < 1e-6);
        assert!((data[0].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[0].band, peak_band);

        // Test case 4: 3 data points in the same band, rising then fading
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459002.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        assert!((data[1].time - r_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 2.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_some(), true);
        assert_eq!(r_stats.fading.is_some(), true);
        let rising_stats = r_stats.rising.clone().unwrap();
        let rising_rate = rising_stats.rate;
        let rising_red_chi2 = rising_stats.red_chi2;
        let rising_nb_data = rising_stats.nb_data;
        let rising_dt = rising_stats.dt;
        assert!((rising_rate + 1.0).abs() < 1e-6); // should be -1 mag/day
        assert!(rising_red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(rising_nb_data, 2);
        assert!((rising_dt - 1.0).abs() < 1e-6);

        let fading_stats = r_stats.fading.clone().unwrap();
        let fading_rate = fading_stats.rate;
        let fading_red_chi2 = fading_stats.red_chi2;
        let fading_nb_data = fading_stats.nb_data;
        let fading_dt = fading_stats.dt;
        assert!((fading_rate - 1.0).abs() < 1e-6); // should be 1 mag/day
        assert!(fading_red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(fading_nb_data, 2);
        assert!((fading_dt - 1.0).abs() < 1e-6);

        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.peak_jd;
        let peak_mag = all_bands_props.peak_mag;
        let peak_mag_err = all_bands_props.peak_mag_err;
        let peak_band = all_bands_props.peak_band;
        assert!((data[1].time - peak_jd).abs() < 1e-6);
        assert!((data[1].mag - peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[1].band, peak_band);

        // Test case 5: multiple bands
        // - rising and fading in r band (3 points)
        // - only rising in g band (2 points)
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459002.5,
                mag: 18.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::G,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 21.0,
                mag_err: 0.1,
                band: Band::G,
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        // r band
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        // the original array was sorted and deduplicated,
        // so the r-band peak is now at index 2 (not 1)
        assert!((data[4].time - r_peak_jd).abs() < 1e-6);
        assert!((data[4].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[4].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 2.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_some(), true);
        assert_eq!(r_stats.fading.is_none(), true);

        // check the rising stats in r band
        let rising_stats = r_stats.rising.clone().unwrap();
        let rising_rate = rising_stats.rate;
        let rising_red_chi2 = rising_stats.red_chi2;
        let rising_nb_data = rising_stats.nb_data;
        let rising_dt = rising_stats.dt;
        assert!((rising_rate + 1.0).abs() < 1e-6); // should be -1 mag/day
        assert!(rising_red_chi2.abs() < 1e-6); // perfect fit
        assert_eq!(rising_nb_data, 3);
        assert!((rising_dt - 2.0).abs() < 1e-6);

        // g band
        let g_stats = results.g.unwrap();
        let g_peak_jd = g_stats.peak_jd;
        let g_peak_mag = g_stats.peak_mag;
        let g_peak_mag_err = g_stats.peak_mag_err;
        let g_dt = g_stats.dt;
        // the original array was sorted and deduplicated,
        // so the g-band peak is now at index 3 (not 4)
        assert!((data[1].time - g_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - g_peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - g_peak_mag_err).abs() < 1e-6);
        assert!((g_dt - 1.0).abs() < 1e-6);
        assert_eq!(g_stats.rising.is_some(), false);
        assert_eq!(g_stats.fading.is_some(), true);
        // check the fading stats in g band
        let fading_stats = g_stats.fading.clone().unwrap();
        let fading_rate = fading_stats.rate;
        let fading_red_chi2 = fading_stats.red_chi2;
        let fading_nb_data = fading_stats.nb_data;
        let fading_dt = fading_stats.dt;
        assert!((fading_rate - 1.0).abs() < 1e-6); // should be 1 mag/day
        assert!(fading_red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(fading_nb_data, 2);
        assert!((fading_dt - 1.0).abs() < 1e-6);

        // the all band properties should match the peak in r band
        let peak_jd = all_bands_props.peak_jd;
        let peak_mag = all_bands_props.peak_mag;
        let peak_mag_err = all_bands_props.peak_mag_err;
        let peak_band = all_bands_props.peak_band;
        assert!((data[4].time - peak_jd).abs() < 1e-6);
        assert!((data[4].mag - peak_mag).abs() < 1e-6);
        assert!((data[4].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[4].band, peak_band);

        // Edge case 1: duplicated points (same time and band, different mag)
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
            // duplicate of the first point
            PhotometryMag {
                time: 2459000.5,
                mag: 20.5,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (results, _, stationary) = analyze_photometry(&data);
        // make sure that only 2 points were used (the duplicate should be removed)
        assert_eq!(stationary, true);
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        // the original array was sorted and deduplicated,
        // so the r-band peak is now at index 1 (not 2)
        assert!((data[1].time - r_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 1.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_some(), true);
        assert_eq!(r_stats.fading.is_none(), true);
        let rising_stats = r_stats.rising.clone().unwrap();
        let rising_nb_data = rising_stats.nb_data;
        let rising_dt = rising_stats.dt;
        assert_eq!(rising_nb_data, 2);
        assert!((rising_dt - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_photometry_to_lc_arrays() {
        let lc = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.5,
                mag_err: 0.15,
                band: Band::G,
            },
        ];
        let (times, mags, mag_errs, bands) = photometry_to_lc_arrays(&lc);
        assert_eq!(times, vec![2459000.5, 2459001.5]);
        assert_eq!(mags, vec![20.0, 19.5]);
        assert_eq!(mag_errs, vec![0.10000000149011612, 0.15000000596046448]); // f32→f64
        assert_eq!(bands, vec!["r", "g"]);
    }

    #[test]
    fn test_cpu_fitting_nonparametric() {
        // Create a synthetic transient lightcurve: rise then fade in g and r bands.
        // ~20 points spanning 60 days, peaking around day 15.
        let mut lc = Vec::new();
        let t0 = 2459000.0;
        for i in 0..30 {
            let t = t0 + (i as f64) * 2.0; // every 2 days
                                           // Simple Gaussian-ish shape in magnitude: brightest (lowest mag) at peak
            let phase = (t - t0 - 30.0) / 10.0;
            let mag_g = 20.0 - 2.0 * (-0.5 * phase * phase).exp();
            let mag_r = 19.5 - 1.8 * (-0.5 * phase * phase).exp();
            lc.push(PhotometryMag {
                time: t,
                mag: mag_g as f32,
                mag_err: 0.05,
                band: Band::G,
            });
            lc.push(PhotometryMag {
                time: t,
                mag: mag_r as f32,
                mag_err: 0.06,
                band: Band::R,
            });
        }

        let (times, mags, mag_errs, bands) = photometry_to_lc_arrays(&lc);
        assert_eq!(times.len(), 60);

        // Build band data and run nonparametric fitting
        let mag_bands = lightcurve_fitting::build_mag_bands(&times, &mags, &mag_errs, &bands);
        assert!(mag_bands.contains_key("g"));
        assert!(mag_bands.contains_key("r"));

        let (np_results, trained_gps) = lightcurve_fitting::fit_nonparametric(&mag_bands);

        // Should get results for both bands
        assert_eq!(np_results.len(), 2);
        assert!(trained_gps.contains_key("g"));
        assert!(trained_gps.contains_key("r"));

        // Each band should have a peak magnitude and some decay metrics
        for result in &np_results {
            assert!(
                result.band == "g" || result.band == "r",
                "unexpected band: {}",
                result.band
            );
            assert!(
                result.peak_mag.is_some(),
                "peak_mag missing for {}",
                result.band
            );
            assert!(
                result.n_obs == 30,
                "expected 30 obs for {}, got {}",
                result.band,
                result.n_obs
            );
        }

        // Thermal fitting with trained GPs
        let thermal = lightcurve_fitting::fit_thermal(&mag_bands, Some(&trained_gps));
        // Thermal may or may not produce a result depending on wavelength coverage,
        // but it should not panic
        let _ = thermal;
    }

    #[test]
    fn test_cpu_fitting_parametric() {
        // Same synthetic lightcurve as above
        let mut lc = Vec::new();
        let t0 = 2459000.0;
        for i in 0..30 {
            let t = t0 + (i as f64) * 2.0;
            let phase = (t - t0 - 30.0) / 10.0;
            let mag_g = 20.0 - 2.0 * (-0.5 * phase * phase).exp();
            lc.push(PhotometryMag {
                time: t,
                mag: mag_g as f32,
                mag_err: 0.05,
                band: Band::G,
            });
        }

        let (times, mags, mag_errs, bands) = photometry_to_lc_arrays(&lc);
        let flux_bands = lightcurve_fitting::build_flux_bands(&times, &mags, &mag_errs, &bands);
        assert!(flux_bands.contains_key("g"));

        let param_results = lightcurve_fitting::fit_parametric(
            &flux_bands,
            false,
            lightcurve_fitting::UncertaintyMethod::Svi,
        );

        // Should get at least one parametric result for g band
        assert!(
            !param_results.is_empty(),
            "parametric fitting returned no results"
        );
        assert_eq!(param_results[0].band, "g");
    }

    #[test]
    fn test_cpu_fitting_full_pipeline() {
        // Exercise the full pipeline as run_fitting_cpu would: nonparametric + parametric
        let mut lc = Vec::new();
        let t0 = 2459000.0;
        for i in 0..25 {
            let t = t0 + (i as f64) * 2.5;
            let phase = (t - t0 - 30.0) / 12.0;
            let mag_r = 19.0 - 1.5 * (-0.5 * phase * phase).exp();
            lc.push(PhotometryMag {
                time: t,
                mag: mag_r as f32,
                mag_err: 0.08,
                band: Band::R,
            });
        }

        let (times, mags, mag_errs, bands) = photometry_to_lc_arrays(&lc);
        let mag_bands = lightcurve_fitting::build_mag_bands(&times, &mags, &mag_errs, &bands);

        // Nonparametric
        let (nonparametric, trained_gps) = lightcurve_fitting::fit_nonparametric(&mag_bands);
        assert_eq!(nonparametric.len(), 1);
        assert_eq!(nonparametric[0].band, "r");

        // Thermal
        let thermal = lightcurve_fitting::fit_thermal(&mag_bands, Some(&trained_gps));

        // Parametric
        let flux_bands = lightcurve_fitting::build_flux_bands(&times, &mags, &mag_errs, &bands);
        let parametric = lightcurve_fitting::fit_parametric(
            &flux_bands,
            false,
            lightcurve_fitting::UncertaintyMethod::Svi,
        );

        // Assemble the result exactly as run_fitting_cpu does
        let result = lightcurve_fitting::LightcurveFittingResult {
            nonparametric,
            parametric,
            thermal,
        };

        // The result should be serializable to BSON (as mongify would do)
        let json = serde_json::to_value(&result).expect("should serialize to JSON");
        assert!(json.get("nonparametric").is_some());
        assert!(json.get("parametric").is_some());
        assert!(json.get("thermal").is_some());

        // nonparametric should have r band result
        let np = json["nonparametric"].as_array().unwrap();
        assert_eq!(np.len(), 1);
        assert_eq!(np[0]["band"], "r");
        assert!(np[0]["peak_mag"].as_f64().is_some());
    }

    /// Test the GPU batch fitting path end-to-end.
    /// Requires a GPU device, so only runs with `cuda` feature + `--ignored`.
    #[cfg(feature = "cuda")]
    #[tokio::test]
    #[ignore]
    async fn test_gpu_fitting_batch() {
        // Create GPU pool with device 0
        let gpu_pool = crate::gpu::GpuPool::new(&[0]).expect("GPU pool creation failed");

        // Generate a batch of synthetic lightcurves (10 sources, 2 bands each)
        let mut lightcurves: Vec<Vec<PhotometryMag>> = Vec::new();
        for src in 0..10 {
            let mut lc = Vec::new();
            let t0 = 2459000.0;
            // Vary the peak time per source for diversity
            let peak_day = 25.0 + (src as f64) * 3.0;
            for i in 0..30 {
                let t = t0 + (i as f64) * 2.0;
                let phase = (t - t0 - peak_day) / 10.0;
                let mag_g = 20.0 - (2.0 + 0.2 * src as f64) * (-0.5 * phase * phase).exp();
                let mag_r = 19.5 - (1.8 + 0.1 * src as f64) * (-0.5 * phase * phase).exp();
                lc.push(PhotometryMag {
                    time: t,
                    mag: mag_g as f32,
                    mag_err: 0.05,
                    band: Band::G,
                });
                lc.push(PhotometryMag {
                    time: t,
                    mag: mag_r as f32,
                    mag_err: 0.06,
                    band: Band::R,
                });
            }
            lightcurves.push(lc);
        }

        // Run GPU batch with both nonparametric and parametric
        let results = run_fitting_gpu_batch(&lightcurves, true, true, gpu_pool).await;

        assert_eq!(results.len(), 10, "should get one result per source");

        for (i, result) in results.iter().enumerate() {
            let result = result
                .as_ref()
                .unwrap_or_else(|| panic!("source {i} returned None"));

            // Nonparametric: should have results for g and r
            assert_eq!(
                result.nonparametric.len(),
                2,
                "source {i}: expected 2 nonparametric band results"
            );
            for np in &result.nonparametric {
                assert!(
                    np.band == "g" || np.band == "r",
                    "source {i}: unexpected band {}",
                    np.band
                );
                assert!(
                    np.peak_mag.is_some(),
                    "source {i}: {}: no peak_mag",
                    np.band
                );
            }

            // Parametric: should have results for at least one band
            assert!(
                !result.parametric.is_empty(),
                "source {i}: no parametric results"
            );

            // Verify serialization works
            let json = serde_json::to_value(result).expect("should serialize");
            assert!(json.get("nonparametric").is_some());
            assert!(json.get("parametric").is_some());
        }

        println!("GPU batch fitting: 10 sources × 2 bands — all passed");
    }
}
