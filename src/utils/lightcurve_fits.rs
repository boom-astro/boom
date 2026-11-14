use crate::utils::lightcurves::PhotometryMag;
use lightcurve_fitting::{
    build_flux_bands, build_mag_bands, fit_parametric_model, fit_thermal, BandData,
    ParametricBandResult, SviModelName, UncertaintyMethod,
};
use rayon::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};

const DAYS_BEFORE_PEAK: f64 = 50.0;
const DAYS_AFTER_PEAK: f64 = 100.0;
const ARNETT_BANDS: [&str; 3] = ["g", "r", "i"];
const MIN_BAND_POINTS: usize = 5;

#[derive(Debug, Serialize)]
pub struct ArnettBandFit {
    pub t0: f64,
    pub t0_err: Option<f64>,
    pub tau_m: f64,
    pub tau_m_err: Option<f64>,
    pub ni_fraction: f64,
    pub ni_fraction_err: Option<f64>,
    pub mag_chi2: Option<f64>,
    pub n_points: i32,
}

#[derive(Debug, Default, Serialize)]
pub struct ArnettFit {
    pub g: Option<ArnettBandFit>,
    pub r: Option<ArnettBandFit>,
    pub i: Option<ArnettBandFit>,
}

#[derive(Debug, Default, Serialize)]
pub struct ThermalFit {
    pub log_temp_start: Option<f64>,
    pub log_temp_start_err: Option<f64>,
    pub log_temp_latest: Option<f64>,
    pub cooling_rate: Option<f64>,
    pub cooling_rate_err: Option<f64>,
    pub chi2: Option<f64>,
    pub n_color_obs: i32,
    pub n_bands_used: i32,
}

#[derive(Debug, Default)]
pub struct LightcurveFits {
    pub arnett: ArnettFit,
    pub thermal: ThermalFit,
}

fn finite(x: f64) -> Option<f64> {
    x.is_finite().then_some(x)
}

fn around_peak(lightcurve: &[PhotometryMag]) -> Vec<&PhotometryMag> {
    let Some(peak) = lightcurve
        .iter()
        .filter(|p| p.mag.is_finite())
        .min_by(|a, b| a.mag.total_cmp(&b.mag))
    else {
        return Vec::new();
    };
    lightcurve
        .iter()
        .filter(|p| (-DAYS_BEFORE_PEAK..=DAYS_AFTER_PEAK).contains(&(p.time - peak.time)))
        .collect()
}

fn arnett_band_fit(result: &ParametricBandResult, jd_min: f64) -> Option<ArnettBandFit> {
    let mu = &result.svi_mu;
    let sigma: Vec<f64> = result.svi_log_sigma.iter().map(|s| s.exp()).collect();
    if mu.len() < 4 || sigma.len() < 4 {
        return None;
    }
    let tau_m = finite(mu[2].exp())?;
    let ni_fraction = finite(1.0 / (1.0 + (-mu[3]).exp()))?;
    Some(ArnettBandFit {
        t0: finite(mu[1] + jd_min)?,
        t0_err: finite(sigma[1]),
        tau_m,
        tau_m_err: finite(tau_m * sigma[2]),
        ni_fraction,
        ni_fraction_err: finite(ni_fraction * (1.0 - ni_fraction) * sigma[3]),
        mag_chi2: result.mag_chi2.and_then(finite),
        n_points: result.n_obs as i32,
    })
}

fn fit_arnett(mut bands: HashMap<String, BandData>, jd_min: f64) -> ArnettFit {
    bands.retain(|name, data| {
        ARNETT_BANDS.contains(&name.as_str()) && data.times.len() >= MIN_BAND_POINTS
    });
    let mut fit = ArnettFit::default();
    if bands.is_empty() {
        return fit;
    }
    let results = fit_parametric_model(
        &bands,
        false,
        UncertaintyMethod::Laplace,
        Some(SviModelName::Arnett),
    );
    for result in &results {
        let slot = match result.band.as_str() {
            "g" => &mut fit.g,
            "r" => &mut fit.r,
            "i" => &mut fit.i,
            _ => continue,
        };
        *slot = arnett_band_fit(result, jd_min);
    }
    fit
}

fn fit_temperature(bands: HashMap<String, BandData>) -> ThermalFit {
    let Some(result) = fit_thermal(&bands, None) else {
        return ThermalFit::default();
    };
    ThermalFit {
        log_temp_start: result.log_temp_peak.and_then(finite),
        log_temp_start_err: result.log_temp_peak_err.and_then(finite),
        log_temp_latest: result.log_temp_latest.and_then(finite),
        cooling_rate: result.cooling_rate.and_then(finite),
        cooling_rate_err: result.cooling_rate_err.and_then(finite),
        chi2: result.chi2.and_then(finite),
        n_color_obs: result.n_color_obs as i32,
        n_bands_used: result.n_bands_used as i32,
    }
}

pub fn fit(lightcurve: &[PhotometryMag]) -> LightcurveFits {
    let points = around_peak(lightcurve);
    if points.is_empty() {
        return LightcurveFits::default();
    }
    let times: Vec<f64> = points.iter().map(|p| p.time).collect();
    let mags: Vec<f64> = points.iter().map(|p| p.mag as f64).collect();
    let errs: Vec<f64> = points.iter().map(|p| p.mag_err as f64).collect();
    let bands: Vec<String> = points.iter().map(|p| p.band.to_string()).collect();
    let jd_min = times.iter().copied().fold(f64::INFINITY, f64::min);
    LightcurveFits {
        arnett: fit_arnett(build_flux_bands(&times, &mags, &errs, &bands), jd_min),
        thermal: fit_temperature(build_mag_bands(&times, &mags, &errs, &bands)),
    }
}

pub fn fit_many(lightcurves: &[Vec<PhotometryMag>]) -> Vec<Option<LightcurveFits>> {
    lightcurves
        .par_iter()
        .map(|lc| catch_unwind(AssertUnwindSafe(|| fit(lc))).ok())
        .collect()
}
