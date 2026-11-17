use crate::utils::lightcurves::PhotometryMag;
use lightcurve_fitting::{
    build_flux_bands, build_mag_bands, fit_gp_2d, fit_parametric_model, fit_thermal, BandData,
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

#[derive(Debug, Default, Serialize)]
pub struct Gp2dFit {
    pub amp: Option<f64>,
    pub ls_time: Option<f64>,
    pub ls_wave: Option<f64>,
    pub train_rms: Option<f64>,
    pub log_likelihood: Option<f64>,
    pub n_train: i32,
    pub n_bands: i32,
}

#[derive(Debug, Default)]
pub struct LightcurveFits {
    pub arnett: ArnettFit,
    pub thermal: ThermalFit,
    pub gp2d: Gp2dFit,
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

fn fit_gp2d(bands: &HashMap<String, BandData>) -> Gp2dFit {
    let Some((_, result)) = fit_gp_2d(bands) else {
        return Gp2dFit::default();
    };
    Gp2dFit {
        amp: finite(result.amp),
        ls_time: finite(result.ls_time),
        ls_wave: finite(result.ls_wave),
        train_rms: finite(result.train_rms),
        log_likelihood: finite(result.log_likelihood),
        n_train: result.n_train as i32,
        n_bands: result.n_bands as i32,
    }
}

fn columns(lightcurve: &[PhotometryMag]) -> Option<(Vec<f64>, Vec<f64>, Vec<f64>, Vec<String>)> {
    let points = around_peak(lightcurve);
    if points.is_empty() {
        return None;
    }
    Some((
        points.iter().map(|p| p.time).collect(),
        points.iter().map(|p| p.mag as f64).collect(),
        points.iter().map(|p| p.mag_err as f64).collect(),
        points.iter().map(|p| p.band.to_string()).collect(),
    ))
}

pub fn fit(lightcurve: &[PhotometryMag]) -> LightcurveFits {
    let Some((times, mags, errs, bands)) = columns(lightcurve) else {
        return LightcurveFits::default();
    };
    let jd_min = times.iter().copied().fold(f64::INFINITY, f64::min);
    let mag_bands = build_mag_bands(&times, &mags, &errs, &bands);
    LightcurveFits {
        arnett: fit_arnett(build_flux_bands(&times, &mags, &errs, &bands), jd_min),
        gp2d: fit_gp2d(&mag_bands),
        thermal: fit_temperature(mag_bands),
    }
}

pub fn fit_gp2d_only(lightcurve: &[PhotometryMag]) -> Gp2dFit {
    let Some((times, mags, errs, bands)) = columns(lightcurve) else {
        return Gp2dFit::default();
    };
    fit_gp2d(&build_mag_bands(&times, &mags, &errs, &bands))
}

pub fn fit_many(lightcurves: &[Vec<PhotometryMag>]) -> Vec<Option<LightcurveFits>> {
    lightcurves
        .par_iter()
        .map(|lc| catch_unwind(AssertUnwindSafe(|| fit(lc))).ok())
        .collect()
}

pub fn fit_gp2d_many(lightcurves: &[Vec<PhotometryMag>]) -> Vec<Option<Gp2dFit>> {
    lightcurves
        .par_iter()
        .map(|lc| catch_unwind(AssertUnwindSafe(|| fit_gp2d_only(lc))).ok())
        .collect()
}
