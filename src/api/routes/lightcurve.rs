use actix_web::{post, web, HttpResponse};
use serde::Deserialize;
use std::collections::HashMap;

use crate::fitting::common::BandData;

const ZP: f64 = 23.9;

#[derive(Deserialize)]
struct AlertPhotometry {
    jd: Option<f64>,
    magpsf: Option<f64>,
    sigmapsf: Option<f64>,
    band: Option<String>,
    snr: Option<f64>,
}

#[derive(Deserialize)]
pub struct LightcurveFitRequest {
    #[serde(alias = "objectId")]
    object_id: String,
    method: String, // "parametric" or "nonparametric"
    candidate: AlertPhotometry,
    prv_candidates: Vec<AlertPhotometry>,
    #[serde(default)]
    fp_hists: Vec<AlertPhotometry>,
}

/// Extract valid photometry points from alert photometry entries.
/// For `prv_candidates` and `candidate`: no SNR filter.
/// For `fp_hists`: filter out entries with |snr| < 3.0.
fn extract_photometry(
    entries: &[AlertPhotometry],
    apply_snr_filter: bool,
) -> Vec<(f64, f64, f64, String)> {
    entries
        .iter()
        .filter_map(|p| {
            let jd = p.jd?;
            let mag = p.magpsf?;
            let sig = p.sigmapsf?;
            let band = p.band.as_ref()?;

            if apply_snr_filter {
                let snr = p.snr?;
                if snr.abs() < 3.0 {
                    return None;
                }
            }

            Some((jd, mag, sig, band.clone()))
        })
        .collect()
}

/// Group photometry points by band name.
/// Returns `HashMap<band_name, (times, mags, mag_errors)>` in magnitude space.
fn group_by_band(points: Vec<(f64, f64, f64, String)>) -> HashMap<String, BandData> {
    let mut bands: HashMap<String, BandData> = HashMap::new();
    for (jd, mag, sig, band) in points {
        let entry = bands.entry(band).or_insert_with(|| BandData {
            times: Vec::new(),
            mags: Vec::new(),
            errors: Vec::new(),
        });
        entry.times.push(jd);
        entry.mags.push(mag);
        entry.errors.push(sig);
    }
    bands
}

/// Convert magnitude-space band data to flux-space tuples for the parametric fitter.
/// flux = 10^((ZP - mag) / 2.5)
/// flux_err = flux * sigmapsf / 1.0857
fn mag_bands_to_flux(
    bands: &HashMap<String, BandData>,
) -> HashMap<String, (Vec<f64>, Vec<f64>, Vec<f64>)> {
    let mut result = HashMap::new();
    for (name, bd) in bands {
        let mut times = Vec::new();
        let mut fluxes = Vec::new();
        let mut flux_errs = Vec::new();
        for i in 0..bd.times.len() {
            let flux = 10.0_f64.powf((ZP - bd.mags[i]) / 2.5);
            let flux_err = flux * bd.errors[i] / 1.0857;
            times.push(bd.times[i]);
            fluxes.push(flux);
            flux_errs.push(flux_err);
        }
        if !times.is_empty() {
            result.insert(name.clone(), (times, fluxes, flux_errs));
        }
    }
    result
}

#[post("/lightcurve/fit")]
pub async fn post_lightcurve_fit(body: web::Json<LightcurveFitRequest>) -> HttpResponse {
    let req = body.into_inner();
    let object_id = req.object_id;
    let method = req.method;

    // Validate method
    if method != "parametric" && method != "nonparametric" {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "method must be 'parametric' or 'nonparametric'"
        }));
    }

    // Extract photometry from candidate + prv_candidates (no SNR filter)
    let mut points = extract_photometry(&[req.candidate], false);
    points.extend(extract_photometry(&req.prv_candidates, false));
    // Extract from fp_hists with SNR >= 3.0 filter
    points.extend(extract_photometry(&req.fp_hists, true));

    if points.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "no valid photometry points found in the alert"
        }));
    }

    // Shift times relative to t_min so they start near 0.
    // The parametric fitter's PSO bounds assume t0 in [-100, +100],
    // which is incompatible with absolute JD values (~2460000).
    let t_min = points
        .iter()
        .map(|(jd, _, _, _)| *jd)
        .fold(f64::INFINITY, f64::min);
    for p in &mut points {
        p.0 -= t_min;
    }

    // Group by band (in magnitude space)
    let bands_mag = group_by_band(points);

    if bands_mag.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "at least one band is required"
        }));
    }

    let result = web::block(move || -> Result<Vec<u8>, String> {
        let tmp = tempfile::Builder::new()
            .suffix(".png")
            .tempfile()
            .map_err(|e| format!("Failed to create temp file: {}", e))?;
        let output_path = tmp.path().to_path_buf();

        match method.as_str() {
            "parametric" => {
                // Parametric fitting expects flux values
                let bands_flux = mag_bands_to_flux(&bands_mag);

                crate::fitting::parametric::process_data(&object_id, bands_flux, &output_path)
                    .map_err(|e| format!("Parametric fitting failed: {}", e))?;
            }
            "nonparametric" => {
                // Nonparametric fitting expects magnitudes (BandData)
                crate::fitting::nonparametric::process_data(&object_id, bands_mag, &output_path)
                    .map_err(|e| format!("Nonparametric fitting failed: {}", e))?;
            }
            _ => unreachable!(),
        }

        std::fs::read(&output_path).map_err(|e| format!("Failed to read PNG: {}", e))
    })
    .await;

    match result {
        Ok(Ok(png_bytes)) => HttpResponse::Ok().content_type("image/png").body(png_bytes),
        Ok(Err(e)) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Blocking task failed: {}", e)
        })),
    }
}
