//! Multi-band Gaussian-process light-curve fits from [`rust_gp`], stored on
//! the alert under `gp_fit`. See `docs/gp-fit.md` for the stored fields.

use crate::utils::lightcurves::Band;
use rayon::prelude::*;
use rust_gp::features::{light_curve_features, BandFeatures, FeatureConfig};
use rust_gp::{fit_robust, mag_to_flux, GpConfig, GpResult, Obs};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tracing::{instrument, trace, warn};

/// Light curves whose detections span longer are not fitted. They are
/// persistent variables, which a zero-mean GP models poorly and which cost
/// ~10x a transient to fit. The cutoff rust-gp's `fit_gp.py` uses.
pub const MAX_DETECTION_SPAN_DAYS: f64 = 500.0;

/// One photometric point. A detection has both `magpsf` and `sigmapsf`; a
/// non-detection has neither and enters at its `diffmaglim`.
#[derive(Debug, Clone)]
pub struct GpPoint {
    pub jd: f64,
    pub magpsf: Option<f64>,
    pub sigmapsf: Option<f64>,
    /// 5σ limiting magnitude of the difference image.
    pub diffmaglim: f64,
    pub band: Band,
}

/// rust-gp's defaults, less the Laplace errors on the hyperparameters, which
/// are not stored.
pub fn gp_config() -> GpConfig {
    GpConfig {
        laplace_errors: false,
        ..GpConfig::default()
    }
}

/// The observations `rust_gp::load_csv_with` would make of these points, so a
/// fit here reproduces one made offline from the same photometry.
///
/// Of the image-quality cuts only the depth cut applies: boom's photometry does
/// not carry `rb`, `fwhm` or `nbad`. One point is kept per exposure, the first
/// given. Non-detections are kept only before the first or after the last
/// detection, as `0 ± f_lim/5`.
pub fn observations(points: impl IntoIterator<Item = GpPoint>, config: &GpConfig) -> Vec<Obs> {
    let min_depth = config
        .quality_cuts
        .map_or(f64::NEG_INFINITY, |cuts| cuts.min_depth);
    let mut detections = Vec::new();
    let mut limits = Vec::new();
    for point in points {
        let band = point.band.to_string();
        // The near-infrared bands have no wavelength in rust-gp.
        if rust_gp::band_wavelength(&band).is_none() {
            continue;
        }
        match (point.magpsf, point.sigmapsf) {
            (Some(mag), Some(mag_err)) if mag_err > 0.0 => {
                if point.diffmaglim < min_depth {
                    continue;
                }
                let (flux, flux_err) = mag_to_flux(mag, mag_err.max(config.mag_err_floor));
                if flux.is_finite() && flux_err.is_finite() && flux > 0.0 {
                    detections.push(Obs {
                        time: point.jd,
                        flux,
                        flux_err,
                        band,
                        upper_limit: false,
                    });
                }
            }
            (None, _) if config.use_upper_limits => {
                let (f_lim, _) = mag_to_flux(point.diffmaglim, 0.0);
                if f_lim.is_finite() && f_lim > 0.0 {
                    limits.push(Obs {
                        time: point.jd,
                        flux: 0.0,
                        flux_err: f_lim / 5.0,
                        band,
                        upper_limit: true,
                    });
                }
            }
            _ => {}
        }
    }
    let mut seen = HashSet::new();
    detections.retain(|o| seen.insert((o.time.to_bits(), o.band.clone())));
    seen.clear();
    limits.retain(|o| seen.insert((o.time.to_bits(), o.band.clone())));
    let (first, last) = detection_window(&detections);
    detections.extend(
        limits
            .into_iter()
            .filter(|o| o.time < first || o.time > last),
    );
    detections
}

/// First and last detection; `(inf, -inf)` when there are none.
fn detection_window(obs: &[Obs]) -> (f64, f64) {
    obs.iter()
        .filter(|o| !o.upper_limit)
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(first, last), o| {
            (first.min(o.time), last.max(o.time))
        })
}

/// Fit with outlier clipping, then measure each band's features.
pub fn fit(obs: &[Obs], config: &GpConfig) -> Result<GpFit, String> {
    let (first, last) = detection_window(obs);
    if last - first >= MAX_DETECTION_SPAN_DAYS {
        return Err(format!(
            "detections span {:.0} days, longer than a transient",
            last - first
        ));
    }
    let fitted = fit_robust(obs, config)?;
    let features = light_curve_features(
        &fitted.prep,
        &fitted.result.hyper,
        &FeatureConfig::default(),
    );
    Ok(GpFit::new(&fitted.result, &features))
}

/// Fits each `(candid, observations)` on the Rayon pool, in input order. `None`
/// where a source is not fitted, which for most alerts means too few detections.
#[instrument(skip_all, fields(sources = sources.len()))]
pub fn fit_many(sources: &[(i64, Vec<Obs>)], config: &GpConfig) -> Vec<Option<GpFit>> {
    sources
        .par_iter()
        .map(|(candid, obs)| {
            // Caught here, or one bad light curve would lose the batch every fit.
            match std::panic::catch_unwind(|| fit(obs, config)) {
                Ok(Ok(fit)) => Some(fit),
                Ok(Err(e)) => {
                    trace!(candid, "skipping GP fit: {}", e);
                    None
                }
                Err(_) => {
                    warn!(candid, "GP fit panicked");
                    None
                }
            }
        })
        .collect()
}

/// A fit as stored under `gp_fit`. Times are JD, fluxes µJy (AB zero point 23.9).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpFit {
    /// Matérn-3/2 time scale up to `warp_start`, days.
    pub length_scale: f64,
    /// How strongly the bands are tied, Å. The larger, the closer the bands
    /// are to one shared curve shape.
    pub wavelength_scale: f64,
    /// White noise added to every point.
    pub jitter: f64,
    /// How fast the time scale grows after `warp_start`; 0 is stationary.
    pub warp_strength: Option<f64>,
    pub warp_start: Option<f64>,
    pub log_likelihood: f64,
    pub log_posterior: f64,
    /// Detections fitted, after cleaning and outlier clipping.
    pub n_detections: i32,
    pub n_limits: i32,
    /// Detections clipped as leave-one-out outliers beyond 5σ.
    pub n_outliers: i32,
    /// Stationary starts, of three, that reached the best optimum. Fewer flags
    /// a short and a long time scale explaining the data about equally well.
    pub n_starts_agree: i32,
    pub converged: bool,
    pub bands: GpPerBand,
}

impl GpFit {
    fn new(result: &GpResult, features: &[BandFeatures]) -> Self {
        let hyper = &result.hyper;
        let mut bands = GpPerBand::default();
        for f in features {
            let Some(&(_, amplitude)) = hyper.amplitudes.iter().find(|(b, _)| *b == f.band) else {
                continue;
            };
            let Some(slot) = bands.get_mut(&f.band) else {
                continue;
            };
            *slot = Some(GpBandFit {
                amplitude,
                n_detections: f.n_points as i32,
                peak_bracketed: f.peak_bracketed,
                peak_jd: f.peak_time,
                peak_mag: f.peak_mag,
                peak_flux: f.peak_flux,
                t_rise: f.t_rise,
                t_fade: f.t_fade,
                fwhm: f.fwhm,
                rise_rate: f.rise_rate,
                fade_rate: f.fade_rate,
                peak_jd_err: f.peak_time_err,
                peak_mag_err: f.peak_mag_err,
                t_rise_err: f.t_rise_err,
                t_fade_err: f.t_fade_err,
                fwhm_err: f.fwhm_err,
                rise_rate_err: f.rise_rate_err,
                fade_rate_err: f.fade_rate_err,
            });
        }
        GpFit {
            length_scale: hyper.length_scale,
            wavelength_scale: hyper.wavelength_scale,
            jitter: hyper.jitter,
            warp_strength: hyper.warp_strength,
            warp_start: hyper.warp_start,
            log_likelihood: result.log_likelihood,
            log_posterior: result.log_posterior,
            n_detections: result.n_obs as i32,
            n_limits: result.n_limits as i32,
            n_outliers: result.n_outliers as i32,
            n_starts_agree: result.n_starts_agree as i32,
            converged: result.converged,
            bands,
        }
    }
}

/// Keyed by band, so a fade rate is `gp_fit.bands.r.fade_rate`. Null for a
/// band without detections. ZTF's bands only, as only ZTF is fitted.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GpPerBand {
    pub g: Option<GpBandFit>,
    pub r: Option<GpBandFit>,
    pub i: Option<GpBandFit>,
}

impl GpPerBand {
    fn get_mut(&mut self, band: &str) -> Option<&mut Option<GpBandFit>> {
        match band {
            "g" => Some(&mut self.g),
            "r" => Some(&mut self.r),
            "i" => Some(&mut self.i),
            _ => None,
        }
    }
}

/// One band's GP amplitude and the shape of its posterior mean curve.
///
/// The features need three detections in the band and a peak inside its data
/// (`peak_bracketed`), or they are all null; a rise or fade not observed down
/// to half maximum is null on its own. Errors are a robust 1σ over 100
/// posterior draws, conditional on the fitted hyperparameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpBandFit {
    pub amplitude: f64,
    pub n_detections: i32,
    pub peak_bracketed: bool,
    pub peak_jd: Option<f64>,
    pub peak_mag: Option<f64>,
    pub peak_flux: Option<f64>,
    /// Days from the half-maximum crossing to the peak.
    pub t_rise: Option<f64>,
    /// Days from the peak to the half-maximum crossing.
    pub t_fade: Option<f64>,
    /// `t_rise + t_fade`, days.
    pub fwhm: Option<f64>,
    /// Mean rate over the 0.753 mag between half maximum and peak, mag/day.
    pub rise_rate: Option<f64>,
    pub fade_rate: Option<f64>,
    pub peak_jd_err: Option<f64>,
    pub peak_mag_err: Option<f64>,
    pub t_rise_err: Option<f64>,
    pub t_fade_err: Option<f64>,
    pub fwhm_err: Option<f64>,
    pub rise_rate_err: Option<f64>,
    pub fade_rate_err: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::db::mongify;

    fn detection(jd: f64, mag: f64, band: Band) -> GpPoint {
        GpPoint {
            jd,
            magpsf: Some(mag),
            sigmapsf: Some(0.05),
            diffmaglim: 20.5,
            band,
        }
    }

    fn limit(jd: f64, band: Band) -> GpPoint {
        GpPoint {
            jd,
            magpsf: None,
            sigmapsf: None,
            diffmaglim: 20.5,
            band,
        }
    }

    /// Two-band Bazin transient peaking near 18 mag, with ZTF-like 20.5 mag
    /// limits wherever it is fainter than that.
    fn bazin_points() -> Vec<GpPoint> {
        let bazin = |t: f64, fall: f64| (-t / fall).exp() / (1.0 + (-t / 3.0).exp());
        let mut points = Vec::new();
        for step in 0..45 {
            let t = -40.0 + 2.0 * step as f64;
            for (band, amp, fall) in [(Band::G, 120.0, 12.0), (Band::R, 150.0, 20.0)] {
                let mag = 23.9 - 2.5 * (amp * bazin(t, fall)).log10();
                let jd = 2_460_000.0 + t;
                points.push(if mag < 20.5 {
                    detection(jd, mag, band)
                } else {
                    limit(jd, band)
                });
            }
        }
        points
    }

    #[test]
    fn limits_are_kept_only_outside_the_detection_window() {
        let mut points: Vec<GpPoint> = (1..=10)
            .map(|d| detection(d as f64, 18.0, Band::R))
            .collect();
        points.extend([0.0, 5.5, 20.0].map(|jd| limit(jd, Band::R)));

        let limit_times: Vec<f64> = observations(points, &gp_config())
            .iter()
            .filter(|o| o.upper_limit)
            .map(|o| o.time)
            .collect();
        assert_eq!(limit_times, vec![0.0, 20.0]);
    }

    // Alert and forced photometry both report a shared exposure.
    #[test]
    fn one_point_is_kept_per_exposure() {
        let obs = observations(
            [
                detection(1.0, 18.0, Band::R),
                detection(1.0, 18.5, Band::R),
                detection(1.0, 18.5, Band::G),
            ],
            &gp_config(),
        );
        assert_eq!(obs.len(), 2);
        assert_eq!(
            obs[0].flux,
            mag_to_flux(18.0, 0.1).0,
            "the first given wins"
        );
    }

    #[test]
    fn detections_take_the_depth_cut_and_the_error_floor() {
        let config = gp_config();
        let shallow = GpPoint {
            diffmaglim: 18.5,
            ..detection(1.0, 18.0, Band::R)
        };
        let obs = observations([shallow, detection(2.0, 18.0, Band::R)], &config);
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].time, 2.0);
        // 0.05 mag reported, raised to the 0.1 mag floor.
        assert_eq!(obs[0].flux_err, mag_to_flux(18.0, config.mag_err_floor).1);
    }

    // A magnitude without an error is neither a detection nor a limit.
    #[test]
    fn unusable_points_are_dropped() {
        let half_measured = GpPoint {
            sigmapsf: None,
            ..detection(1.0, 18.0, Band::R)
        };
        let infrared = detection(2.0, 18.0, Band::J);
        assert!(observations([half_measured, infrared], &gp_config()).is_empty());
    }

    #[test]
    fn fits_a_two_band_transient() {
        let config = gp_config();
        let fit = fit(&observations(bazin_points(), &config), &config).expect("fittable");
        assert!(fit.n_limits > 0, "pre-explosion limits reach the fit");

        // Bazin peak: t0 + τ_rise ln(τ_fall / τ_rise - 1).
        let r = fit.bands.r.as_ref().expect("r is fitted");
        let peak_jd = 2_460_000.0 + 3.0 * (20.0f64 / 3.0 - 1.0).ln();
        assert!(r.peak_bracketed);
        assert!((r.peak_jd.unwrap() - peak_jd).abs() < 1.5, "{r:?}");
        assert!(r.t_rise.is_some() && r.t_fade.is_some(), "{r:?}");
        assert!(fit.bands.i.is_none());

        let doc = mongify(&fit);
        let bands = doc.get_document("bands").unwrap();
        assert!(bands.get_document("g").unwrap().get_f64("peak_mag").is_ok());
        assert_eq!(bands.get("i"), Some(&mongodb::bson::Bson::Null));
    }

    #[test]
    fn sparse_and_long_light_curves_are_not_fitted() {
        let config = gp_config();
        let sparse: Vec<GpPoint> = (0..4).map(|d| detection(d as f64, 18.0, Band::R)).collect();
        let long: Vec<GpPoint> = (0..20)
            .map(|d| detection(30.0 * d as f64, 18.0 + 0.1 * d as f64, Band::R))
            .collect();
        let sources = vec![
            (1, observations(sparse, &config)),
            (2, observations(long, &config)),
            (3, observations(bazin_points(), &config)),
        ];

        let fits = fit_many(&sources, &config);
        assert!(fits[0].is_none() && fits[1].is_none());
        assert!(fits[2].is_some(), "order follows the input");
    }
}
