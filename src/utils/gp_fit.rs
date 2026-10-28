//! Multi-band Gaussian-process light-curve fits from [`rust_gp`], stored on
//! the alert under `gp_fit`. See `docs/gp-fit.md` for the stored fields.

use crate::utils::lightcurves::Band;
use mongodb::bson::{doc, Bson, Document};
use rayon::prelude::*;
use rust_gp::features::{light_curve_features, BandFeatures, FeatureConfig};
use rust_gp::{fit_robust, mag_to_flux, FitQuality, GpConfig, Obs};
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

/// ZTF's bands, as only ZTF is fitted.
const BANDS: [&str; 3] = ["g", "r", "i"];

/// Fit with outlier clipping, then measure each band, as the document stored
/// under `gp_fit`.
pub fn fit(obs: &[Obs], config: &GpConfig) -> Result<Document, String> {
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
    Ok(to_document(&fitted.result.fit_quality, &features))
}

/// A stored field's name and where rust-gp keeps its value.
type Field = (&'static str, fn(&BandFeatures) -> Option<f64>);

/// Flat, one key per field and band (`peak_mag_r`), like `villar_fit`. Every
/// key is written, null where a band has no value, so the schema is fixed.
///
/// The reduced χ² is over the detections left after outlier clipping, with the
/// GP's effective number of parameters taken off the degrees of freedom.
fn to_document(quality: &FitQuality, features: &[BandFeatures]) -> Document {
    let fields: [Field; 10] = [
        ("peak_jd", |f| f.peak_time),
        ("peak_jd_err", |f| f.peak_time_err),
        ("peak_mag", |f| f.peak_mag),
        ("peak_mag_err", |f| f.peak_mag_err),
        ("fwhm", |f| f.fwhm),
        ("fwhm_err", |f| f.fwhm_err),
        ("rise_rate", |f| f.rise_rate),
        ("rise_rate_err", |f| f.rise_rate_err),
        ("fade_rate", |f| f.fade_rate),
        ("fade_rate_err", |f| f.fade_rate_err),
    ];
    let per_band: Vec<Option<&BandFeatures>> = BANDS
        .iter()
        .map(|band| features.iter().find(|f| f.band == *band))
        .collect();

    let mut doc = doc! {
        // "chi2": quality.chi2,
        // "dof": quality.dof,
        "reduced_chi2": quality.reduced_chi2,
    };
    for (name, value) in fields {
        for (band, f) in BANDS.iter().zip(&per_band) {
            doc.insert(format!("{name}_{band}"), f.and_then(value));
        }
    }
    for (band, f) in BANDS.iter().zip(&per_band) {
        doc.insert(
            format!("n_detections_{band}"),
            f.map_or(0, |f| f.n_points as i32),
        );
    }
    doc
}

/// Every key a fit writes, all null, so a light curve that was not fitted has
/// the same schema as one that was.
pub fn unfitted() -> Document {
    to_document(&FitQuality::default(), &[])
        .into_iter()
        .map(|(key, _)| (key, Bson::Null))
        .collect()
}

/// Fits each `(candid, observations)` on the Rayon pool, in input order. A
/// source that is not fitted, which for most alerts means too few detections,
/// gets [`unfitted`].
#[instrument(skip_all, fields(sources = sources.len()))]
pub fn fit_many(sources: &[(i64, Vec<Obs>)], config: &GpConfig) -> Vec<Document> {
    sources
        .par_iter()
        .map(|(candid, obs)| {
            // Caught here, or one bad light curve would lose the batch every fit.
            match std::panic::catch_unwind(|| fit(obs, config)) {
                Ok(Ok(fit)) => fit,
                Ok(Err(e)) => {
                    trace!(candid, "skipping GP fit: {}", e);
                    unfitted()
                }
                Err(_) => {
                    warn!(candid, "GP fit panicked");
                    unfitted()
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::Bson;

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
        let obs = observations(bazin_points(), &config);
        assert!(obs.iter().any(|o| o.upper_limit), "pre-explosion limits");
        let doc = fit(&obs, &config).expect("fittable");
        assert_eq!(
            doc.len(),
            34,
            "1 for the fit, 11 for each of g, r and i: {doc}"
        );

        // Noiseless points against errors floored at 0.1 mag.
        let reduced_chi2 = doc.get_f64("reduced_chi2").unwrap();
        assert!((0.0..1.0).contains(&reduced_chi2), "{doc}");

        // Bazin peak: t0 + τ_rise ln(τ_fall / τ_rise - 1).
        let peak_jd = 2_460_000.0 + 3.0 * (20.0f64 / 3.0 - 1.0).ln();
        assert!(
            (doc.get_f64("peak_jd_r").unwrap() - peak_jd).abs() < 1.5,
            "{doc}"
        );
        for key in [
            "peak_mag_g",
            "peak_mag_err_g",
            "fwhm_r",
            "rise_rate_r",
            "fade_rate_r",
        ] {
            assert!(doc.get_f64(key).is_ok(), "{key} in {doc}");
        }
        assert!(doc.get_i32("n_detections_r").unwrap() > 0);

        // No i-band data: counted as zero, every value null.
        assert_eq!(doc.get_i32("n_detections_i"), Ok(0));
        assert_eq!(doc.get("peak_mag_i"), Some(&Bson::Null));
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
        for not_fitted in &fits[..2] {
            assert!(
                not_fitted.values().all(|v| *v == Bson::Null),
                "{not_fitted}"
            );
        }
        assert!(
            fits[2].get_f64("reduced_chi2").is_ok(),
            "order follows the input"
        );

        let keys = |d: &Document| d.keys().cloned().collect::<Vec<_>>();
        assert_eq!(keys(&fits[0]), keys(&fits[2]), "one schema, fitted or not");
    }
}
