use crate::utils::lightcurves::Band;
use crate::utils::outburst::median;
use apache_avro_macros::serdavro;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;

pub const MIN_VARIABILITY_POINTS: usize = 3;

#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, ToSchema)]
pub struct VariabilityStats {
    pub n: i32,
    pub chi2red: f32,
    pub inv_vonneumannratio: f32,
    pub stetson_j: f32,
    pub stetson_k: f32,
    pub skew: f32,
    pub median_abs_dev: f32,
}

#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, ToSchema)]
pub struct PerBandVariability {
    pub g: Option<VariabilityStats>,
    pub r: Option<VariabilityStats>,
    pub i: Option<VariabilityStats>,
    pub z: Option<VariabilityStats>,
    pub y: Option<VariabilityStats>,
    pub u: Option<VariabilityStats>,
    pub j: Option<VariabilityStats>,
    pub h: Option<VariabilityStats>,
    pub k: Option<VariabilityStats>,
}

impl PerBandVariability {
    pub fn from_points<I>(points: I) -> Self
    where
        I: IntoIterator<Item = (f64, f64, f64, Band)>,
    {
        let mut by_band: BTreeMap<Band, Vec<(f64, f64, f64)>> = BTreeMap::new();
        for (jd, flux, flux_err, band) in points {
            if flux.is_finite() && flux_err.is_finite() && flux_err > 0.0 {
                by_band.entry(band).or_default().push((jd, flux, flux_err));
            }
        }

        let mut out = Self::default();
        for (band, mut points) in by_band {
            points.sort_by(|a, b| a.0.total_cmp(&b.0));
            points.dedup_by(|a, b| a.0 == b.0);
            let stats = VariabilityStats::from_points(&points);
            match band {
                Band::G => out.g = stats,
                Band::R => out.r = stats,
                Band::I => out.i = stats,
                Band::Z => out.z = stats,
                Band::Y => out.y = stats,
                Band::U => out.u = stats,
                Band::J => out.j = stats,
                Band::H => out.h = stats,
                Band::K => out.k = stats,
            }
        }
        out
    }
}

impl VariabilityStats {
    fn from_points(points: &[(f64, f64, f64)]) -> Option<Self> {
        if points.len() < MIN_VARIABILITY_POINTS {
            return None;
        }
        let n = points.len() as f64;

        let weights: Vec<f64> = points.iter().map(|p| p.2.powi(-2)).collect();
        let weight_sum: f64 = weights.iter().sum();
        let wmean = points
            .iter()
            .zip(&weights)
            .map(|(p, w)| w * p.1)
            .sum::<f64>()
            / weight_sum;
        let chi2: f64 = points
            .iter()
            .zip(&weights)
            .map(|(p, w)| w * (p.1 - wmean).powi(2))
            .sum();
        let chi2red = chi2 / (n - 1.0);
        let wvar = chi2 / weight_sum;

        let (num, den) = points.windows(2).fold((0.0, 0.0), |(num, den), pair| {
            let w = (pair[1].0 - pair[0].0).powi(-2);
            (num + w * (pair[1].1 - pair[0].1).powi(2), den + w)
        });
        let inv_vonneumannratio = den * wvar / num;

        let scale = (n / (n - 1.0)).sqrt();
        let d: Vec<f64> = points.iter().map(|p| scale * (p.1 - wmean) / p.2).collect();
        let stetson_j: f64 = d
            .windows(2)
            .map(|pair| {
                let p = pair[0] * pair[1];
                p.signum() * p.abs().sqrt()
            })
            .sum();
        let stetson_k = d.iter().map(|x| x.abs()).sum::<f64>()
            / n
            / (d.iter().map(|x| x * x).sum::<f64>() / n).sqrt();

        let skew = n / ((n - 1.0) * (n - 2.0))
            * points
                .iter()
                .map(|p| ((p.1 - wmean) / p.2).powi(3))
                .sum::<f64>();

        let fluxes: Vec<f64> = points.iter().map(|p| p.1).collect();
        let center = median(&fluxes)?;
        let deviations: Vec<f64> = fluxes.iter().map(|f| (f - center).abs()).collect();
        let median_abs_dev = median(&deviations)?;

        [
            chi2red,
            inv_vonneumannratio,
            stetson_j,
            stetson_k,
            skew,
            median_abs_dev,
        ]
        .iter()
        .all(|v| v.is_finite())
        .then_some(VariabilityStats {
            n: points.len() as i32,
            chi2red: chi2red as f32,
            inv_vonneumannratio: inv_vonneumannratio as f32,
            stetson_j: stetson_j as f32,
            stetson_k: stetson_k as f32,
            skew: skew as f32,
            median_abs_dev: median_abs_dev as f32,
        })
    }
}
