use crate::conf::AppConfig;
use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::{fetch_timeseries_op, get_array_element, mongify};
use crate::utils::enums::Survey;
use crate::utils::host::HostGalaxyAssociation;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, summarise_detections, Band, BandProperties,
    DetectionHistory, EpisodeHistory, PhotometryMag, EPISODE_GAP_DAYS,
};
use lightcurve_fitting::{fit_flares, FlareFitOptions};
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use rayon::prelude::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use tracing::{instrument, warn};

const BETA: i32 = 1;
const RHO: i32 = 2;
const SECONDS_PER_DAY: f64 = 86_400.0;
const FLARE_WINDOW_DAYS: f64 = 1.0;
const FLARE_MAX_POINTS: usize = 256;
const FLARE_MIN_POINTS: usize = 8;
const FLARE_MAX_COMPONENTS: usize = 3;

pub fn create_argus_alert_pipeline() -> Vec<Document> {
    vec![
        doc! {
            "$match": {
                "_id": {"$in": []}
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "kind": 1,
                "alertCadence": 1,
                "candidate.jd": 1,
                "candidate.filter": 1,
                "candidate.psfFluxSci": 1,
                "candidate.psfFluxDif": 1,
            }
        },
        doc! {
            "$lookup": {
                "from": "ARGUS_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "kind": 1,
                "alertCadence": 1,
                "candidate": 1,
                "prv_candidates": fetch_timeseries_op(
                    "aux.prv_candidates",
                    "candidate.jd",
                    365,
                    None
                ),
                "host_galaxy": {"$arrayElemAt": ["$aux.host_galaxy", 0]},
                "flare_window": {
                    "$cond": [
                        {"$gt": ["$candidate.psfFluxDif", 0]},
                        {"$map": {
                            "input": {"$slice": [
                                {"$filter": {
                                    "input": get_array_element("aux.prv_candidates"),
                                    "as": "x",
                                    "cond": {"$and": [
                                        {"$lte": ["$$x.jd", "$candidate.jd"]},
                                        {"$lte": [{"$subtract": ["$candidate.jd", "$$x.jd"]}, FLARE_WINDOW_DAYS]},
                                        {"$eq": ["$$x.cadence", "$alertCadence"]},
                                        {"$eq": ["$$x.filter", "$candidate.filter"]},
                                        {"$isNumber": "$$x.psfFluxDif"},
                                        {"$gt": ["$$x.psfFluxDifErr", 0]},
                                    ]},
                                }},
                                -(FLARE_MAX_POINTS as i32),
                            ]},
                            "as": "p",
                            "in": {
                                "jd": "$$p.jd",
                                "psfFluxDif": "$$p.psfFluxDif",
                                "psfFluxDifErr": "$$p.psfFluxDifErr",
                                "isFluxPositive": "$$p.isFluxPositive",
                            },
                        }},
                        [],
                    ]
                },
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "kind": 1,
                "alertCadence": 1,
                "candidate": 1,
                "prv_candidates.jd": 1,
                "prv_candidates.cadence": 1,
                "prv_candidates.filter": 1,
                "prv_candidates.psfMagnitudeDif": 1,
                "prv_candidates.psfMagnitudeDifErr": 1,
                "prv_candidates.isFluxPositive": 1,
                "host_galaxy": 1,
                "flare_window": 1,
            }
        },
    ]
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusPoint {
    pub jd: f64,
    pub cadence: i32,
    pub filter: i32,
    pub psf_magnitude_dif: Option<f32>,
    pub psf_magnitude_dif_err: Option<f32>,
    pub is_flux_positive: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusFlarePoint {
    pub jd: f64,
    pub psf_flux_dif: Option<f32>,
    pub psf_flux_dif_err: Option<f32>,
    pub is_flux_positive: bool,
}

impl ArgusPoint {
    fn to_photometry_mag(&self) -> Option<PhotometryMag> {
        let band = match self.filter {
            BETA => Band::G,
            RHO => Band::R,
            _ => return None,
        };
        let mag = self.psf_magnitude_dif.filter(|m| m.is_finite())?;
        let mag_err = self.psf_magnitude_dif_err.filter(|e| e.is_finite())?;
        Some(PhotometryMag {
            time: self.jd,
            mag,
            mag_err,
            band,
        })
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusCandidateForEnrichment {
    pub jd: f64,
    pub filter: i32,
    pub psf_flux_sci: Option<f32>,
    pub psf_flux_dif: Option<f32>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    pub kind: String,
    pub alert_cadence: i32,
    pub candidate: ArgusCandidateForEnrichment,
    #[serde(default, rename = "prv_candidates")]
    pub prv_candidates: Vec<ArgusPoint>,
    #[serde(default, rename = "host_galaxy")]
    pub host_galaxy: Option<HostGalaxyAssociation>,
    #[serde(default, rename = "flare_window")]
    pub flare_window: Vec<ArgusFlarePoint>,
}

#[apache_avro_macros::serdavro]
#[derive(Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct ArgusPhotstats {
    pub beta: Option<BandProperties>,
    pub rho: Option<BandProperties>,
}

#[apache_avro_macros::serdavro]
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ArgusFlareFit {
    pub filter: i32,
    pub n_points: i32,
    pub n_flares: i32,
    pub delta_bic: f64,
    pub reduced_chi2: f64,
    pub ed_total_s: f64,
    pub t_peak: Option<f64>,
    pub t_peak_err_s: Option<f64>,
    pub fwhm_s: Option<f64>,
    pub fwhm_err_s: Option<f64>,
    pub amplitude: Option<f64>,
    pub amplitude_err: Option<f64>,
    pub ed_s: Option<f64>,
    pub ed_err_s: Option<f64>,
}

#[apache_avro_macros::serdavro]
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ArgusAlertProperties {
    pub stationary: bool,
    pub hosted: Option<bool>,
    pub photstats: ArgusPhotstats,
    pub cadences: Vec<i32>,
    pub detection_history: DetectionHistory,
    pub episode_history: EpisodeHistory,
    pub flare_fit: Option<ArgusFlareFit>,
}

struct FlareInput {
    filter: i32,
    jd: f64,
    exposure_s: f64,
    times_s: Vec<f64>,
    flux: Vec<f64>,
    flux_err: Vec<f64>,
}

impl FlareInput {
    fn from_alert(alert: &ArgusAlertForEnrichment) -> Option<Self> {
        let candidate = &alert.candidate;
        let flux_dif = candidate.psf_flux_dif?;
        if !(flux_dif.is_finite() && flux_dif > 0.0) {
            return None;
        }
        let reference = (candidate.psf_flux_sci? - flux_dif) as f64;
        if !(reference.is_finite() && reference > 0.0) {
            return None;
        }
        let points: Vec<(f64, f64, f64)> = alert
            .flare_window
            .iter()
            .filter_map(|p| {
                let flux = p.psf_flux_dif.filter(|f| f.is_finite())?.abs() as f64;
                let err = p.psf_flux_dif_err.filter(|e| e.is_finite() && *e > 0.0)? as f64;
                let flux = if p.is_flux_positive { flux } else { -flux };
                Some((p.jd, flux / reference, err / reference))
            })
            .collect();
        if points.len() < FLARE_MIN_POINTS {
            return None;
        }
        Some(FlareInput {
            filter: candidate.filter,
            jd: candidate.jd,
            exposure_s: alert.alert_cadence as f64,
            times_s: points
                .iter()
                .map(|p| (p.0 - candidate.jd) * SECONDS_PER_DAY)
                .collect(),
            flux: points.iter().map(|p| p.1).collect(),
            flux_err: points.iter().map(|p| p.2).collect(),
        })
    }

    fn fit(&self) -> Option<ArgusFlareFit> {
        let opts = FlareFitOptions {
            max_flares: FLARE_MAX_COMPONENTS,
            exposure_time: Some(self.exposure_s),
            ..Default::default()
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            fit_flares(&self.times_s, &self.flux, &self.flux_err, &opts)
        }))
        .ok()??;
        let last = result
            .flares
            .iter()
            .max_by(|a, b| a.t_peak.total_cmp(&b.t_peak));
        Some(ArgusFlareFit {
            filter: self.filter,
            n_points: result.n_obs as i32,
            n_flares: result.flares.len() as i32,
            delta_bic: result.per_k_bic.first().map_or(0.0, |b0| b0 - result.bic),
            reduced_chi2: result.reduced_chi2,
            ed_total_s: result.flares.iter().map(|f| f.equivalent_duration).sum(),
            t_peak: last.map(|f| self.jd + f.t_peak / SECONDS_PER_DAY),
            t_peak_err_s: last.and_then(|f| f.t_peak_err),
            fwhm_s: last.map(|f| f.t_half),
            fwhm_err_s: last.and_then(|f| f.t_half_err),
            amplitude: last.map(|f| f.amplitude),
            amplitude_err: last.and_then(|f| f.amplitude_err),
            ed_s: last.map(|f| f.equivalent_duration),
            ed_err_s: last.and_then(|f| f.equivalent_duration_err),
        })
    }
}

pub struct ArgusEnrichmentWorker {
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
}

impl ArgusEnrichmentWorker {
    fn get_alert_properties(alert: &ArgusAlertForEnrichment) -> ArgusAlertProperties {
        let mut lightcurve: Vec<PhotometryMag> = alert
            .prv_candidates
            .iter()
            .filter(|point| point.cadence == alert.alert_cadence)
            .filter_map(ArgusPoint::to_photometry_mag)
            .collect();
        prepare_photometry(&mut lightcurve);
        let (per_band, _, stationary) = analyze_photometry(&lightcurve);

        let mut cadences: Vec<i32> = alert.prv_candidates.iter().map(|p| p.cadence).collect();
        cadences.sort_unstable();
        cadences.dedup();

        let mut epochs: Vec<(f64, Option<bool>)> = alert
            .prv_candidates
            .iter()
            .map(|point| (point.jd, Some(!point.is_flux_positive)))
            .collect();
        epochs.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        epochs.dedup_by(|a, b| a.0 == b.0);
        let (detection_history, episode_history) = summarise_detections(
            epochs,
            std::iter::empty::<(f64, Option<f64>)>(),
            alert.candidate.jd,
            EPISODE_GAP_DAYS,
        );

        ArgusAlertProperties {
            stationary,
            hosted: (alert.kind == "event")
                .then(|| alert.host_galaxy.as_ref().map(|h| h.best_host.is_some()))
                .flatten(),
            photstats: ArgusPhotstats {
                beta: per_band.g,
                rho: per_band.r,
            },
            cadences,
            detection_history,
            episode_history,
            flare_fit: None,
        }
    }
}

#[async_trait::async_trait]
impl EnrichmentWorker for ArgusEnrichmentWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        _shared_models: Option<std::sync::Arc<crate::enrichment::models::SharedModels>>,
    ) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        Ok(ArgusEnrichmentWorker {
            client: db.client().clone(),
            alert_collection: db.collection("ARGUS_alerts"),
            alert_pipeline: create_argus_alert_pipeline(),
        })
    }

    fn survey() -> Survey {
        Survey::Argus
    }

    fn disable_babamul(&mut self) {}

    fn input_queue_name(&self) -> String {
        "ARGUS_alerts_enrichment_queue".to_string()
    }

    fn output_queue_name(&self) -> String {
        "ARGUS_alerts_filter_queue".to_string()
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(
        &mut self,
        candids: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError> {
        let alerts: Vec<ArgusAlertForEnrichment> =
            fetch_alerts(candids, &self.alert_pipeline, &self.alert_collection).await?;

        if alerts.len() != candids.len() {
            warn!(
                "only {} alerts fetched from {} candids",
                alerts.len(),
                candids.len()
            );
        }
        if alerts.is_empty() {
            return Ok(vec![]);
        }

        let flare_inputs: Vec<Option<FlareInput>> =
            alerts.iter().map(FlareInput::from_alert).collect();
        let span = tracing::Span::current();
        let mut flare_fits = tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                flare_inputs
                    .par_iter()
                    .map(|input| input.as_ref().and_then(FlareInput::fit))
                    .collect::<Vec<_>>()
            })
        })
        .await
        .unwrap_or_else(|e| {
            warn!("flare fitting failed for the whole batch: {}", e);
            Vec::new()
        })
        .into_iter();

        let now = flare::Time::now().to_jd();
        let mut updates = Vec::with_capacity(alerts.len());
        let mut processed = Vec::with_capacity(alerts.len());
        for alert in &alerts {
            let mut properties = Self::get_alert_properties(alert);
            properties.flare_fit = flare_fits.next().flatten();
            updates.push(WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(doc! {"_id": alert.candid})
                    .update(doc! {
                        "$set": {
                            "properties": mongify(&properties),
                            "updated_at": now,
                        }
                    })
                    .build(),
            ));
            processed.push(alert.candid.to_string());
        }
        self.client.bulk_write(updates).await?;

        Ok(processed)
    }
}
