use crate::conf::AppConfig;
use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::{fetch_timeseries_op, mongify};
use crate::utils::enums::Survey;
use crate::utils::host::HostGalaxyAssociation;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, summarise_detections, Band, BandProperties,
    DetectionHistory, EpisodeHistory, PhotometryMag, EPISODE_GAP_DAYS,
};
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

const BETA: i32 = 1;
const RHO: i32 = 2;

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
pub struct ArgusCandidateTime {
    pub jd: f64,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    pub kind: String,
    pub alert_cadence: i32,
    pub candidate: ArgusCandidateTime,
    #[serde(default, rename = "prv_candidates")]
    pub prv_candidates: Vec<ArgusPoint>,
    #[serde(default, rename = "host_galaxy")]
    pub host_galaxy: Option<HostGalaxyAssociation>,
}

#[apache_avro_macros::serdavro]
#[derive(Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct ArgusPhotstats {
    pub beta: Option<BandProperties>,
    pub rho: Option<BandProperties>,
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

        let now = flare::Time::now().to_jd();
        let mut updates = Vec::with_capacity(alerts.len());
        let mut processed = Vec::with_capacity(alerts.len());
        for alert in &alerts {
            let properties = Self::get_alert_properties(alert);
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
