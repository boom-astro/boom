use crate::alert::DecamCandidate;
use crate::conf::AppConfig;
use crate::enrichment::{
    fetch_alert_cutouts, fetch_alerts,
    models::{decam::DecamDrbModel, Model},
    EnrichmentWorker, EnrichmentWorkerError,
};
use crate::utils::db::{fetch_timeseries_op, mongify};
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, Band, PerBandProperties, PhotometryMag,
};
use apache_avro_derive::AvroSchema;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// Represents DECAM photometry data we retrieve from the database
pub struct DecamPhotometry {
    pub jd: f64,
    pub magap: Option<f64>,
    pub sigmagap: Option<f64>,
    pub band: Band,
    pub snr: Option<f64>,
}

// it should return an optional PhotometryMag
impl DecamPhotometry {
    pub fn to_photometry_mag(&self, min_snr: Option<f64>) -> Option<PhotometryMag> {
        // If snr, magap, and sigmagap are all present, this returns Some(PhotometryMag)
        // optionally applying an SNR filter: when min_snr is None, no SNR filtering is
        // applied; when it is Some(thresh), points with |snr| below thresh are filtered out.
        match (self.snr, self.magap, self.sigmagap) {
            (Some(snr), Some(mag), Some(sig)) => match min_snr {
                Some(thresh) if snr.abs() < thresh => None,
                _ => Some(PhotometryMag {
                    time: self.jd,
                    mag: mag as f32,
                    mag_err: sig as f32,
                    band: self.band.clone(),
                }),
            },
            _ => None,
        }
    }
}

pub fn create_decam_alert_pipeline() -> Vec<Document> {
    vec![
        doc! {
            "$match": {
                "_id": {"$in": []}
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "candidate": 1,
            }
        },
        doc! {
            "$lookup": {
                "from": "DECAM_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates": fetch_timeseries_op(
                    "aux.prv_candidates",
                    "candidate.jd",
                    365,
                    None
                ),
                "fp_hists": fetch_timeseries_op(
                    "aux.fp_hists",
                    "candidate.jd",
                    365,
                    Some(vec![doc! {
                        "$gte": [
                            "$$x.snr",
                            3.0
                        ]
                    }]),
                )
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates.jd": 1,
                "prv_candidates.magap": 1,
                "prv_candidates.sigmagap": 1,
                "prv_candidates.band": 1,
                "prv_candidates.snr": 1,
                "fp_hists.jd": 1,
                "fp_hists.magap": 1,
                "fp_hists.sigmagap": 1,
                "fp_hists.band": 1,
                "fp_hists.snr": 1,
            }
        },
    ]
}

/// DECAM alert structure used to deserialize alerts
/// from the database, used by the enrichment worker
/// to compute features and ML scores
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct DecamAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: DecamCandidate,
    pub prv_candidates: Vec<DecamPhotometry>,
    pub fp_hists: Vec<DecamPhotometry>,
}

/// DECAM alert properties computed during enrichment
/// and inserted back into the alert document
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct DecamAlertProperties {
    pub stationary: bool,
    pub photstats: PerBandProperties,
}

/// DECAM alert ML classifier scores
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct DecamAlertClassifications {
    pub drb: f32,
}

pub struct DecamEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_cutout_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
    drb_model: DecamDrbModel,
}

#[async_trait::async_trait]
impl EnrichmentWorker for DecamEnrichmentWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("DECAM_alerts");
        let alert_cutout_collection = db.collection("DECAM_alerts_cutouts");

        let input_queue = "DECAM_alerts_enrichment_queue".to_string();
        let output_queue = "DECAM_alerts_filter_queue".to_string();

        // we load the DRB model here, but we will likely need to load more models in the future
        let drb_model = DecamDrbModel::new("data/models/DECam_mnv3_CNN.onnx")?;

        Ok(DecamEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_cutout_collection,
            alert_pipeline: create_decam_alert_pipeline(),
            drb_model,
        })
    }

    fn input_queue_name(&self) -> String {
        self.input_queue.clone()
    }

    fn output_queue_name(&self) -> String {
        self.output_queue.clone()
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(
        &mut self,
        candids: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError> {
        let alerts: Vec<DecamAlertForEnrichment> =
            fetch_alerts(&candids, &self.alert_pipeline, &self.alert_collection).await?;

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

        let mut candid_to_cutouts =
            fetch_alert_cutouts(&candids, &self.alert_cutout_collection).await?;

        if candid_to_cutouts.len() != alerts.len() {
            warn!(
                "only {} cutouts fetched from {} candids",
                candid_to_cutouts.len(),
                alerts.len()
            );
        }

        let now = flare::Time::now().to_jd();

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for alert in alerts {
            let candid = alert.candid;
            let cutouts = candid_to_cutouts
                .remove(&candid)
                .ok_or_else(|| EnrichmentWorkerError::MissingCutouts(candid))?;

            let properties = self.get_alert_properties(&alert).await?;

            // Now, prepare inputs for ML models and run inference
            let triplet = self.drb_model.get_triplet(&[&cutouts], 121, 31)?;
            let drb_score = self.drb_model.predict(&triplet)?;

            let classifications = DecamAlertClassifications { drb: drb_score[0] };

            let update_alert_document = doc! {
                "$set": {
                    "classifications": mongify(&classifications),
                    "properties": mongify(&properties),
                    "updated_at": now,
                }
            };

            let update = WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(doc! {"_id": candid})
                    .update(update_alert_document)
                    .build(),
            );

            updates.push(update);
            processed_alerts.push(format!("{}", candid));
        }

        let _ = self.client.bulk_write(updates).await?.modified_count;

        Ok(processed_alerts)
    }
}

impl DecamEnrichmentWorker {
    async fn get_alert_properties(
        &self,
        alert: &DecamAlertForEnrichment,
    ) -> Result<DecamAlertProperties, EnrichmentWorkerError> {
        let prv_candidates: Vec<PhotometryMag> = alert
            .prv_candidates
            .iter()
            .filter(|p| p.jd <= alert.candidate.jd)
            .filter_map(|p| p.to_photometry_mag(None))
            .collect();
        let fp_hists: Vec<PhotometryMag> = alert
            .fp_hists
            .iter()
            .filter(|p| p.jd <= alert.candidate.jd)
            .filter_map(|p| p.to_photometry_mag(Some(3.0)))
            .collect();

        // lightcurve is prv_candidates + fp_hists, no need for parse_photometry here
        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);
        let (photstats, _, stationary) = analyze_photometry(&lightcurve);

        Ok(DecamAlertProperties {
            stationary,
            photstats,
        })
    }
}
