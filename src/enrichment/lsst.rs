use crate::alert::LsstCandidate;
use crate::conf::AppConfig;
use crate::enrichment::babamul::Babamul;
use crate::enrichment::{
    fetch_alert_cutouts, fetch_alerts, EnrichmentWorker, EnrichmentWorkerError,
};
use crate::utils::db::{fetch_timeseries_op, get_array_element, mongify};
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, PerBandProperties, PhotometryMag,
};
use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use schemars::JsonSchema;
use std::collections::HashMap;
use tracing::{error, instrument, warn};

pub fn create_lsst_alert_pipeline() -> Vec<Document> {
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
                "from": "LSST_alerts_aux",
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
                ),
                "aliases": get_array_element("aux.aliases"),
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates.jd": 1,
                "prv_candidates.magpsf": 1,
                "prv_candidates.sigmapsf": 1,
                "prv_candidates.band": 1,
                "fp_hists.jd": 1,
                "fp_hists.magpsf": 1,
                "fp_hists.sigmapsf": 1,
                "fp_hists.band": 1,
            }
        },
    ]
}

/// LSST alert structure used to deserialize alerts
/// from the database, used by the enrichment worker
/// to compute features and ML scores
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, JsonSchema)]
pub struct LsstAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: LsstCandidate,
    pub prv_candidates: Vec<PhotometryMag>,
    pub fp_hists: Vec<PhotometryMag>,
}

/// LSST alert properties computed during enrichment and inserted back into the alert document
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, JsonSchema)]
pub struct LsstAlertProperties {
    pub rock: bool,
    pub stationary: bool,
    pub photstats: PerBandProperties,
}

/// Enriched LSST alert
#[serdavro]
#[derive(Debug, serde::Deserialize, serde::Serialize, JsonSchema)]
pub struct EnrichedLsstAlert {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: LsstCandidate,
    pub prv_candidates: Vec<PhotometryMag>,
    pub fp_hists: Vec<PhotometryMag>,
    pub properties: LsstAlertProperties,
    #[serde(rename = "cutoutScience")]
    pub cutout_science: Option<Vec<u8>>,
    #[serde(rename = "cutoutTemplate")]
    pub cutout_template: Option<Vec<u8>>,
    #[serde(rename = "cutoutDifference")]
    pub cutout_difference: Option<Vec<u8>>,
}

impl EnrichedLsstAlert {
    pub fn from_alert_properties_and_cutouts(
        alert: LsstAlertForEnrichment,
        cutout_science: Option<Vec<u8>>,
        cutout_template: Option<Vec<u8>>,
        cutout_difference: Option<Vec<u8>>,
        properties: LsstAlertProperties,
    ) -> Self {
        EnrichedLsstAlert {
            candid: alert.candid,
            object_id: alert.object_id,
            candidate: alert.candidate,
            prv_candidates: alert.prv_candidates,
            fp_hists: alert.fp_hists,
            properties,
            cutout_science,
            cutout_template,
            cutout_difference,
        }
    }
}

pub struct LsstEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_cutout_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
    babamul: Option<Babamul>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for LsstEnrichmentWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("LSST_alerts");
        let alert_cutout_collection = db.collection("LSST_alerts_cutouts");

        let input_queue = "LSST_alerts_enrichment_queue".to_string();
        let output_queue = "LSST_alerts_filter_queue".to_string();

        // Detect if Babamul is enabled from the config
        let babamul: Option<Babamul> = if config.babamul.enabled {
            Some(Babamul::new(&config))
        } else {
            None
        };

        Ok(LsstEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_cutout_collection,
            alert_pipeline: create_lsst_alert_pipeline(),
            babamul,
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
        let alerts: Vec<LsstAlertForEnrichment> =
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

        let mut candid_to_cutouts = if self.babamul.is_some() {
            fetch_alert_cutouts(&candids, &self.alert_cutout_collection).await?
        } else {
            HashMap::new()
        };

        if candid_to_cutouts.len() != alerts.len() {
            warn!(
                "only {} cutouts fetched from {} candids",
                candid_to_cutouts.len(),
                alerts.len()
            );
        }

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        let mut enriched_alerts: Vec<EnrichedLsstAlert> = Vec::new();
        for alert in alerts {
            let candid = alert.candid;

            // Compute numerical and boolean features from lightcurve and candidate analysis
            let properties = self.get_alert_properties(&alert).await?;

            let update_alert_document = doc! {
                "$set": {
                    "properties": mongify(&properties),
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

            // If Babamul is enabled, add the enriched alert to the batch
            if self.babamul.is_some() {
                let cutouts = candid_to_cutouts
                    .remove(&candid)
                    .ok_or_else(|| EnrichmentWorkerError::MissingCutouts(candid))?;
                let enriched_alert = EnrichedLsstAlert::from_alert_properties_and_cutouts(
                    alert,
                    Some(cutouts.cutout_science),
                    Some(cutouts.cutout_template),
                    Some(cutouts.cutout_difference),
                    properties,
                );
                enriched_alerts.push(enriched_alert);
            }
        }

        let _ = self.client.bulk_write(updates).await?.modified_count;

        // Send to Babamul for batch processing
        match self.babamul.as_ref() {
            Some(babamul) => {
                if let Err(e) = babamul.process_lsst_alerts(enriched_alerts).await {
                    error!("Failed to process enriched alerts in Babamul: {}", e);
                }
            }
            None => {}
        }

        Ok(processed_alerts)
    }
}

impl LsstEnrichmentWorker {
    async fn get_alert_properties(
        &self,
        alert: &LsstAlertForEnrichment,
    ) -> Result<LsstAlertProperties, EnrichmentWorkerError> {
        // Compute numerical and boolean features from lightcurve and candidate analysis
        let candidate = &alert.candidate;

        let is_rock = candidate.is_sso;

        let prv_candidates = alert.prv_candidates.clone();
        let fp_hists = alert.fp_hists.clone();

        // lightcurve is prv_candidates + fp_hists, no need for parse_photometry here
        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);
        let (photstats, _, stationary) = analyze_photometry(&lightcurve);

        Ok(LsstAlertProperties {
            rock: is_rock,
            stationary,
            photstats,
        })
    }
}
