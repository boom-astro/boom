use crate::alert::WinterCandidate;
use crate::conf::AppConfig;
use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::{fetch_timeseries_op, mongify};
use crate::utils::enums::Survey;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, PerBandProperties, PhotometryMag,
};
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

pub fn create_winter_alert_pipeline() -> Vec<Document> {
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
                "from": "WNTR_alerts_aux",
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
                    1000,
                    None
                ),
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
            }
        },
    ]
}

/// WINTER alert structure used to deserialize alerts from the database, used by
/// the enrichment worker to compute lightcurve features.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct WinterAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: WinterCandidate,
    pub prv_candidates: Vec<PhotometryMag>,
}

/// WINTER alert properties computed during enrichment and inserted back into the
/// alert document.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct WinterAlertProperties {
    pub stationary: bool,
    pub photstats: PerBandProperties,
}

pub struct WinterEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for WinterEnrichmentWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        _shared_models: Option<std::sync::Arc<crate::enrichment::models::SharedModels>>,
    ) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("WNTR_alerts");

        let input_queue = "WNTR_alerts_enrichment_queue".to_string();
        let output_queue = "WNTR_alerts_filter_queue".to_string();

        Ok(WinterEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_pipeline: create_winter_alert_pipeline(),
        })
    }

    fn survey() -> Survey {
        Survey::Wntr
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
        let alerts: Vec<WinterAlertForEnrichment> =
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

        let now = flare::Time::now().to_jd();

        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for alert in alerts {
            let candid = alert.candid;

            let properties = self.get_alert_properties(&alert).await?;

            let update_alert_document = doc! {
                "$set": {
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

impl WinterEnrichmentWorker {
    async fn get_alert_properties(
        &self,
        alert: &WinterAlertForEnrichment,
    ) -> Result<WinterAlertProperties, EnrichmentWorkerError> {
        let mut lightcurve = alert.prv_candidates.clone();

        prepare_photometry(&mut lightcurve);
        let (photstats, _, stationary) = analyze_photometry(&lightcurve);

        Ok(WinterAlertProperties {
            stationary,
            photstats,
        })
    }
}
