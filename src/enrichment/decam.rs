use crate::enrichment::{EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::fetch_timeseries_op;
use crate::utils::lightcurves::{analyze_photometry, parse_photometry};
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

pub struct DecamEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_pipeline: Vec<Document>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for DecamEnrichmentWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError> {
        let config_file = crate::conf::load_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let client = db.client().clone();
        let alert_collection = db.collection("DECAM_alerts");

        let input_queue = "DECAM_alerts_enrichment_queue".to_string();
        let output_queue = "DECAM_alerts_filter_queue".to_string();

        let alert_pipeline = vec![
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
                    "fp_hists": fetch_timeseries_op(
                        "aux.fp_hists",
                        "candidate.jd",
                        365,
                        None
                    )
                }
            },
            doc! {
                "$project": doc! {
                    "objectId": 1,
                    "candidate": 1,
                    "fp_hists.jd": 1,
                    "fp_hists.magap": 1,
                    "fp_hists.sigmagap": 1,
                    "fp_hists.band": 1,
                }
            },
        ];

        Ok(DecamEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_pipeline,
        })
    }

    fn input_queue_name(&self) -> String {
        self.input_queue.clone()
    }

    fn output_queue_name(&self) -> String {
        self.output_queue.clone()
    }

    #[instrument(skip_all, err)]
    async fn fetch_alerts(
        &self,
        candids: &[i64], // this is a slice of candids to process
    ) -> Result<Vec<Document>, EnrichmentWorkerError> {
        let mut alert_pipeline = self.alert_pipeline.clone();
        if let Some(first_stage) = alert_pipeline.first_mut() {
            *first_stage = doc! {
                "$match": {
                    "_id": {"$in": candids}
                }
            };
        }
        let mut alert_cursor = self.alert_collection.aggregate(alert_pipeline).await?;

        let mut alerts: Vec<Document> = Vec::new();
        while let Some(result) = alert_cursor.next().await {
            match result {
                Ok(document) => {
                    alerts.push(document);
                }
                _ => {
                    continue;
                }
            }
        }

        Ok(alerts)
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(
        &mut self,
        candids: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError> {
        let alerts = self.fetch_alerts(&candids).await?;

        if alerts.len() != candids.len() {
            warn!(
                "FEATURE WORKER: only {} alerts fetched from {} candids",
                alerts.len(),
                candids.len()
            );
        }

        if alerts.is_empty() {
            return Ok(vec![]);
        }

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for i in 0..alerts.len() {
            let candid = alerts[i].get_i64("_id")?;

            // Compute numerical and boolean features from lightcurve and candidate analysis
            let candidate = alerts[i].get_document("candidate")?;

            let jd = candidate.get_f64("jd")?;

            let fp_hists = alerts[i].get_array("fp_hists")?;

            let lightcurve = parse_photometry(fp_hists, "jd", "magap", "sigmagap", "band", jd);

            let (photstats, _, stationary) = analyze_photometry(lightcurve, jd);

            let find_document = doc! {
                "_id": candid
            };

            let update_alert_document = doc! {
                "$set": {
                    // properties
                    "properties.stationary": stationary,
                    "properties.photstats": photstats,
                }
            };

            let update = WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(find_document)
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
