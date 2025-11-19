use std::collections::HashMap;

use crate::alert::LsstCandidate;
use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::{fetch_timeseries_op, get_array_element, mongify};
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, PerBandProperties, PhotometryMag,
};
use apache_avro::{Schema, Writer};
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{error, instrument, warn};

// Avro schema for enriched LSST alerts sent to Babamul
// TODO: This is just a placeholder for now and needs to be defined properly
const ENRICHED_LSST_ALERT_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "EnrichedLsstAlert",
    "fields": [
        {"name": "candid", "type": "long"},
        {"name": "objectId", "type": "string"},
        {"name": "candidate", "type": "string"},
        {"name": "prv_candidates", "type": "string"},
        {"name": "fp_hists", "type": "string"},
        {"name": "properties", "type": "string"}
    ]
}
"#;

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
                "fp_hists.snr": 1,
            }
        },
    ]
}

pub struct Babamul {
    kafka_producer: rdkafka::producer::FutureProducer,
    avro_schema: Schema,
}

impl Babamul {
    fn new(config: config::Config) -> Self {
        // Read Kafka producer config from kafka: producer in the config
        let kafka_producer_host = config
            .get_string("kafka.producer")
            .unwrap_or_else(|_| "broker:29092".to_string());

        // Create Kafka producer
        let kafka_producer: rdkafka::producer::FutureProducer =
            rdkafka::config::ClientConfig::new()
                // Uncomment the following to get logs from kafka (RUST_LOG doesn't work):
                // .set("debug", "broker,topic,msg")
                .set("bootstrap.servers", &kafka_producer_host)
                .set("message.timeout.ms", "5000")
                // it's best to increase batch.size if the cluster
                // is running on another machine. Locally, lower means less
                // latency, since we are not limited by network speed anyways
                .set("batch.size", "16384")
                .set("linger.ms", "5")
                .set("acks", "1")
                .set("max.in.flight.requests.per.connection", "5")
                .set("retries", "3")
                .create()
                .expect("Failed to create Babamul Kafka producer");

        // Parse the Avro schema
        let avro_schema =
            Schema::parse_str(ENRICHED_LSST_ALERT_SCHEMA).expect("Failed to parse Avro schema");

        Babamul {
            kafka_producer,
            avro_schema,
        }
    }

    /// Convert an enriched LSST alert to Avro bytes
    fn to_avro_bytes(&self, alert: &EnrichedLsstAlert) -> Result<Vec<u8>, EnrichmentWorkerError> {
        // Serialize complex fields to JSON strings for the Avro schema
        let candidate_json = serde_json::to_string(&alert.candidate)
            .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
        let prv_candidates_json = serde_json::to_string(&alert.prv_candidates)
            .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
        let fp_hists_json = serde_json::to_string(&alert.fp_hists)
            .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
        let properties_json = serde_json::to_string(&alert.properties)
            .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;

        // Create a simplified structure for Avro encoding
        let avro_record = serde_json::json!({
            "candid": alert.candid,
            "objectId": alert.object_id,
            "candidate": candidate_json,
            "prv_candidates": prv_candidates_json,
            "fp_hists": fp_hists_json,
            "properties": properties_json,
        });

        let mut writer =
            Writer::with_codec(&self.avro_schema, Vec::new(), apache_avro::Codec::Snappy);
        writer
            .append_ser(avro_record)
            .inspect_err(|e| {
                error!("Failed to serialize alert to Avro: {}", e);
            })
            .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;

        let encoded = writer
            .into_inner()
            .inspect_err(|e| {
                error!("Failed to finalize Avro writer: {}", e);
            })
            .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;

        Ok(encoded)
    }

    async fn process_alerts(
        &self,
        alerts: Vec<EnrichedLsstAlert>,
    ) -> Result<(), EnrichmentWorkerError> {
        // Create a hash map for alerts to send to each topic
        // For now, we will just send all alerts to "babamul.none"
        // In the future, we will determine the topic based on the alert properties
        let mut alerts_by_topic: HashMap<String, Vec<EnrichedLsstAlert>> = HashMap::new();

        // Determine if this alert is worth sending to Babamul
        let min_reliability = 0.5;
        let sso = false;

        // Iterate over the alerts
        for alert in alerts {
            if alert.candidate.dia_source.reliability.unwrap_or(0.0) < min_reliability
                || alert.candidate.dia_source.pixel_flags.unwrap_or(false)
                || alert.properties.rock == sso
            {
                // Skip this alert, it doesn't meet the criteria
                continue;
            }

            // Determine which topic this alert should go to
            // Is it a star, galaxy, or none, and does it have a ZTF crossmatch?
            // TODO: Get this implemented
            // For now, all LSST alerts go to "babamul.none"
            let category: String = "none".to_string();
            let topic_name = format!("babamul.{}", category);
            alerts_by_topic
                .entry(topic_name)
                .or_insert_with(Vec::new)
                .push(alert);
        }

        // Now iterate over topic, alerts vectors to send them to Kafka
        for (topic_name, alerts) in alerts_by_topic {
            println!("Sending {} alerts to topic {}", alerts.len(), topic_name);

            // Convert all alerts to Avro format first (to avoid lifetime issues)
            let mut payloads = Vec::new();
            for alert in &alerts {
                let payload = self.to_avro_bytes(alert)?;
                payloads.push(payload);
            }

            // Send all messages to Kafka without awaiting (allows batching)
            let mut send_futures = Vec::new();
            for payload in &payloads {
                // Create Kafka record
                let record: rdkafka::producer::FutureRecord<'_, (), Vec<u8>> =
                    rdkafka::producer::FutureRecord::to(&topic_name).payload(payload);

                // Send to Kafka (non-blocking) and collect the future
                let future = self
                    .kafka_producer
                    .send(record, std::time::Duration::from_secs(5));
                send_futures.push(future);
            }

            // Now await all the sends
            // This allows Kafka to batch them efficiently based on batch.size and linger.ms settings
            for send_result in send_futures {
                send_result.await.map_err(|(e, _)| {
                    EnrichmentWorkerError::Kafka(format!("Failed to send to Kafka: {}", e))
                })?;
            }
        }

        Ok(())
    }
}

/// LSST alert structure used to deserialize alerts
/// from the database, used by the enrichment worker
/// to compute features and ML scores
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct LsstAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: LsstCandidate,
    pub prv_candidates: Vec<PhotometryMag>,
    pub fp_hists: Vec<PhotometryMag>,
}

/// LSST alert properties computed during enrichment
/// and inserted back into the alert document
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct LsstAlertProperties {
    pub rock: bool,
    pub stationary: bool,
    pub photstats: PerBandProperties,
}

/// LSST with propertied (i.e., it's enriched)
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct EnrichedLsstAlert {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: LsstCandidate,
    pub prv_candidates: Vec<PhotometryMag>,
    pub fp_hists: Vec<PhotometryMag>,
    pub properties: LsstAlertProperties,
}

impl EnrichedLsstAlert {
    pub fn from_alert_and_properties(
        alert: LsstAlertForEnrichment,
        properties: LsstAlertProperties,
    ) -> Self {
        EnrichedLsstAlert {
            candid: alert.candid,
            object_id: alert.object_id,
            candidate: alert.candidate,
            prv_candidates: alert.prv_candidates,
            fp_hists: alert.fp_hists,
            properties,
        }
    }
}

pub struct LsstEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_pipeline: Vec<Document>,
    babamul: Option<Babamul>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for LsstEnrichmentWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError> {
        let config_file = crate::conf::load_raw_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let client = db.client().clone();
        let alert_collection = db.collection("LSST_alerts");

        let input_queue = "LSST_alerts_enrichment_queue".to_string();
        let output_queue = "LSST_alerts_filter_queue".to_string();

        // Detect if Babamul is enabled from the config
        let babamul_enabled = crate::conf::babamul_enabled(&config_file);
        let babamul: Option<Babamul> = if babamul_enabled {
            Some(Babamul::new(config_file))
        } else {
            None
        };

        Ok(LsstEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
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

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        let mut enriched_alerts: Vec<EnrichedLsstAlert> = Vec::new();
        for i in 0..alerts.len() {
            let candid = alerts[i].candid;

            // Compute numerical and boolean features from lightcurve and candidate analysis
            let properties = self.get_alert_properties(&alerts[i]).await?;

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
                let enriched_alert =
                    EnrichedLsstAlert::from_alert_and_properties(alerts[i].clone(), properties);
                enriched_alerts.push(enriched_alert);
            }
        }

        let _ = self.client.bulk_write(updates).await?.modified_count;

        // Send to Babamul for batch processing
        match self.babamul.as_ref() {
            Some(babamul) => {
                if let Err(e) = babamul.process_alerts(enriched_alerts).await {
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
