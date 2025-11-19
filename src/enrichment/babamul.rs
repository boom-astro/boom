//! Babamul is an optional component of ZTF and LSST enrichment pipelines.

use crate::enrichment::lsst::EnrichedLsstAlert;
use crate::enrichment::ztf::EnrichedZtfAlert;
use crate::enrichment::EnrichmentWorkerError;
use apache_avro::{Schema, Writer};
use std::collections::HashMap;
use tracing::error;

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

const ENRICHED_ZTF_ALERT_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "EnrichedZtfAlert",
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

pub struct Babamul {
    kafka_producer: rdkafka::producer::FutureProducer,
    lsst_avro_schema: Schema,
    ztf_avro_schema: Schema,
}

impl Babamul {
    pub fn new(config: config::Config) -> Self {
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

        // Parse the Avro schemas
        let lsst_avro_schema =
            Schema::parse_str(ENRICHED_LSST_ALERT_SCHEMA).expect("Failed to parse Avro schema");
        let ztf_avro_schema =
            Schema::parse_str(ENRICHED_ZTF_ALERT_SCHEMA).expect("Failed to parse Avro schema");

        Babamul {
            kafka_producer,
            lsst_avro_schema,
            ztf_avro_schema,
        }
    }

    /// Convert an enriched LSST alert to Avro bytes
    fn lsst_alert_to_avro_bytes(
        &self,
        alert: &EnrichedLsstAlert,
    ) -> Result<Vec<u8>, EnrichmentWorkerError> {
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

        let mut writer = Writer::with_codec(
            &self.lsst_avro_schema,
            Vec::new(),
            apache_avro::Codec::Snappy,
        );
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

    /// Convert an enriched ZTF alert to Avro bytes
    fn ztf_alert_to_avro_bytes(
        &self,
        alert: &EnrichedZtfAlert,
    ) -> Result<Vec<u8>, EnrichmentWorkerError> {
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

        let mut writer = Writer::with_codec(
            &self.ztf_avro_schema,
            Vec::new(),
            apache_avro::Codec::Snappy,
        );
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

    pub async fn process_lsst_alerts(
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
                let payload = self.lsst_alert_to_avro_bytes(alert)?;
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

    pub async fn process_ztf_alerts(
        &self,
        alerts: Vec<EnrichedZtfAlert>,
    ) -> Result<(), EnrichmentWorkerError> {
        // Create a hash map for alerts to send to each topic
        // For now, we will just send all alerts to "babamul.none"
        // In the future, we will determine the topic based on the alert properties
        let mut alerts_by_topic: HashMap<String, Vec<EnrichedZtfAlert>> = HashMap::new();

        // Iterate over the alerts
        for alert in alerts {
            // Determine which topic this alert should go to
            // Is it a star, galaxy, or none, and does it have an LSST crossmatch?
            // TODO: Get this implemented
            // For now, all ZTF alerts go to "babamul.none"
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
                let payload = self.ztf_alert_to_avro_bytes(alert)?;
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
