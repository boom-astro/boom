//! Babamul is an optional component of ZTF and LSST enrichment pipelines,
//! which sends enriched alerts to various Kafka topics for public consumption.

use crate::conf::AppConfig;
use crate::enrichment::lsst::EnrichedLsstAlert;
use crate::enrichment::ztf::EnrichedZtfAlert;
use crate::enrichment::EnrichmentWorkerError;
use apache_avro::schema::derive::AvroSchemaComponent;
use apache_avro::{Schema, Writer};
use std::collections::HashMap;

// Helper to obtain a Schema for a type that derives AvroSchema
fn derived_schema<T: AvroSchemaComponent>() -> Schema {
    let mut named = std::collections::HashMap::new();
    <T as AvroSchemaComponent>::get_schema_in_ctxt(&mut named, &None)
}

pub enum EnrichedAlert<'a> {
    Lsst(&'a EnrichedLsstAlert),
    Ztf(&'a EnrichedZtfAlert),
}

pub struct Babamul {
    kafka_producer: rdkafka::producer::FutureProducer,
    lsst_avro_schema: Schema,
    ztf_avro_schema: Schema,
}

impl Babamul {
    pub fn new(config: &AppConfig) -> Self {
        // Read Kafka producer config from kafka: producer in the config
        let kafka_producer_host = config.kafka.producer.server.clone();

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

        // Generate Avro schemas via apache-avro-derive
        let lsst_avro_schema = derived_schema::<EnrichedLsstAlert>();
        let ztf_avro_schema = derived_schema::<EnrichedZtfAlert>();

        Babamul {
            kafka_producer,
            lsst_avro_schema,
            ztf_avro_schema,
        }
    }

    async fn send_alerts_by_topic<T, F>(
        &self,
        alerts_by_topic: HashMap<String, Vec<T>>,
        to_enriched: F,
    ) -> Result<(), EnrichmentWorkerError>
    where
        for<'a> F: Fn(&'a T) -> EnrichedAlert<'a>,
    {
        for (topic_name, alerts) in alerts_by_topic {
            tracing::info!("Sending {} alerts to topic {}", alerts.len(), topic_name);

            // Convert alerts to Avro payloads
            let mut payloads = Vec::new();
            for alert in &alerts {
                let payload = self.alert_to_avro_bytes(to_enriched(alert))?;
                payloads.push(payload);
            }

            // Send all messages to Kafka without awaiting immediately (allow batching)
            let mut send_futures = Vec::new();
            for payload in &payloads {
                let record: rdkafka::producer::FutureRecord<'_, (), Vec<u8>> =
                    rdkafka::producer::FutureRecord::to(&topic_name).payload(payload);
                let future = self
                    .kafka_producer
                    .send(record, std::time::Duration::from_secs(5));
                send_futures.push(future);
            }

            // Await all sends and map errors
            for send_result in send_futures {
                send_result.await.map_err(|(e, _)| {
                    EnrichmentWorkerError::Kafka(format!("Failed to send to Kafka: {}", e))
                })?;
            }
        }

        Ok(())
    }

    fn alert_to_avro_bytes(&self, alert: EnrichedAlert) -> Result<Vec<u8>, EnrichmentWorkerError> {
        let (schema, ser_value) = match alert {
            EnrichedAlert::Lsst(a) => {
                let v = apache_avro::to_value(a)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
                (&self.lsst_avro_schema, v)
            }
            EnrichedAlert::Ztf(a) => {
                let v = apache_avro::to_value(a)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
                (&self.ztf_avro_schema, v)
            }
        };
        let mut writer = Writer::with_codec(schema, Vec::new(), apache_avro::Codec::Snappy);
        writer
            .append(ser_value)
            .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
        writer
            .into_inner()
            .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))
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

        // Iterate over the alerts
        for alert in alerts {
            if alert.candidate.dia_source.reliability.unwrap_or(0.0) < min_reliability
                || alert.candidate.dia_source.pixel_flags.unwrap_or(false)
                || alert.properties.rock
            {
                // Skip this alert, it doesn't meet the criteria
                continue;
            }

            // Determine which topic this alert should go to
            // Is it a star, galaxy, or none, and does it have a ZTF crossmatch?
            // TODO: Get this implemented
            // For now, all LSST alerts go to "babamul.none"
            let category: String = "none".to_string();
            let topic_name = format!("babamul.lsst.{}", category);
            alerts_by_topic
                .entry(topic_name)
                .or_insert_with(Vec::new)
                .push(alert);
        }

        // Send all grouped alerts using shared helper
        self.send_alerts_by_topic(alerts_by_topic, |a| EnrichedAlert::Lsst(a))
            .await
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
            if alert.properties.rock {
                // Skip this alert, it doesn't meet the criteria
                continue;
            }

            // Determine which topic this alert should go to
            // Is it a star, galaxy, or none, and does it have an LSST crossmatch?
            // TODO: Get this implemented
            // For now, all ZTF alerts go to "babamul.none"
            let category: String = "none".to_string();
            let topic_name = format!("babamul.ztf.{}", category);
            alerts_by_topic
                .entry(topic_name)
                .or_insert_with(Vec::new)
                .push(alert);
        }

        // Send all grouped alerts using shared helper
        self.send_alerts_by_topic(alerts_by_topic, |a| EnrichedAlert::Ztf(a))
            .await
    }
}
