//! Babamul is an optional component of ZTF and LSST enrichment pipelines,
//! which sends enriched alerts to various Kafka topics for public consumption.
use crate::alert::{LsstCandidate, ZtfCandidate};
use crate::conf::AppConfig;
use crate::enrichment::lsst::{
    is_in_footprint, LsstAlertForEnrichment, LsstAlertProperties, IS_HOSTED_SCORE_THRESH,
    IS_NEAR_BRIGHTSTAR_DISTANCE_THRESH_ARCSEC, IS_NEAR_BRIGHTSTAR_MAG_THRESH,
    IS_STELLAR_DISTANCE_THRESH_ARCSEC,
};
use crate::enrichment::ztf::{ZtfAlertForEnrichment, ZtfAlertProperties};
use crate::enrichment::{EnrichmentWorkerError, LsstPhotometry, ZtfPhotometry};
use crate::utils::derive_avro_schema::SerdavroWriter;
use apache_avro::{AvroSchema, Schema, Writer};
use apache_avro_macros::serdavro;
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use std::collections::{HashMap, HashSet};
use tokio::sync::Mutex;
use tracing::{info, instrument};

const ZTF_HOSTED_SG_SCORE_THRESH: f32 = 0.5;

// Wrapper around cutout bytes, so we can implement
// AvroSchemaComponent for it, to serialize as bytes in Avro
#[derive(Debug, serde::Deserialize)]
pub struct CutoutBytes(Vec<u8>);

// Wrapper for cross_matches that implements AvroSchemaComponent as a no-op
// since we don't want to serialize it to Avro
#[derive(Debug, Clone, Default)]
pub struct CrossMatchesWrapper(
    pub Option<std::collections::HashMap<String, Vec<serde_json::Value>>>,
);

impl apache_avro::schema::derive::AvroSchemaComponent for CrossMatchesWrapper {
    fn get_schema_in_ctxt(
        _named_schemas: &mut HashMap<apache_avro::schema::Name, apache_avro::schema::Schema>,
        _enclosing_namespace: &apache_avro::schema::Namespace,
    ) -> apache_avro::Schema {
        // Return null schema since this field is never serialized
        apache_avro::Schema::Null
    }
}

impl apache_avro::schema::derive::AvroSchemaComponent for CutoutBytes {
    fn get_schema_in_ctxt(
        _named_schemas: &mut HashMap<apache_avro::schema::Name, apache_avro::schema::Schema>,
        _enclosing_namespace: &apache_avro::schema::Namespace,
    ) -> apache_avro::Schema {
        apache_avro::Schema::Bytes
    }
}

impl serde::Serialize for CutoutBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        apache_avro::serde_avro_bytes::serialize(&self.0, serializer)
    }
}

/// Enriched LSST alert
#[serdavro]
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct EnrichedLsstAlert {
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: LsstCandidate,
    pub prv_candidates: Vec<LsstPhotometry>,
    pub fp_hists: Vec<LsstPhotometry>,
    pub properties: LsstAlertProperties,
    #[serde(rename = "cutoutScience")]
    pub cutout_science: Option<CutoutBytes>,
    #[serde(rename = "cutoutTemplate")]
    pub cutout_template: Option<CutoutBytes>,
    #[serde(rename = "cutoutDifference")]
    pub cutout_difference: Option<CutoutBytes>,
    pub survey_matches: Option<crate::enrichment::lsst::LsstSurveyMatches>,
    // Not serialized - kept in memory for category computation only
    #[serde(skip)]
    #[allow(dead_code)] // Would show up unused erroneously
    pub cross_matches: CrossMatchesWrapper,
}

impl EnrichedLsstAlert {
    pub fn from_alert_properties_and_cutouts(
        alert: LsstAlertForEnrichment,
        cutout_science: Option<Vec<u8>>,
        cutout_template: Option<Vec<u8>>,
        cutout_difference: Option<Vec<u8>>,
        properties: LsstAlertProperties,
    ) -> Self {
        let cross_matches = CrossMatchesWrapper(alert.cross_matches);
        EnrichedLsstAlert {
            candid: alert.candid,
            object_id: alert.object_id,
            candidate: alert.candidate,
            prv_candidates: alert.prv_candidates,
            fp_hists: alert.fp_hists,
            properties,
            cutout_science: cutout_science.map(CutoutBytes),
            cutout_template: cutout_template.map(CutoutBytes),
            cutout_difference: cutout_difference.map(CutoutBytes),
            survey_matches: alert.survey_matches,
            cross_matches,
        }
    }

    pub fn compute_babamul_category(&self) -> String {
        // If we have a ZTF match, category starts with "ztf-match."
        // Otherwise, "no-ztf-match."
        let category = match &self.survey_matches {
            Some(survey_matches) => match &survey_matches.ztf {
                Some(_) => "ztf-match.".to_string(),
                None => "no-ztf-match.".to_string(),
            },
            None => "no-ztf-match.".to_string(),
        };

        // The enrichment worker already uses the LSPSC matches to classify stars
        // by creating 2 properties: star (bool) and near_brightstar (bool)
        if self.properties.star.unwrap_or(false) || self.properties.near_brightstar.unwrap_or(false)
        {
            return category + "stellar";
        }

        // Check if we have LSPSC cross-matches
        let empty_vec = vec![];
        let lspsc_matches = self
            .cross_matches
            .0
            .as_ref()
            .and_then(|xmatches| xmatches.get("LSPSC"))
            .unwrap_or(&empty_vec);

        // No matches: check if in footprint
        if lspsc_matches.is_empty() {
            return if is_in_footprint(self.candidate.dia_source.ra, self.candidate.dia_source.dec) {
                category + "hostless"
            } else {
                category + "unknown"
            };
        }

        // Evaluate matches (stellar || near_brightstar > hosted > hostless).
        let mut label = "hostless";
        for m in lspsc_matches {
            let distance = match m.get("distance_arcsec").and_then(|v| v.as_f64()) {
                Some(d) => d,
                None => continue,
            };
            let score = match m.get("score").and_then(|v| v.as_f64()) {
                Some(s) => s,
                None => continue,
            };
            if score < IS_HOSTED_SCORE_THRESH {
                label = "hosted";
            }
            if distance <= IS_STELLAR_DISTANCE_THRESH_ARCSEC && score > IS_HOSTED_SCORE_THRESH {
                label = "stellar";
                break;
            }
            let mag = match m.get("mag_white").and_then(|v| v.as_f64()) {
                Some(m) => m,
                None => continue,
            };
            if distance <= IS_NEAR_BRIGHTSTAR_DISTANCE_THRESH_ARCSEC
                && mag <= IS_NEAR_BRIGHTSTAR_MAG_THRESH
            {
                label = "stellar";
                break;
            }
        }
        category + label
    }
}

/// Enriched ZTF alert
#[serdavro]
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct EnrichedZtfAlert {
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: ZtfCandidate,
    pub prv_candidates: Vec<ZtfPhotometry>,
    pub prv_nondetections: Vec<ZtfPhotometry>,
    pub fp_hists: Vec<ZtfPhotometry>,
    pub properties: ZtfAlertProperties,
    pub survey_matches: Option<crate::enrichment::ztf::ZtfSurveyMatches>,
    #[serde(rename = "cutoutScience")]
    pub cutout_science: Option<CutoutBytes>,
    #[serde(rename = "cutoutTemplate")]
    pub cutout_template: Option<CutoutBytes>,
    #[serde(rename = "cutoutDifference")]
    pub cutout_difference: Option<CutoutBytes>,
}

impl EnrichedZtfAlert {
    pub fn from_alert_properties_and_cutouts(
        alert: ZtfAlertForEnrichment,
        cutout_science: Option<Vec<u8>>,
        cutout_template: Option<Vec<u8>>,
        cutout_difference: Option<Vec<u8>>,
        properties: ZtfAlertProperties,
    ) -> Self {
        EnrichedZtfAlert {
            survey_matches: alert.survey_matches,
            candid: alert.candid,
            object_id: alert.object_id,
            candidate: alert.candidate,
            prv_candidates: alert.prv_candidates,
            prv_nondetections: alert.prv_nondetections,
            fp_hists: alert.fp_hists,
            properties,
            cutout_science: cutout_science.map(CutoutBytes),
            cutout_template: cutout_template.map(CutoutBytes),
            cutout_difference: cutout_difference.map(CutoutBytes),
        }
    }

    pub fn compute_babamul_category(&self) -> String {
        // If we have an LSST match, category starts with "lsst-match."
        // Otherwise, "no-lsst-match."
        let category = match &self.survey_matches {
            Some(survey_matches) => match &survey_matches.lsst {
                Some(_) => "lsst-match.".to_string(),
                None => "no-lsst-match.".to_string(),
            },
            None => "no-lsst-match.".to_string(),
        };

        // Already classified as stellar (by the enrichment worker), return that
        if self.properties.star {
            return category + "stellar";
        }

        // Check star-galaxy scores (sgscore1, sgscore2, sgscore3) to determine if hosted
        // TODO: Confirm the catalog has full ZTF footprint coverage
        // Scores < 0.5 (and >= 0) indicate galaxy-like objects (hosted transients)
        // Negative values (-99, etc.) are ZTF pipeline placeholders for "no match"
        let sgscores = [
            self.candidate.candidate.sgscore1,
            self.candidate.candidate.sgscore2,
            self.candidate.candidate.sgscore3,
        ];

        for score in sgscores.iter().flatten() {
            // Only consider valid scores (>= 0)
            if *score >= 0.0 && *score < ZTF_HOSTED_SG_SCORE_THRESH {
                return category + "hosted";
            }
        }

        // Not a star and no valid sgscores, so classify as hostless
        category + "hostless"
    }
}

enum EnrichedAlert<'a> {
    Lsst(&'a EnrichedLsstAlert),
    Ztf(&'a EnrichedZtfAlert),
}

pub struct Babamul {
    kafka_producer: rdkafka::producer::FutureProducer,
    lsst_avro_schema: Schema,
    ztf_avro_schema: Schema,
    kafka_admin_client: AdminClient<DefaultClientContext>,
    topic_retention_ms: i64,
    // A cache for Kafka topics we've checked exist and match retention policy
    checked_topics: Mutex<HashSet<String>>,
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

        // Generate Avro schemas
        let lsst_avro_schema = EnrichedLsstAlert::get_schema();
        let ztf_avro_schema = EnrichedZtfAlert::get_schema();

        // Create Kafka Admin client
        let admin_client: AdminClient<DefaultClientContext> = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", &kafka_producer_host)
            .create()
            .expect("Failed to create Babamul Kafka admin client");

        // Compute retention in milliseconds from config (days)
        let babamul_retention_ms: i64 =
            (config.babamul.retention_days as i64) * 24 * 60 * 60 * 1000;

        Babamul {
            kafka_producer,
            lsst_avro_schema,
            ztf_avro_schema,
            kafka_admin_client: admin_client,
            topic_retention_ms: babamul_retention_ms,
            checked_topics: Mutex::new(HashSet::new()),
        }
    }

    #[instrument(skip_all, err)]
    async fn send_alerts_by_topic<T, F>(
        &self,
        alerts_by_topic: HashMap<String, Vec<T>>,
        to_enriched: F,
    ) -> Result<usize, EnrichmentWorkerError>
    where
        for<'a> F: Fn(&'a T) -> EnrichedAlert<'a>,
    {
        let mut total_sent: usize = 0;
        for (topic_name, alerts) in alerts_by_topic {
            tracing::info!("Sending {} alerts to topic {}", alerts.len(), topic_name);

            // Check topic exists with the desired retention policy
            self.check_topic(&topic_name).await?;

            // Convert alerts to Avro payloads
            let mut payloads = Vec::new();
            for alert in &alerts {
                let payload = self.alert_to_avro_bytes(to_enriched(alert))?;
                payloads.push(payload);
            }

            info!(
                "Prepared {} payloads for topic {}",
                payloads.len(),
                topic_name
            );

            // Send all messages to Kafka without awaiting immediately (allow batching)
            let mut send_futures = Vec::new();
            for payload in &payloads {
                let record: rdkafka::producer::FutureRecord<'_, (), Vec<u8>> =
                    rdkafka::producer::FutureRecord::to(&topic_name).payload(payload);
                let future = self
                    .kafka_producer
                    .send(record, std::time::Duration::from_secs(15));
                send_futures.push(future);
            }

            // Await all sends and map errors
            for send_result in send_futures {
                send_result.await.map_err(|(e, _)| {
                    EnrichmentWorkerError::Kafka(format!("Failed to send to Kafka: {}", e))
                })?;
                total_sent += 1;
            }
        }

        Ok(total_sent)
    }

    #[instrument(skip_all, err)]
    fn alert_to_avro_bytes(&self, alert: EnrichedAlert) -> Result<Vec<u8>, EnrichmentWorkerError> {
        match alert {
            EnrichedAlert::Lsst(a) => {
                let mut writer = Writer::with_codec(
                    &self.lsst_avro_schema,
                    Vec::new(),
                    apache_avro::Codec::Snappy,
                );
                writer
                    .append_serdavro(a)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
                writer
                    .into_inner()
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))
            }
            EnrichedAlert::Ztf(a) => {
                let mut writer = Writer::with_codec(
                    &self.ztf_avro_schema,
                    Vec::new(),
                    apache_avro::Codec::Snappy,
                );
                writer
                    .append_serdavro(a)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
                writer
                    .into_inner()
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))
            }
        }
    }

    /// Ensure the given topic exists with the desired retention policy
    #[instrument(skip_all, err)]
    async fn check_topic(&self, topic_name: &str) -> Result<(), EnrichmentWorkerError> {
        // Fast-path: skip if already checked in this process
        {
            let checked = self.checked_topics.lock().await;
            if checked.contains(topic_name) {
                return Ok(());
            }
        }

        // Create topic with retention if it does not exist; ignore "already exists" errors
        let retention_ms_string = self.topic_retention_ms.to_string();
        let new_topic = NewTopic::new(topic_name, 1, TopicReplication::Fixed(1))
            .set("retention.ms", &retention_ms_string)
            .set("cleanup.policy", "delete");

        let opts = AdminOptions::new();
        let results: Vec<Result<String, (String, RDKafkaErrorCode)>> = self
            .kafka_admin_client
            .create_topics(&[new_topic], &opts)
            .await
            .map_err(|e| {
                EnrichmentWorkerError::Kafka(format!("Admin create_topics error: {}", e))
            })?;

        for r in results.into_iter() {
            match r {
                Ok(_created) => (),
                Err((_topic, code)) => {
                    if code == RDKafkaErrorCode::TopicAlreadyExists {
                        // Continue to alter configs below
                        continue;
                    }
                    return Err(EnrichmentWorkerError::Kafka(format!(
                        "Failed to ensure topic {} with retention (code: {:?})",
                        topic_name, code
                    )));
                }
            }
        }
        // Apply or update retention policy even if topic already existed
        let mut entries: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
        entries.insert("retention.ms", &retention_ms_string);
        entries.insert("cleanup.policy", "delete");
        let alter = AlterConfig {
            specifier: ResourceSpecifier::Topic(topic_name),
            entries,
        };
        let alter_results = self
            .kafka_admin_client
            .alter_configs(&[alter], &opts)
            .await
            .map_err(|e| {
                EnrichmentWorkerError::Kafka(format!("Admin alter_configs error: {}", e))
            })?;
        for res in alter_results {
            if let Err((_spec, code)) = res {
                return Err(EnrichmentWorkerError::Kafka(format!(
                    "Failed to set retention for {} (code: {:?})",
                    topic_name, code
                )));
            }
        }

        // Record that we've checked this topic during this process lifetime
        let mut checked = self.checked_topics.lock().await;
        checked.insert(topic_name.to_string());
        Ok(())
    }

    #[instrument(skip_all, err)]
    pub async fn process_lsst_alerts(
        &self,
        alerts: Vec<EnrichedLsstAlert>,
    ) -> Result<usize, EnrichmentWorkerError> {
        // Create a hash map for alerts to send to each topic
        let mut alerts_by_topic: HashMap<String, Vec<EnrichedLsstAlert>> = HashMap::new();

        // Determine if this alert is worth sending to Babamul
        let min_reliability = 0.5;

        // Iterate over the alerts
        for mut alert in alerts {
            // Filter ZTF matches to only include public (programid=1)
            if let Some(ref mut survey_matches) = alert.survey_matches {
                if let Some(ref mut ztf_match) = survey_matches.ztf {
                    ztf_match.prv_candidates.retain(|p| p.programid == 1);
                    ztf_match.prv_nondetections.retain(|p| p.programid == 1);
                    ztf_match.fp_hists.retain(|p| p.programid == 1);
                }
            }

            if alert.candidate.dia_source.reliability.unwrap_or(0.0) < min_reliability
                || alert.candidate.dia_source.pixel_flags.unwrap_or(false)
                || alert.properties.rock
            {
                // Skip this alert, it doesn't meet the criteria
                continue;
            }

            // Compute the category for this alert to determine the topic
            let category = alert.compute_babamul_category();
            let topic_name = format!("babamul.lsst.{}", category);
            alerts_by_topic
                .entry(topic_name)
                .or_insert_with(Vec::new)
                .push(alert);
        }

        // If there is nothing to send, return early
        if alerts_by_topic.is_empty() {
            info!("No LSST alerts to send to Babamul");
            return Ok(0);
        }

        // Send all grouped alerts using shared helper
        self.send_alerts_by_topic(alerts_by_topic, |a| EnrichedAlert::Lsst(a))
            .await
    }

    #[instrument(skip_all, err)]
    pub async fn process_ztf_alerts(
        &self,
        alerts: Vec<EnrichedZtfAlert>,
    ) -> Result<usize, EnrichmentWorkerError> {
        // Create a hash map for alerts to send to each topic
        // For now, we will just send all alerts to "babamul.none"
        // In the future, we will determine the topic based on the alert properties
        let mut alerts_by_topic: HashMap<String, Vec<EnrichedZtfAlert>> = HashMap::new();

        // Iterate over the alerts
        for mut alert in alerts {
            // Only send public ZTF alerts (programid=1) to Babamul
            if alert.candidate.candidate.programid != 1 || alert.properties.rock {
                // Skip this alert, it doesn't meet the criteria
                continue;
            }

            // Filter photometry to only include public (programid=1)
            alert.prv_candidates.retain(|p| p.programid == 1);
            alert.prv_nondetections.retain(|p| p.programid == 1);
            alert.fp_hists.retain(|p| p.programid == 1);

            // Determine which topic this alert should go to
            let category: String = alert.compute_babamul_category();
            let topic_name = format!("babamul.ztf.{}", category);
            alerts_by_topic
                .entry(topic_name)
                .or_insert_with(Vec::new)
                .push(alert);
        }

        for (topic_name, alerts) in &alerts_by_topic {
            info!("Prepared {} alerts for topic {}", alerts.len(), topic_name);
        }

        // If there is nothing to send, return early
        if alerts_by_topic.is_empty() {
            info!("No ZTF alerts to send to Babamul");
            return Ok(0);
        }

        // Send all grouped alerts using shared helper
        self.send_alerts_by_topic(alerts_by_topic, |a| EnrichedAlert::Ztf(a))
            .await
    }
}
