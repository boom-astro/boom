//! Babamul is an optional component of ZTF and LSST enrichment pipelines,
//! which sends enriched alerts to various Kafka topics for public consumption.
use crate::alert::{LsstCandidate, ZtfCandidate};
use crate::conf::AppConfig;
use crate::enrichment::lsst::{
    is_in_footprint, LsstAlertForEnrichment, LsstAlertProperties, IS_HOSTED_SCORE_THRESH,
};
use crate::enrichment::ztf::{ZtfAlertForEnrichment, ZtfAlertProperties};
use crate::enrichment::EnrichmentWorkerError;
use crate::kafka::ensure_topic;
use crate::utils::{derive_avro_schema::SerdavroWriter, lightcurves::Band};
use apache_avro::{AvroSchema, Schema, Writer};
use apache_avro_macros::serdavro;
use rdkafka::producer::Producer;
use std::collections::HashMap;
use tracing::instrument;

const ZTF_HOSTED_SG_SCORE_THRESH: f32 = 0.5;
const LSST_MIN_RELIABILITY: f32 = 0.0; // TODO: Temporary value; update once an appropriate LSST reliability threshold is determined with the new reliability model
const ZTF_MIN_DRB: f32 = 0.2;
const ZTF_TOPIC_CATEGORIES: [&str; 6] = [
    "lsst-match.stellar",
    "lsst-match.hosted",
    "lsst-match.hostless",
    "no-lsst-match.stellar",
    "no-lsst-match.hosted",
    "no-lsst-match.hostless",
];
const LSST_TOPIC_CATEGORIES: [&str; 8] = [
    "ztf-match.stellar",
    "ztf-match.hosted",
    "ztf-match.hostless",
    "ztf-match.unknown",
    "no-ztf-match.stellar",
    "no-ztf-match.hosted",
    "no-ztf-match.hostless",
    "no-ztf-match.unknown",
];

#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AlertPhotometry {
    pub jd: f64,
    #[serde(rename = "psfFlux")]
    pub flux: f64, // in nJy
    #[serde(rename = "psfFluxErr")]
    pub flux_err: f64, // in nJy
    pub band: Band,
    pub ra: f64,
    pub dec: f64,
}

#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NonDetectionPhotometry {
    pub jd: f64,
    #[serde(rename = "psfFluxErr")]
    pub flux_err: f64, // in nJy
    pub band: Band,
}

#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ForcedPhotometry {
    pub jd: f64,
    #[serde(rename = "psfFlux")]
    pub flux: Option<f64>, // in nJy
    #[serde(rename = "psfFluxErr")]
    pub flux_err: f64, // in nJy
    pub band: Band,
}

#[serdavro]
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
pub struct BabamulSurveyMatch {
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub ra: f64,
    pub dec: f64,
    pub prv_candidates: Vec<AlertPhotometry>,
    pub prv_nondetections: Vec<NonDetectionPhotometry>,
    pub fp_hists: Vec<ForcedPhotometry>,
}

#[serdavro]
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
pub struct BabamulSurveyMatches {
    pub ztf: Option<BabamulSurveyMatch>,
    pub lsst: Option<BabamulSurveyMatch>,
}

impl Default for BabamulSurveyMatches {
    fn default() -> Self {
        BabamulSurveyMatches {
            ztf: None,
            lsst: None,
        }
    }
}

impl From<crate::enrichment::ztf::ZtfMatch> for BabamulSurveyMatch {
    fn from(ztf_match: crate::enrichment::ztf::ZtfMatch) -> Self {
        BabamulSurveyMatch {
            object_id: ztf_match.object_id,
            ra: ztf_match.ra,
            dec: ztf_match.dec,
            prv_candidates: ztf_match
                .prv_candidates
                .into_iter()
                .filter(|p| {
                    p.programid == 1 && p.flux.is_some() && p.ra.is_some() && p.dec.is_some()
                })
                .map(|p| AlertPhotometry {
                    jd: p.jd,
                    flux: p.flux.unwrap(),
                    flux_err: p.flux_err,
                    band: p.band,
                    ra: p.ra.unwrap(),
                    dec: p.dec.unwrap(),
                })
                .collect(),
            prv_nondetections: ztf_match
                .prv_nondetections
                .into_iter()
                .filter(|p| p.programid == 1)
                .map(|p| NonDetectionPhotometry {
                    jd: p.jd,
                    flux_err: p.flux_err,
                    band: p.band,
                })
                .collect(),
            fp_hists: ztf_match
                .fp_hists
                .into_iter()
                .filter(|p| p.programid == 1)
                .map(|p| ForcedPhotometry {
                    jd: p.jd,
                    flux: p.flux,
                    flux_err: p.flux_err,
                    band: p.band,
                })
                .collect(),
        }
    }
}

impl From<crate::enrichment::lsst::LsstMatch> for BabamulSurveyMatch {
    fn from(lsst_match: crate::enrichment::lsst::LsstMatch) -> Self {
        BabamulSurveyMatch {
            object_id: lsst_match.object_id,
            ra: lsst_match.ra,
            dec: lsst_match.dec,
            prv_candidates: lsst_match
                .prv_candidates
                .into_iter()
                // Only include measurements with valid flux and coordinates (should always be true for prv_candidates)
                .filter(|p| p.flux.is_some() && p.ra.is_some() && p.dec.is_some())
                .map(|p| AlertPhotometry {
                    jd: p.jd,
                    flux: p.flux.unwrap(),
                    flux_err: p.flux_err,
                    band: p.band,
                    ra: p.ra.unwrap(),
                    dec: p.dec.unwrap(),
                })
                .collect(),
            prv_nondetections: vec![], // LSST matches don't have nondetections
            fp_hists: lsst_match
                .fp_hists
                .into_iter()
                .map(|p| ForcedPhotometry {
                    jd: p.jd,
                    flux: p.flux,
                    flux_err: p.flux_err,
                    band: p.band,
                })
                .collect(),
        }
    }
}

impl From<Option<crate::enrichment::lsst::LsstSurveyMatches>> for BabamulSurveyMatches {
    fn from(opt: Option<crate::enrichment::lsst::LsstSurveyMatches>) -> Self {
        match opt {
            Some(lsst_matches) => {
                // for ztf matches, if we are left with an empty prv_candidates after filtering,
                // then it means we have no public alert to share for that match and we should not include it in Babamul
                BabamulSurveyMatches {
                    ztf: lsst_matches
                        .ztf
                        .map(|m| m.into())
                        .filter(|m: &BabamulSurveyMatch| !m.prv_candidates.is_empty()),
                    lsst: None, // Naturally, no LSST match for an LSST alert
                }
            }
            None => BabamulSurveyMatches::default(),
        }
    }
}

impl From<Option<crate::enrichment::ztf::ZtfSurveyMatches>> for BabamulSurveyMatches {
    fn from(opt: Option<crate::enrichment::ztf::ZtfSurveyMatches>) -> Self {
        match opt {
            Some(ztf_matches) => BabamulSurveyMatches {
                ztf: None, // Naturally, no ZTF match for a ZTF alert
                lsst: ztf_matches.lsst.map(|m| m.into()),
            },
            None => BabamulSurveyMatches::default(),
        }
    }
}

/// Enriched LSST alert
#[serdavro]
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct BabamulLsstAlert {
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: LsstCandidate,
    pub prv_candidates: Vec<AlertPhotometry>,
    pub fp_hists: Vec<ForcedPhotometry>,
    pub properties: LsstAlertProperties,
    pub survey_matches: BabamulSurveyMatches,
}

impl BabamulLsstAlert {
    pub fn from_alert_and_properties(
        alert: LsstAlertForEnrichment,
        properties: LsstAlertProperties,
    ) -> (
        Self,
        std::collections::HashMap<String, Vec<serde_json::Value>>,
    ) {
        (
            BabamulLsstAlert {
                candid: alert.candid,
                object_id: alert.object_id,
                candidate: alert.candidate,
                prv_candidates: alert
                    .prv_candidates
                    .into_iter()
                    // Only include measurements with valid flux and coordinates (should always be true for prv_candidates)
                    .filter(|p| p.flux.is_some() && p.ra.is_some() && p.dec.is_some())
                    .map(|p| AlertPhotometry {
                        jd: p.jd,
                        flux: p.flux.unwrap(),
                        flux_err: p.flux_err,
                        band: p.band,
                        ra: p.ra.unwrap(),
                        dec: p.dec.unwrap(),
                    })
                    .collect(),
                fp_hists: alert
                    .fp_hists
                    .into_iter()
                    .map(|p| ForcedPhotometry {
                        jd: p.jd,
                        flux: p.flux,
                        flux_err: p.flux_err,
                        band: p.band,
                    })
                    .collect(),
                properties,
                survey_matches: alert.survey_matches.into(),
            },
            alert.cross_matches.unwrap_or_default(),
        )
    }

    pub fn compute_babamul_category(
        &self,
        cross_matches: &std::collections::HashMap<String, Vec<serde_json::Value>>,
    ) -> String {
        // If we have a ZTF match, category starts with "ztf-match."
        // Otherwise, "no-ztf-match."
        let category = if self.survey_matches.ztf.is_some() {
            "ztf-match.".to_string()
        } else {
            "no-ztf-match.".to_string()
        };

        // The enrichment worker already uses the LSPSC matches to classify stars
        // by creating 2 properties: star (bool) and near_brightstar (bool)
        if self.properties.star.unwrap_or(false) || self.properties.near_brightstar.unwrap_or(false)
        {
            return category + "stellar";
        }

        // Check if we have LSPSC cross-matches
        let empty_vec = vec![];
        let lspsc_matches = cross_matches.get("LSPSC").unwrap_or(&empty_vec);

        // No matches: check if in footprint
        if lspsc_matches.is_empty() {
            return if is_in_footprint(self.candidate.dia_source.ra, self.candidate.dia_source.dec) {
                category + "hostless"
            } else {
                category + "unknown"
            };
        }

        // Evaluate matches (hosted > hostless).
        // Stellar was already evaluated in the enrichment worker
        // and used above to break early with "stellar" classification.
        let mut label = "hostless";
        for m in lspsc_matches {
            let score = match m.get("score").and_then(|v| v.as_f64()) {
                Some(s) => s,
                None => continue,
            };
            if score <= IS_HOSTED_SCORE_THRESH {
                label = "hosted";
                break;
            }
        }
        category + label
    }
}

/// Enriched ZTF alert
#[serdavro]
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct BabamulZtfAlert {
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: ZtfCandidate,
    pub prv_candidates: Vec<AlertPhotometry>,
    pub prv_nondetections: Vec<NonDetectionPhotometry>,
    pub fp_hists: Vec<ForcedPhotometry>,
    pub properties: ZtfAlertProperties,
    pub survey_matches: BabamulSurveyMatches,
}

impl BabamulZtfAlert {
    pub fn from_alert_and_properties(
        alert: ZtfAlertForEnrichment,
        properties: ZtfAlertProperties,
    ) -> Self {
        BabamulZtfAlert {
            candid: alert.candid,
            object_id: alert.object_id,
            candidate: alert.candidate,
            // for prv_candidates, prv_nondetections, and fp_hists, we only include entries with programid=1 (public ZTF data)
            prv_candidates: alert
                .prv_candidates
                .into_iter()
                // Only include measurements with valid flux and coordinates (should always be true for prv_candidates)
                .filter(|p| {
                    p.programid == 1 && p.flux.is_some() && p.ra.is_some() && p.dec.is_some()
                })
                .map(|p| AlertPhotometry {
                    jd: p.jd,
                    flux: p.flux.unwrap(),
                    flux_err: p.flux_err,
                    band: p.band,
                    ra: p.ra.unwrap(),
                    dec: p.dec.unwrap(),
                })
                .collect(),
            prv_nondetections: alert
                .prv_nondetections
                .into_iter()
                .filter(|p| p.programid == 1)
                .map(|p| NonDetectionPhotometry {
                    jd: p.jd,
                    flux_err: p.flux_err,
                    band: p.band,
                })
                .collect(),
            fp_hists: alert
                .fp_hists
                .into_iter()
                .filter(|p| p.programid == 1)
                .map(|p| ForcedPhotometry {
                    jd: p.jd,
                    flux: p.flux,
                    flux_err: p.flux_err,
                    band: p.band,
                })
                .collect(),
            properties,
            survey_matches: alert.survey_matches.into(),
        }
    }

    pub fn compute_babamul_category(&self) -> String {
        // If we have an LSST match, category starts with "lsst-match."
        // Otherwise, "no-lsst-match."
        let category = if self.survey_matches.lsst.is_some() {
            "lsst-match.".to_string()
        } else {
            "no-lsst-match.".to_string()
        };

        // Already classified as stellar (by the enrichment worker), return that
        if self.properties.star || self.properties.near_brightstar {
            return category + "stellar";
        }

        // Check star-galaxy scores (sgscore1, sgscore2, sgscore3) to determine if hosted
        // TODO: Confirm the catalog has full ZTF footprint coverage
        // Scores <= 0.5 (and >= 0) indicate galaxy-like objects (hosted transients)
        // Negative values (-99, etc.) are ZTF pipeline placeholders for "no match"
        let sgscores = [
            self.candidate.candidate.sgscore1,
            self.candidate.candidate.sgscore2,
            self.candidate.candidate.sgscore3,
        ];

        for score in sgscores.iter().flatten() {
            // Only consider valid scores (>= 0)
            if *score >= 0.0 && *score <= ZTF_HOSTED_SG_SCORE_THRESH {
                return category + "hosted";
            }
        }

        // Not a star and no valid sgscores, so classify as hostless
        category + "hostless"
    }
}

enum EnrichedAlert<'a> {
    Lsst(&'a BabamulLsstAlert),
    Ztf(&'a BabamulZtfAlert),
}

pub struct Babamul {
    kafka_producer: rdkafka::producer::FutureProducer,
    lsst_avro_schema: Schema,
    ztf_avro_schema: Schema,
    kafka_bootstrap_servers: String,
    // Retention in milliseconds for Babamul topics, derived from config retention_days
    topic_retention_ms: i64,
    // Number of partitions for Babamul topics, from config. Helps to scale to multiple producer instances.
    topic_partitions: i32,
    // Timeout for flushing the producer; since we do a single flush for the entire batch,
    // we can afford to set a higher timeout than if we flushed after every message
    producer_flush_timeout: std::time::Duration,
}

impl Babamul {
    pub async fn new(config: &AppConfig) -> Result<Self, EnrichmentWorkerError> {
        // Read Kafka producer config from kafka: producer in the config
        let kafka_producer_host = config.kafka.producer.server.clone();
        let producer_message_timeout_ms = config.babamul.producer_message_timeout_ms.to_string();
        let producer_batch_size = config.babamul.producer_batch_size.to_string();
        let producer_linger_ms = config.babamul.producer_linger_ms.to_string();
        let producer_max_in_flight_requests_per_connection = config
            .babamul
            .producer_max_in_flight_requests_per_connection
            .to_string();
        let producer_retries = config.babamul.producer_retries.to_string();

        // Create Kafka producer
        let kafka_producer: rdkafka::producer::FutureProducer =
            rdkafka::config::ClientConfig::new()
                // Uncomment the following to get logs from kafka (RUST_LOG doesn't work):
                // .set("debug", "broker,topic,msg")
                .set("bootstrap.servers", &kafka_producer_host)
                .set("message.timeout.ms", &producer_message_timeout_ms)
                // it's best to increase batch.size if the cluster
                // is running on another machine. Locally, lower means less
                // latency, since we are not limited by network speed anyways
                .set("batch.size", &producer_batch_size)
                .set("linger.ms", &producer_linger_ms)
                .set("acks", &config.babamul.producer_acks)
                .set(
                    "max.in.flight.requests.per.connection",
                    &producer_max_in_flight_requests_per_connection,
                )
                .set("retries", &producer_retries)
                .set(
                    "compression.type",
                    &config.babamul.producer_compression_type,
                )
                .create()
                .map_err(|e| {
                    EnrichmentWorkerError::Kafka(format!(
                        "Failed to create Babamul Kafka producer: {}",
                        e
                    ))
                })?;

        // Generate Avro schemas
        let lsst_avro_schema = BabamulLsstAlert::get_schema();
        let ztf_avro_schema = BabamulZtfAlert::get_schema();

        // Compute retention in milliseconds from config (days)
        let babamul_retention_ms: i64 =
            (config.babamul.retention_days as i64) * 24 * 60 * 60 * 1000;

        let babamul = Babamul {
            kafka_producer,
            lsst_avro_schema,
            ztf_avro_schema,
            kafka_bootstrap_servers: kafka_producer_host,
            topic_retention_ms: babamul_retention_ms,
            topic_partitions: config.babamul.topic_partitions,
            producer_flush_timeout: std::time::Duration::from_millis(
                config.babamul.producer_flush_timeout_ms as u64,
            ),
        };

        // Ensure topics are initialized at construction time so downstream
        // processing does not need to perform topic checks.
        babamul.initialize_ztf_topics().await?;
        babamul.initialize_lsst_topics().await?;

        Ok(babamul)
    }

    /// Ensure all ZTF topics exist with the correct retention policy. This should be called at startup before processing any alerts.
    #[instrument(skip_all, err)]
    pub async fn initialize_ztf_topics(&self) -> Result<(), EnrichmentWorkerError> {
        for category in ZTF_TOPIC_CATEGORIES {
            let topic_name = format!("babamul.ztf.{}", category);
            self.check_topic(&topic_name).await?;
        }
        Ok(())
    }

    /// Ensure all LSST topics exist with the correct retention policy. This should be called at startup before processing any alerts.
    #[instrument(skip_all, err)]
    pub async fn initialize_lsst_topics(&self) -> Result<(), EnrichmentWorkerError> {
        for category in LSST_TOPIC_CATEGORIES {
            let topic_name = format!("babamul.lsst.{}", category);
            self.check_topic(&topic_name).await?;
        }
        Ok(())
    }

    /// Helper function to send alerts to Kafka topics based on their category, with a single flush at the end for efficiency.
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
        let start_all_enqueue = std::time::Instant::now();

        // Reuse a single buffer across all alerts to reduce heap allocations.
        // Each alert gets serialized into this buffer, which is then cloned for sending.
        // The producer internally copies the data, so reusing the buffer is safe.
        let mut reusable_buffer = Vec::with_capacity(8192);

        // Enqueue all alerts across all topics without flushing yet.
        // This allows the producer to batch messages more efficiently.
        for (topic_name, alerts) in alerts_by_topic {
            tracing::debug!("Sending {} alerts to topic {}", alerts.len(), topic_name);

            for alert in &alerts {
                let payload =
                    self.alert_to_avro_bytes_with_buffer(to_enriched(alert), &mut reusable_buffer)?;
                let record: rdkafka::producer::FutureRecord<'_, (), Vec<u8>> =
                    rdkafka::producer::FutureRecord::to(&topic_name).payload(&payload);

                self.kafka_producer.send_result(record).map_err(|(e, _)| {
                    EnrichmentWorkerError::Kafka(format!("Failed to enqueue Kafka record: {}", e))
                })?;
                total_sent += 1;
            }
        }

        tracing::debug!(
            "Enqueued {} total payloads across all topics in {} ms",
            total_sent,
            start_all_enqueue.elapsed().as_millis()
        );

        // Single flush at the end for all topics.
        // This maximizes producer-side batching and reduces context switches.
        let start_flush = std::time::Instant::now();
        self.kafka_producer
            .flush(self.producer_flush_timeout)
            .map_err(|e| EnrichmentWorkerError::Kafka(format!("Kafka flush failed: {}", e)))?;

        tracing::debug!(
            "Flushed {} messages in {} ms",
            total_sent,
            start_flush.elapsed().as_millis()
        );

        Ok(total_sent)
    }

    /// Serialize an enriched alert to Avro bytes using the appropriate schema, reusing a provided buffer to minimize allocations.
    #[instrument(skip_all, err)]
    fn alert_to_avro_bytes_with_buffer(
        &self,
        alert: EnrichedAlert,
        buffer: &mut Vec<u8>,
    ) -> Result<Vec<u8>, EnrichmentWorkerError> {
        buffer.clear(); // Reuse the buffer; keep its capacity
        match alert {
            EnrichedAlert::Lsst(a) => {
                let mut writer = Writer::with_codec(
                    &self.lsst_avro_schema,
                    std::mem::take(buffer), // Take ownership; Writer will populate it
                    apache_avro::Codec::Null,
                );
                writer
                    .append_serdavro(a)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
                let result = writer
                    .into_inner()
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
                *buffer = result.clone(); // Clone result, store capacity back in buffer
                Ok(result)
            }
            EnrichedAlert::Ztf(a) => {
                let mut writer = Writer::with_codec(
                    &self.ztf_avro_schema,
                    std::mem::take(buffer),
                    apache_avro::Codec::Null,
                );
                writer
                    .append_serdavro(a)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
                let result = writer
                    .into_inner()
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?;
                *buffer = result.clone();
                Ok(result)
            }
        }
    }

    /// Ensure the given topic exists with the desired retention policy
    #[instrument(skip_all, err)]
    async fn check_topic(&self, topic_name: &str) -> Result<(), EnrichmentWorkerError> {
        let desired_partitions = usize::try_from(self.topic_partitions)
            .ok()
            .filter(|partitions| *partitions > 0)
            .ok_or_else(|| {
                EnrichmentWorkerError::Kafka(format!(
                    "Invalid topic_partitions for {}: {}. Must be > 0",
                    topic_name, self.topic_partitions
                ))
            })?;
        ensure_topic(
            &self.kafka_bootstrap_servers,
            topic_name,
            desired_partitions,
            Some(self.topic_retention_ms),
            Some("delete"),
        )
        .await
        .map_err(|e| {
            EnrichmentWorkerError::Kafka(format!("Failed to ensure topic {}: {}", topic_name, e))
        })?;

        Ok(())
    }

    #[instrument(skip_all, err)]
    pub async fn process_lsst_alerts(
        &self,
        alerts: Vec<(
            BabamulLsstAlert,
            std::collections::HashMap<String, Vec<serde_json::Value>>,
        )>,
    ) -> Result<usize, EnrichmentWorkerError> {
        // Create a hash map for alerts to send to each topic
        let mut alerts_by_topic: HashMap<String, Vec<BabamulLsstAlert>> = HashMap::new();

        // Iterate over the alerts
        for (alert, cross_matches) in alerts {
            if alert.candidate.dia_source.reliability.unwrap_or(0.0) < LSST_MIN_RELIABILITY
                || alert.candidate.dia_source.pixel_flags.unwrap_or(false)
                || alert.properties.rock
            {
                // Skip this alert, it doesn't meet the criteria
                continue;
            }

            // Compute the category for this alert to determine the topic
            let category = alert.compute_babamul_category(&cross_matches);
            let topic_name = format!("babamul.lsst.{}", category);
            alerts_by_topic
                .entry(topic_name)
                .or_insert_with(Vec::new)
                .push(alert);
        }

        for (topic_name, alerts) in &alerts_by_topic {
            tracing::debug!("Prepared {} alerts for topic {}", alerts.len(), topic_name);
        }

        // If there is nothing to send, return early
        if alerts_by_topic.is_empty() {
            tracing::debug!("No LSST alerts to send to Babamul");
            return Ok(0);
        }

        // Send all grouped alerts using shared helper
        let start = std::time::Instant::now();
        let send_results = self
            .send_alerts_by_topic(alerts_by_topic, |a| EnrichedAlert::Lsst(a))
            .await;
        tracing::info!(
            "Sent LSST alerts to Babamul in {} ms",
            start.elapsed().as_millis()
        );
        send_results
    }

    #[instrument(skip_all, err)]
    pub async fn process_ztf_alerts(
        &self,
        alerts: Vec<BabamulZtfAlert>,
    ) -> Result<usize, EnrichmentWorkerError> {
        // Create a hash map for alerts to send to each topic
        // For now, we will just send all alerts to "babamul.none"
        // In the future, we will determine the topic based on the alert properties
        let mut alerts_by_topic: HashMap<String, Vec<BabamulZtfAlert>> = HashMap::new();

        // Iterate over the alerts
        for alert in alerts {
            // Only send public ZTF alerts (programid=1), non-rocks, and with sufficient DRB to Babamul
            if alert.candidate.candidate.programid != 1
                || alert.properties.rock
                || alert.candidate.candidate.drb.unwrap_or(0.0) < ZTF_MIN_DRB
            {
                // Skip this alert, it doesn't meet the criteria
                continue;
            }

            // Determine which topic this alert should go to
            let category: String = alert.compute_babamul_category();
            let topic_name = format!("babamul.ztf.{}", category);
            alerts_by_topic
                .entry(topic_name)
                .or_insert_with(Vec::new)
                .push(alert);
        }

        for (topic_name, alerts) in &alerts_by_topic {
            tracing::debug!("Prepared {} alerts for topic {}", alerts.len(), topic_name);
        }

        // If there is nothing to send, return early
        if alerts_by_topic.is_empty() {
            tracing::debug!("No ZTF alerts to send to Babamul");
            return Ok(0);
        }

        // Send all grouped alerts using shared helper
        let start = std::time::Instant::now();
        let send_results = self
            .send_alerts_by_topic(alerts_by_topic, |a| EnrichedAlert::Ztf(a))
            .await;
        tracing::info!(
            "Sent ZTF alerts to Babamul in {} ms",
            start.elapsed().as_millis()
        );
        send_results
    }
}
