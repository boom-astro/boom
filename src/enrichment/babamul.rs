//! Babamul is an optional component of ZTF and LSST enrichment pipelines,
//! which sends enriched alerts to various Kafka topics for public consumption.
use crate::alert::{LsstCandidate, ZtfCandidate};
use crate::conf::AppConfig;
use crate::enrichment::lsst::{
    is_in_footprint, LsstAlertForEnrichment, LsstAlertProperties, IS_HOSTED_SCORE_THRESH,
};
use crate::enrichment::ztf::{ZtfAlertForEnrichment, ZtfAlertProperties};
use crate::enrichment::EnrichmentWorkerError;
use crate::utils::{derive_avro_schema::SerdavroWriter, lightcurves::Band};
use apache_avro::{AvroSchema, DeflateSettings, Schema, Writer};
use apache_avro_macros::serdavro;
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use std::collections::{HashMap, HashSet};
use tokio::sync::Mutex;
use tracing::instrument;

const ZTF_HOSTED_SG_SCORE_THRESH: f32 = 0.5;
const LSST_MIN_RELIABILITY: f32 = 0.5; // TODO: Temporary value; update once an appropriate LSST reliability threshold is determined with the new reliability model
const ZTF_MIN_DRB: f32 = 0.2;

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
        let lsst_avro_schema = BabamulLsstAlert::get_schema();
        let ztf_avro_schema = BabamulZtfAlert::get_schema();

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
            tracing::debug!("Sending {} alerts to topic {}", alerts.len(), topic_name);

            // Check topic exists with the desired retention policy
            self.check_topic(&topic_name).await?;

            // Convert alerts to Avro payloads
            let mut payloads = Vec::new();
            for alert in &alerts {
                let payload = self.alert_to_avro_bytes(to_enriched(alert))?;
                payloads.push(payload);
            }

            tracing::debug!(
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
                    apache_avro::Codec::Deflate(DeflateSettings::default()),
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
                    apache_avro::Codec::Deflate(DeflateSettings::default()),
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
                || alert.candidate.dia_source.psf_flux_flag.unwrap_or(false)
                || alert.candidate.dia_source.ap_flux_flag.unwrap_or(false)
                || alert.candidate.dia_source.pixel_flags.unwrap_or(false)
                || alert.candidate.dia_source.pixel_flags_bad.unwrap_or(false)
                || alert
                    .candidate
                    .dia_source
                    .pixel_flags_saturated
                    .unwrap_or(false)
                || alert.candidate.dia_source.shape_flag.unwrap_or(false)
                || alert.candidate.dia_source.centroid_flag.unwrap_or(false)
                || alert.candidate.dia_source.extendedness.is_none()
                || alert.candidate.dia_source.extendedness.unwrap_or(0_f32) == 1_f32
                || alert.candidate.snr_psf.unwrap_or(0.0) < 3.0
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
        self.send_alerts_by_topic(alerts_by_topic, |a| EnrichedAlert::Lsst(a))
            .await
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
        self.send_alerts_by_topic(alerts_by_topic, |a| EnrichedAlert::Ztf(a))
            .await
    }
}
