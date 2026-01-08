//! Babamul is an optional component of ZTF and LSST enrichment pipelines,
//! which sends enriched alerts to various Kafka topics for public consumption.
use crate::alert::{LsstCandidate, ZtfCandidate};
use crate::conf::AppConfig;
use crate::enrichment::lsst::{LsstAlertForEnrichment, LsstAlertProperties};
use crate::enrichment::ztf::{ZtfAlertForEnrichment, ZtfAlertProperties};
use crate::enrichment::{EnrichmentWorkerError, LsstPhotometry, ZtfPhotometry};
use crate::utils::derive_avro_schema::SerdavroWriter;
use apache_avro::{AvroSchema, Schema, Writer};
use apache_avro_macros::serdavro;
use cdshealpix::nested::get;
use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType, MocType};
use moc::moc::range::RangeMOC;
use moc::moc::{CellMOCIntoIterator, CellMOCIterator, HasMaxDepth};
use moc::qty::Hpx;
use std::collections::HashMap;
use std::sync::OnceLock;
use tracing::{info, instrument};

pub const IS_HOSTED_SCORE_THRESH: f64 = 0.5;
const MOC_FOOTPRINT_PATH: &str = "./data/ls_footprint_moc.fits";
const MOC_DEPTH: u8 = 11;

// Lazy-loaded footprint MOC
static FOOTPRINT_MOC: OnceLock<Option<RangeMOC<u64, Hpx<u64>>>> = OnceLock::new();

fn load_footprint_moc() -> Option<RangeMOC<u64, Hpx<u64>>> {
    let file = match std::fs::File::open(MOC_FOOTPRINT_PATH) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("Failed to open footprint MOC file: {}", e);
            return None;
        }
    };

    let reader = std::io::BufReader::new(file);
    match from_fits_ivoa(reader) {
        Ok(MocIdxType::U64(MocQtyType::Hpx(MocType::Ranges(moc)))) => {
            Some(RangeMOC::new(moc.depth_max(), moc.collect()))
        }
        Ok(MocIdxType::U64(MocQtyType::Hpx(MocType::Cells(cell_moc)))) => {
            let depth = cell_moc.depth_max();
            let ranges = cell_moc.into_cell_moc_iter().ranges().collect();
            Some(RangeMOC::new(depth, ranges))
        }
        Ok(_) => {
            tracing::warn!("Unexpected MOC type");
            None
        }
        Err(e) => {
            tracing::warn!("Failed to parse footprint MOC: {}", e);
            None
        }
    }
}

fn is_in_footprint(ra_deg: f64, dec_deg: f64) -> bool {
    let moc = FOOTPRINT_MOC.get_or_init(load_footprint_moc);
    if let Some(moc) = moc {
        let ra_rad = ra_deg.to_radians();
        let dec_rad = dec_deg.to_radians();
        let layer = get(MOC_DEPTH);
        let cell = layer.hash(ra_rad, dec_rad);
        moc.contains_cell(MOC_DEPTH, cell)
    } else {
        false
    }
}

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
    #[serde(rename = "_id")]
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
        let category = if let Some(survey_matches) = &self.survey_matches {
            if survey_matches.ztf.is_some() {
                "ztf-match.".to_string()
            } else {
                "no-ztf-match.".to_string()
            }
        } else {
            "no-ztf-match.".to_string()
        };
        if self.properties.star {
            return category + "stellar";
        }
        // Check if we have LSSG cross-matches
        if let Some(xmatches) = &self.cross_matches.0 {
            if let Some(lssg_matches) = xmatches.get("LSSG") {
                if !lssg_matches.is_empty() {
                    // Check for stellar matches: distance ≤ 1.0 arcsec AND score > 0.5
                    if lssg_matches.iter().any(|m| {
                        let distance = m
                            .get("distance_arcsec")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(f64::MAX);
                        let score = m.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        distance <= 1.0 && score > IS_HOSTED_SCORE_THRESH
                    }) {
                        return category + "stellar";
                    }
                    // Check if any match has score below threshold (hosted)
                    if lssg_matches.iter().any(|m| {
                        m.get("score")
                            .and_then(|v| v.as_f64())
                            .map_or(false, |s| s < IS_HOSTED_SCORE_THRESH)
                    }) {
                        return category + "hosted";
                    }
                    // Matches exist but none are stellar or hosted
                    return category + "hostless";
                }
            }
        }

        // No matches: check if in footprint
        let in_footprint =
            is_in_footprint(self.candidate.dia_source.ra, self.candidate.dia_source.dec);
        if in_footprint {
            category + "hostless"
        } else {
            category + "unknown"
        }
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
}

enum EnrichedAlert<'a> {
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

        // Generate Avro schemas
        let lsst_avro_schema = EnrichedLsstAlert::get_schema();
        let ztf_avro_schema = EnrichedZtfAlert::get_schema();

        Babamul {
            kafka_producer,
            lsst_avro_schema,
            ztf_avro_schema,
        }
    }

    #[instrument(skip_all, err)]
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
            }
        }

        Ok(())
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

    #[instrument(skip_all, err)]
    pub async fn process_lsst_alerts(
        &self,
        alerts: Vec<EnrichedLsstAlert>,
    ) -> Result<(), EnrichmentWorkerError> {
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
            return Ok(());
        }

        // Send all grouped alerts using shared helper
        self.send_alerts_by_topic(alerts_by_topic, |a| EnrichedAlert::Lsst(a))
            .await
    }

    #[instrument(skip_all, err)]
    pub async fn process_ztf_alerts(
        &self,
        alerts: Vec<EnrichedZtfAlert>,
    ) -> Result<(), EnrichmentWorkerError> {
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
            // Is it a star, galaxy, or none, and does it have an LSST crossmatch?
            // TODO: Get this implemented
            // For now, all ZTF alerts go to "babamul.ztf.none"
            let category: String = "none".to_string();
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
            return Ok(());
        }

        // Send all grouped alerts using shared helper
        self.send_alerts_by_topic(alerts_by_topic, |a| EnrichedAlert::Ztf(a))
            .await
    }
}
