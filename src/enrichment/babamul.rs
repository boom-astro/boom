//! Babamul is an optional component of ZTF and LSST enrichment pipelines,
//! which sends enriched alerts to various Kafka topics for public consumption.

use crate::enrichment::lsst::EnrichedLsstAlert;
use crate::enrichment::ztf::EnrichedZtfAlert;
use crate::enrichment::EnrichmentWorkerError;
use apache_avro::{Schema, Writer};
use schemars::schema_for;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};
use tracing::error;

// Generate a small Avro schema from a Rust type inspected via `schemars`.
// We only include the fields used by Babamul (the fields that are put into
// the Avro record). Complex nested Rust types (objects/arrays) are mapped
// to Avro `string` since we serialize those nested values as JSON strings
// when producing the Avro payload.
// Convert a schemars JSON Schema node to an Avro JSON type.
fn json_schema_node_to_avro(
    node: &JsonValue,
    definitions: Option<&JsonValue>,
    name_hint: &str,
    seen_defs: &mut BTreeMap<String, JsonValue>,
) -> JsonValue {
    // Handle $ref
    if let Some(r) = node.get("$ref").and_then(|v| v.as_str()) {
        if let Some(def_name) = r.split('/').last() {
            if let Some(defs) = definitions {
                if let Some(def_node) = defs.get(def_name) {
                    if let Some(existing) = seen_defs.get(def_name) {
                        return existing.clone();
                    }
                    let rec = json_schema_node_to_avro(def_node, definitions, def_name, seen_defs);
                    seen_defs.insert(def_name.to_string(), rec.clone());
                    return rec;
                }
            }
        }
        return serde_json::json!("string");
    }

    // Handle anyOf / oneOf
    if let Some(any_of) = node.get("anyOf").or_else(|| node.get("oneOf")) {
        if let Some(arr) = any_of.as_array() {
            let mut sub_types: Vec<JsonValue> = Vec::new();
            let mut has_null = false;
            for variant in arr {
                if variant == &JsonValue::String("null".to_string())
                    || variant.get("type") == Some(&JsonValue::String("null".to_string()))
                {
                    has_null = true;
                    continue;
                }
                let t = json_schema_node_to_avro(variant, definitions, name_hint, seen_defs);
                sub_types.push(t);
            }
            if sub_types.len() == 1 && has_null {
                return serde_json::json!(["null", sub_types[0].clone()]);
            }
            return serde_json::json!("string");
        }
    }

    if let Some(t) = node.get("type") {
        let res = match t {
            JsonValue::String(s) => match s.as_str() {
                "integer" => serde_json::json!("long"),
                "number" => serde_json::json!("double"),
                "string" => serde_json::json!("string"),
                "boolean" => serde_json::json!("boolean"),
                "array" => {
                    let items = node.get("items").unwrap_or(&JsonValue::Null);
                    let items_avro =
                        json_schema_node_to_avro(items, definitions, name_hint, seen_defs);
                    serde_json::json!({"type": "array", "items": items_avro})
                }
                "object" => {
                    let rec_name = name_hint.to_string();
                    let mut fields: Vec<JsonValue> = Vec::new();
                    if let Some(props) = node.get("properties").and_then(|p| p.as_object()) {
                        let mut required: std::collections::HashSet<String> =
                            std::collections::HashSet::new();
                        if let Some(req) = node.get("required").and_then(|r| r.as_array()) {
                            for rn in req {
                                if let Some(s) = rn.as_str() {
                                    required.insert(s.to_string());
                                }
                            }
                        }
                        for (k, v) in props.iter() {
                            let child_name_hint = format!("{}_{}", rec_name, k);
                            let mut child_avro = json_schema_node_to_avro(
                                v,
                                definitions,
                                &child_name_hint,
                                seen_defs,
                            );
                            if !required.contains(k) {
                                child_avro = serde_json::json!(["null", child_avro]);
                            }
                            fields.push(serde_json::json!({"name": k, "type": child_avro}));
                        }
                    }
                    serde_json::json!({"type": "record", "name": rec_name, "fields": fields})
                }
                _ => serde_json::json!("string"),
            },
            JsonValue::Array(arr) => {
                let mut has_null = false;
                let mut primary: Option<&JsonValue> = None;
                for elem in arr {
                    if elem == &JsonValue::String("null".to_string()) {
                        has_null = true;
                    } else {
                        primary = Some(elem);
                    }
                }
                if let Some(p) = primary {
                    let mut av = json_schema_node_to_avro(p, definitions, name_hint, seen_defs);
                    if has_null {
                        av = serde_json::json!(["null", av]);
                    }
                    av
                } else {
                    serde_json::json!("string")
                }
            }
            _ => serde_json::json!("string"),
        };
        return res;
    }

    serde_json::json!("string")
}

fn avro_schema_from_type<T: schemars::JsonSchema>(record_name: &str) -> Schema {
    let root = schema_for!(T);
    let root_json = serde_json::to_value(&root).unwrap_or(JsonValue::Null);
    let schema_node = root_json.get("schema").unwrap_or(&root_json);
    let definitions = root_json.get("definitions");
    let props = schema_node.get("properties").and_then(|p| p.as_object());

    let field_names = vec![
        ("candid", "candid"),
        ("object_id", "objectId"),
        ("candidate", "candidate"),
        ("prv_candidates", "prv_candidates"),
        ("fp_hists", "fp_hists"),
        ("properties", "properties"),
    ];

    let mut fields: Vec<JsonValue> = Vec::new();
    let mut seen_defs: BTreeMap<String, JsonValue> = BTreeMap::new();

    for (rust_name, avro_name) in field_names {
        let node = props
            .and_then(|m| m.get(rust_name))
            .cloned()
            .unwrap_or(JsonValue::Null);
        let child_hint = format!("{}_{}", record_name, rust_name);
        let mut avro_type =
            json_schema_node_to_avro(&node, definitions, &child_hint, &mut seen_defs);

        let is_required = schema_node
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| arr.iter().any(|v| v.as_str() == Some(rust_name)))
            .unwrap_or(false);
        if !is_required {
            if !(avro_type.is_array() && avro_type.as_array().unwrap().iter().any(|v| v == "null"))
            {
                avro_type = serde_json::json!(["null", avro_type]);
            }
        }

        fields.push(serde_json::json!({"name": avro_name, "type": avro_type}));
    }

    let avro_obj = serde_json::json!({
        "type": "record",
        "name": record_name,
        "fields": fields,
    });

    Schema::parse_str(&avro_obj.to_string()).expect("Failed to parse generated Avro schema")
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

        // Generate the Avro schemas for enriched alerts from the Rust types
        // using schemars; this will include nested record and array types.
        let lsst_avro_schema = avro_schema_from_type::<EnrichedLsstAlert>("EnrichedLsstAlert");
        let ztf_avro_schema = avro_schema_from_type::<EnrichedZtfAlert>("EnrichedZtfAlert");

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
        let (
            schema,
            candid,
            object_id,
            candidate_json,
            prv_candidates_json,
            fp_hists_json,
            properties_json,
        ) = match alert {
            EnrichedAlert::Lsst(alert) => (
                &self.lsst_avro_schema,
                alert.candid,
                &alert.object_id,
                serde_json::to_string(&alert.candidate)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?,
                serde_json::to_string(&alert.prv_candidates)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?,
                serde_json::to_string(&alert.fp_hists)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?,
                serde_json::to_string(&alert.properties)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?,
            ),
            EnrichedAlert::Ztf(alert) => (
                &self.ztf_avro_schema,
                alert.candid,
                &alert.object_id,
                serde_json::to_string(&alert.candidate)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?,
                serde_json::to_string(&alert.prv_candidates)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?,
                serde_json::to_string(&alert.fp_hists)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?,
                serde_json::to_string(&alert.properties)
                    .map_err(|e| EnrichmentWorkerError::Serialization(e.to_string()))?,
            ),
        };

        // Create a simplified structure for Avro encoding
        let avro_record = serde_json::json!({
            "candid": candid,
            "objectId": object_id,
            "candidate": candidate_json,
            "prv_candidates": prv_candidates_json,
            "fp_hists": fp_hists_json,
            "properties": properties_json,
        });

        let mut writer = Writer::with_codec(schema, Vec::new(), apache_avro::Codec::Snappy);
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
