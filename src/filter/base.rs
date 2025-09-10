use crate::{
    conf,
    utils::{
        db::get_array_element,
        enums::Survey,
        o11y::metrics::SCHEDULER_METER,
        worker::{should_terminate, WorkerCmd},
    },
};

use std::{num::NonZero, sync::LazyLock};

use apache_avro::Schema;
use apache_avro::{serde_avro_bytes, Writer};
use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use opentelemetry::{
    metrics::{Counter, UpDownCounter},
    KeyValue,
};
use rdkafka::producer::FutureProducer;
use rdkafka::{config::ClientConfig, producer::FutureRecord};
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, trace, warn};
use uuid::Uuid;

// NOTE: Global instruments are defined here because reusing instruments is
// considered a best practice. See boom::alert::base.

// UpDownCounter for the number of alerts currently being processed by the filter workers.
static ACTIVE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .i64_up_down_counter("filter_worker.active")
        .with_unit("{alert}")
        .with_description("Number of alerts currently being processed by the filter worker.")
        .build()
});

// Counter for the number of alert batches processed by the filter workers.
static BATCH_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("filter_worker.batch.processed")
        .with_unit("{batch}")
        .with_description("Number of alert batches processed by the filter worker.")
        .build()
});

// Counter for the number of alerts processed by the filter workers.
static ALERT_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("filter_worker.alert.processed")
        .with_unit("{alert}")
        .with_description("Number of alerts processed by the filter worker.")
        .build()
});

// This is the schema of the avro object that we will send to kafka
// that includes the alert data and filter results
const ALERT_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "Alert",
    "fields": [
        {"name": "candid", "type": "long"},
        {"name": "objectId", "type": "string"},
        {"name": "jd", "type": "double"},
        {"name": "ra", "type": "double"},
        {"name": "dec", "type": "double"},
        {"name": "filters", "type": {
            "type": "array",
            "items": {
                "type": "record",
                "name": "FilterResults",
                "fields": [
                    {"name": "filter_id", "type": "string"},
                    {"name": "passed_at", "type": "double"},
                    {"name": "annotations", "type": "string"}
                ]
            }
        }},
        {"name": "classifications", "type": {
            "type": "array",
            "items": {
                "type": "record",
                "name": "Classification",
                "fields": [
                    {"name": "classifier", "type": "string"},
                    {"name": "score", "type": "double"}
                ]
            }
        }},
        {"name": "photometry", "type": {
            "type": "array",
            "items": {
                "type": "record",
                "name": "Photometry",
                "fields": [
                    {"name": "jd", "type": "double"},
                    {"name": "flux",  "type": ["null", "double"]},
                    {"name": "flux_err",  "type":"double"},
                    {"name":"band","type":"string"},
                    {"name":"zero_point","type":"double"},
                    {"name":"origin","type": "string"},
                    {"name":"programid","type":"int"},
                    {"name":"survey","type":"string"},
                    {"name":"ra","type":["null","double"]},
                    {"name":"dec","type":["null","double"]}
                ]
            }
        }},
        {"name":"cutoutScience","type":{"type":"bytes"}},
        {"name":"cutoutTemplate","type":{"type":"bytes"}},
        {"name":"cutoutDifference","type":{"type":"bytes"}},
        {"name":"survey","type":"string"}
    ]
}
"#;

#[derive(thiserror::Error, Debug)]
pub enum FilterError {
    #[error("value access error from bson")]
    BsonValueAccess(#[from] mongodb::bson::document::ValueAccessError),
    #[error("serialization error from bson")]
    BsonSerialization(#[from] mongodb::bson::ser::Error),
    #[error("error from mongodb")]
    Mongodb(#[from] mongodb::error::Error),
    #[error("error from serde_json")]
    SerdeJson(#[from] serde_json::Error),
    #[error("invalid filter permissions")]
    InvalidFilterPermissions,
    #[error("filter not found in database")]
    FilterNotFound,
    #[error("filter pipeline could not be parsed")]
    FilterPipelineError,
    #[error("invalid filter pipeline")]
    InvalidFilterPipeline,
    #[error("invalid filter id")]
    InvalidFilterId,
}

pub fn parse_programid_candid_tuple(tuple_str: &str) -> Option<(i32, i64)> {
    // We know that we have the programid first, followed by a comma, and then the candid.
    // the programid is always a single digit (0-9) and the candid is a larger number.
    // so we don't know to look for the comma to split the string.
    // and can directly use the indexes to read the values.
    // while this makes it very specific to this format, it is twice as fast.
    let first_part = &tuple_str[0..1];
    let second_part = &tuple_str[2..];
    let first = first_part.parse::<i32>();
    let second = second_part.parse::<i64>();
    if let (Ok(first_value), Ok(second_value)) = (first, second) {
        return Some((first_value, second_value));
    }
    None
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Origin {
    Alert,
    ForcedPhot,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Photometry {
    pub jd: f64,
    pub flux: Option<f64>,
    pub flux_err: f64,
    pub band: String,
    pub zero_point: f64,
    pub origin: Origin,
    pub programid: i32,
    pub survey: Survey,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Classification {
    pub classifier: String,
    pub score: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FilterResults {
    pub filter_id: String,
    pub passed_at: f64, // timestamp in seconds
    pub annotations: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Alert {
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub jd: f64,
    pub ra: f64,
    pub dec: f64,
    pub filters: Vec<FilterResults>,
    pub classifications: Vec<Classification>,
    pub photometry: Vec<Photometry>,
    #[serde(with = "serde_avro_bytes", rename = "cutoutScience")]
    pub cutout_science: Vec<u8>,
    #[serde(with = "serde_avro_bytes", rename = "cutoutTemplate")]
    pub cutout_template: Vec<u8>,
    #[serde(with = "serde_avro_bytes", rename = "cutoutDifference")]
    pub cutout_difference: Vec<u8>,
    pub survey: Survey,
}

pub fn load_schema(schema_str: &str) -> Result<Schema, FilterWorkerError> {
    let schema =
        Schema::parse_str(schema_str).inspect_err(|e| error!("Failed to parse schema: {}", e))?;

    Ok(schema)
}

pub fn load_alert_schema() -> Result<Schema, FilterWorkerError> {
    load_schema(ALERT_SCHEMA)
}

#[instrument(skip_all, err)]
pub fn to_avro_bytes<T>(value: &T, schema: &Schema) -> Result<Vec<u8>, FilterWorkerError>
where
    T: serde::Serialize,
{
    let mut writer = Writer::with_codec(schema, Vec::new(), apache_avro::Codec::Snappy);
    writer.append_ser(value).inspect_err(|e| {
        error!("Failed to serialize alert to Avro: {}", e);
    })?;
    let encoded = writer.into_inner().inspect_err(|e| {
        error!("Failed to finalize Avro writer: {}", e);
    })?;

    Ok(encoded)
}

#[instrument(skip(alert, schema), fields(candid = alert.candid, object_id = alert.object_id), err)]
pub fn alert_to_avro_bytes(alert: &Alert, schema: &Schema) -> Result<Vec<u8>, FilterWorkerError> {
    to_avro_bytes(alert, schema)
}

// TODO, use the config file to get the kafka server
pub async fn create_producer(
    kafka_config: &conf::SurveyKafkaConfig,
) -> Result<FutureProducer, FilterWorkerError> {
    let producer: FutureProducer = ClientConfig::new()
        // Uncomment the following to get logs from kafka (RUST_LOG doesn't work):
        // .set("debug", "broker,topic,msg")
        .set("bootstrap.servers", &kafka_config.producer)
        .set("message.timeout.ms", "5000")
        // it's best to increase batch.size if the cluster
        // is running on another machine. Locally, lower means less
        // latency, since we are not limited by network speed anyways
        .set("batch.size", "16384")
        .set("linger.ms", "5")
        .set("acks", "1")
        .set("max.in.flight.requests.per.connection", "5")
        .set("retries", "3")
        .create()?;

    Ok(producer)
}

#[instrument(skip(alert, schema, producer), fields(candid = alert.candid, object_id = alert.object_id), err)]
pub async fn send_alert_to_kafka(
    alert: &Alert,
    schema: &Schema,
    producer: &FutureProducer,
    topic: &str,
) -> Result<(), FilterWorkerError> {
    let encoded = alert_to_avro_bytes(alert, schema)?;

    let record: FutureRecord<'_, (), Vec<u8>> = FutureRecord::to(&topic).payload(&encoded);

    producer
        .send(record, std::time::Duration::from_secs(30))
        .await
        .map_err(|(e, _)| {
            warn!("Failed to send filter result to Kafka: {}", e);
            e
        })?;

    Ok(())
}

#[instrument(skip_all)]
pub fn uses_field_in_stage(stage: &serde_json::Value, field: &str) -> bool {
    // we consider a value is a match with field if it is:
    // - equal to the field
    // - equal to the field with a $ prefix
    // - starts with the field and a dot (for nested fields)
    // - starts with the field with a $ prefix and a dot
    // then we found it
    if let Some(array) = stage.as_array() {
        return array.iter().any(|item| uses_field_in_stage(item, field));
    } else if let Some(obj) = stage.as_object() {
        // The unwrap here is ok, the key was already a json value
        return obj
            .iter()
            .map(|(key, value)| (serde_json::to_value(key).unwrap(), value))
            .any(|(key, value)| {
                uses_field_in_stage(&key, field) || uses_field_in_stage(value, field)
            });
    } else if let Some(stage_str) = stage.as_str() {
        let stage_str = stage_str.trim();
        if stage_str == field
            || stage_str == &format!("${}", field)
            || stage_str.starts_with(&format!("{}.", field))
            || stage_str.starts_with(&format!("${}.", field))
        {
            return true;
        }
    }

    false
}

#[instrument(skip_all)]
pub fn uses_field_in_filter(filter_pipeline: &[serde_json::Value], field: &str) -> Option<usize> {
    for (i, stage) in filter_pipeline.iter().enumerate() {
        if uses_field_in_stage(stage, field) {
            return Some(i);
        }
    }
    None
}

#[instrument(skip_all, err)]
pub fn validate_filter_pipeline(filter_pipeline: &[serde_json::Value]) -> Result<(), FilterError> {
    // mongodb aggregation pipelines have project stages that can include or exclude fields,
    // (not both at the same time), and unset stages that remove fields.
    // We need the objectId and _id to always be present in the output
    // so we make sure that:
    // - project stages that are an include stages (no "field: 0") specify objectId: 1
    // - project stages that are an exclude stage (with "field: 0") do not mention objectId
    // - project stages do not exclude the _id field or objectId
    // - unset stages do not delete the objectId or _id fields
    // - we don't have any group, unwind, or lookup stages
    // - that the last stage is a project that includes objectId
    let nb_stages = filter_pipeline.len();
    for (i, stage) in filter_pipeline.iter().enumerate() {
        if stage.get("$group").is_some()
            || stage.get("$unwind").is_some()
            || stage.get("$lookup").is_some()
        {
            return Err(FilterError::InvalidFilterPipeline);
        }
        // check for project stages
        if stage.get("$project").is_some() {
            // dont convert to a string here, just look over key/values
            // we build the following variables:
            // - includes_object_id: bool, if the stage includes objectId
            // - excludes_object_id: bool, if the stage excludes objectId
            // - excludes_id: bool, if the stage excludes _id
            // - include_stage: bool, if the stage is an include stage (no "field: 0")
            let project_stage = stage.get("$project").unwrap();
            let mut includes_object_id = false;
            let mut excludes_object_id = false;
            let mut excludes_id = false;
            let mut include_stage = true;
            if let Some(project_obj) = project_stage.as_object() {
                for (key, value) in project_obj.iter() {
                    if key == "objectId" {
                        if value == &serde_json::Value::Number(1.into()) {
                            includes_object_id = true;
                        } else if value == &serde_json::Value::Number(0.into()) {
                            excludes_object_id = true;
                        }
                    } else if key == "_id" {
                        if value == &serde_json::Value::Number(0.into()) {
                            excludes_id = true;
                        }
                    } else if value == &serde_json::Value::Number(0.into()) {
                        include_stage = false;
                    }
                }
            }
            // make sure that _id is never excluded
            if excludes_id {
                return Err(FilterError::InvalidFilterPipeline);
            }
            // if it's an exclude, make sure that objectId is not excluded
            if !include_stage && excludes_object_id {
                return Err(FilterError::InvalidFilterPipeline);
            }
            // if it's an include, make sure that objectId is included
            if include_stage && !includes_object_id {
                return Err(FilterError::InvalidFilterPipeline);
            }
        }

        // check for unset stages
        if stage.get("$unset").is_some() {
            // unset can just be a string or an array of strings
            let unset_stage = stage.get("$unset").unwrap();
            if let Some(unset_array) = unset_stage.as_array() {
                for value in unset_array {
                    if value == &serde_json::Value::String("objectId".to_string())
                        || value == &serde_json::Value::String("_id".to_string())
                    {
                        return Err(FilterError::InvalidFilterPipeline);
                    }
                }
            } else if let Some(unset_str) = unset_stage.as_str() {
                if unset_str == "objectId" || unset_str == "_id" {
                    return Err(FilterError::InvalidFilterPipeline);
                }
            } else {
                return Err(FilterError::InvalidFilterPipeline);
            }
        }

        // check for the last stage
        if i == nb_stages - 1 {
            // the last stage must be a project stage that includes objectId
            if let Some(project_stage) = stage.get("$project") {
                if let Some(project_obj) = project_stage.as_object() {
                    if !project_obj.contains_key("objectId")
                        || project_obj.get("objectId") != Some(&serde_json::Value::Number(1.into()))
                    {
                        return Err(FilterError::InvalidFilterPipeline);
                    }
                } else {
                    return Err(FilterError::InvalidFilterPipeline);
                }
            } else {
                return Err(FilterError::InvalidFilterPipeline);
            }
        }
    }

    Ok(())
}

#[instrument(skip(filter_collection), err)]
pub async fn get_filter_object(
    filter_id: &str,
    catalog: &str,
    filter_collection: &mongodb::Collection<mongodb::bson::Document>,
) -> Result<Document, FilterError> {
    let mut filter_obj = filter_collection
        .aggregate(vec![
            doc! {
                "$match": doc! {
                    "_id": filter_id,
                    "active": true,
                    "catalog": catalog
                }
            },
            doc! {
                "$project": doc! {
                    "fv": doc! {
                        "$filter": doc! {
                            "input": "$fv",
                            "as": "x",
                            "cond": doc! {
                                "$eq": [
                                    "$$x.fid",
                                    "$active_fid"
                                ]
                            }
                        }
                    },
                    "group_id": 1,
                    "permissions": 1,
                    "catalog": 1
                }
            },
            doc! {
                "$project": doc! {
                    "pipeline": get_array_element("fv.pipeline"),
                    "group_id": 1,
                    "permissions": 1,
                    "catalog": 1
                }
            },
        ])
        .await?;

    let advance = filter_obj.advance().await?;
    let filter_obj = if advance {
        filter_obj.deserialize_current()?
    } else {
        return Err(FilterError::FilterNotFound);
    };

    Ok(filter_obj)
}

#[instrument(skip(candids, pipeline, alert_collection), err)]
pub async fn run_filter(
    candids: Vec<i64>,
    filter_id: &str,
    mut pipeline: Vec<Document>,
    alert_collection: &mongodb::Collection<Document>,
) -> Result<Vec<Document>, FilterError> {
    if candids.len() == 0 {
        return Ok(vec![]);
    }
    if pipeline.len() == 0 {
        panic!("filter pipeline is empty, ensure filter has been built before running");
    }

    // insert candids into filter
    pipeline[0].get_document_mut("$match")?.insert(
        "_id",
        doc! {
            "$in": candids
        },
    );

    // run filter
    let mut result = alert_collection.aggregate(pipeline).await?;

    let mut out_documents: Vec<Document> = Vec::new();

    while let Some(doc) = result.next().await {
        out_documents.push(doc?);
    }

    Ok(out_documents)
}

#[async_trait::async_trait]
pub trait Filter {
    async fn build(
        filter_id: &str,
        filter_collection: &mongodb::Collection<mongodb::bson::Document>,
    ) -> Result<Self, FilterError>
    where
        Self: Sized;
}

#[derive(thiserror::Error, Debug)]
pub enum FilterWorkerError {
    #[error("error from avro")]
    Avro(#[from] apache_avro::Error),
    #[error("value access error from bson")]
    BsonValueAccess(#[from] mongodb::bson::document::ValueAccessError),
    #[error("error from kafka")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("error from mongo")]
    Mongodb(#[from] mongodb::error::Error),
    #[error("error from redis")]
    Redis(#[from] redis::RedisError),
    #[error("error from serde_json")]
    SerdeJson(#[from] serde_json::Error),
    #[error("failed to load config")]
    LoadConfigError(#[from] crate::conf::BoomConfigError),
    #[error("filter error")]
    FilterError(#[from] FilterError),
    #[error("failed to get filter by queue")]
    GetFilterByQueueError,
    #[error("could not find alert")]
    AlertNotFound,
    #[error("filter not found")]
    FilterNotFound,
}

#[async_trait::async_trait]
pub trait FilterWorker {
    async fn new(
        config_path: &str,
        filter_ids: Option<Vec<String>>,
    ) -> Result<Self, FilterWorkerError>
    where
        Self: Sized;
    fn input_queue_name(&self) -> String;
    fn output_topic_name(&self) -> String;
    fn has_filters(&self) -> bool;
    fn survey() -> crate::utils::enums::Survey;
    async fn process_alerts(&mut self, alerts: &[String]) -> Result<Vec<Alert>, FilterWorkerError>;
}

#[tokio::main]
#[instrument(skip_all, err)]
pub async fn run_filter_worker<T: FilterWorker>(
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
    worker_id: Uuid,
) -> Result<(), FilterWorkerError> {
    debug!(?config_path);

    let config = conf::load_config(config_path)?;
    let kafka_config = conf::build_kafka_config(&config, &T::survey())
        .inspect_err(|e| error!("Failed to build Kafka config: {}", e))?;

    let mut filter_worker = T::new(config_path, None).await?;

    if !filter_worker.has_filters() {
        info!("no filters available for processing");
        return Ok(());
    }

    // in a never ending loop, loop over the queues
    let mut con = conf::build_redis(&config).await?;

    let input_queue = filter_worker.input_queue_name();
    let output_topic = filter_worker.output_topic_name();

    let producer = create_producer(&kafka_config).await?;
    let schema = load_alert_schema()?;

    let command_interval: usize = 500;
    let mut command_check_countdown = command_interval;

    let worker_id_attr = KeyValue::new("worker.id", worker_id.to_string());
    let active_attrs = [worker_id_attr.clone()];
    let ok_attrs = [worker_id_attr.clone(), KeyValue::new("status", "ok")];
    let ok_included_attrs = [
        worker_id_attr.clone(),
        KeyValue::new("status", "ok"),
        KeyValue::new("reason", "included"),
    ];
    let ok_excluded_attrs = [
        worker_id_attr.clone(),
        KeyValue::new("status", "ok"),
        KeyValue::new("reason", "excluded"),
    ];
    let input_error_attrs = [
        worker_id_attr.clone(),
        KeyValue::new("status", "error"),
        KeyValue::new("reason", "input_queue"),
    ];
    let processing_error_attrs = [
        worker_id_attr.clone(),
        KeyValue::new("status", "error"),
        KeyValue::new("reason", "processing"),
    ];
    let output_error_attrs = [
        worker_id_attr,
        KeyValue::new("status", "error"),
        KeyValue::new("reason", "kafka_send"),
    ];
    loop {
        if command_check_countdown == 0 {
            if should_terminate(&mut receiver) {
                break;
            }
            command_check_countdown = command_interval + 1;
        }

        ACTIVE.add(1, &active_attrs);
        let alerts: Vec<String> = con
            .rpop::<&str, Vec<String>>(&input_queue, NonZero::new(1000))
            .await
            .inspect_err(|_| {
                ACTIVE.add(-1, &active_attrs);
                BATCH_PROCESSED.add(1, &input_error_attrs);
            })?;

        if alerts.is_empty() {
            ACTIVE.add(-1, &active_attrs);
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            command_check_countdown = 0;
            continue;
        }

        let alerts_output = filter_worker
            .process_alerts(&alerts)
            .await
            .inspect_err(|_| {
                ACTIVE.add(-1, &active_attrs);
                BATCH_PROCESSED.add(1, &processing_error_attrs);
            })?;
        command_check_countdown = command_check_countdown.saturating_sub(alerts.len());

        BATCH_PROCESSED.add(1, &ok_attrs);
        ALERT_PROCESSED.add(
            (alerts.len() - alerts_output.len()) as u64,
            &ok_excluded_attrs,
        );
        for alert in alerts_output {
            send_alert_to_kafka(&alert, &schema, &producer, &output_topic)
                .await
                .inspect_err(|_| {
                    let attributes = &output_error_attrs;
                    ACTIVE.add(-1, &active_attrs);
                    BATCH_PROCESSED.add(1, attributes);
                    ALERT_PROCESSED.add(1, attributes);
                })?;
            trace!(
                "Sent alert with candid {} to Kafka topic {}",
                &alert.candid,
                &output_topic
            );
            // Incrementing by alerts_output.len() outside this loop may be more
            // efficient, but incrementing by 1 here is more accurate.
            ALERT_PROCESSED.add(1, &ok_included_attrs);
        }

        ACTIVE.add(-1, &active_attrs);
    }

    Ok(())
}
