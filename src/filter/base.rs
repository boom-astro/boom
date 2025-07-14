use crate::{
    conf,
    utils::{
        enums::Survey,
        worker::{should_terminate, WorkerCmd},
    },
};

use apache_avro::Schema;
use apache_avro::{serde_avro_bytes, Writer};
use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use rdkafka::producer::FutureProducer;
use rdkafka::{config::ClientConfig, producer::FutureRecord};
use redis::AsyncCommands;
use std::num::NonZero;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, trace, warn};

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
                    {"name": "filter_id", "type": "int"},
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
        {"name":"cutoutDifference","type":{"type":"bytes"}}
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Origin {
    Alert,
    ForcedPhot,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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
    pub filter_id: i32,
    pub passed_at: f64, // timestamp in seconds
    pub annotations: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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
}

pub fn load_alert_schema() -> Result<Schema, FilterWorkerError> {
    let schema = Schema::parse_str(ALERT_SCHEMA)
        .inspect_err(|e| error!("Failed to parse alert schema: {}", e))?;

    Ok(schema)
}

#[instrument(skip(alert, schema), fields(candid = alert.candid, object_id = alert.object_id), err)]
pub fn alert_to_avro_bytes(alert: &Alert, schema: &Schema) -> Result<Vec<u8>, FilterWorkerError> {
    let mut writer = Writer::new(schema, Vec::new());
    writer.append_ser(alert).inspect_err(|e| {
        error!("Failed to serialize alert to Avro: {}", e);
    })?;
    let encoded = writer.into_inner().inspect_err(|e| {
        error!("Failed to finalize Avro writer: {}", e);
    })?;

    Ok(encoded)
}

// TODO, use the config file to get the kafka server
pub async fn create_producer(
    kafka_config: &conf::SurveyKafkaConfig,
) -> Result<FutureProducer, FilterWorkerError> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_config.producer)
        .set("message.timeout.ms", "5000")
        .create()?;

    Ok(producer)
}

#[instrument(skip(alert, schema, producer), fields(candid = alert.candid, object_id = alert.object_id), err)]
pub async fn send_alert_to_kafka(
    alert: &Alert,
    schema: &Schema,
    producer: &FutureProducer,
    topic: &str,
    key: &str,
) -> Result<(), FilterWorkerError> {
    let encoded = alert_to_avro_bytes(alert, schema)?;

    let record = FutureRecord::to(&topic).key(key).payload(&encoded);

    producer
        .send(record, std::time::Duration::from_secs(0))
        .await
        .map_err(|(e, _)| {
            warn!("Failed to send filter result to Kafka: {}", e);
            e
        })?;

    Ok(())
}

#[instrument(skip(filter_collection), err)]
pub async fn get_filter_object(
    filter_id: i32,
    catalog: &str,
    filter_collection: &mongodb::Collection<mongodb::bson::Document>,
) -> Result<Document, FilterError> {
    let mut filter_obj = filter_collection
        .aggregate(vec![
            doc! {
                "$match": doc! {
                    "filter_id": filter_id,
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
                    "pipeline": doc! {
                        "$arrayElemAt": [
                            "$fv.pipeline",
                            0
                        ]
                    },
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
    filter_id: i32,
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
        filter_id: i32,
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
}

#[async_trait::async_trait]
pub trait FilterWorker {
    async fn new(config_path: &str) -> Result<Self, FilterWorkerError>
    where
        Self: Sized;
    fn input_queue_name(&self) -> String;
    fn output_topic_name(&self) -> String;
    fn has_filters(&self) -> bool;
    fn survey() -> crate::utils::enums::Survey;
    async fn build_alert(
        &self,
        candid: i64,
        filter_results: Vec<FilterResults>,
    ) -> Result<Alert, FilterWorkerError>;
    async fn process_alerts(&mut self, alerts: &[String]) -> Result<Vec<Alert>, FilterWorkerError>;
}

#[tokio::main]
#[instrument(skip_all, err)]
pub async fn run_filter_worker<T: FilterWorker>(
    key: String,
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
) -> Result<(), FilterWorkerError> {
    debug!(?config_path);

    let config = conf::load_config(config_path)?;
    let kafka_config = conf::build_kafka_config(&config, &T::survey())
        .inspect_err(|e| error!("Failed to build Kafka config: {}", e))?;

    let mut filter_worker = T::new(config_path).await?;

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

    loop {
        if command_check_countdown == 0 {
            if should_terminate(&mut receiver) {
                break;
            } else {
                command_check_countdown = command_interval + 1;
            }
        }
        command_check_countdown -= 1;
        // if the queue is empty, wait for a bit and continue the loop
        let queue_len: i64 = con.llen(&input_queue).await?;
        if queue_len == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            command_check_countdown = 0;
            continue;
        }

        // get candids from redis
        let alerts: Vec<String> = con
            .rpop::<&str, Vec<String>>(&input_queue, NonZero::new(1000))
            .await?;

        let nb_alerts = alerts.len();
        if nb_alerts == 0 {
            // sleep for a bit if no alerts were found
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }

        let alerts_output = filter_worker.process_alerts(&alerts).await?;
        command_check_countdown -= nb_alerts - 1; // As if iterated this many times

        for alert in alerts_output {
            send_alert_to_kafka(&alert, &schema, &producer, &output_topic, &key).await?;
            trace!(
                "Sent alert with candid {} to Kafka topic {}",
                &alert.candid,
                &output_topic
            );
        }
    }

    Ok(())
}
