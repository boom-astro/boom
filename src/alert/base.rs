use crate::utils::worker::WorkerCmd;
use crate::{conf, utils::db::CreateIndexError};
use apache_avro::{from_avro_datum, Reader, Schema};
use mongodb::bson::Document;
use redis::AsyncCommands;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::io::Read;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{error, info, trace, warn};

#[derive(thiserror::Error, Debug)]
pub enum SchemaRegistryError {
    #[error("invalid schema")]
    InvalidSchema(#[source] apache_avro::Error),
    #[error("invalid version")]
    InvalidVersion,
    #[error("invalid subject")]
    InvalidSubject,
    #[error("connection error")]
    ConnectionError(#[source] reqwest::Error),
    #[error("parsing error")]
    ParsingError(#[source] reqwest::Error),
    #[error("could not find expected content in response")]
    InvalidResponse,
    #[error("cursor error")]
    CursorError(#[source] std::io::Error),
    #[error("could not find avro magic bytes")]
    MagicBytesError,
    #[error("incorrect number of records in the avro file")]
    InvalidRecordCount(usize),
    #[error("integer overflow")]
    IntegerOverflow,
}

#[derive(thiserror::Error, Debug)]
pub enum AlertError {
    #[error("failed to decode alert")]
    DecodeError(#[source] apache_avro::Error),
    #[error("failed to find candid in the alert collection")]
    FindCandIdError(#[source] mongodb::error::Error),
    #[error("failed to insert into the alert collection")]
    InsertAlertError(#[source] mongodb::error::Error),
    #[error("failed to find objectid in the aux alert collection")]
    FindObjectIdError(#[source] mongodb::error::Error),
    #[error("failed to insert into the alert aux collection")]
    InsertAlertAuxError(#[source] mongodb::error::Error),
    #[error("failed to update the alert aux collection")]
    UpdateAuxAlertError(#[source] mongodb::error::Error),
    #[error("failed to insert into the alert cutout collection")]
    InsertCutoutError(#[source] mongodb::error::Error),
    #[error("failed to retrieve schema for the alert")]
    SchemaError(#[source] apache_avro::Error),
    #[error("avro magic bytes not found")]
    MagicBytesError,
    #[error("schema registry error")]
    SchemaRegistryError(#[from] SchemaRegistryError),
    #[error("alert already exists")]
    AlertExists,
    #[error("alert aux already exists")]
    AlertAuxExists,
    #[error("missing object_id")]
    MissingObjectId,
    #[error("missing cutout")]
    MissingCutout,
    #[error("missing alert in avro")]
    EmptyAlertError,
    #[error("failed to read avro")]
    AvroReadError(#[source] apache_avro::Error),
    #[error("missing psf flux")]
    MissingFluxPSF,
    #[error("missing psf flux error")]
    MissingFluxPSFError,
    #[error("missing ap flux")]
    MissingFluxAperture,
    #[error("missing ap flux error")]
    MissingFluxApertureError,
    #[error("missing mag zero point")]
    MissingMagZPSci,
    #[error("error from mongodb")]
    Mongodb(#[from] mongodb::error::Error),
    #[error("value access error from bson")]
    BsonValueAccess(#[from] mongodb::bson::document::ValueAccessError),
}

#[derive(Clone, Debug)]
pub struct SchemaRegistry {
    client: reqwest::Client,
    cache: HashMap<String, Schema>,
    url: String,
}

impl SchemaRegistry {
    pub fn new(url: &str) -> Self {
        let client = reqwest::Client::new();
        let cache = HashMap::new();
        SchemaRegistry {
            client,
            cache,
            url: url.to_string(),
        }
    }

    async fn get_subjects(&self) -> Result<Vec<String>, SchemaRegistryError> {
        let response = self
            .client
            .get(&format!("{}/subjects", &self.url))
            .send()
            .await
            .map_err(SchemaRegistryError::ConnectionError)?;

        let response = response
            .json::<Vec<String>>()
            .await
            .map_err(SchemaRegistryError::ParsingError)?;

        Ok(response)
    }

    async fn get_versions(&self, subject: &str) -> Result<Vec<u32>, SchemaRegistryError> {
        // first we check if the subject exists
        let subjects = self.get_subjects().await?;
        if !subjects.contains(&subject.to_string()) {
            return Err(SchemaRegistryError::InvalidSubject);
        }

        let response = self
            .client
            .get(&format!("{}/subjects/{}/versions", &self.url, subject))
            .send()
            .await
            .map_err(SchemaRegistryError::ConnectionError)?;

        let response = response
            .json::<Vec<u32>>()
            .await
            .map_err(SchemaRegistryError::ParsingError)?;

        Ok(response)
    }

    async fn _get_schema_by_id(
        &self,
        subject: &str,
        version: u32,
    ) -> Result<Schema, SchemaRegistryError> {
        let versions = self.get_versions(subject).await?;
        if !versions.contains(&version) {
            return Err(SchemaRegistryError::InvalidVersion);
        }

        let response = self
            .client
            .get(&format!(
                "{}/subjects/{}/versions/{}",
                &self.url, subject, version
            ))
            .send()
            .await
            .map_err(SchemaRegistryError::ConnectionError)?;

        let response = response
            .json::<serde_json::Value>()
            .await
            .map_err(SchemaRegistryError::ParsingError)?;

        let schema_str = response["schema"]
            .as_str()
            .ok_or(SchemaRegistryError::InvalidResponse)?;

        let schema = Schema::parse_str(schema_str).map_err(SchemaRegistryError::InvalidSchema)?;
        Ok(schema)
    }

    pub async fn get_schema(
        &mut self,
        subject: &str,
        version: u32,
    ) -> Result<&Schema, SchemaRegistryError> {
        let key = format!("{}:{}", subject, version);
        if !self.cache.contains_key(&key) {
            let schema = self._get_schema_by_id(subject, version).await?;
            self.cache.insert(key.clone(), schema);
        }
        Ok(self.cache.get(&key).unwrap())
    }
}

fn decode_variable<R: Read>(reader: &mut R) -> Result<u64, SchemaRegistryError> {
    let mut i = 0u64;
    let mut buf = [0u8; 1];

    let mut j = 0;
    loop {
        if j > 9 {
            return Err(SchemaRegistryError::IntegerOverflow);
        }
        reader
            .read_exact(&mut buf[..])
            .map_err(SchemaRegistryError::CursorError)?;

        i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
        if (buf[0] >> 7) == 0 {
            break;
        } else {
            j += 1;
        }
    }

    Ok(i)
}

pub fn zag_i64<R: Read>(reader: &mut R) -> Result<i64, SchemaRegistryError> {
    let z = decode_variable(reader)?;
    if z & 0x1 == 0 {
        Ok((z >> 1) as i64)
    } else {
        Ok(!(z >> 1) as i64)
    }
}

fn decode_long<R: Read>(reader: &mut R) -> Result<i64, SchemaRegistryError> {
    Ok(zag_i64(reader)?)
}

pub fn get_schema_and_startidx(avro_bytes: &[u8]) -> Result<(Schema, usize), SchemaRegistryError> {
    // First, we extract the schema from the avro bytes
    let cursor = std::io::Cursor::new(avro_bytes);
    let reader = Reader::new(cursor).map_err(SchemaRegistryError::InvalidSchema)?;
    let schema = reader.writer_schema();

    // Then, we look for the index of the start of the data
    // this is based on the Apache Avro specification 1.3.2
    // (https://avro.apache.org/docs/1.3.2/spec.html#Object+Container+Files)
    let mut cursor = std::io::Cursor::new(avro_bytes);

    // Four bytes, ASCII 'O', 'b', 'j', followed by 1
    let mut buf = [0; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(SchemaRegistryError::CursorError)?;
    if buf != [b'O', b'b', b'j', 1u8] {
        return Err(SchemaRegistryError::MagicBytesError);
    }

    // Then there is the file metadata, including the schema
    let meta_schema = Schema::map(Schema::Bytes);
    from_avro_datum(&meta_schema, &mut cursor, None).map_err(SchemaRegistryError::InvalidSchema)?;

    // Then the 16-byte, randomly-generated sync marker for this file.
    let mut buf = [0; 16];
    cursor
        .read_exact(&mut buf)
        .map_err(SchemaRegistryError::CursorError)?;

    // each avro record is preceded by:
    // 1. a variable-length integer, the number of records in the block
    // 2. a variable-length integer, the number of bytes in the block
    let nb_records = decode_long(&mut cursor)?;
    if nb_records != 1 {
        return Err(SchemaRegistryError::InvalidRecordCount(nb_records as usize));
    }
    let _ = decode_long(&mut cursor)?;

    // we now have the start index of the data
    let start_idx = cursor.position();

    Ok((schema.to_owned(), start_idx as usize))
}

#[derive(thiserror::Error, Debug)]
pub enum AlertWorkerError {
    #[error("failed to load config")]
    LoadConfigError(#[from] conf::BoomConfigError),
    #[error("failed to create index")]
    CreateIndexError(#[from] CreateIndexError),
    #[error("failed to connect to redis")]
    ConnectRedisError(#[source] redis::RedisError),
    #[error("failed to connect to mongodb")]
    ConnectMongoError(#[source] mongodb::error::Error),
    #[error("failed to get alert schema")]
    GetAlertSchemaError,
    #[error("failed to pop from the alert queue")]
    PopAlertError(#[source] redis::RedisError),
    #[error("failed to get avro bytes from the alert queue")]
    GetAvroBytesError,
    #[error("failed to push candid onto the candid queue")]
    PushCandidError(#[source] redis::RedisError),
    #[error("failed to remove alert from the alert queue")]
    RemoveAlertError(#[source] redis::RedisError),
    #[error("failed to push alert onto the alert queue")]
    PushAlertError(#[source] redis::RedisError),
}

pub fn deserialize_mjd<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let mjd = <f64 as Deserialize>::deserialize(deserializer)?;
    Ok(mjd + 2400000.5)
}

#[async_trait::async_trait]
pub trait AlertWorker {
    type ObjectId;
    async fn new(config_path: &str) -> Result<Self, AlertWorkerError>
    where
        Self: Sized;
    fn stream_name(&self) -> String;
    fn input_queue_name(&self) -> String;
    fn output_queue_name(&self) -> String;
    async fn insert_aux(
        self: &mut Self,
        object_id: impl Into<Self::ObjectId> + Send,
        ra: f64,
        dec: f64,
        prv_candidates_doc: &Vec<Document>,
        prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError>;
    async fn update_aux(
        self: &mut Self,
        object_id: impl Into<Self::ObjectId> + Send,
        prv_candidates_doc: &Vec<Document>,
        prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError>;
    async fn process_alert(self: &mut Self, avro_bytes: &[u8]) -> Result<i64, AlertError>;
}

#[tokio::main]
pub async fn run_alert_worker<T: AlertWorker>(
    id: String,
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
) -> Result<(), AlertWorkerError> {
    let config = conf::load_config(config_path)?;

    let mut alert_processor = T::new(config_path).await?;
    let stream_name = alert_processor.stream_name();

    let input_queue_name = alert_processor.input_queue_name();
    let temp_queue_name = format!("{}_temp", input_queue_name);
    let output_queue_name = alert_processor.output_queue_name();

    let mut con = conf::build_redis(&config).await?;

    let command_interval: i64 = 500;
    let mut command_check_countdown = command_interval;
    let mut count = 0;

    let start = std::time::Instant::now();
    loop {
        // check for command from threadpool
        if command_check_countdown == 0 {
            match receiver.try_recv() {
                Ok(WorkerCmd::TERM) => {
                    info!("alert worker {} received termination command", id);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    warn!("alert worker {} receiver disconnected, terminating", id);
                    break;
                }
                Err(TryRecvError::Empty) => {
                    command_check_countdown = command_interval;
                }
            }
        }
        // retrieve candids from redis
        let Some(mut value): Option<Vec<Vec<u8>>> = con
            .rpoplpush(&input_queue_name, &temp_queue_name)
            .await
            .map_err(AlertWorkerError::PopAlertError)?
        else {
            info!("ALERT WORKER {}: Queue is empty", id);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            command_check_countdown = 0;
            continue;
        };
        let avro_bytes = (!value.is_empty())
            .then_some(value.remove(0))
            .ok_or(AlertWorkerError::GetAvroBytesError)?;

        let result = alert_processor.process_alert(&avro_bytes).await;
        match result {
            Ok(candid) => {
                // queue the candid for processing by the classifier
                con.lpush::<&str, i64, isize>(&output_queue_name, candid)
                    .await
                    .map_err(AlertWorkerError::PushCandidError)?;
                con.lrem::<&str, Vec<u8>, isize>(&temp_queue_name, 1, avro_bytes)
                    .await
                    .map_err(AlertWorkerError::RemoveAlertError)?;
            }
            Err(error) => match error {
                AlertError::AlertExists => {
                    trace!("Alert already exists");
                    con.lrem::<&str, Vec<u8>, isize>(&temp_queue_name, 1, avro_bytes)
                        .await
                        .map_err(AlertWorkerError::RemoveAlertError)?;
                }
                _ => {
                    warn!(error = %error, "Error processing alert, skipping");
                }
            },
        }
        if count % 1000 == 0 {
            let elapsed = start.elapsed().as_secs();
            info!(
                "\nProcessed {} {} alerts in {} seconds, avg: {:.4} alerts/s\n",
                count,
                stream_name,
                elapsed,
                count as f64 / elapsed as f64
            );
        }
        count += 1;
        command_check_countdown -= 1;
    }
    Ok(())
}
