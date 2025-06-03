use crate::utils::worker::WorkerCmd;
use crate::{
    conf,
    utils::{db::CreateIndexError, spatial::XmatchError},
};
use apache_avro::{from_avro_datum, Reader, Schema};
use mongodb::bson::Document;
use redis::AsyncCommands;
use serde::{de::Deserializer, Deserialize};
use std::collections::HashMap;
use std::io::Read;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{error, info, trace, warn};

fn decode_variable<R: Read>(reader: &mut R) -> Result<u64, SchemaRegistryError> {
    let mut i = 0u64;
    let mut buf = [0u8; 1];

    let mut j = 0;
    loop {
        if j > 9 {
            return Err(SchemaRegistryError::IntegerOverflow);
        }
        reader.read_exact(&mut buf[..])?;

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
    let reader = Reader::new(cursor)?;
    let schema = reader.writer_schema();

    // Then, we look for the index of the start of the data
    // this is based on the Apache Avro specification 1.3.2
    // (https://avro.apache.org/docs/1.3.2/spec.html#Object+Container+Files)
    let mut cursor = std::io::Cursor::new(avro_bytes);

    // Four bytes, ASCII 'O', 'b', 'j', followed by 1
    let mut buf = [0; 4];
    cursor.read_exact(&mut buf)?;
    if buf != [b'O', b'b', b'j', 1u8] {
        return Err(SchemaRegistryError::MagicBytesError);
    }

    // Then there is the file metadata, including the schema
    let meta_schema = Schema::map(Schema::Bytes);
    from_avro_datum(&meta_schema, &mut cursor, None)?;

    // Then the 16-byte, randomly-generated sync marker for this file.
    let mut buf = [0; 16];
    cursor.read_exact(&mut buf)?;

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

pub fn deserialize_mjd<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let mjd = <f64 as Deserialize>::deserialize(deserializer)?;
    Ok(mjd + 2400000.5)
}

pub fn deserialize_mjd_option<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let mjd = <Option<f64> as Deserialize>::deserialize(deserializer)?;
    match mjd {
        Some(mjd) => Ok(Some(mjd + 2400000.5)),
        None => Ok(None),
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SchemaRegistryError {
    #[error("error from avro")]
    Avro(#[from] apache_avro::Error),
    #[error("error from reqwest")]
    Reqwest(#[from] reqwest::Error),
    #[error("error from std::io")]
    Io(#[from] std::io::Error),
    #[error("invalid version")]
    InvalidVersion,
    #[error("invalid subject")]
    InvalidSubject,
    #[error("could not find expected content in response")]
    InvalidResponse,
    #[error("could not find avro magic bytes")]
    MagicBytesError,
    #[error("incorrect number of records in the avro file")]
    InvalidRecordCount(usize),
    #[error("integer overflow")]
    IntegerOverflow,
}

#[derive(thiserror::Error, Debug)]
pub enum AlertError {
    #[error("error from avro")]
    Avro(#[from] apache_avro::Error),
    #[error("value access error from bson")]
    BsonValueAccess(#[from] mongodb::bson::document::ValueAccessError),
    #[error("error from mongodb")]
    Mongodb(#[from] mongodb::error::Error),
    #[error("schema registry error")]
    SchemaRegistryError(#[from] SchemaRegistryError),
    #[error("error from xmatch")]
    Xmatch(#[from] XmatchError),
    #[error("alert already exists")]
    AlertExists,
    #[error("alert aux already exists")]
    AlertAuxExists,
    #[error("missing object_id")]
    MissingObjectId,
    #[error("missing cutout")]
    MissingCutout,
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
    #[error("could not find avro magic bytes")]
    MagicBytesError,
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
            .await?;

        let response = response.json::<Vec<String>>().await?;

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
            .await?;

        let response = response.json::<Vec<u32>>().await?;

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
            .await?;

        let response = response.json::<serde_json::Value>().await?;

        let schema_str = response["schema"]
            .as_str()
            .ok_or(SchemaRegistryError::InvalidResponse)?;

        let schema = Schema::parse_str(schema_str)?;
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

#[derive(thiserror::Error, Debug)]
pub enum AlertWorkerError {
    #[error("failed to load config")]
    LoadConfigError(#[from] conf::BoomConfigError),
    #[error("failed to create index")]
    CreateIndexError(#[from] CreateIndexError),
    #[error("error from redis")]
    Redis(#[from] redis::RedisError),
    #[error("failed to get avro bytes from the alert queue")]
    GetAvroBytesError,
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
        let Some(mut value): Option<Vec<Vec<u8>>> =
            con.rpoplpush(&input_queue_name, &temp_queue_name).await?
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
                    .await?;
                con.lrem::<&str, Vec<u8>, isize>(&temp_queue_name, 1, avro_bytes)
                    .await?;
            }
            Err(error) => match error {
                AlertError::AlertExists => {
                    trace!("Alert already exists");
                    con.lrem::<&str, Vec<u8>, isize>(&temp_queue_name, 1, avro_bytes)
                        .await?;
                }
                _ => {
                    warn!(error = %error, "Error processing alert, skipping");
                    // TODO: Handle alerts that we could not parse from avro
                    // so we don't re-push them to the queue
                    // con.lpush::<&str, Vec<u8>, isize>(&input_queue_name, avro_bytes.clone())
                    //     .await
                    //     .map_err(AlertWorkerError::PushAlertError)?;
                    // con.lrem::<&str, Vec<u8>, isize>(&temp_queue_name, 1, avro_bytes)
                    //     .await
                    //     .map_err(AlertWorkerError::RemoveAlertError)?;
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
