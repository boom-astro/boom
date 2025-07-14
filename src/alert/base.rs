use crate::utils::worker::WorkerCmd;
use crate::{
    conf,
    utils::{
        db::{cutout2bsonbinary, get_coordinates},
        o11y::{as_error, log_error, WARN},
        spatial::XmatchError,
        worker::should_terminate,
    },
};
use apache_avro::{from_avro_datum, Reader, Schema};
use mongodb::{
    bson::{doc, Document},
    Collection,
};
use redis::AsyncCommands;
use serde::{de::Deserializer, Deserialize};
use std::io::Read;
use std::{collections::HashMap, fmt::Debug};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument};

#[instrument(skip_all, err)]
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

#[instrument(skip_all, err)]
pub fn zag_i64<R: Read>(reader: &mut R) -> Result<i64, SchemaRegistryError> {
    let z = decode_variable(reader)?;
    if z & 0x1 == 0 {
        Ok((z >> 1) as i64)
    } else {
        Ok(!(z >> 1) as i64)
    }
}

#[instrument(skip_all, err)]
fn decode_long<R: Read>(reader: &mut R) -> Result<i64, SchemaRegistryError> {
    Ok(zag_i64(reader)?)
}

#[instrument(skip_all, err)]
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

pub trait Alert:
    Debug + PartialEq + Clone + serde::Deserialize<'static> + serde::Serialize
{
    fn object_id(&self) -> String;
    fn candid(&self) -> i64;
    fn ra(&self) -> f64;
    fn dec(&self) -> f64;
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
    #[error("alert aux already exists")]
    AlertAuxExists,
    #[error("missing object_id")]
    MissingObjectId,
    #[error("ambiguous object_id")]
    AmbiguousObjectId(String, String),
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

#[derive(Debug, PartialEq)]
pub enum ProcessAlertStatus {
    Added(i64),
    Exists(i64),
}

#[derive(Clone, Debug)]
pub struct SchemaRegistry {
    client: reqwest::Client,
    cache: HashMap<String, Schema>,
    url: String,
}

impl SchemaRegistry {
    #[instrument]
    pub fn new(url: &str) -> Self {
        let client = reqwest::Client::new();
        let cache = HashMap::new();
        SchemaRegistry {
            client,
            cache,
            url: url.to_string(),
        }
    }

    #[instrument(skip(self), err)]
    async fn get_subjects(&self) -> Result<Vec<String>, SchemaRegistryError> {
        let response = self
            .client
            .get(&format!("{}/subjects", &self.url))
            .send()
            .await
            .inspect_err(as_error!("GET request failed for subjects"))?;

        let response = response
            .json::<Vec<String>>()
            .await
            .inspect_err(as_error!("failed to get subjects as JSON"))?;

        Ok(response)
    }

    #[instrument(skip(self), err)]
    async fn get_versions(&self, subject: &str) -> Result<Vec<u32>, SchemaRegistryError> {
        // first we check if the subject exists
        let subjects = self
            .get_subjects()
            .await
            .inspect_err(as_error!("failed to get subjects"))?;
        if !subjects.contains(&subject.to_string()) {
            return Err(SchemaRegistryError::InvalidSubject);
        }

        let response = self
            .client
            .get(&format!("{}/subjects/{}/versions", &self.url, subject))
            .send()
            .await
            .inspect_err(as_error!("GET request failed for versions"))?;

        let response = response
            .json::<Vec<u32>>()
            .await
            .inspect_err(as_error!("failed to get versions as JSON"))?;

        Ok(response)
    }

    async fn _get_schema_by_id(
        &self,
        subject: &str,
        version: u32,
    ) -> Result<Schema, SchemaRegistryError> {
        let versions = self
            .get_versions(subject)
            .await
            .inspect_err(as_error!("failed to get versions"))?;
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
            .inspect_err(as_error!("GET request failed for version"))?;

        let response = response
            .json::<serde_json::Value>()
            .await
            .inspect_err(as_error!("failed to get version as JSON"))?;

        let schema_str = response["schema"]
            .as_str()
            .ok_or(SchemaRegistryError::InvalidResponse)?;

        let schema =
            Schema::parse_str(schema_str).inspect_err(as_error!("failed to parse schema"))?;
        Ok(schema)
    }

    #[instrument(skip(self), err)]
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
    #[error("error from redis")]
    Redis(#[from] redis::RedisError),
    #[error("failed to get avro bytes from the alert queue")]
    GetAvroBytesError,
}

#[async_trait::async_trait]
pub trait AlertWorker {
    async fn new(config_path: &str) -> Result<Self, AlertWorkerError>
    where
        Self: Sized;
    fn stream_name(&self) -> String;
    fn input_queue_name(&self) -> String;
    fn output_queue_name(&self) -> String;
    async fn alert_from_avro_bytes(&mut self, avro_bytes: &[u8]) -> Result<impl Alert, AlertError>;
    #[instrument(skip(self, ra, dec, candidate_doc, now, collection), err)]
    async fn format_and_insert_alert(
        &self,
        candid: i64,
        object_id: &str,
        ra: f64,
        dec: f64,
        candidate_doc: &Document,
        now: f64,
        collection: &mongodb::Collection<Document>,
    ) -> Result<ProcessAlertStatus, AlertError> {
        let alert_doc = doc! {
            "_id": candid,
            "objectId": object_id,
            "candidate": candidate_doc,
            "coordinates": get_coordinates(ra, dec),
            "created_at": now,
            "updated_at": now,
        };

        let status = collection
            .insert_one(alert_doc)
            .await
            .map(|_| ProcessAlertStatus::Added(candid))
            .or_else(|error| match *error.kind {
                mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                    write_error,
                )) if write_error.code == 11000 => Ok(ProcessAlertStatus::Exists(candid)),
                _ => Err(error),
            })?;
        Ok(status)
    }
    async fn insert_aux(
        self: &mut Self,
        object_id: &str,
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
        object_id: &str,
        prv_candidates_doc: &Vec<Document>,
        prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError>;
    #[instrument(skip(self, cutout_science, cutout_template, cutout_difference), err)]
    async fn format_and_insert_cutouts(
        &self,
        candid: i64,
        cutout_science: Vec<u8>,
        cutout_template: Vec<u8>,
        cutout_difference: Vec<u8>,
        collection: &Collection<Document>,
    ) -> Result<(), AlertError> {
        let cutout_doc = doc! {
            "_id": &candid,
            "cutoutScience": cutout2bsonbinary(cutout_science),
            "cutoutTemplate": cutout2bsonbinary(cutout_template),
            "cutoutDifference": cutout2bsonbinary(cutout_difference),
        };

        collection.insert_one(cutout_doc).await?;
        Ok(())
    }
    #[instrument(skip(self, dec_range, radius_rad, collection), fields(xmatch_survey = collection.name()), err)]
    async fn get_matches(
        &self,
        ra: f64,
        dec: f64,
        dec_range: (f64, f64),
        radius_rad: f64,
        collection: &Collection<Document>,
    ) -> Result<Vec<String>, AlertError> {
        let matches = if dec >= dec_range.0 && dec <= dec_range.1 {
            let result = collection
                .find_one(doc! {
                    "coordinates.radec_geojson": {
                        "$nearSphere": [ra - 180.0, dec],
                        "$maxDistance": radius_rad,
                    },
                })
                .projection(doc! {
                    "_id": 1
                })
                .await;
            match result {
                Ok(Some(doc)) => {
                    let object_id = doc.get_str("_id")?;
                    vec![object_id.to_string()]
                }
                Ok(None) => vec![],
                Err(e) => {
                    error!("Error cross-matching with {}: {}", collection.name(), e);
                    vec![]
                }
            }
        } else {
            vec![]
        };
        Ok(matches)
    }
    async fn process_alert(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<ProcessAlertStatus, AlertError>;
}

#[instrument(skip_all)]
fn report_progress(start: &std::time::Instant, stream: &str, count: u64, message: &str) {
    let elapsed = start.elapsed().as_secs();
    info!(
        stream,
        count,
        elapsed,
        average_rate = count as f64 / elapsed as f64,
        "{}",
        message,
    );
}

#[instrument(skip_all, err)]
async fn retrieve_avro_bytes(
    con: &mut redis::aio::MultiplexedConnection,
    input_queue_name: &str,
    temp_queue_name: &str,
) -> Result<Option<Vec<u8>>, AlertWorkerError> {
    let result: Option<Vec<Vec<u8>>> = con
        .rpoplpush(&input_queue_name, &temp_queue_name)
        .await
        .inspect_err(as_error!("failed to pop from input queue"))?;

    match result {
        Some(mut value) => match value.remove(0) {
            avro_bytes if !avro_bytes.is_empty() => Ok(Some(avro_bytes)),
            _ => Err(AlertWorkerError::GetAvroBytesError),
        },
        None => Ok(None),
    }
}

#[instrument(skip_all, err)]
async fn handle_process_result(
    con: &mut redis::aio::MultiplexedConnection,
    temp_queue_name: &str,
    output_queue_name: &str,
    avro_bytes: Vec<u8>,
    result: Result<ProcessAlertStatus, AlertError>,
) -> Result<(), AlertWorkerError> {
    match result {
        Ok(ProcessAlertStatus::Added(candid)) => {
            // queue the candid for processing by the classifier
            con.lpush::<&str, i64, isize>(&output_queue_name, candid)
                .await
                .inspect_err(as_error!("failed to push to output queue"))?;
            con.lrem::<&str, Vec<u8>, isize>(temp_queue_name, 1, avro_bytes)
                .await
                .inspect_err(as_error!("failed to remove new alert from temp queue"))?;
        }
        Ok(ProcessAlertStatus::Exists(candid)) => {
            debug!(?candid, "alert already exists");
            con.lrem::<&str, Vec<u8>, isize>(temp_queue_name, 1, avro_bytes)
                .await
                .inspect_err(as_error!("failed to remove existing alert from temp queue"))?;
        }
        Err(error) => {
            log_error!(WARN, error, "error processing alert, skipping");
        }
    }
    Ok(())
}

#[tokio::main]
#[instrument(skip_all, err)]
pub async fn run_alert_worker<T: AlertWorker>(
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
) -> Result<(), AlertWorkerError> {
    debug!(?config_path);
    let config = conf::load_config(config_path).inspect_err(as_error!("failed to load config"))?; // BoomConfigError

    let mut alert_processor = T::new(config_path).await?;
    let stream_name = alert_processor.stream_name();

    let input_queue_name = alert_processor.input_queue_name();
    let temp_queue_name = format!("{}_temp", input_queue_name);
    let output_queue_name = alert_processor.output_queue_name();

    let mut con = conf::build_redis(&config)
        .await
        .inspect_err(as_error!("failed to create redis client"))?;

    let command_interval: usize = 500;
    let mut command_check_countdown = command_interval;
    let mut count = 0;

    let start = std::time::Instant::now();
    loop {
        // check for command from threadpool
        if command_check_countdown == 0 {
            if should_terminate(&mut receiver) {
                break;
            } else {
                command_check_countdown = command_interval + 1;
            }
        }
        command_check_countdown -= 1;

        let result = retrieve_avro_bytes(&mut con, &input_queue_name, &temp_queue_name).await;

        let avro_bytes = match result {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                info!("queue is empty");
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                command_check_countdown = 0;
                continue;
            }
            Err(e) => {
                error!(?e, "failed to retrieve avro bytes");
                continue;
            }
        };

        let result = alert_processor.process_alert(&avro_bytes).await;
        handle_process_result(
            &mut con,
            &temp_queue_name,
            &output_queue_name,
            avro_bytes,
            result,
        )
        .await
        .inspect_err(as_error!("failed to handle process result"))?;
        if count > 0 && count % 1000 == 0 {
            report_progress(&start, &stream_name, count, "progress");
        }
        count += 1;
    }
    report_progress(&start, &stream_name, count, "summary");
    Ok(())
}
