use crate::conf::AppConfig;
use crate::utils::cutouts::AlertCutout;
use crate::utils::enums::Survey;
use crate::utils::worker::WorkerCmd;
use crate::{
    conf,
    utils::{
        cutouts::{CutoutStorage, CutoutStorageError},
        db::mongify,
        o11y::{
            logging::{as_error, log_error, WARN},
            metrics::SCHEDULER_METER,
        },
        spatial::XmatchError,
        worker::should_terminate,
    },
};

use std::collections::HashSet;
use std::{collections::HashMap, fmt::Debug, io::Read, sync::LazyLock, time::Instant};

use apache_avro::{from_avro_datum, from_value, Reader, Schema};
use futures::future::join_all;
use mongodb::{
    bson::{doc, Document},
    Collection,
};
use opentelemetry::{
    metrics::{Counter, UpDownCounter},
    KeyValue,
};
use redis::AsyncCommands;
use serde::{de::Deserializer, Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, trace, warn};
use uuid::Uuid;

const SCHEMA_REGISTRY_MAGIC_BYTE: u8 = 0;

/// Delay (in milliseconds) when the input queue is empty before checking again.
/// This prevents busy-waiting when there's no work to do.
const QUEUE_EMPTY_DELAY_MS: u64 = 500;

/// Delay (in seconds) after a valkey/redis error before retrying.
/// This prevents log spam when valkey is unavailable.
const VALKEY_ERROR_DELAY_SECS: u64 = 5;

// NOTE: Global instruments are defined here because reusing instruments is
// considered a best practice. According to the `opentelemetry` crate,
// "Instruments are designed for reuse. Avoid creating new instruments
// repeatedly." One solution is to clone (cloning instruments is cheap). Another
// is to use static items, with `LazyLock` to ensure each one is only
// initialized once.

// UpDownCounter for the number of alerts currently being processed by the alert workers.
static ACTIVE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .i64_up_down_counter("alert_worker.active")
        .with_unit("{alert}")
        .with_description("Number of alerts currently being processed by the alert worker.")
        .build()
});

// Counter for the number of alerts processed by the alert workers.
static ALERT_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("alert_worker.alert.processed")
        .with_unit("{alert}")
        .with_description("Number of alerts processed by the alert worker.")
        .build()
});

#[derive(Deserialize, Serialize, Clone)]
pub struct LightcurveJdOnly {
    pub jd: f64,
}

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
    #[error("failed to fetch schema from github")]
    GithubFetchFailed,
    #[error("failed to resolve schema references")]
    SchemaResolutionFailed,
}

#[derive(thiserror::Error, Debug)]
pub enum AlertError {
    #[error("error from avro")]
    Avro(#[from] apache_avro::Error),
    #[error("no records in avro data")]
    AvroNoRecords,
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
    AmbiguousObjectId(i64, i64),
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
    #[error("missing alert aux")]
    AlertAuxNotFound,
    #[error("unexpected fid value")]
    UnknownFid(i32),
    #[error("missing diffmaglim value")]
    MissingDiffmaglim,
    #[error("cutout storage error")]
    CutoutStorageError(#[from] CutoutStorageError),
    #[error("invalid timeseries input: {0}")]
    InvalidTimeseriesInput(String),
    #[error("failed to run fallback aux update (no match with existing aux for {0})")]
    AlertAuxFallbackUpdateFailed(String),
    #[error("concurrent aux update detected for {0}")]
    ConcurrentAuxUpdate(String),
}

#[derive(Debug, PartialEq)]
pub enum ProcessAlertStatus {
    Added(i64),
    Exists(i64),
}

#[derive(Clone, Debug)]
pub struct SchemaRegistry {
    survey: Survey,
    client: reqwest::Client,
    cache: HashMap<String, Schema>,
    url: String,
    github_fallback_url: Option<String>,
}

impl SchemaRegistry {
    #[instrument]
    pub fn new(survey: Survey, url: &str, github_fallback_url: Option<String>) -> Self {
        let client = reqwest::Client::new();
        let cache = HashMap::new();
        SchemaRegistry {
            survey,
            client,
            cache,
            url: url.to_string(),
            github_fallback_url,
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

    /// Attempts to get a schema first from the schema registry, and falls back to GitHub if that fails.
    ///
    /// This is useful when a schema registry is temporarily unavailable. The fallback fetches
    /// schemas from the provided GitHub URL (e.g.
    /// https://github.com/lsst/alert_packet/tree/main/python/lsst/alert/packet/schema/{major}/{minor})
    /// where major and minor are derived from the version number.
    async fn _get_schema_by_id_with_fallback(
        &self,
        subject: &str,
        version: u32,
    ) -> Result<Schema, SchemaRegistryError> {
        // Try to get schema from registry first
        match self._get_schema_by_id(subject, version).await {
            Ok(schema) => Ok(schema),
            Err(registry_error) => {
                if self.github_fallback_url.is_none() {
                    return Err(registry_error);
                }
                warn!(
                    "Schema registry lookup failed for subject {} version {}, attempting GitHub fallback: {:?}",
                    subject,
                    version,
                    registry_error
                );
                self.get_github_schema(version / 100, version % 100)
                    .await
                    .map_err(|github_error| {
                        error!(
                            ?registry_error,
                            ?github_error,
                            "Both schema registry and GitHub fallback failed"
                        );
                        SchemaRegistryError::GithubFetchFailed
                    })
            }
        }
    }

    /// Fetches the alert packet schema from GitHub and resolves all nested schema references.
    #[instrument(skip(self), err)]
    async fn get_github_schema(
        &self,
        major: u32,
        minor: u32,
    ) -> Result<Schema, SchemaRegistryError> {
        // Build the RAW URL for the GitHub repository
        let github_fallback_url = match &self.github_fallback_url {
            Some(url) => format!("{}/{}/{}", url, major, minor),
            None => {
                return Err(SchemaRegistryError::GithubFetchFailed);
            }
        };
        let raw_url_base = if github_fallback_url.contains("raw.githubusercontent.com") {
            // Already a raw GitHub URL; use as-is.
            github_fallback_url
        } else if github_fallback_url.contains("github.com") {
            // Convert a standard GitHub URL (e.g., with /tree/) to its raw equivalent.
            github_fallback_url
                .replace("github.com", "raw.githubusercontent.com")
                .replace("/tree/", "/")
        } else {
            // Unknown pattern; return an error.
            error!(
                "GitHub fallback URL does not appear to be a valid GitHub URL: {}",
                github_fallback_url
            );
            return Err(SchemaRegistryError::GithubFetchFailed);
        };

        // Fetch the main alert schema file
        let schema_url = format!(
            "{}/{}.v{}_{}.alert.avsc",
            raw_url_base,
            self.survey.to_string().to_lowercase(),
            major,
            minor
        );
        let response = self
            .client
            .get(&schema_url)
            .send()
            .await
            .inspect_err(as_error!("failed to fetch alert schema from github"))?;
        if !response.status().is_success() {
            error!(
                "Failed to fetch schema from GitHub URL: {}. HTTP status: {}",
                schema_url,
                response.status()
            );
            return Err(SchemaRegistryError::GithubFetchFailed);
        }
        let schema_str = response
            .text()
            .await
            .inspect_err(as_error!("failed to get schema text from github response"))?;

        // Parse the schema to find all referenced schemas
        let mut schema_files = HashSet::new();
        let schema_lines: Vec<&str> = schema_str.lines().collect();
        for line in schema_lines {
            if let Some(start_idx) = line.find(
                format!(
                    "{}.v{}_{}.",
                    self.survey.to_string().to_lowercase(),
                    major,
                    minor
                )
                .as_str(),
            ) {
                let end_idx = line[start_idx..]
                    .find('"')
                    .map(|idx| start_idx + idx)
                    .unwrap_or(line.len());
                let schema_ref = &line[start_idx..end_idx];
                let file_name = format!("{}.avsc", schema_ref);
                schema_files.insert(file_name);
            }
        }

        // Fetch all referenced schemas concurrently
        let fetch_futures: Vec<_> = schema_files
            .iter()
            .map(|file_name| {
                let url = format!("{}/{}", raw_url_base, file_name);
                let client = self.client.clone();
                let url_for_logging = url.clone();
                async move { (url_for_logging, client.get(&url).send().await) }
            })
            .collect();

        let fetch_results = join_all(fetch_futures).await;
        let mut schema_strs = vec![];

        for (file_url, response_result) in fetch_results {
            let response = response_result
                .inspect_err(as_error!("failed to fetch schema file from github"))?;
            if !response.status().is_success() {
                error!(
                    "Failed to fetch schema file from GitHub URL: {}. HTTP status: {}",
                    file_url,
                    response.status()
                );
                return Err(SchemaRegistryError::GithubFetchFailed);
            }

            let schema_str = response
                .text()
                .await
                .inspect_err(as_error!("failed to get schema text from github response"))?;
            schema_strs.push(schema_str);
        }

        // Finally, resolve all references to get the full - independent - canonical schema
        let (schema, schemas) = apache_avro::schema::Schema::parse_str_with_list(
            &schema_str,
            schema_strs.iter().map(|s| s.as_str()),
        )?;
        let canonical_schema_str = schema.independent_canonical_form(&schemas)?;
        let schema = Schema::parse_str(&canonical_schema_str).inspect_err(as_error!(
            "failed to parse resolved alert schema from github"
        ))?;
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
            let schema = self
                ._get_schema_by_id_with_fallback(subject, version)
                .await?;
            self.cache.insert(key.clone(), schema);
        }
        Ok(self.cache.get(&key).unwrap())
    }

    #[instrument(skip_all, err)]
    pub async fn alert_from_avro_bytes<T: for<'a> Deserialize<'a>>(
        &mut self,
        avro_bytes: &[u8],
    ) -> Result<T, AlertError> {
        let magic = avro_bytes[0];
        if magic != SCHEMA_REGISTRY_MAGIC_BYTE {
            Err(AlertError::MagicBytesError)?;
        }
        let schema_id =
            u32::from_be_bytes([avro_bytes[1], avro_bytes[2], avro_bytes[3], avro_bytes[4]]);
        let schema = self.get_schema("alert-packet", schema_id).await?;
        let mut slice = &avro_bytes[5..];
        let value = from_avro_datum(&schema, &mut slice, None)?;

        let alert: T = from_value::<T>(&value)?;

        Ok(alert)
    }
}

pub struct SchemaCache {
    cached_schema: Option<Schema>,
    cached_start_idx: Option<usize>,
}

impl SchemaCache {
    #[instrument(skip_all, err)]
    pub fn alert_from_avro_bytes<T: for<'a> Deserialize<'a>>(
        &mut self,
        avro_bytes: &[u8],
    ) -> Result<T, AlertError> {
        // if the schema is not cached, get it from the avro_bytes
        let (schema_ref, start_idx) = match (self.cached_schema.as_ref(), self.cached_start_idx) {
            (Some(schema), Some(start_idx)) => (schema, start_idx),
            _ => {
                let (schema, startidx) =
                    get_schema_and_startidx(avro_bytes).inspect_err(as_error!())?;
                self.cached_schema = Some(schema);
                self.cached_start_idx = Some(startidx);
                (self.cached_schema.as_ref().unwrap(), startidx)
            }
        };

        let value = from_avro_datum(schema_ref, &mut &avro_bytes[start_idx..], None);

        // if value is an error, try recomputing the schema from the avro_bytes
        // as it could be that the schema has changed
        let value = match value {
            Ok(value) => value,
            Err(error) => {
                log_error!(
                    WARN,
                    error,
                    "Error deserializing avro message with cached schema"
                );
                let (schema, startidx) =
                    get_schema_and_startidx(avro_bytes).inspect_err(as_error!())?;

                // try deserializing again with the schemaless approach
                // Reader::new expects the full Avro container (header included),
                // not the raw datum bytes, so pass the whole slice here.
                let reader = apache_avro::Reader::new(avro_bytes)?;

                let value = reader
                    .into_iter()
                    .next()
                    .ok_or_else(|| AlertError::AvroNoRecords)??;

                self.cached_schema = Some(schema);
                self.cached_start_idx = Some(startidx);

                value
            }
        };

        let alert: T = from_value::<T>(&value).inspect_err(as_error!())?;

        Ok(alert)
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        SchemaCache {
            cached_schema: None,
            cached_start_idx: None,
        }
    }
}

#[cfg(test)]
impl SchemaCache {
    /// Overwrite the cached start index with an arbitrary value to simulate a
    /// schema-cache corruption for testing the fallback path.
    pub fn set_cached_start_idx(&mut self, idx: usize) {
        self.cached_start_idx = Some(idx);
    }

    /// Return the currently cached start index (for assertions in tests).
    pub fn get_cached_start_idx(&self) -> Option<usize> {
        self.cached_start_idx
    }
}

/// Convert a Julian Date (JD) to a normalized u64 bit representation for consistent hashing and deduplication.
/// This function normalizes -0.0 to 0.0 and all NaN values to a single representation
/// to ensure that equivalent time values hash to the same value.
fn to_bits_normalized(jd: f64) -> u64 {
    let mut bits = jd.to_bits();
    // Normalize -0.0 and NaN to ensure consistent hashing and deduplication
    if jd == 0.0 {
        bits = 0.0f64.to_bits(); // Normalize -0.0 to 0.0
    } else if jd.is_nan() {
        bits = f64::NAN.to_bits(); // Normalize all NaNs to a single representation
    }
    bits
}

pub trait TimeSeries {
    /// Return the time value of this TimeSeries point as a Julian Date (JD).
    fn time(&self) -> f64;

    /// Validate that a TimeSeries slice is strictly monotonically increasing by time, and contains
    /// only finite time values.
    /// Returns an error if the timeseries is invalid, with a message that includes the provided series
    /// name for easier debugging.
    fn validate_monotonic_increasing(
        timeseries: &[Self],
        series_name: &str,
    ) -> Result<(), AlertError>
    where
        Self: Sized,
    {
        let mut iter = timeseries.iter();
        let Some(first) = iter.next() else {
            return Ok(());
        };

        let mut prev_time = first.time();
        if !prev_time.is_finite() {
            return Err(AlertError::InvalidTimeseriesInput(format!(
                "{} contains non-finite jd value {}",
                series_name, prev_time
            )));
        }

        for point in iter {
            let time = point.time();
            if !time.is_finite() {
                return Err(AlertError::InvalidTimeseriesInput(format!(
                    "{} contains non-finite jd value {}",
                    series_name, time
                )));
            }
            if time <= prev_time {
                return Err(AlertError::InvalidTimeseriesInput(format!(
                    "{} is not strictly increasing ({} <= {})",
                    series_name, time, prev_time
                )));
            }
            prev_time = time;
        }

        Ok(())
    }

    /// Sanitize a TimeSeries vector by sorting it by time, deduplicating any points with the same
    /// time (keeping the first occurrence),and removing any points with non-finite time values.
    fn sanitize_timeseries(timeseries: &mut Vec<Self>)
    where
        Self: Sized,
    {
        timeseries.sort_by(|a, b| a.time().total_cmp(&b.time()));
        timeseries.dedup_by(|a, b| a.time() == b.time());
        timeseries.retain(|point| point.time().is_finite());
    }

    /// Prepare a timeseries update by merging new data with existing data, ensuring the
    /// result is strictly increasing by time and contains no duplicate time values.
    /// Returns a tuple of (prepared_data, needs_sorting) where prepared_data is the merged
    /// and deduplicated data ready for insertion, and needs_sorting indicates whether the
    /// prepared data needs to be sorted before insertion (it can be safely appended if false).
    fn prepare_timeseries_update(
        new_data: &[Self],
        existing_data: &[LightcurveJdOnly],
        series_name: &str,
    ) -> Result<(Vec<mongodb::bson::Document>, bool), AlertError>
    where
        Self: Sized + serde::Serialize,
    {
        // Validate existing data is also strictly increasing, to ensure the correctness of the merge logic below.
        LightcurveJdOnly::validate_monotonic_increasing(existing_data, series_name).inspect_err(
            |error| {
                warn!(
                    ?error,
                    "prepare_timeseries_update rejected existing {} input", series_name
                );
            },
        )?;

        // Validate new data is strictly increasing, which allows for optimizations in the merge logic below.
        Self::validate_monotonic_increasing(new_data, series_name).inspect_err(|error| {
            warn!(
                ?error,
                "prepare_timeseries_update rejected new {} input", series_name
            );
        })?;

        if new_data.is_empty() {
            // No new data, so no update needed. Unless existing data can't be validated
            return Ok((vec![], false));
        }

        // if there is no existing data, we can just append without sorting
        if existing_data.is_empty() {
            let docs = new_data.iter().map(|item| mongify(item)).collect();
            return Ok((docs, false));
        }

        // After strict validation above, new_data is non-empty and strictly increasing.
        let min_new_jd = new_data[0].time();

        // Existing series is validated (upstream) as strictly increasing by callers,
        // so the last element is the maximum jd.
        let max_existing_jd = existing_data[existing_data.len() - 1].jd;

        // if all new data is newer than existing data, we can just append without sorting
        if min_new_jd > max_existing_jd {
            let docs = new_data.iter().map(|item| mongify(item)).collect();
            return Ok((docs, false));
        }

        // Existing data is strictly increasing, so only jds >= min_new_jd can collide
        // with any new point. Skip older points to reduce hash-set size and work.
        let overlap_start = existing_data.partition_point(|item| item.jd < min_new_jd);
        let relevant_existing = &existing_data[overlap_start..];
        let mut existing_jds = HashSet::with_capacity(relevant_existing.len());
        for item in relevant_existing {
            existing_jds.insert(to_bits_normalized(item.jd));
        }

        // filter out points already present in existing data (deduplication)
        // and track the minimum jd of the post-deduplication data to check if sorting can be skipped
        let mut docs = Vec::with_capacity(new_data.len());
        let mut min_new_jd_deduped = f64::INFINITY;
        for item in new_data {
            let jd = item.time();
            if !existing_jds.contains(&to_bits_normalized(jd)) {
                docs.push(mongify(item));
                if jd < min_new_jd_deduped {
                    min_new_jd_deduped = jd;
                }
            }
        }

        // if all the deduplicated new data is newer than existing data, we can just append without sorting
        if min_new_jd_deduped > max_existing_jd {
            return Ok((docs, false));
        }

        // else, we need the full update with sorting
        Ok((docs, true))
    }
}

impl TimeSeries for LightcurveJdOnly {
    fn time(&self) -> f64 {
        self.jd
    }
}

#[cfg(test)]
mod timeseries_tests {
    use super::{AlertError, LightcurveJdOnly, TimeSeries};
    use mongodb::bson::Document;
    use serde::Serialize;

    #[derive(Clone, Debug, Serialize)]
    struct TestPoint {
        jd: f64,
        id: i32,
    }

    impl TimeSeries for TestPoint {
        fn time(&self) -> f64 {
            self.jd
        }
    }

    fn point(jd: f64, id: i32) -> TestPoint {
        TestPoint { jd, id }
    }

    fn existing(jds: &[f64]) -> Vec<LightcurveJdOnly> {
        jds.iter().map(|jd| LightcurveJdOnly { jd: *jd }).collect()
    }

    fn jd_values(docs: &[Document]) -> Vec<f64> {
        docs.iter()
            .map(|doc| {
                doc.get_f64("jd")
                    .expect("prepared docs should contain a numeric jd")
            })
            .collect()
    }

    #[test]
    fn sanitize_timeseries_sorts_and_deduplicates() {
        let mut data = vec![
            point(3.0, 1),
            point(f64::NAN, 2),
            point(1.0, 2),
            point(2.0, 3),
            point(2.0, 4),
        ];

        TestPoint::sanitize_timeseries(&mut data);

        let jds = data.iter().map(|p| p.jd).collect::<Vec<_>>();
        assert_eq!(jds, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn validate_monotonic_increasing_accepts_strictly_increasing_input() {
        let data = vec![point(1.0, 1), point(2.0, 2), point(3.0, 3)];
        let result = TestPoint::validate_monotonic_increasing(&data, "test_series");
        assert!(result.is_ok());
    }

    #[test]
    fn validate_monotonic_increasing_rejects_equal_or_decreasing_values() {
        let dup = vec![point(1.0, 1), point(1.0, 2)];
        let dec = vec![point(2.0, 1), point(1.0, 2)];

        let dup_err = TestPoint::validate_monotonic_increasing(&dup, "test_series");
        let dec_err = TestPoint::validate_monotonic_increasing(&dec, "test_series");

        assert!(matches!(
            dup_err,
            Err(AlertError::InvalidTimeseriesInput(_))
        ));
        assert!(matches!(
            dec_err,
            Err(AlertError::InvalidTimeseriesInput(_))
        ));
    }

    #[test]
    fn validate_monotonic_increasing_rejects_non_finite_values() {
        let first_nan = vec![point(f64::NAN, 1), point(2.0, 2)];
        let inner_inf = vec![point(1.0, 1), point(f64::INFINITY, 2)];

        let first_err = TestPoint::validate_monotonic_increasing(&first_nan, "test_series");
        let inner_err = TestPoint::validate_monotonic_increasing(&inner_inf, "test_series");

        assert!(matches!(
            first_err,
            Err(AlertError::InvalidTimeseriesInput(_))
        ));
        assert!(matches!(
            inner_err,
            Err(AlertError::InvalidTimeseriesInput(_))
        ));
    }

    #[test]
    fn prepare_timeseries_update_empty_new_data_returns_no_update() {
        let (docs, need_sort) =
            TestPoint::prepare_timeseries_update(&[], &existing(&[1.0, 2.0]), "test_series")
                .expect("prepare should succeed");

        assert!(docs.is_empty());
        assert!(!need_sort);
    }

    #[test]
    fn prepare_timeseries_update_existing_empty_appends_without_sort() {
        let new_data = vec![point(10.0, 1), point(11.0, 2)];

        let (docs, need_sort) = TestPoint::prepare_timeseries_update(&new_data, &[], "test_series")
            .expect("prepare should succeed");

        assert_eq!(jd_values(&docs), vec![10.0, 11.0]);
        assert!(!need_sort);
    }

    #[test]
    fn prepare_timeseries_update_all_newer_than_existing_appends_without_sort() {
        let new_data = vec![point(6.0, 1), point(7.0, 2)];

        let (docs, need_sort) = TestPoint::prepare_timeseries_update(
            &new_data,
            &existing(&[1.0, 2.0, 5.0]),
            "test_series",
        )
        .expect("prepare should succeed");

        assert_eq!(jd_values(&docs), vec![6.0, 7.0]);
        assert!(!need_sort);
    }

    #[test]
    fn prepare_timeseries_update_overlap_deduped_all_newer_still_skips_sort() {
        let new_data = vec![point(2.0, 1), point(6.0, 2)];

        let (docs, need_sort) = TestPoint::prepare_timeseries_update(
            &new_data,
            &existing(&[1.0, 2.0, 5.0]),
            "test_series",
        )
        .expect("prepare should succeed");

        assert_eq!(jd_values(&docs), vec![6.0]);
        assert!(!need_sort);
    }

    #[test]
    fn prepare_timeseries_update_overlap_requires_full_update_with_sort() {
        let new_data = vec![point(4.0, 1), point(6.0, 2)];

        let (docs, need_sort) = TestPoint::prepare_timeseries_update(
            &new_data,
            &existing(&[1.0, 2.0, 5.0]),
            "test_series",
        )
        .expect("prepare should succeed");

        assert_eq!(jd_values(&docs), vec![4.0, 6.0]);
        assert!(need_sort);
    }

    #[test]
    fn prepare_timeseries_update_overlap_with_only_duplicates_returns_empty_without_sort() {
        let new_data = vec![point(2.0, 1), point(5.0, 2)];

        let (docs, need_sort) = TestPoint::prepare_timeseries_update(
            &new_data,
            &existing(&[1.0, 2.0, 5.0]),
            "test_series",
        )
        .expect("prepare should succeed");

        assert!(docs.is_empty());
        assert!(!need_sort);
    }

    #[test]
    fn prepare_timeseries_update_rejects_unsorted_or_duplicate_input() {
        let unsorted = vec![point(2.0, 1), point(1.0, 2)];
        let duplicate = vec![point(1.0, 1), point(1.0, 2)];

        let unsorted_result =
            TestPoint::prepare_timeseries_update(&unsorted, &existing(&[0.0]), "test_series");
        let duplicate_result =
            TestPoint::prepare_timeseries_update(&duplicate, &existing(&[0.0]), "test_series");

        assert!(matches!(
            unsorted_result,
            Err(AlertError::InvalidTimeseriesInput(_))
        ));
        assert!(matches!(
            duplicate_result,
            Err(AlertError::InvalidTimeseriesInput(_))
        ));
    }

    #[test]
    fn validate_lightcurve_jd_only_accepts_strictly_increasing_input() {
        let data = existing(&[1.0, 2.0, 3.0]);
        assert!(LightcurveJdOnly::validate_monotonic_increasing(&data, "existing_series").is_ok());
    }

    #[test]
    fn validate_lightcurve_jd_only_rejects_duplicates_and_unsorted_values() {
        let dup = existing(&[1.0, 1.0]);
        let unsorted = existing(&[2.0, 1.0]);

        assert!(matches!(
            LightcurveJdOnly::validate_monotonic_increasing(&dup, "existing_series"),
            Err(AlertError::InvalidTimeseriesInput(_))
        ));
        assert!(matches!(
            LightcurveJdOnly::validate_monotonic_increasing(&unsorted, "existing_series"),
            Err(AlertError::InvalidTimeseriesInput(_))
        ));
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
    #[error("worker config missing for survey: {0}")]
    WorkerConfigMissing(Survey),
}

#[async_trait::async_trait]
pub trait AlertWorker {
    async fn new(config_path: &str) -> Result<Self, AlertWorkerError>
    where
        Self: Sized;
    fn survey() -> Survey;
    fn input_queue_name(&self) -> String;
    fn output_queue_name(&self) -> String;
    #[instrument(skip(self, alert, collection), err)]
    async fn format_and_insert_alert<T: Serialize + Send + Sync>(
        &self,
        candid: i64,
        alert: &T,
        collection: &mongodb::Collection<T>,
    ) -> Result<ProcessAlertStatus, AlertError> {
        let status = collection
            .insert_one(alert)
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
    #[instrument(skip(self, obj, alert_aux_collection), err)]
    async fn insert_aux<T>(
        &self,
        obj: &T,
        alert_aux_collection: &Collection<T>,
    ) -> Result<(), AlertError>
    where
        T: Send + Sync + Serialize,
    {
        alert_aux_collection
            .insert_one(obj)
            .await
            .map_err(|e| match *e.kind {
                mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                    write_error,
                )) if write_error.code == 11000 => AlertError::AlertAuxExists,
                _ => e.into(),
            })?;
        Ok(())
    }
    #[instrument(skip(self, alert_aux_collection), err)]
    async fn check_alert_aux_exists<T>(
        &self,
        object_id: &str,
        alert_aux_collection: &Collection<T>,
    ) -> Result<bool, AlertError>
    where
        T: Send + Sync + Serialize,
    {
        let alert_aux_exists = alert_aux_collection
            .count_documents(doc! { "_id": object_id })
            .await?
            > 0;
        Ok(alert_aux_exists)
    }
    #[instrument(
        skip(
            self,
            cutout_science,
            cutout_template,
            cutout_difference,
            cutout_storage
        ),
        err
    )]
    async fn format_and_insert_cutouts(
        &self,
        candid: i64,
        object_id: &str,
        cutout_science: Vec<u8>,
        cutout_template: Vec<u8>,
        cutout_difference: Vec<u8>,
        cutout_storage: &CutoutStorage,
    ) -> Result<ProcessAlertStatus, AlertError> {
        let cutouts = AlertCutout {
            candid: candid,
            object_id: object_id.to_string(),
            science: cutout_science,
            template: cutout_template,
            difference: cutout_difference,
        };
        match cutout_storage.insert_cutouts(cutouts).await {
            Ok(_) => Ok(ProcessAlertStatus::Added(candid)),
            Err(CutoutStorageError::CutoutAlreadyExists(_)) => {
                Ok(ProcessAlertStatus::Exists(candid))
            }
            Err(e) => Err(AlertError::from(e)),
        }
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

    /// Update the alert auxiliary collection (object-level table) with new survey matches,
    /// and adding new lightcurve points to the existing ones, while ensuring that the lightcurves
    /// remains strictly increasing by time and contains no duplicate time values.
    async fn db_only_aux_update<T, K>(
        object_id: &str,
        mut lc_set_update: Document,
        survey_matches: T,
        now: f64,
        alert_aux_collection: &mongodb::Collection<K>,
    ) -> Result<(), AlertError>
    where
        T: Serialize + Send + Sync,
        K: Serialize + Unpin + Send + Sync,
    {
        lc_set_update.insert("aliases", mongify(&survey_matches));
        lc_set_update.insert("updated_at", now);
        lc_set_update.insert(
            "version",
            doc! { "$add": [ { "$ifNull": [ "$version", 0 ] }, 1 ] },
        );

        let update_pipeline = vec![doc! { "$set": lc_set_update }];

        let update_result = alert_aux_collection
            .update_one(doc! { "_id": object_id }, update_pipeline)
            .await?;
        if update_result.matched_count == 0 {
            return Err(AlertError::AlertAuxFallbackUpdateFailed(
                object_id.to_string(),
            ));
        }
        Ok(())
    }

    /// Add a new prepared lightcurve update to the push_updates document for the auxiliary update.
    /// The prepared lightcurve update is a tuple of (new_docs, need_sort) where new_docs is the vector
    /// of new lightcurve points to add, and need_sort indicates whether the new_docs need to be sorted
    /// before insertion (it can be safely appended if false).
    fn add_to_push_aux_update(
        push_updates: &mut Document,
        field_name: &str,
        prepared_lc: (Vec<Document>, bool),
    ) {
        let (new_docs, need_sort) = prepared_lc;
        if !new_docs.is_empty() {
            if need_sort {
                push_updates.insert(field_name, doc! { "$each": new_docs, "$sort": { "jd": 1 } });
            } else {
                push_updates.insert(field_name, doc! { "$each": new_docs });
            }
        }
    }

    /// Make the filter document for the auxiliary update, which includes a version check to ensure that
    /// the update is only applied if the version in the database matches that retrieved when the document
    /// was fetched before preparing the update. This allows us to detect concurrent modifications to the
    /// same document and fallback to a DB-only update if needed.
    fn make_find_doc_aux_update(object_id: &str, current_version: Option<i32>) -> Document {
        match current_version {
            Some(version) => doc! { "_id": object_id, "version": version },
            None => doc! {
                "_id": object_id,
                "$or": [
                    doc! { "version": { "$exists": false } },
                    doc! { "version": mongodb::bson::Bson::Null },
                ]
            },
        }
    }

    /// Make the update document for the auxiliary update, which includes setting the new survey matches
    /// and updated_at time, incrementing the version, and pushing any new lightcurve points.
    /// The update document is structured to be used in an update_one operation with a filter
    /// that includes a version check for concurrency control.
    fn make_filter_doc_aux_update<T>(
        push_updates: Document,
        survey_matches: &Option<T>,
        current_version: Option<i32>,
        now: f64,
    ) -> Document
    where
        T: Serialize,
    {
        let mut update_doc = doc! {
            "$set": {
                "aliases": mongify(survey_matches),
                "updated_at": now,
                "version": current_version.unwrap_or(0) + 1,
            }
        };

        if !push_updates.is_empty() {
            update_doc.insert("$push", push_updates);
        };
        update_doc
    }

    /// Finalize the auxiliary update by performing an update_one with a filter that includes a
    /// version check for concurrency control. If the update fails due to a concurrent modification
    /// (matched_count == 0), an error is returned to trigger a fallback to a DB-only update.
    async fn finalize_aux_update<T, K>(
        object_id: &str,
        push_updates: Document,
        survey_matches: &Option<T>,
        current_version: Option<i32>,
        now: f64,
        alert_aux_collection: &mongodb::Collection<K>,
    ) -> Result<(), AlertError>
    where
        T: Serialize + Sync,
        K: Serialize + Unpin + Send + Sync,
    {
        let update_doc =
            Self::make_filter_doc_aux_update(push_updates, survey_matches, current_version, now);

        let find_doc = Self::make_find_doc_aux_update(object_id, current_version);

        let update_result = alert_aux_collection
            .update_one(find_doc, update_doc)
            .await?;
        if update_result.matched_count == 0 {
            warn!(
                "Concurrent modification detected for object_id {}. Using DB-only update.",
                object_id
            );
            return Err(AlertError::ConcurrentAuxUpdate(object_id.to_string()));
        }
        Ok(())
    }

    async fn process_alert(&mut self, avro_bytes: &[u8]) -> Result<ProcessAlertStatus, AlertError>;
}

#[instrument(skip_all)]
fn report_progress(start: &Instant, stream: &Survey, count: u64, message: &str) {
    let elapsed = start.elapsed().as_secs();
    info!(
        ?stream,
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
    worker_id: Uuid,
) -> Result<(), AlertWorkerError> {
    debug!(?config_path);
    let config = AppConfig::from_path(config_path)?;
    let survey = T::survey();
    let worker_config = config
        .workers
        .get(&survey)
        .ok_or(AlertWorkerError::WorkerConfigMissing(survey.clone()))?;

    let mut alert_processor = T::new(config_path).await?;

    let input_queue_name = alert_processor.input_queue_name();
    let temp_queue_name = format!("{}_temp", input_queue_name);
    let output_queue_name = alert_processor.output_queue_name();

    let mut con = config
        .build_redis()
        .await
        .inspect_err(as_error!("failed to create redis client"))?;

    let command_interval: usize = worker_config.command_interval;
    let mut command_check_countdown = command_interval;
    let mut count = 0;

    let start = std::time::Instant::now();
    let worker_id_attr = KeyValue::new("worker.id", worker_id.to_string());
    let survey_attr = KeyValue::new("survey", survey.to_string());
    let active_attrs = [worker_id_attr.clone(), survey_attr.clone()];
    let ok_added_attrs = vec![
        worker_id_attr.clone(),
        survey_attr.clone(),
        KeyValue::new("status", "ok"),
        KeyValue::new("reason", "added"),
    ];
    let ok_exists_attrs = vec![
        worker_id_attr.clone(),
        survey_attr.clone(),
        KeyValue::new("status", "ok"),
        KeyValue::new("reason", "exists"),
    ];
    let input_error_attrs = vec![
        worker_id_attr.clone(),
        survey_attr.clone(),
        KeyValue::new("status", "error"),
        KeyValue::new("reason", "input_queue"),
    ];
    let processing_error_attrs = vec![
        worker_id_attr.clone(),
        survey_attr.clone(),
        KeyValue::new("status", "error"),
        KeyValue::new("reason", "processing"),
    ];
    let output_error_attrs = vec![
        worker_id_attr,
        survey_attr,
        KeyValue::new("status", "error"),
        KeyValue::new("reason", "output_queue"),
    ];
    loop {
        // check for command from threadpool
        if command_check_countdown == 0 {
            if should_terminate(&mut receiver) {
                break;
            }
            command_check_countdown = command_interval;
        }

        ACTIVE.add(1, &active_attrs);

        command_check_countdown -= 1;
        let result = retrieve_avro_bytes(&mut con, &input_queue_name, &temp_queue_name).await;

        let avro_bytes = match result {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                trace!("queue is empty");
                ACTIVE.add(-1, &active_attrs);
                tokio::time::sleep(tokio::time::Duration::from_millis(QUEUE_EMPTY_DELAY_MS)).await;
                command_check_countdown = 0;
                continue;
            }
            Err(e) => {
                log_error!(e, "failed to retrieve avro bytes");
                ACTIVE.add(-1, &active_attrs);
                ALERT_PROCESSED.add(1, &input_error_attrs);
                tokio::time::sleep(tokio::time::Duration::from_secs(VALKEY_ERROR_DELAY_SECS)).await;
                command_check_countdown = 0;
                continue;
            }
        };

        let process_result = alert_processor.process_alert(&avro_bytes).await;
        let mut attributes = match process_result {
            Ok(ProcessAlertStatus::Added(_)) => &ok_added_attrs,
            Ok(ProcessAlertStatus::Exists(_)) => &ok_exists_attrs,
            Err(_) => &processing_error_attrs,
        };
        let handle_result = handle_process_result(
            &mut con,
            &temp_queue_name,
            &output_queue_name,
            avro_bytes,
            process_result,
        )
        .await
        .inspect_err(as_error!("failed to handle process result"));
        if handle_result.is_err() {
            attributes = &output_error_attrs;
        }

        ACTIVE.add(-1, &active_attrs);
        ALERT_PROCESSED.add(1, attributes);

        handle_result?;
        if count > 0 && count % 1000 == 0 {
            report_progress(&start, &survey, count, "progress");
        }
        count += 1;
    }
    report_progress(&start, &survey, count, "summary");
    Ok(())
}
