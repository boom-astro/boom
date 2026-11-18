use crate::conf::TnsConfig;
use crate::utils::{bson_number, spatial::Coordinates};
use chrono::NaiveDateTime;
use flate2::read::DeflateDecoder;
use mongodb::bson::{doc, to_document, Document};
use mongodb::Database;
use reqwest::{header::HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use utoipa::ToSchema;

pub const TNS_COLLECTION: &str = "TNS";
pub const PHOTOMETRY_COLLECTION: &str = "TNS_photometry";

const OBJECTS_PATH: &str = "system/files/tns_public_objects";
const GET_OBJECT_PATH: &str = "api/get/object";
const AB_MAG: &str = "ABMag";
const MAX_ATTEMPTS: u32 = 5;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_RATE_LIMIT_WAIT: Duration = Duration::from_secs(60);

#[derive(thiserror::Error, Debug)]
pub enum TnsError {
    #[error("tns.bot_id must be the bot's numeric id, got {0:?}")]
    BotId(String),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("TNS answered {status} for {url}: {body}")]
    Status {
        status: StatusCode,
        url: String,
        body: String,
    },
    #[error("TNS kept answering 429 for {0}")]
    RateLimited(String),
    #[error("could not read the TNS objects file: {0}")]
    Archive(String),
    #[error("canceled")]
    Canceled,
}

pub enum ObjectsFile {
    Full,
    Hourly { start: i64 },
}

impl ObjectsFile {
    pub fn name(&self) -> String {
        match self {
            Self::Full => "tns_public_objects.csv.zip".to_string(),
            Self::Hourly { start } => {
                format!(
                    "tns_public_objects_{:02}.csv.zip",
                    (start + 3600).div_euclid(3600).rem_euclid(24)
                )
            }
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ObjectRow {
    pub objid: String,
    pub name_prefix: String,
    pub name: String,
    pub ra: String,
    pub declination: String,
    pub redshift: String,
    #[serde(rename = "type")]
    pub object_type: String,
    pub discoverydate: String,
    pub discoverymag: String,
    pub filter: String,
    pub reporters: String,
    pub internal_names: String,
    pub lastmodified: String,
}

pub struct ObjectsTable {
    pub start: i64,
    pub rows: Vec<ObjectRow>,
    pub unreadable: usize,
}

pub struct ObjectDocument {
    pub objid: i64,
    pub ra: f64,
    pub dec: f64,
    pub document: Document,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TnsPhotometryPoint {
    pub jd: f64,
    pub mag: Option<f64>,
    pub mag_err: Option<f64>,
    pub lim_mag: Option<f64>,
    pub flux: Option<f64>,
    pub flux_err: Option<f64>,
    pub lim_flux: Option<f64>,
    pub unit: Option<String>,
    pub filter: Option<String>,
    pub instrument: Option<String>,
    pub telescope: Option<String>,
    pub reporting_group: Option<String>,
    pub survey: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TnsPhotometry {
    #[serde(rename = "_id")]
    pub objid: i64,
    pub name: String,
    pub fetched_at: f64,
    pub photometry: Vec<TnsPhotometryPoint>,
}

pub struct TnsClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    resume_at: Option<Instant>,
}

impl TnsClient {
    pub fn new(config: &TnsConfig) -> Result<Self, TnsError> {
        let bot_id = config.bot_id.trim();
        if bot_id.is_empty() || !bot_id.chars().all(|c| c.is_ascii_digit()) {
            return Err(TnsError::BotId(bot_id.to_string()));
        }
        let marker = format!(
            r#"tns_marker{{"tns_id":{bot_id},"type":"bot","name":"{}"}}"#,
            config.bot_name.trim()
        );
        let http = reqwest::Client::builder()
            .user_agent(marker)
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self {
            http,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            api_key: config.api_key.trim().to_string(),
            resume_at: None,
        })
    }

    pub async fn objects_file(
        &mut self,
        file: &ObjectsFile,
        canceled: &AtomicBool,
    ) -> Result<Option<Vec<u8>>, TnsError> {
        let url = format!("{}/{OBJECTS_PATH}/{}", self.base_url, file.name());
        let form = [("api_key", self.api_key.clone())];
        let response = self.post(&url, &form, canceled).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body = checked(response, &url).await?.bytes().await?;
        Ok(Some(body.to_vec()))
    }

    pub async fn object_photometry(
        &mut self,
        name: &str,
        canceled: &AtomicBool,
    ) -> Result<Vec<TnsPhotometryPoint>, TnsError> {
        let url = format!("{}/{GET_OBJECT_PATH}", self.base_url);
        let data = serde_json::json!({
            "objname": name,
            "objid": "",
            "photometry": "1",
            "spectra": "0",
        });
        let form = [
            ("api_key", self.api_key.clone()),
            ("data", data.to_string()),
        ];
        let response = self.post(&url, &form, canceled).await?;
        let reply: Value = checked(response, &url).await?.json().await?;
        Ok(photometry_from_reply(&reply))
    }

    async fn post(
        &mut self,
        url: &str,
        form: &[(&str, String)],
        canceled: &AtomicBool,
    ) -> Result<reqwest::Response, TnsError> {
        for _ in 0..MAX_ATTEMPTS {
            if let Some(resume_at) = self.resume_at.take() {
                pause_until(resume_at, canceled).await?;
            }
            let response = self.http.post(url).form(form).send().await?;
            if let Some(wait) = rate_limit_wait(response.status(), response.headers()) {
                self.resume_at = Some(Instant::now() + wait);
            }
            if response.status() != StatusCode::TOO_MANY_REQUESTS {
                return Ok(response);
            }
        }
        Err(TnsError::RateLimited(url.to_string()))
    }
}

async fn checked(response: reqwest::Response, url: &str) -> Result<reqwest::Response, TnsError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response.text().await.unwrap_or_default();
    Err(TnsError::Status {
        status,
        url: url.to_string(),
        body: body.chars().take(300).collect(),
    })
}

async fn pause_until(resume_at: Instant, canceled: &AtomicBool) -> Result<(), TnsError> {
    while Instant::now() < resume_at {
        if canceled.load(Ordering::Relaxed) {
            return Err(TnsError::Canceled);
        }
        tokio::time::sleep_until(resume_at.min(Instant::now() + Duration::from_secs(1))).await;
    }
    Ok(())
}

pub fn rate_limit_wait(status: StatusCode, headers: &HeaderMap) -> Option<Duration> {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
    };
    let exhausted = header("x-rate-limit-remaining") == Some(0);
    if status != StatusCode::TOO_MANY_REQUESTS && !exhausted {
        return None;
    }
    Some(
        header("x-rate-limit-reset")
            .map(|seconds| Duration::from_secs(seconds + 1))
            .unwrap_or(DEFAULT_RATE_LIMIT_WAIT),
    )
}

fn first_zip_entry(archive: &[u8]) -> Result<Vec<u8>, TnsError> {
    let u16_at = |at: usize| {
        archive
            .get(at..at + 2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]))
    };
    let u32_at = |at: usize| {
        archive
            .get(at..at + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    };
    if u32_at(0) != Some(0x0403_4b50) {
        return Err(TnsError::Archive("not a zip archive".to_string()));
    }
    let (Some(flags), Some(method), Some(size), Some(name_len), Some(extra_len)) =
        (u16_at(6), u16_at(8), u32_at(18), u16_at(26), u16_at(28))
    else {
        return Err(TnsError::Archive("truncated zip header".to_string()));
    };
    let data = archive
        .get(30 + name_len as usize + extra_len as usize..)
        .ok_or_else(|| TnsError::Archive("truncated zip entry".to_string()))?;
    match method {
        0 if flags & 0x08 == 0 => data
            .get(..size as usize)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| TnsError::Archive("truncated zip entry".to_string())),
        8 => {
            let mut out = Vec::new();
            DeflateDecoder::new(data)
                .read_to_end(&mut out)
                .map_err(|e| TnsError::Archive(e.to_string()))?;
            Ok(out)
        }
        other => Err(TnsError::Archive(format!(
            "unsupported zip compression method {other}"
        ))),
    }
}

fn datetime(value: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value.trim(), "%Y-%m-%d %H:%M:%S%.f").ok()
}

fn file_start(line: &str) -> Option<i64> {
    let line = line.trim_start_matches('\u{feff}').trim().trim_matches('"');
    datetime(line.split(" - ").next()?).map(|t| t.and_utc().timestamp())
}

pub fn read_objects(archive: &[u8]) -> Result<ObjectsTable, TnsError> {
    let csv = first_zip_entry(archive)?;
    let newline = csv
        .iter()
        .position(|&b| b == b'\n')
        .ok_or_else(|| TnsError::Archive("empty file".to_string()))?;
    let first_line = String::from_utf8_lossy(&csv[..newline]);
    let start = file_start(&first_line)
        .ok_or_else(|| TnsError::Archive(format!("unexpected first line {first_line:?}")))?;
    let mut reader = csv::Reader::from_reader(&csv[newline + 1..]);
    let mut rows = Vec::new();
    let mut unreadable = 0;
    for row in reader.deserialize::<ObjectRow>() {
        match row {
            Ok(row) => rows.push(row),
            Err(_) => unreadable += 1,
        }
    }
    Ok(ObjectsTable {
        start,
        rows,
        unreadable,
    })
}

fn text(value: &str) -> Option<String> {
    Some(value.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn number(value: &str) -> Option<f64> {
    value.trim().parse::<f64>().ok().filter(|v| v.is_finite())
}

fn jd(value: &str) -> Option<f64> {
    datetime(value).map(|t| t.and_utc().timestamp_millis() as f64 / 86_400_000.0 + 2_440_587.5)
}

impl ObjectRow {
    pub fn modified_at(&self) -> Option<i64> {
        datetime(&self.lastmodified).map(|t| t.and_utc().timestamp())
    }
}

pub fn object_document(row: &ObjectRow) -> Option<ObjectDocument> {
    let objid = row.objid.trim().parse::<i64>().ok()?;
    let name = text(&row.name)?;
    let (ra, dec) = (number(&row.ra)?, number(&row.declination)?);
    let coordinates = to_document(&Coordinates::try_new(ra, dec)?).ok()?;
    let internal_names: Vec<String> = row.internal_names.split(',').filter_map(text).collect();
    Some(ObjectDocument {
        objid,
        ra,
        dec,
        document: doc! {
            "name_prefix": text(&row.name_prefix),
            "name": name,
            "ra": ra,
            "dec": dec,
            "type": text(&row.object_type),
            "redshift": number(&row.redshift),
            "discovery_date": text(&row.discoverydate),
            "discovery_mag": number(&row.discoverymag),
            "discovery_mag_filter": text(&row.filter),
            "reporters": text(&row.reporters),
            "internal_names": internal_names,
            "discovery_jd": jd(&row.discoverydate),
            "coordinates": coordinates,
        },
    })
}

fn json_number(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
    .filter(|v: &f64| v.is_finite())
}

fn json_name(value: Option<&Value>) -> Option<String> {
    let name = match value? {
        Value::Object(map) => map.get("name").or_else(|| map.get("group_name"))?,
        other => other,
    };
    name.as_str().and_then(text)
}

fn photometry_point(entry: &Value) -> Option<TnsPhotometryPoint> {
    let public = entry.get("public");
    if public == Some(&Value::Bool(false)) || json_number(public) == Some(0.0) {
        return None;
    }
    let value = json_number(entry.get("flux"));
    let error = json_number(entry.get("fluxerr"));
    let limit = json_number(entry.get("limflux"));
    if value.is_none() && limit.is_none() {
        return None;
    }
    let unit = json_name(entry.get("flux_unit"));
    let mut point = TnsPhotometryPoint {
        jd: json_number(entry.get("jd"))?,
        filter: json_name(entry.get("filters")),
        instrument: json_name(entry.get("instrument")),
        telescope: json_name(entry.get("telescope")),
        reporting_group: json_name(entry.get("reporting_group"))
            .or_else(|| json_name(entry.get("reporting_group_name"))),
        survey: json_name(entry.get("source_group"))
            .or_else(|| json_name(entry.get("source_group_name"))),
        unit: unit.clone(),
        ..Default::default()
    };
    if unit.as_deref() == Some(AB_MAG) {
        (point.mag, point.mag_err, point.lim_mag) = (value, error, limit);
    } else {
        (point.flux, point.flux_err, point.lim_flux) = (value, error, limit);
    }
    Some(point)
}

pub fn photometry_from_reply(reply: &Value) -> Vec<TnsPhotometryPoint> {
    let mut points: Vec<TnsPhotometryPoint> = reply
        .pointer("/data/photometry")
        .and_then(Value::as_array)
        .map(|entries| entries.iter().filter_map(photometry_point).collect())
        .unwrap_or_default();
    points.sort_by(|a, b| a.jd.total_cmp(&b.jd));
    points
}

pub fn nearest_objid(matches: &[Document]) -> Option<i64> {
    matches
        .iter()
        .filter_map(|m| {
            let objid = bson_number(m.get("_id")?)? as i64;
            let distance = m
                .get("distance_arcsec")
                .and_then(bson_number)
                .unwrap_or(f64::INFINITY);
            Some((objid, distance))
        })
        .min_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(objid, _)| objid)
}

pub async fn photometry_for_matches(
    db: &Database,
    cross_matches: Option<&HashMap<String, Vec<Document>>>,
) -> Result<Option<TnsPhotometry>, mongodb::error::Error> {
    let Some(objid) = cross_matches
        .and_then(|matches| matches.get(TNS_COLLECTION))
        .and_then(|matches| nearest_objid(matches))
    else {
        return Ok(None);
    };
    db.collection::<TnsPhotometry>(PHOTOMETRY_COLLECTION)
        .find_one(doc! { "_id": objid })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    const HOURLY: &[u8] = include_bytes!("../../tests/data/tns/tns_public_objects_09.csv.zip");
    const GET_OBJECT: &str = include_str!("../../tests/data/tns/get_object_photometry.json");

    #[test]
    fn hourly_file_reads_into_the_stored_tns_shape() {
        let table = read_objects(HOURLY).unwrap();
        assert_eq!(table.start, 1_769_328_000);
        assert_eq!(table.rows.len(), 3);
        assert_eq!(table.unreadable, 0);

        let object = object_document(&table.rows[0]).unwrap();
        assert_eq!(object.objid, 182_301);
        let doc = object.document;
        assert_eq!(doc.get_str("name_prefix").unwrap(), "AT");
        assert_eq!(doc.get_str("name").unwrap(), "2026bna");
        assert_eq!(doc.get_f64("ra").unwrap(), 150.123456);
        assert_eq!(doc.get_f64("dec").unwrap(), -12.345678);
        assert!(doc.get("type").unwrap().as_null().is_some());
        assert!(doc.get("redshift").unwrap().as_null().is_some());
        assert_eq!(
            doc.get_str("discovery_date").unwrap(),
            "2026-01-24 17:58:00.768"
        );
        assert_eq!(doc.get_f64("discovery_mag").unwrap(), 19.41);
        assert_eq!(doc.get_str("discovery_mag_filter").unwrap(), "r");
        let internal_names: Vec<&str> = doc
            .get_array("internal_names")
            .unwrap()
            .iter()
            .filter_map(|n| n.as_str())
            .collect();
        assert_eq!(internal_names, ["ZTF26aabcdef", "ATLAS26xyz"]);
        let discovery_jd = doc.get_f64("discovery_jd").unwrap();
        assert!((discovery_jd - 2_461_065.248_62).abs() < 1e-6);
        let point = doc
            .get_document("coordinates")
            .unwrap()
            .get_document("radec_geojson")
            .unwrap();
        assert_eq!(point.get_str("type").unwrap(), "Point");
        let lonlat: Vec<f64> = point
            .get_array("coordinates")
            .unwrap()
            .iter()
            .filter_map(|v| v.as_f64())
            .collect();
        assert_eq!(lonlat, [150.123456 - 180.0, -12.345678]);

        let classified = object_document(&table.rows[1]).unwrap().document;
        assert_eq!(classified.get_str("name_prefix").unwrap(), "SN");
        assert_eq!(classified.get_str("type").unwrap(), "SN Ia");
        assert_eq!(classified.get_f64("redshift").unwrap(), 0.021);

        assert!(object_document(&table.rows[2]).is_none());
    }

    #[test]
    fn hourly_files_are_named_after_the_hour_they_end() {
        assert_eq!(
            ObjectsFile::Hourly {
                start: 1_769_328_000
            }
            .name(),
            "tns_public_objects_09.csv.zip"
        );
        assert_eq!(
            ObjectsFile::Hourly {
                start: 1_769_382_000
            }
            .name(),
            "tns_public_objects_00.csv.zip"
        );
    }

    #[test]
    fn photometry_keeps_detections_and_limits_apart() {
        let reply: Value = serde_json::from_str(GET_OBJECT).unwrap();
        let points = photometry_from_reply(&reply);
        assert_eq!(points.len(), 3);

        let limit = &points[0];
        assert_eq!(limit.jd, 2_461_062.791_550_9);
        assert_eq!(limit.mag, None);
        assert_eq!(limit.lim_mag, Some(20.12));
        assert_eq!(limit.filter.as_deref(), Some("r"));
        assert_eq!(limit.instrument.as_deref(), Some("ZTF-Cam"));
        assert_eq!(limit.telescope.as_deref(), Some("P48"));
        assert_eq!(limit.survey.as_deref(), Some("ZTF"));

        let detection = &points[1];
        assert_eq!(detection.mag, Some(19.41));
        assert_eq!(detection.mag_err, Some(0.08));
        assert_eq!(detection.lim_mag, None);

        let flux = &points[2];
        assert_eq!(flux.mag, None);
        assert_eq!(flux.flux, Some(12.5));
        assert_eq!(flux.unit.as_deref(), Some("mJy"));
    }

    #[test]
    fn rate_limit_waits_out_the_window() {
        let mut headers = HeaderMap::new();
        headers.insert("x-rate-limit-remaining", HeaderValue::from_static("7"));
        headers.insert("x-rate-limit-reset", HeaderValue::from_static("42"));
        assert_eq!(rate_limit_wait(StatusCode::OK, &headers), None);

        headers.insert("x-rate-limit-remaining", HeaderValue::from_static("0"));
        assert_eq!(
            rate_limit_wait(StatusCode::OK, &headers),
            Some(Duration::from_secs(43))
        );

        headers.insert(
            "x-rate-limit-remaining",
            HeaderValue::from_static("Exceeded"),
        );
        assert_eq!(
            rate_limit_wait(StatusCode::TOO_MANY_REQUESTS, &headers),
            Some(Duration::from_secs(43))
        );
        assert_eq!(
            rate_limit_wait(StatusCode::TOO_MANY_REQUESTS, &HeaderMap::new()),
            Some(DEFAULT_RATE_LIMIT_WAIT)
        );
    }

    #[test]
    fn photometry_is_read_for_the_nearest_tns_match() {
        let matches = vec![
            doc! { "_id": 7_i32, "distance_arcsec": 1.8 },
            doc! { "_id": 9_i64, "distance_arcsec": 0.4 },
        ];
        assert_eq!(nearest_objid(&matches), Some(9));
        assert_eq!(nearest_objid(&[]), None);
    }
}
