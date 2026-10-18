// Internal ZTF shadow-store HTTP client and normalized-record mapping.
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::Duration};

pub const SCHEMA: &str = include_str!("schema.sql");
pub const BINNED_SQL: &str = include_str!("bins.sql");
pub const READ_SQL: &str = "SELECT kind, payload FROM boom_photometry FINAL \
    WHERE object_id = {object_id:String} ORDER BY jd, band, kind, programid FORMAT JSONEachRow";
// Aggregate AFTER FINAL; do not feed an incremental sum MV from overlapping
// packet histories. Keep programs and measurement kinds separate.
pub const NIGHTLY_SQL: &str = "SELECT kind, band, programid, toDate(ts, 'UTC') AS night, \
    count() AS n_points, sum(point_flux / (point_error * point_error)) / sum(1 / (point_error * point_error)) AS flux, \
    1 / sqrt(sum(1 / (point_error * point_error))) AS flux_err \
    FROM (SELECT kind, band, programid, ts, flux AS point_flux, flux_err AS point_error \
          FROM boom_photometry FINAL WHERE object_id = {object_id:String} \
          AND kind != 'upper_limit' AND isNotNull(flux) AND isNotNull(flux_err)) \
    GROUP BY kind, band, programid, night ORDER BY night, band, kind, programid FORMAT JSONEachRow";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub object_id: String,
    pub kind: String,
    pub band: String,
    pub jd: f64,
    pub programid: i32,
    pub revision: u64,
    pub flux: Option<f64>,
    pub flux_err: Option<f64>,
    pub payload: String,
}

/// Holds the exact bytes to resend after an ambiguous HTTP failure.
pub struct PreparedBatch {
    pub body: String,
    pub token: String,
    pub rows: usize,
}

impl PreparedBatch {
    /// Combine already normalized snapshots in a stable object order. Each row
    /// retains its source revision; the retry token covers the exact body.
    pub fn combine(batches: &[&Self]) -> Self {
        let mut body = String::with_capacity(batches.iter().map(|b| b.body.len()).sum());
        for batch in batches {
            body.push_str(&batch.body);
        }
        let digest: String = Sha256::digest(body.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let token = format!("boom-ztf-v1:{digest}");
        Self {
            body,
            token,
            rows: batches.iter().map(|b| b.rows).sum(),
        }
    }

    /// `revision` is a source-assigned monotonic version, NOT the retry time.
    /// A fixed zero is sufficient only for immutable measurement records.
    pub fn ztf<D: Serialize, U: Serialize, F: Serialize>(
        object_id: &str,
        revision: u64,
        detections: &[D],
        upper_limits: &[U],
        forced: &[F],
    ) -> Result<Self> {
        ensure!(!object_id.is_empty(), "empty ZTF object ID");
        let groups = [
            ("detection", serde_json::to_value(detections)?),
            ("upper_limit", serde_json::to_value(upper_limits)?),
            ("forced", serde_json::to_value(forced)?),
        ];
        let mut rows = BTreeMap::new();
        for (kind, records) in groups {
            for record in records.as_array().context("expected an array of records")? {
                let jd = record["jd"].as_f64().context("record has no numeric jd")?;
                ensure!(
                    (2440587.5..2440587.5 + 4294967296.0 / 86400.0).contains(&jd),
                    "jd outside DateTime range"
                );
                let band = record["band"].as_str().context("record has no band")?;
                ensure!(
                    ["g", "r", "i"].contains(&band),
                    "unsupported ZTF band: {band}"
                );
                let programid = i32::try_from(
                    record["programid"]
                        .as_i64()
                        .context("record has no programid")?,
                )?;
                let flux = optional_number(record, "psfFlux")?;
                let flux_err = optional_number(record, "psfFluxErr")?;
                ensure!(flux_err.is_none_or(|e| e > 0.0), "nonpositive psfFluxErr");
                ensure!(
                    flux.is_none() || flux_err.is_some(),
                    "flux without an error"
                );
                ensure!(
                    kind != "upper_limit" || flux.is_none(),
                    "upper limit has a measured flux"
                );
                let payload = canonical_json(record)?;
                // Positive finite JDs sort numerically by their IEEE bits.
                let key = (kind.to_owned(), band.to_owned(), jd.to_bits(), programid);
                let row = Row {
                    object_id: object_id.into(),
                    kind: kind.into(),
                    band: band.into(),
                    jd,
                    programid,
                    revision,
                    flux,
                    flux_err,
                    payload,
                };
                if let Some(existing) = rows.get(&key) {
                    let existing: &Row = existing;
                    ensure!(
                        existing.payload == row.payload,
                        "conflicting records at the same measurement key"
                    );
                }
                rows.insert(key, row);
            }
        }
        let mut body = String::new();
        for row in rows.values() {
            body.push_str(&serde_json::to_string(row)?);
            body.push('\n');
        }
        let digest: String = Sha256::digest(body.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let token = format!("boom-ztf-v1:{digest}");
        Ok(Self {
            body,
            token,
            rows: rows.len(),
        })
    }
}

fn optional_number(record: &Value, field: &str) -> Result<Option<f64>> {
    match record.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let n = value.as_f64().with_context(|| format!("invalid {field}"))?;
            ensure!(n.is_finite(), "nonfinite {field}");
            Ok(Some(n))
        }
    }
}

// Sort recursively even when a host crate enables serde_json/preserve_order.
fn canonical_json(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map.iter().collect();
            let mut fields = Vec::new();
            for (key, value) in sorted {
                fields.push(format!(
                    "{}:{}",
                    serde_json::to_string(key)?,
                    canonical_json(value)?
                ));
            }
            format!("{{{}}}", fields.join(","))
        }
        Value::Array(values) => {
            let values = values
                .iter()
                .map(canonical_json)
                .collect::<Result<Vec<_>>>()?;
            format!("[{}]", values.join(","))
        }
        _ => serde_json::to_string(value)?,
    })
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ZtfHistory {
    pub prv_candidates: Vec<Value>,
    pub prv_nondetections: Vec<Value>,
    pub fp_hists: Vec<Value>,
}

impl ZtfHistory {
    pub fn from_json_each_row(body: &str) -> Result<Self> {
        #[derive(Deserialize)]
        struct Stored {
            kind: String,
            payload: String,
        }
        let mut result = Self::default();
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            let stored: Stored = serde_json::from_str(line)?;
            let record = serde_json::from_str(&stored.payload)?;
            match stored.kind.as_str() {
                "detection" => result.prv_candidates.push(record),
                "upper_limit" => result.prv_nondetections.push(record),
                "forced" => result.fp_hists.push(record),
                other => bail!("unknown stored measurement kind: {other}"),
            }
        }
        Ok(result)
    }
}

#[derive(Clone)]
pub struct Store {
    client: reqwest::Client,
    url: reqwest::Url,
    database: String,
    user: String,
    password: String,
}

impl Store {
    pub fn new(config: &crate::conf::PhotometryShadowConfig) -> Result<Self> {
        let url: reqwest::Url = config.url.parse()?;
        ensure!(
            ["http", "https"].contains(&url.scheme()),
            "expected an HTTP(S) URL"
        );
        ensure!(
            url.username().is_empty() && url.password().is_none() && url.query().is_none(),
            "put credentials in environment variables, not the URL"
        );
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            url,
            database: config.database.clone(),
            user: config.username.clone(),
            password: config.password.clone(),
        })
    }

    async fn execute(
        &self,
        sql: &str,
        parameters: &[(&str, &str)],
        body: String,
    ) -> Result<String> {
        let mut url = self.url.clone();
        url.query_pairs_mut()
            .extend_pairs([
                ("database", self.database.as_str()),
                ("query", sql),
                ("wait_end_of_query", "1"),
            ])
            .extend_pairs(parameters.iter().copied());
        let response = self
            .client
            .post(url)
            .basic_auth(&self.user, Some(&self.password))
            .header(reqwest::header::CONTENT_LENGTH, body.len())
            .body(body)
            .send()
            .await
            .context("ClickHouse request failed")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("reading ClickHouse response")?;
        ensure!(status.is_success(), "ClickHouse returned {status}: {body}");
        Ok(body)
    }

    pub async fn initialize(&self) -> Result<()> {
        self.execute(SCHEMA, &[], String::new()).await?;
        Ok(())
    }

    /// No implicit retries: caller retains this batch and retries the exact
    /// bytes/token. A timeout is an ambiguous result, not proof of no write.
    pub async fn insert(&self, batch: &PreparedBatch) -> Result<()> {
        if batch.rows == 0 {
            return Ok(());
        }
        self.execute("INSERT INTO boom_photometry \
            (object_id, kind, band, jd, programid, revision, flux, flux_err, payload) FORMAT JSONEachRow",
            &[("async_insert", "0"), ("insert_deduplication_token", &batch.token)], batch.body.clone()).await?;
        Ok(())
    }

    pub async fn history(&self, object_id: &str) -> Result<ZtfHistory> {
        let body = self
            .execute(READ_SQL, &[("param_object_id", object_id)], String::new())
            .await?;
        ZtfHistory::from_json_each_row(&body)
    }

    pub async fn nightly(&self, object_id: &str) -> Result<String> {
        self.execute(
            NIGHTLY_SQL,
            &[("param_object_id", object_id)],
            String::new(),
        )
        .await
    }

    /// On-demand fixed-width UTC bins. Upper limits and missing flux contribute
    /// counts, never weighted flux. Programs and measurement kinds stay separate.
    /// Raw histories remain authoritative; this is not a persisted bin cache.
    pub async fn bins(&self, object_id: &str, cadence_seconds: u32) -> Result<Vec<Value>> {
        ensure!(
            (1..=31_536_000).contains(&cadence_seconds),
            "cadence must be 1..31536000 seconds"
        );
        let cadence = cadence_seconds.to_string();
        let body = self
            .execute(
                BINNED_SQL,
                &[
                    ("param_object_id", object_id),
                    ("param_cadence_seconds", &cadence),
                ],
                String::new(),
            )
            .await?;
        body.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| Ok(serde_json::from_str(line)?))
            .collect()
    }

    pub async fn storage(&self) -> Result<Value> {
        let body = self.execute("SELECT sum(rows) AS physical_rows, sum(data_compressed_bytes) AS compressed_column_bytes, \
            sum(bytes_on_disk) AS bytes_on_disk FROM system.parts WHERE active AND database=currentDatabase() \
            AND table='boom_photometry' FORMAT JSONEachRow", &[], String::new()).await?;
        let mut physical: Value = serde_json::from_str(body.trim())?;
        let logical = self
            .execute(
                "SELECT count() AS unique_rows FROM boom_photometry FINAL FORMAT JSONEachRow",
                &[],
                String::new(),
            )
            .await?;
        physical["unique_rows"] =
            serde_json::from_str::<Value>(logical.trim())?["unique_rows"].clone();
        let columns=self.execute("SELECT name, type, data_compressed_bytes, data_uncompressed_bytes, marks_bytes \
            FROM system.columns WHERE database=currentDatabase() AND table='boom_photometry' ORDER BY position FORMAT JSONEachRow",&[],String::new()).await?;
        physical["columns"] = serde_json::to_value(
            columns
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(serde_json::from_str::<Value>)
                .collect::<std::result::Result<Vec<_>, _>>()?,
        )?;
        let logical_payload=self.execute("SELECT sum(length(payload)) AS bytes FROM boom_photometry FINAL FORMAT JSONEachRow",&[],String::new()).await?;
        physical["logical_payload_bytes"] =
            serde_json::from_str::<Value>(logical_payload.trim())?["bytes"].clone();
        let events=self.execute("SELECT event, value, description FROM system.events WHERE event IN \
            ('InsertedRows','InsertedBytes','DuplicatedInsertedBlocks','MergedRows','MergedUncompressedBytes', \
             'MergeTreeDataWriterRows','MergeTreeDataWriterUncompressedBytes','MergeTreeDataWriterCompressedBytes', \
             'WriteBufferFromFileDescriptorWriteBytes') FORMAT JSONEachRow",&[],String::new()).await?;
        physical["server_profile_events"] = serde_json::to_value(
            events
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(serde_json::from_str::<Value>)
                .collect::<std::result::Result<Vec<_>, _>>()?,
        )?;
        Ok(physical)
    }
}
