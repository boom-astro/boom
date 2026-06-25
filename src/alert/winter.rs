use std::collections::HashMap;

use crate::{
    alert::{
        base::{
            AlertError, AlertWorker, AlertWorkerError, LightcurveJdOnly, ProcessAlertStatus,
            SchemaCache,
        },
        lsst, ztf, TimeSeries,
    },
    conf::{self, AppConfig},
    utils::{
        cutouts::CutoutStorage,
        db::{mongify_vec, update_timeseries_op},
        enums::Survey,
        lightcurves::Band,
        o11y::logging::as_error,
        spatial::{xmatch, Coordinates},
    },
};
use constcat::concat;
use flare::Time;
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use tracing::{debug, error, instrument, warn};

pub const STREAM_NAME: &str = "WINTER";
// WINTER observes from Palomar; it covers roughly the same northern sky as ZTF.
pub const WINTER_DEC_RANGE: (f64, f64) = (-30.0, 90.0);
// Position uncertainty in arcsec used to set cross-match radii. WINTER's astrometry
// is comparable to ZTF's; Kowalski cross-matches WINTER against ZTF using a 2" cone.
pub const WINTER_POSITION_UNCERTAINTY: f64 = 2.0;
pub const ALERT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts");
pub const ALERT_AUX_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_aux");

pub const WINTER_ZTF_XMATCH_RADIUS: f64 =
    (WINTER_POSITION_UNCERTAINTY.max(ztf::ZTF_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();
pub const WINTER_LSST_XMATCH_RADIUS: f64 =
    (WINTER_POSITION_UNCERTAINTY.max(lsst::LSST_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();

/// Map a WINTER filter id to a photometric [`Band`].
///
/// WINTER `fid` encoding (from the alert schema): 0=Y, 1=J, 2=H, 3=K.
/// Unknown ids default to Y so ingestion never fails on an unexpected filter.
pub fn fid_to_band(fid: i32) -> Band {
    match fid {
        0 => Band::Y,
        1 => Band::J,
        2 => Band::H,
        3 => Band::K,
        _ => Band::Y,
    }
}

// serde default for the (avro-absent) `band` field; the real value is filled in
// from `fid` during processing.
fn default_band() -> Band {
    Band::Y
}

/// WINTER candidate record.
///
/// Mirrors the upstream `winter.alert.candidate` avro record (which is modelled
/// closely on the ZTF candidate schema). The `band` field is not present in the
/// avro packet; it defaults during deserialization and is populated from `fid`.
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct WinterCandidate {
    pub candid: i64,
    pub deprecated: bool,
    pub jd: f64,
    pub fid: i32,
    pub exptime: f32,
    pub ndethist: i32,
    pub jdstarthist: f64,
    pub jdendhist: f64,
    pub progname: String,
    pub programid: i32,
    pub isdiffpos: bool,
    pub field: i32,
    pub ra: f64,
    pub dec: f64,
    pub magzpsci: Option<f32>,
    pub magzpsciunc: Option<f32>,
    pub magzpscirms: Option<f32>,
    pub diffmaglim: Option<f32>,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub chipsf: Option<f32>,
    pub magap: Option<f32>,
    pub sigmagap: Option<f32>,
    pub magapbig: Option<f32>,
    pub sigmagapbig: Option<f32>,
    pub magdiff: Option<f32>,
    pub magfromlim: Option<f32>,
    pub distnr: Option<f32>,
    pub magnr: Option<f32>,
    pub sigmagnr: Option<f32>,
    pub xpos: Option<f32>,
    pub ypos: Option<f32>,
    pub sky: Option<f32>,
    pub fwhm: Option<f32>,
    pub classtar: Option<f32>,
    pub mindtoedge: Option<f32>,
    pub seeratio: Option<f32>,
    pub aimage: Option<f32>,
    pub bimage: Option<f32>,
    pub aimagerat: Option<f32>,
    pub bimagerat: Option<f32>,
    pub elong: Option<f32>,
    pub nneg: Option<i32>,
    pub nbad: Option<i32>,
    pub sumrat: Option<f32>,
    pub dsnrms: Option<f32>,
    pub ssnrms: Option<f32>,
    pub dsdiff: Option<f32>,
    pub scorr: Option<f64>,
    pub clrcoeff: Option<f32>,
    pub clrcounc: Option<f32>,
    pub zpclrcov: Option<f32>,
    pub zpmed: Option<f32>,
    pub clrmed: Option<f32>,
    pub clrrms: Option<f32>,
    pub rb: Option<f32>,
    pub rbversion: Option<String>,
    pub ssdistnr: Option<f32>,
    pub ssmagnr: Option<f32>,
    pub ssnamenr: Option<String>,
    pub tooflag: bool,
    pub nmtchps: i32,
    pub psra1: Option<f32>,
    pub psdec1: Option<f32>,
    pub psobjectid1: Option<f32>,
    pub sgmag1: Option<f32>,
    pub srmag1: Option<f32>,
    pub simag1: Option<f32>,
    pub szmag1: Option<f32>,
    pub sgscore1: Option<f32>,
    pub distpsnr1: Option<f32>,
    pub psobjectid2: Option<f32>,
    pub sgmag2: Option<f32>,
    pub srmag2: Option<f32>,
    pub simag2: Option<f32>,
    pub szmag2: Option<f32>,
    pub sgscore2: Option<f32>,
    pub distpsnr2: Option<f32>,
    pub psobjectid3: Option<f32>,
    pub sgmag3: Option<f32>,
    pub srmag3: Option<f32>,
    pub simag3: Option<f32>,
    pub szmag3: Option<f32>,
    pub sgscore3: Option<f32>,
    pub distpsnr3: Option<f32>,
    pub nmtchtm: i32,
    pub tmjmag1: Option<f32>,
    pub tmhmag1: Option<f32>,
    pub tmkmag1: Option<f32>,
    pub tmobjectid1: Option<String>,
    pub tmjmag2: Option<f32>,
    pub tmhmag2: Option<f32>,
    pub tmkmag2: Option<f32>,
    pub tmobjectid2: Option<String>,
    pub tmjmag3: Option<f32>,
    pub tmhmag3: Option<f32>,
    pub tmkmag3: Option<f32>,
    pub tmobjectid3: Option<String>,
    pub neargaia: Option<f32>,
    pub neargaiabright: Option<f32>,
    pub maggaia: Option<f32>,
    pub maggaiabright: Option<f32>,
    // Not present in the avro packet; derived from `fid` during processing.
    #[serde(default = "default_band")]
    pub band: Band,
}

impl TimeSeries for WinterCandidate {
    fn time(&self) -> f64 {
        self.jd
    }
}

/// WINTER previous-candidate (historical detection) record.
///
/// Mirrors the upstream `winter.alert.prv_candidate` avro record. As with the
/// candidate, `band` is derived from `fid` after deserialization.
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct WinterPrvCandidate {
    pub candid: Option<i64>,
    pub progname: String,
    pub jd: f64,
    pub fid: i32,
    pub isdiffpos: bool,
    pub fieldid: i32,
    pub ra: f64,
    pub dec: f64,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub fwhm: Option<f32>,
    pub scorr: Option<f64>,
    #[serde(default = "default_band")]
    pub band: Band,
}

impl TimeSeries for WinterPrvCandidate {
    fn time(&self) -> f64 {
        self.jd
    }
}

impl WinterPrvCandidate {
    /// Build a previous-candidate (lightcurve) point from the current detection,
    /// so the latest epoch is included in the stored lightcurve.
    fn from_candidate(c: &WinterCandidate) -> WinterPrvCandidate {
        WinterPrvCandidate {
            candid: Some(c.candid),
            progname: c.progname.clone(),
            jd: c.jd,
            fid: c.fid,
            isdiffpos: c.isdiffpos,
            fieldid: c.field,
            ra: c.ra,
            dec: c.dec,
            magpsf: c.magpsf,
            sigmapsf: c.sigmapsf,
            fwhm: c.fwhm,
            scorr: c.scorr,
            band: c.band.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct WinterRawAvroAlert {
    pub publisher: String,
    #[serde(rename = "objectid")]
    pub object_id: String,
    pub candid: i64,
    pub candidate: WinterCandidate,
    #[serde(default)]
    pub prv_candidates: Option<Vec<WinterPrvCandidate>>,
    #[serde(rename = "cutout_science")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub cutout_science: Vec<u8>,
    #[serde(rename = "cutout_template")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub cutout_template: Vec<u8>,
    #[serde(rename = "cutout_difference")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub cutout_difference: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WinterAliases {
    #[serde(rename = "ZTF")]
    pub ztf: Vec<String>,
    #[serde(rename = "LSST")]
    pub lsst: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WinterObject {
    #[serde(rename = "_id")]
    pub object_id: String,
    pub prv_candidates: Vec<WinterPrvCandidate>,
    pub cross_matches: Option<HashMap<String, Vec<Document>>>,
    pub aliases: Option<WinterAliases>,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct WinterAlert {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: WinterCandidate,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Deserialize, Serialize)]
struct AlertAuxForUpdate {
    #[serde(default)]
    pub prv_candidates: Vec<LightcurveJdOnly>,
    pub version: Option<i32>,
}

// ---------------------------------------------------------------------------
// Avro schema sanitization
// ---------------------------------------------------------------------------
//
// The upstream WINTER alert schema contains a duplicate field name (`sgmag1`
// appears twice in the candidate record). Python's fastavro tolerates this, but
// the Avro spec forbids duplicate field names and Rust's apache-avro rejects the
// container outright ("Duplicate field name sgmag1"). To stay faithful to the
// real stream rather than depend on a fixed-upstream schema, we rewrite the
// embedded schema at read time: later duplicates are renamed (e.g. `sgmag1` ->
// `sgmag1__dup1`). Field order, types, and count are unchanged, so the binary
// data still decodes correctly; the renamed field is simply ignored by the
// strongly-typed structs above.

#[derive(thiserror::Error, Debug)]
pub enum WinterAvroError {
    #[error("not an avro object container file (bad magic)")]
    BadMagic,
    #[error("unexpected end of avro header")]
    UnexpectedEof,
    #[error("avro metadata is missing the avro.schema entry")]
    MissingSchema,
    #[error("failed to parse embedded avro schema as JSON")]
    SchemaJson(#[from] serde_json::Error),
    #[error("invalid utf-8 in embedded avro schema")]
    SchemaUtf8(#[from] std::str::Utf8Error),
}

fn decode_long(buf: &[u8], pos: &mut usize) -> Result<i64, WinterAvroError> {
    let mut shift = 0u32;
    let mut result: u64 = 0;
    loop {
        let b = *buf.get(*pos).ok_or(WinterAvroError::UnexpectedEof)?;
        *pos += 1;
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(((result >> 1) as i64) ^ -((result & 1) as i64))
}

fn encode_long(n: i64, out: &mut Vec<u8>) {
    let mut zz = ((n << 1) ^ (n >> 63)) as u64;
    loop {
        let mut b = (zz & 0x7f) as u8;
        zz >>= 7;
        if zz != 0 {
            b |= 0x80;
        }
        out.push(b);
        if zz == 0 {
            break;
        }
    }
}

fn encode_bytes(value: &[u8], out: &mut Vec<u8>) {
    encode_long(value.len() as i64, out);
    out.extend_from_slice(value);
}

/// Recursively rename duplicate field names within every avro record so the
/// schema satisfies the (strict) Rust avro parser.
fn dedupe_record_field_names(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            if map.get("type").and_then(|t| t.as_str()) == Some("record") {
                if let Some(serde_json::Value::Array(fields)) = map.get_mut("fields") {
                    let mut seen: HashMap<String, usize> = HashMap::new();
                    for field in fields.iter_mut() {
                        if let Some(name) = field.get("name").and_then(|n| n.as_str()) {
                            let name = name.to_string();
                            let count = seen.entry(name.clone()).or_insert(0);
                            if *count > 0 {
                                let new_name = format!("{}__dup{}", name, count);
                                if let Some(obj) = field.as_object_mut() {
                                    obj.insert(
                                        "name".to_string(),
                                        serde_json::Value::String(new_name),
                                    );
                                }
                            }
                            *count += 1;
                        }
                    }
                }
            }
            for (_, v) in map.iter_mut() {
                dedupe_record_field_names(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                dedupe_record_field_names(v);
            }
        }
        _ => {}
    }
}

/// Rewrite a WINTER avro object-container file so its embedded schema has no
/// duplicate field names. Returns bytes that the standard avro `Reader` accepts.
/// Idempotent for already-clean inputs.
pub fn sanitize_winter_avro(bytes: &[u8]) -> Result<Vec<u8>, WinterAvroError> {
    if bytes.len() < 4 || &bytes[0..4] != b"Obj\x01" {
        return Err(WinterAvroError::BadMagic);
    }
    let mut pos = 4usize;

    // metadata map<bytes>
    let mut meta: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    loop {
        let mut count = decode_long(bytes, &mut pos)?;
        if count == 0 {
            break;
        }
        if count < 0 {
            count = -count;
            // block byte-size prefix (present for negative block counts)
            let _block_size = decode_long(bytes, &mut pos)?;
        }
        for _ in 0..count {
            let klen = decode_long(bytes, &mut pos)? as usize;
            let key = bytes
                .get(pos..pos + klen)
                .ok_or(WinterAvroError::UnexpectedEof)?
                .to_vec();
            pos += klen;
            let vlen = decode_long(bytes, &mut pos)? as usize;
            let val = bytes
                .get(pos..pos + vlen)
                .ok_or(WinterAvroError::UnexpectedEof)?
                .to_vec();
            pos += vlen;
            meta.push((key, val));
        }
    }

    // 16-byte sync marker, then the data blocks begin
    let sync = bytes
        .get(pos..pos + 16)
        .ok_or(WinterAvroError::UnexpectedEof)?
        .to_vec();
    pos += 16;
    let data_start = pos;

    // Sanitize the embedded schema JSON.
    let schema_idx = meta
        .iter()
        .position(|(k, _)| k == b"avro.schema")
        .ok_or(WinterAvroError::MissingSchema)?;
    let schema_str = std::str::from_utf8(&meta[schema_idx].1)?;
    let mut schema_json: serde_json::Value = serde_json::from_str(schema_str)?;
    dedupe_record_field_names(&mut schema_json);
    meta[schema_idx].1 = serde_json::to_vec(&schema_json)?;

    // Rebuild the header (magic + metadata map + sync), reusing the original
    // sync marker so the trailing data blocks stay consistent.
    let mut out = Vec::with_capacity(bytes.len() + 64);
    out.extend_from_slice(b"Obj\x01");
    encode_long(meta.len() as i64, &mut out);
    for (k, v) in &meta {
        encode_bytes(k, &mut out);
        encode_bytes(v, &mut out);
    }
    encode_long(0, &mut out); // map terminator
    out.extend_from_slice(&sync);
    out.extend_from_slice(&bytes[data_start..]);

    Ok(out)
}

pub struct WinterAlertWorker {
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<WinterAlert>,
    alert_aux_collection: mongodb::Collection<WinterObject>,
    alert_cutout_storage: CutoutStorage,
    alert_aux_collection_update: mongodb::Collection<AlertAuxForUpdate>,
    ztf_alert_aux_collection: mongodb::Collection<Document>,
    lsst_alert_aux_collection: mongodb::Collection<Document>,
    schema_cache: SchemaCache,
}

impl WinterAlertWorker {
    #[instrument(skip(self), err)]
    async fn get_survey_matches(&self, ra: f64, dec: f64) -> Result<WinterAliases, AlertError> {
        let ztf_matches = self
            .get_matches(
                ra,
                dec,
                ztf::ZTF_DEC_RANGE,
                WINTER_ZTF_XMATCH_RADIUS,
                &self.ztf_alert_aux_collection,
            )
            .await?;

        let lsst_matches = self
            .get_matches(
                ra,
                dec,
                lsst::LSST_DEC_RANGE,
                WINTER_LSST_XMATCH_RADIUS,
                &self.lsst_alert_aux_collection,
            )
            .await?;
        Ok(WinterAliases {
            ztf: ztf_matches,
            lsst: lsst_matches,
        })
    }

    async fn get_existing_aux(
        &self,
        object_id: &str,
    ) -> Result<Option<AlertAuxForUpdate>, AlertError> {
        let result = self
            .alert_aux_collection_update
            .find_one(doc! { "_id": object_id })
            .projection(doc! { "prv_candidates.jd": 1, "version": 1 })
            .await
            .inspect_err(as_error!())?;
        Ok(result)
    }

    #[instrument(skip(self, prv_candidates, survey_matches), err)]
    async fn update_aux_fallback(
        &mut self,
        object_id: &str,
        prv_candidates: &Vec<WinterPrvCandidate>,
        survey_matches: &Option<WinterAliases>,
        now: f64,
    ) -> Result<(), AlertError> {
        Self::db_only_aux_update(
            object_id,
            doc! {
                "prv_candidates": update_timeseries_op("prv_candidates", "jd", &mongify_vec(prv_candidates)),
            },
            survey_matches,
            now,
            &self.alert_aux_collection,
        )
        .await
    }

    #[instrument(skip(self, prv_candidates, survey_matches, existing_alert_aux))]
    async fn update_aux_inner(
        &mut self,
        object_id: &str,
        prv_candidates: &Vec<WinterPrvCandidate>,
        survey_matches: &Option<WinterAliases>,
        now: f64,
        existing_alert_aux: &AlertAuxForUpdate,
    ) -> Result<(), AlertError> {
        let current_version = existing_alert_aux.version;

        let prepared_prv_candidates = WinterPrvCandidate::prepare_timeseries_update(
            prv_candidates,
            &existing_alert_aux.prv_candidates,
            "prv_candidates",
        )?;

        let mut push_updates = Document::new();
        Self::add_to_push_aux_update(&mut push_updates, "prv_candidates", prepared_prv_candidates);

        Self::finalize_aux_update(
            object_id,
            push_updates,
            survey_matches,
            current_version,
            now,
            &self.alert_aux_collection,
        )
        .await
    }

    async fn update_aux(
        &mut self,
        object_id: &str,
        prv_candidates: &Vec<WinterPrvCandidate>,
        survey_matches: &Option<WinterAliases>,
        now: f64,
        existing_alert_aux: &AlertAuxForUpdate,
    ) -> Result<(), AlertError> {
        match self
            .update_aux_inner(
                object_id,
                prv_candidates,
                survey_matches,
                now,
                existing_alert_aux,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                match &e {
                    AlertError::ConcurrentAuxUpdate(_) => debug!(error = %e),
                    _ => error!(error = %e),
                }
                self.update_aux_fallback(object_id, prv_candidates, survey_matches, now)
                    .await
            }
        }
    }
}

#[async_trait::async_trait]
impl AlertWorker for WinterAlertWorker {
    async fn new(config_path: &str) -> Result<WinterAlertWorker, AlertWorkerError> {
        let config = AppConfig::from_path(config_path)?;

        let xmatch_configs = config
            .crossmatch
            .get(&Survey::Winter)
            .cloned()
            .unwrap_or_default();

        let db: mongodb::Database = config
            .build_db()
            .await
            .inspect_err(as_error!("failed to create mongo client"))?;

        let alert_collection = db.collection(&ALERT_COLLECTION);
        let alert_aux_collection = db.collection(&ALERT_AUX_COLLECTION);
        let alert_cutout_storage = config
            .build_cutout_storage(&Survey::Winter)
            .await
            .inspect_err(as_error!("failed to create cutout storage"))?;
        let alert_aux_collection_update = db.collection(&ALERT_AUX_COLLECTION);

        let ztf_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&ztf::ALERT_AUX_COLLECTION);

        let lsst_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&lsst::ALERT_AUX_COLLECTION);

        let worker = WinterAlertWorker {
            xmatch_configs,
            db,
            alert_collection,
            alert_aux_collection,
            alert_cutout_storage,
            alert_aux_collection_update,
            ztf_alert_aux_collection,
            lsst_alert_aux_collection,
            schema_cache: SchemaCache::default(),
        };
        Ok(worker)
    }

    fn survey() -> Survey {
        Survey::Winter
    }

    fn input_queue_name(&self) -> String {
        format!("{}_alerts_packets_queue", WinterAlertWorker::survey())
    }

    fn output_queue_name(&self) -> String {
        format!("{}_alerts_enrichment_queue", WinterAlertWorker::survey())
    }

    async fn process_alert(&mut self, avro_bytes: &[u8]) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();

        // Work around the upstream WINTER schema's duplicate field name before
        // handing the bytes to the (strict) avro reader.
        let sanitized =
            sanitize_winter_avro(avro_bytes).map_err(|e| AlertError::DecodeError(e.to_string()))?;

        let mut avro_alert: WinterRawAvroAlert = self
            .schema_cache
            .alert_from_avro_bytes(&sanitized)
            .inspect_err(as_error!())?;

        // Fill in derived bands from `fid` (not present in the avro packet).
        avro_alert.candidate.band = fid_to_band(avro_alert.candidate.fid);

        let candid = avro_alert.candid;
        let object_id = avro_alert.object_id;
        let ra = avro_alert.candidate.ra;
        let dec = avro_alert.candidate.dec;

        // Lightcurve = current detection + historical prv_candidates.
        let mut prv_candidates = vec![WinterPrvCandidate::from_candidate(&avro_alert.candidate)];
        if let Some(mut history) = avro_alert.prv_candidates {
            for p in history.iter_mut() {
                p.band = fid_to_band(p.fid);
            }
            prv_candidates.extend(history);
        }
        WinterPrvCandidate::sanitize_timeseries(&mut prv_candidates);

        let alert = WinterAlert {
            candid,
            object_id: object_id.clone(),
            candidate: avro_alert.candidate,
            coordinates: Coordinates::new(ra, dec),
            created_at: now,
            updated_at: now,
        };

        let status = self
            .format_and_insert_alert(candid, &alert, &self.alert_collection)
            .await
            .inspect_err(as_error!())?;

        if let ProcessAlertStatus::Exists(_) = status {
            return Ok(status);
        }

        let survey_matches = Some(
            self.get_survey_matches(ra, dec)
                .await
                .inspect_err(as_error!())?,
        );

        let existing_alert_aux = self.get_existing_aux(&object_id).await?;

        if let Some(existing) = existing_alert_aux {
            self.update_aux(&object_id, &prv_candidates, &survey_matches, now, &existing)
                .await
                .inspect_err(as_error!())?;
        } else {
            let xmatches = xmatch(ra, dec, &self.xmatch_configs, &self.db).await?;
            let obj = WinterObject {
                object_id: object_id.clone(),
                prv_candidates,
                cross_matches: Some(xmatches),
                aliases: survey_matches,
                coordinates: Coordinates::new(ra, dec),
                created_at: now,
                updated_at: now,
            };
            let result = self.insert_aux(&obj, &self.alert_aux_collection).await;
            if let Err(AlertError::AlertAuxExists) = result {
                warn!(
                    "Alert aux document for object_id {} already exists. Using fallback update.",
                    object_id
                );
                self.update_aux_fallback(&object_id, &obj.prv_candidates, &obj.aliases, now)
                    .await
                    .inspect_err(as_error!())?;
            } else {
                result.inspect_err(as_error!())?;
            }
        }

        let status = self
            .format_and_insert_cutouts(
                candid,
                &object_id,
                avro_alert.cutout_science,
                avro_alert.cutout_template,
                avro_alert.cutout_difference,
                &self.alert_cutout_storage,
            )
            .await
            .inspect_err(as_error!())?;

        Ok(status)
    }
}
