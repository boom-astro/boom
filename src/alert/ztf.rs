use crate::{
    alert::base::{AlertError, AlertWorker, AlertWorkerError, SchemaRegistryError},
    conf,
    utils::{
        conversions::{flux2mag, fluxerr2diffmaglim, SNT},
        db::{cutout2bsonbinary, get_coordinates, mongify},
        spatial::xmatch,
    },
};
use apache_avro::from_value;
use apache_avro::{from_avro_datum, Reader, Schema};
use flare::Time;
use mongodb::bson::doc;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use std::io::Read;
use tracing::{error, trace};

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

    // each avro record is preceded by a 4-byte length field. We know alert packets
    // contain only one record so we can read the first 4 bytes, and consider
    // everything else after that as the data
    cursor
        .read_exact(&mut [0u8; 4])
        .map_err(SchemaRegistryError::CursorError)?;

    // we now have the start index of the data
    let start_idx = cursor.position();

    Ok((schema.to_owned(), start_idx as usize))
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct Cutout {
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "stampData")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub stamp_data: Vec<u8>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct PrvCandidate {
    pub jd: f64,
    #[serde(rename(deserialize = "fid", serialize = "band"))]
    #[serde(deserialize_with = "deserialize_fid")]
    pub band: String,
    pub pid: i64,
    pub diffmaglim: Option<f32>,
    pub programpi: Option<String>,
    pub programid: i32,
    pub candid: Option<i64>,
    #[serde(deserialize_with = "deserialize_isdiffpos_option")]
    pub isdiffpos: Option<bool>,
    pub nid: Option<i32>,
    pub rcid: Option<i32>,
    pub field: Option<i32>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub chipsf: Option<f32>,
    pub magap: Option<f32>,
    pub sigmagap: Option<f32>,
    pub distnr: Option<f32>,
    pub magnr: Option<f32>,
    pub sigmagnr: Option<f32>,
    pub chinr: Option<f32>,
    pub sharpnr: Option<f32>,
    pub sky: Option<f32>,
    pub fwhm: Option<f32>,
    pub mindtoedge: Option<f32>,
    pub seeratio: Option<f32>,
    pub aimage: Option<f32>,
    pub bimage: Option<f32>,
    pub elong: Option<f32>,
    pub nneg: Option<i32>,
    pub nbad: Option<i32>,
    pub rb: Option<f32>,
    pub ssdistnr: Option<f32>,
    pub ssmagnr: Option<f32>,
    #[serde(deserialize_with = "deserialize_ssnamenr")]
    pub ssnamenr: Option<String>,
    pub ranr: Option<f64>,
    pub decnr: Option<f64>,
    pub scorr: Option<f64>,
    pub magzpsci: Option<f32>,
}

/// avro alert schema
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct FpHist {
    pub field: Option<i32>,
    pub rcid: Option<i32>,
    #[serde(rename(deserialize = "fid", serialize = "band"))]
    #[serde(deserialize_with = "deserialize_fid")]
    pub band: String,
    pub pid: i64,
    pub rfid: i64,
    pub magzpsci: Option<f32>,
    pub magzpsciunc: Option<f32>,
    pub magzpscirms: Option<f32>,
    pub exptime: Option<f32>,
    pub diffmaglim: Option<f32>,
    pub programid: i32,
    pub jd: f64,
    #[serde(deserialize_with = "deserialize_missing_flux")]
    pub forcediffimflux: Option<f32>,
    #[serde(deserialize_with = "deserialize_missing_flux")]
    pub forcediffimfluxunc: Option<f32>,
    pub procstatus: Option<String>,
    pub distnr: Option<f32>,
    pub magnr: Option<f32>,
    pub sigmagnr: Option<f32>,
    pub chinr: Option<f32>,
    pub sharpnr: Option<f32>,
}

// we want a custom deserializer for forcediffimflux, to avoid NaN values and -9999.0
fn deserialize_missing_flux<'de, D>(deserializer: D) -> Result<Option<f32>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<f32> = Option::deserialize(deserializer)?;
    Ok(value.filter(|&x| x != -9999.0 && !x.is_nan()))
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct ForcedPhot {
    #[serde(flatten)]
    pub fp_hist: FpHist,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub diffmaglim: f32,
    pub isdiffpos: Option<bool>,
    pub snr: Option<f32>,
}

impl TryFrom<FpHist> for ForcedPhot {
    type Error = AlertError;
    fn try_from(fp_hist: FpHist) -> Result<Self, Self::Error> {
        let psf_flux_err = fp_hist
            .forcediffimfluxunc
            .ok_or(AlertError::MissingFluxPSF)?;

        let magzpsci = fp_hist.magzpsci.ok_or(AlertError::MissingMagZPSci)?;

        let (magpsf, sigmapsf, isdiffpos, snr) = match fp_hist.forcediffimflux {
            Some(psf_flux) if (psf_flux / psf_flux_err) > SNT => {
                let (magpsf, sigmapsf) = flux2mag(psf_flux, psf_flux_err, magzpsci);
                let isdiffpos = psf_flux > 0.0;
                (
                    Some(magpsf),
                    Some(sigmapsf),
                    Some(isdiffpos),
                    Some(psf_flux / psf_flux_err),
                )
            }
            _ => (None, None, None, None),
        };

        let diffmaglim = fluxerr2diffmaglim(psf_flux_err, magzpsci);

        Ok(ForcedPhot {
            fp_hist,
            magpsf,
            sigmapsf,
            diffmaglim,
            isdiffpos,
            snr,
        })
    }
}

/// avro alert schema
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Candidate {
    pub jd: f64,
    #[serde(rename(deserialize = "fid", serialize = "band"))]
    #[serde(deserialize_with = "deserialize_fid")]
    pub band: String,
    pub pid: i64,
    pub diffmaglim: Option<f32>,
    pub programpi: Option<String>,
    pub programid: i32,
    pub candid: i64,
    #[serde(deserialize_with = "deserialize_isdiffpos")]
    pub isdiffpos: bool,
    pub nid: Option<i32>,
    pub rcid: Option<i32>,
    pub field: Option<i32>,
    pub ra: f64,
    pub dec: f64,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub chipsf: Option<f32>,
    pub magap: Option<f32>,
    pub sigmagap: Option<f32>,
    pub distnr: Option<f32>,
    pub magnr: Option<f32>,
    pub sigmagnr: Option<f32>,
    pub chinr: Option<f32>,
    pub sharpnr: Option<f32>,
    pub sky: Option<f32>,
    pub fwhm: Option<f32>,
    pub mindtoedge: Option<f32>,
    pub seeratio: Option<f32>,
    pub aimage: Option<f32>,
    pub bimage: Option<f32>,
    pub elong: Option<f32>,
    pub nneg: Option<i32>,
    pub nbad: Option<i32>,
    pub rb: Option<f32>,
    pub ssdistnr: Option<f32>,
    pub ssmagnr: Option<f32>,
    #[serde(deserialize_with = "deserialize_ssnamenr")]
    pub ssnamenr: Option<String>,
    pub ranr: f64,
    pub decnr: f64,
    pub sgmag1: Option<f32>,
    pub srmag1: Option<f32>,
    pub simag1: Option<f32>,
    pub szmag1: Option<f32>,
    pub sgscore1: Option<f32>,
    pub distpsnr1: Option<f32>,
    pub ndethist: i32,
    pub ncovhist: i32,
    pub jdstarthist: Option<f64>,
    pub scorr: Option<f64>,
    pub sgmag2: Option<f32>,
    pub srmag2: Option<f32>,
    pub simag2: Option<f32>,
    pub szmag2: Option<f32>,
    pub sgscore2: Option<f32>,
    pub distpsnr2: Option<f32>,
    pub sgmag3: Option<f32>,
    pub srmag3: Option<f32>,
    pub simag3: Option<f32>,
    pub szmag3: Option<f32>,
    pub sgscore3: Option<f32>,
    pub distpsnr3: Option<f32>,
    pub dsnrms: Option<f32>,
    pub ssnrms: Option<f32>,
    pub dsdiff: Option<f32>,
    pub magzpsci: Option<f32>,
    pub magzpsciunc: Option<f32>,
    pub magzpscirms: Option<f32>,
    pub zpmed: Option<f32>,
    pub exptime: Option<f32>,
    pub drb: Option<f32>,
}

fn deserialize_isdiffpos_option<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: serde_json::Value = Deserialize::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => {
            // if s is in t, T, true, True, "1"
            if s.eq_ignore_ascii_case("t")
                || s.eq_ignore_ascii_case("true")
                || s.eq_ignore_ascii_case("1")
            {
                Ok(Some(true))
            } else {
                Ok(Some(false))
            }
        }
        serde_json::Value::Number(n) => Ok(Some(
            n.as_i64().ok_or(serde::de::Error::custom(
                "Failed to convert isdiffpos to i64",
            ))? == 1,
        )),
        serde_json::Value::Bool(b) => Ok(Some(b)),
        _ => Ok(None),
    }
}

fn deserialize_isdiffpos<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_isdiffpos_option(deserializer).map(|x| x.unwrap())
}

fn deserialize_fid<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    // the fid is a mapper: 1 = g, 2 = r, 3 = i
    let fid: i32 = Deserialize::deserialize(deserializer)?;
    match fid {
        1 => Ok("g".to_string()),
        2 => Ok("r".to_string()),
        3 => Ok("i".to_string()),
        _ => Err(serde::de::Error::custom(format!("Unknown fid: {}", fid))),
    }
}

fn deserialize_prv_forced_sources<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<ForcedPhot>>, D::Error>
where
    D: Deserializer<'de>,
{
    let dia_forced_sources = <Vec<FpHist> as Deserialize>::deserialize(deserializer)?;
    let forced_phots = dia_forced_sources
        .into_iter()
        .map(ForcedPhot::try_from)
        .collect::<Result<Vec<ForcedPhot>, AlertError>>()
        .map_err(serde::de::Error::custom)?;
    Ok(Some(forced_phots))
}

fn deserialize_ssnamenr<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    // if the value is null, "null", "", return None
    let value: Option<String> = Deserialize::deserialize(deserializer)?;
    Ok(value.filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("null")))
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct ZtfAlert {
    pub schemavsn: String,
    pub publisher: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candid: i64,
    pub candidate: Candidate,
    pub prv_candidates: Option<Vec<PrvCandidate>>,
    #[serde(deserialize_with = "deserialize_prv_forced_sources")]
    pub fp_hists: Option<Vec<ForcedPhot>>,
    #[serde(
        rename = "cutoutScience",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    pub cutout_science: Option<Vec<u8>>,
    #[serde(
        rename = "cutoutTemplate",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    pub cutout_template: Option<Vec<u8>>,
    #[serde(
        rename = "cutoutDifference",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    pub cutout_difference: Option<Vec<u8>>,
}

fn deserialize_cutout_as_bytes<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let cutout: Option<Cutout> = Option::deserialize(deserializer)?;
    Ok(cutout.map(|cutout| cutout.stamp_data))
}

pub struct ZtfAlertWorker {
    stream_name: String,
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_aux_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_cutout_collection: mongodb::Collection<mongodb::bson::Document>,
    cached_schema: Option<Schema>,
    cached_start_idx: Option<usize>,
}

impl ZtfAlertWorker {
    pub async fn alert_from_avro_bytes(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<ZtfAlert, AlertError> {
        // if the schema is not cached, get it from the avro_bytes
        let (schema_ref, start_idx) = match (self.cached_schema.as_ref(), self.cached_start_idx) {
            (Some(schema), Some(start_idx)) => (schema, start_idx),
            _ => {
                let (schema, startidx) = get_schema_and_startidx(avro_bytes)?;
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
            Err(e) => {
                error!("Error deserializing avro message with cached schema: {}", e);
                let (schema, startidx) = get_schema_and_startidx(avro_bytes)?;
                let value = from_avro_datum(&schema, &mut &avro_bytes[startidx..], None);

                // if it's not an error this time, cache the new schema
                // otherwise return the error
                match value {
                    Ok(value) => {
                        self.cached_schema = Some(schema);
                        self.cached_start_idx = Some(startidx);
                        value
                    }
                    Err(e) => {
                        return Err(AlertError::DecodeError(e));
                    }
                }
            }
        };

        let alert: ZtfAlert = from_value::<ZtfAlert>(&value).map_err(AlertError::DecodeError)?;

        Ok(alert)
    }
}

#[async_trait::async_trait]
impl AlertWorker for ZtfAlertWorker {
    async fn new(config_path: &str) -> Result<ZtfAlertWorker, AlertWorkerError> {
        let stream_name = "ZTF".to_string();

        let config_file = conf::load_config(&config_path)?;

        let xmatch_configs = conf::build_xmatch_configs(&config_file, &stream_name)?;

        let db: mongodb::Database = conf::build_db(&config_file).await?;

        let alert_collection = db.collection(&format!("{}_alerts", stream_name));
        let alert_aux_collection = db.collection(&format!("{}_alerts_aux", stream_name));
        let alert_cutout_collection = db.collection(&format!("{}_alerts_cutouts", stream_name));

        let worker = ZtfAlertWorker {
            stream_name: stream_name.clone(),
            xmatch_configs,
            db,
            alert_collection,
            alert_aux_collection,
            alert_cutout_collection,
            cached_schema: None,
            cached_start_idx: None,
        };
        Ok(worker)
    }

    fn stream_name(&self) -> String {
        self.stream_name.clone()
    }

    fn input_queue_name(&self) -> String {
        format!("{}_alerts_packets_queue", self.stream_name)
    }

    fn output_queue_name(&self) -> String {
        format!("{}_alerts_classifier_queue", self.stream_name)
    }

    async fn process_alert(self: &mut Self, avro_bytes: &[u8]) -> Result<i64, AlertError> {
        let now = Time::now().to_jd();

        let start = std::time::Instant::now();

        let mut alert = self.alert_from_avro_bytes(avro_bytes).await?;

        trace!("Decoding alert: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        let prv_candidates = alert.prv_candidates.take();
        let fp_hist = alert.fp_hists.take();

        let candid = alert.candid;
        let object_id = alert.object_id;
        let ra = alert.candidate.ra;
        let dec = alert.candidate.dec;

        let candidate_doc = mongify(&alert.candidate);

        let alert_doc = doc! {
            "_id": &candid,
            "objectId": &object_id,
            "candidate": &candidate_doc,
            "coordinates": get_coordinates(ra, dec),
            "created_at": now,
            "updated_at": now,
        };

        self.alert_collection
            .insert_one(alert_doc)
            .await
            .map_err(|e| match *e.kind {
                mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                    write_error,
                )) if write_error.code == 11000 => AlertError::AlertExists,
                _ => AlertError::InsertAlertError(e),
            })?;

        trace!("Formatting & Inserting alert: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        let cutout_doc = doc! {
            "_id": &candid,
            "cutoutScience": cutout2bsonbinary(alert.cutout_science.ok_or(AlertError::MissingCutout)?),
            "cutoutTemplate": cutout2bsonbinary(alert.cutout_template.ok_or(AlertError::MissingCutout)?),
            "cutoutDifference": cutout2bsonbinary(alert.cutout_difference.ok_or(AlertError::MissingCutout)?),
        };

        self.alert_cutout_collection
            .insert_one(cutout_doc)
            .await
            .map_err(AlertError::InsertCutoutError)?;

        trace!("Formatting & Inserting cutout: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        let alert_aux_exists = self
            .alert_aux_collection
            .count_documents(doc! { "_id": &object_id })
            .await
            .map_err(AlertError::FindObjectIdError)?
            > 0;

        trace!("Checking if alert_aux exists: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        // we split the prv_candidates into detections and non-detections
        let mut prv_candidates_doc = vec![];
        let mut prv_nondetections_doc = vec![];

        for prv_candidate in prv_candidates.unwrap_or(vec![]) {
            if prv_candidate.magpsf.is_some() {
                prv_candidates_doc.push(mongify(&prv_candidate));
            } else {
                prv_nondetections_doc.push(mongify(&prv_candidate));
            }
        }
        prv_candidates_doc.push(candidate_doc);

        let fp_hist_doc = fp_hist
            .unwrap_or(vec![])
            .into_iter()
            .map(|x| mongify(&x))
            .collect::<Vec<_>>();

        trace!("Formatting prv_candidates & fp_hist: {:?}", start.elapsed());

        if !alert_aux_exists {
            let start = std::time::Instant::now();
            let xmatches = xmatch(ra, dec, &self.xmatch_configs, &self.db).await;
            trace!("Xmatch took: {:?}", start.elapsed());

            let start = std::time::Instant::now();
            let alert_aux_doc = doc! {
                "_id": &object_id,
                "prv_candidates": prv_candidates_doc,
                "prv_nondetections": prv_nondetections_doc,
                "fp_hists": fp_hist_doc,
                "cross_matches": xmatches,
                "created_at": now,
                "updated_at": now,
                "coordinates": {
                    "radec_geojson": {
                        "type": "Point",
                        "coordinates": [ra - 180.0, dec],
                    },
                },
            };
            self.alert_aux_collection
                .insert_one(alert_aux_doc)
                .await
                .map_err(AlertError::InsertAuxAlertError)?;

            trace!("Inserting alert_aux: {:?}", start.elapsed());
        } else {
            let start = std::time::Instant::now();
            let update_doc = doc! {
                "$addToSet": {
                    "prv_candidates": { "$each": prv_candidates_doc },
                    "prv_nondetections": { "$each": prv_nondetections_doc },
                    "fp_hists": { "$each": fp_hist_doc }
                },
                "$set": {
                    "updated_at": now,
                }
            };

            self.alert_aux_collection
                .update_one(doc! { "_id": &object_id }, update_doc)
                .await
                .map_err(AlertError::UpdateAuxAlertError)?;

            trace!("Updating alert_aux: {:?}", start.elapsed());
        }

        Ok(candid)
    }
}
