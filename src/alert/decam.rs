use crate::{
    alert::base::{
        deserialize_mjd, get_schema_and_startidx, AlertError, AlertWorker, AlertWorkerError,
    },
    conf,
    utils::{
        db::{cutout2bsonbinary, get_coordinates, mongify},
        spatial::xmatch,
    },
};
use apache_avro::from_value;
use apache_avro::{from_avro_datum, Schema};
use constcat::concat;
use flare::Time;
use mongodb::bson::{doc, Document};
use tracing::{error, trace};

pub const STREAM_NAME: &str = "DECAM";
pub const DECAM_DEC_RANGE: (f64, f64) = (-20.0, 20.0);
pub const DECAM_UNCERTAINTY: f64 = 0.1; // 0.1 arcsec
pub const ALERT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts");
pub const ALERT_AUX_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_aux");
pub const ALERT_CUTOUT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_cutouts");

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct FpHist {
    #[serde(rename(deserialize = "mjd"))]
    #[serde(deserialize_with = "deserialize_mjd")]
    pub jd: f64,
    pub forcediffimflux: f64,
    pub forcediffimfluxunc: f64,
    #[serde(rename(deserialize = "forcediffimmag"))]
    pub magap: f64,
    #[serde(rename(deserialize = "forcediffimmagunc"))]
    pub sigmagap: f64,
    pub band: String,
    pub diffmaglim: f64,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Candidate {
    #[serde(rename(deserialize = "mjd"))]
    #[serde(deserialize_with = "deserialize_mjd")]
    pub jd: f64,
    pub forcediffimflux: f64,
    pub forcediffimfluxunc: f64,
    #[serde(rename(deserialize = "forcediffimmag"))]
    pub magap: f64,
    #[serde(rename(deserialize = "forcediffimmagunc"))]
    pub sigmagap: f64,
    pub band: String,
    pub diffmaglim: f64,
    pub ra: f64,
    pub dec: f64,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DecamAlert {
    pub publisher: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candid: i64,
    pub candidate: Candidate,
    pub fp_hists: Vec<FpHist>,
    #[serde(rename = "cutoutScience")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub cutout_science: Vec<u8>,
    #[serde(rename = "cutoutTemplate")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub cutout_template: Vec<u8>,
    #[serde(rename = "cutoutDifference")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub cutout_difference: Vec<u8>,
}

pub struct DecamAlertWorker {
    stream_name: String,
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<Document>,
    alert_aux_collection: mongodb::Collection<Document>,
    alert_cutout_collection: mongodb::Collection<Document>,
    cached_schema: Option<Schema>,
    cached_start_idx: Option<usize>,
}

impl DecamAlertWorker {
    pub async fn alert_from_avro_bytes(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<DecamAlert, AlertError> {
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

                // if it's not an error this time, cache the new schema
                // otherwise return the error
                let value = from_avro_datum(&schema, &mut &avro_bytes[startidx..], None)?;
                self.cached_schema = Some(schema);
                self.cached_start_idx = Some(startidx);
                value
            }
        };

        let alert: DecamAlert = from_value::<DecamAlert>(&value)?;

        Ok(alert)
    }
}

#[async_trait::async_trait]
impl AlertWorker for DecamAlertWorker {
    type ObjectId = String;

    async fn new(config_path: &str) -> Result<DecamAlertWorker, AlertWorkerError> {
        let config_file = conf::load_config(&config_path)?;

        let xmatch_configs = conf::build_xmatch_configs(&config_file, STREAM_NAME)?;

        let db: mongodb::Database = conf::build_db(&config_file).await?;

        let alert_collection = db.collection(&ALERT_COLLECTION);
        let alert_aux_collection = db.collection(&ALERT_AUX_COLLECTION);
        let alert_cutout_collection = db.collection(&ALERT_CUTOUT_COLLECTION);

        let worker = DecamAlertWorker {
            stream_name: STREAM_NAME.to_string(),
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
        format!("{}_alerts_filter_queue", self.stream_name)
    }

    async fn insert_aux(
        self: &mut Self,
        object_id: impl Into<Self::ObjectId> + Send,
        ra: f64,
        dec: f64,
        _prv_candidates_doc: &Vec<Document>,
        _prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError> {
        let start = std::time::Instant::now();
        let xmatches = xmatch(ra, dec, &self.xmatch_configs, &self.db).await?;
        trace!("Xmatch took: {:?}", start.elapsed());

        let start = std::time::Instant::now();
        let alert_aux_doc = doc! {
            "_id": object_id.into(),
            "fp_hists": fp_hist_doc,
            "cross_matches": xmatches,
            "aliases": survey_matches,
            "created_at": now,
            "updated_at": now,
            "coordinates": {
                "radec_geojson": {
                    "type": "Point",
                    "coordinates": [ra - 180.0, dec],
                },
            }
        };

        self.alert_aux_collection
            .insert_one(alert_aux_doc)
            .await
            .map_err(|e| match *e.kind {
                mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                    write_error,
                )) if write_error.code == 11000 => AlertError::AlertAuxExists,
                _ => e.into(),
            })?;

        trace!("Inserting alert_aux: {:?}", start.elapsed());

        Ok(())
    }

    async fn update_aux(
        self: &mut Self,
        object_id: impl Into<Self::ObjectId> + Send,
        _prv_candidates_doc: &Vec<Document>,
        _prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError> {
        let start = std::time::Instant::now();

        let update_doc = doc! {
            "$addToSet": {
                "fp_hists": { "$each": fp_hist_doc }
            },
            "$set": {
                "updated_at": now,
                "aliases": survey_matches,
            }
        };

        self.alert_aux_collection
            .update_one(doc! { "_id": object_id.into() }, update_doc)
            .await?;

        trace!("Updating alert_aux: {:?}", start.elapsed());

        Ok(())
    }

    async fn process_alert(self: &mut Self, avro_bytes: &[u8]) -> Result<i64, AlertError> {
        let now = Time::now().to_jd();

        let start = std::time::Instant::now();

        let alert = self.alert_from_avro_bytes(avro_bytes).await?;

        trace!("Decoding alert: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        let fp_hist = alert.fp_hists;

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
                _ => e.into(),
            })?;

        trace!("Formatting & Inserting alert: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        let cutout_doc = doc! {
            "_id": &candid,
            "cutoutScience": cutout2bsonbinary(alert.cutout_science),
            "cutoutTemplate": cutout2bsonbinary(alert.cutout_template),
            "cutoutDifference": cutout2bsonbinary(alert.cutout_difference),
        };

        self.alert_cutout_collection.insert_one(cutout_doc).await?;

        trace!("Formatting & Inserting cutout: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        let alert_aux_exists = self
            .alert_aux_collection
            .count_documents(doc! { "_id": &object_id })
            .await?
            > 0;

        trace!("Checking if alert_aux exists: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        let fp_hist_doc = fp_hist.into_iter().map(|x| mongify(&x)).collect::<Vec<_>>();

        trace!("Formatting fp_hist: {:?}", start.elapsed());

        if !alert_aux_exists {
            let result = self
                .insert_aux(
                    &object_id,
                    ra,
                    dec,
                    &Vec::new(),
                    &Vec::new(),
                    &fp_hist_doc,
                    &None,
                    now,
                )
                .await;
            if let Err(AlertError::AlertAuxExists) = result {
                self.update_aux(
                    &object_id,
                    &Vec::new(),
                    &Vec::new(),
                    &fp_hist_doc,
                    &None,
                    now,
                )
                .await?;
            } else {
                result?;
            }
        } else {
            self.update_aux(
                &object_id,
                &Vec::new(),
                &Vec::new(),
                &fp_hist_doc,
                &None,
                now,
            )
            .await?;
        }

        Ok(candid)
    }
}
