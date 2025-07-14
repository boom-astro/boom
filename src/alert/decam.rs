use crate::{
    alert::base::{
        deserialize_mjd, get_schema_and_startidx, Alert, AlertError, AlertWorker, AlertWorkerError,
        ProcessAlertStatus,
    },
    conf,
    utils::{
        db::mongify,
        o11y::{as_error, log_error, WARN},
        spatial::xmatch,
    },
};
use apache_avro::from_value;
use apache_avro::{from_avro_datum, Schema};
use constcat::concat;
use flare::Time;
use mongodb::bson::{doc, Document};
use tracing::{instrument, warn};

pub const STREAM_NAME: &str = "DECAM";
pub const DECAM_DEC_RANGE: (f64, f64) = (-90.0, 33.5);
// Position uncertainty in arcsec (median FHWM from Table 1 in https://iopscience.iop.org/article/10.3847/1538-4365/ac78eb)
pub const DECAM_POSITION_UNCERTAINTY: f64 = 1.24;
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

impl Alert for DecamAlert {
    fn object_id(&self) -> String {
        self.object_id.clone()
    }
    fn candid(&self) -> i64 {
        self.candid
    }
    fn ra(&self) -> f64 {
        self.candidate.ra
    }
    fn dec(&self) -> f64 {
        self.candidate.dec
    }
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
    #[instrument(skip(self), err)]
    async fn check_alert_aux_exists(&self, object_id: &str) -> Result<bool, AlertError> {
        let alert_aux_exists = self
            .alert_aux_collection
            .count_documents(doc! { "_id": object_id })
            .await?
            > 0;
        Ok(alert_aux_exists)
    }

    #[instrument(skip_all)]
    fn format_fp_hist(&self, fp_hist: Vec<FpHist>) -> Vec<Document> {
        fp_hist.into_iter().map(|x| mongify(&x)).collect::<Vec<_>>()
    }

    #[instrument(skip(self, fp_hist_doc, xmatches,), err)]
    async fn insert_alert_aux(
        &self,
        object_id: &str,
        ra: f64,
        dec: f64,
        fp_hist_doc: &Vec<Document>,
        xmatches: Document,
        now: f64,
    ) -> Result<(), AlertError> {
        let alert_aux_doc = doc! {
            "_id": object_id,
            "fp_hists": fp_hist_doc,
            "cross_matches": xmatches,
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
        Ok(())
    }
}

#[async_trait::async_trait]
impl AlertWorker for DecamAlertWorker {
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

    #[instrument(skip_all, err)]
    async fn alert_from_avro_bytes(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<DecamAlert, AlertError> {
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

                // if it's not an error this time, cache the new schema
                // otherwise return the error
                let value = from_avro_datum(&schema, &mut &avro_bytes[startidx..], None)
                    .inspect_err(as_error!())?;
                self.cached_schema = Some(schema);
                self.cached_start_idx = Some(startidx);
                value
            }
        };

        let alert: DecamAlert = from_value::<DecamAlert>(&value).inspect_err(as_error!())?;

        Ok(alert)
    }

    #[instrument(
        skip(
            self,
            ra,
            dec,
            _prv_candidates_doc,
            _prv_nondetections_doc,
            fp_hist_doc,
            _survey_matches
        ),
        err
    )]
    async fn insert_aux(
        self: &mut Self,
        object_id: &str,
        ra: f64,
        dec: f64,
        _prv_candidates_doc: &Vec<Document>,
        _prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        _survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError> {
        let xmatches = xmatch(ra, dec, &self.xmatch_configs, &self.db).await?;
        self.insert_alert_aux(object_id.into(), ra, dec, fp_hist_doc, xmatches, now)
            .await?;
        Ok(())
    }

    async fn update_aux(
        self: &mut Self,
        object_id: &str,
        _prv_candidates_doc: &Vec<Document>,
        _prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError> {
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
            .update_one(doc! { "_id": object_id }, update_doc)
            .await?;

        Ok(())
    }

    async fn process_alert(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();
        let alert = self
            .alert_from_avro_bytes(avro_bytes)
            .await
            .inspect_err(as_error!())?;

        let candid = alert.candid();
        let object_id = alert.object_id();
        let ra = alert.ra();
        let dec = alert.dec();

        let fp_hist = alert.fp_hists;

        let candidate_doc = mongify(&alert.candidate);

        let status = self
            .format_and_insert_alert(
                candid,
                &object_id,
                ra,
                dec,
                &candidate_doc,
                now,
                &self.alert_collection,
            )
            .await
            .inspect_err(as_error!())?;
        if let ProcessAlertStatus::Exists(_) = status {
            return Ok(status);
        }

        self.format_and_insert_cutouts(
            candid,
            alert.cutout_science,
            alert.cutout_template,
            alert.cutout_difference,
            &self.alert_cutout_collection,
        )
        .await
        .inspect_err(as_error!())?;
        let alert_aux_exists = self
            .check_alert_aux_exists(&object_id)
            .await
            .inspect_err(as_error!())?;

        let fp_hist_doc = self.format_fp_hist(fp_hist);

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
                .await
                .inspect_err(as_error!())?;
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
            .await
            .inspect_err(as_error!())?;
        }

        Ok(status)
    }
}
