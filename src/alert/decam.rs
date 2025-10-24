use crate::{
    alert::base::{
        deserialize_mjd, Alert, AlertError, AlertWorker, AlertWorkerError, ProcessAlertStatus,
        SchemaCache,
    },
    conf,
    utils::{
        db::{mongify, update_timeseries_op},
        enums::Survey,
        o11y::logging::as_error,
        spatial::xmatch,
    },
};
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
    schema_cache: SchemaCache,
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

        let xmatch_configs = conf::build_xmatch_configs(&config_file, &Survey::Decam)?;

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
            schema_cache: SchemaCache::default(),
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
        format!("{}_alerts_enrichment_queue", self.stream_name)
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
        let update_pipeline = vec![doc! {
            "$set": {
                "fp_hists": update_timeseries_op("fp_hists", "jd", fp_hist_doc),
                "aliases": survey_matches,
                "updated_at": now,
            }
        }];

        self.alert_aux_collection
            .update_one(doc! { "_id": object_id }, update_pipeline)
            .await?;

        Ok(())
    }

    async fn process_alert(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();
        let alert: DecamAlert = self
            .schema_cache
            .alert_from_avro_bytes(avro_bytes)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{
        enums::Survey,
        testing::{decam_alert_worker, AlertRandomizer},
    };

    #[tokio::test]
    async fn test_decam_alert_from_avro_bytes() {
        let mut alert_worker = decam_alert_worker().await;

        let (candid, object_id, ra, dec, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Decam).get().await;
        let alert = alert_worker
            .schema_cache
            .alert_from_avro_bytes(&bytes_content);
        assert!(alert.is_ok());

        // validate the alert
        let alert: DecamAlert = alert.unwrap();
        assert_eq!(alert.publisher, "DESIRT");
        assert_eq!(alert.object_id, object_id);
        assert_eq!(alert.candid, candid);
        assert_eq!(alert.candidate.ra, ra);
        assert_eq!(alert.candidate.dec, dec);

        // validate the fp_hists
        let fp_hists = alert.clone().fp_hists;
        assert_eq!(fp_hists.len(), 61);

        let fp_positive_det = fp_hists.get(0).unwrap();
        assert!((fp_positive_det.magap - 22.595936).abs() < 1e-6);
        assert!((fp_positive_det.sigmagap - 0.093660).abs() < 1e-6);
        assert!((fp_positive_det.jd - 2460709.838387).abs() < 1e-6);
        assert_eq!(fp_positive_det.band, "g");

        // validate the cutouts
        assert_eq!(alert.cutout_science.clone().len(), 54561);
        assert_eq!(alert.cutout_template.clone().len(), 49810);
        assert_eq!(alert.cutout_difference.clone().len(), 54569);
    }
}
