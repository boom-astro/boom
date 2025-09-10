use crate::{
    alert::{
        base::{Alert, AlertError, AlertWorker, AlertWorkerError, ProcessAlertStatus, SchemaCache},
        decam, lsst,
    },
    conf,
    utils::{
        db::{mongify, update_timeseries_op},
        lightcurves::{flux2mag, fluxerr2diffmaglim, SNT},
        o11y::logging::as_error,
        spatial::xmatch,
    },
};
use constcat::concat;
use flare::Time;
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use std::fmt::Debug;
use tracing::{instrument, warn};

pub const STREAM_NAME: &str = "ZTF";
pub const ZTF_DEC_RANGE: (f64, f64) = (-30.0, 90.0);
// Position uncertainty in arcsec (median FHWM from https://www.ztf.caltech.edu/ztf-camera.html)
pub const ZTF_POSITION_UNCERTAINTY: f64 = 2.;
pub const ALERT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts");
pub const ALERT_AUX_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_aux");
pub const ALERT_CUTOUT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_cutouts");

pub const ZTF_LSST_XMATCH_RADIUS: f64 =
    (ZTF_POSITION_UNCERTAINTY.max(lsst::LSST_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();
pub const ZTF_DECAM_XMATCH_RADIUS: f64 =
    (ZTF_POSITION_UNCERTAINTY.max(decam::DECAM_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();
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
    Ok(value.filter(|&x| x != -99999.0 && !x.is_nan()))
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
            Some(psf_flux) => {
                let psf_flux_abs = psf_flux.abs();
                if (psf_flux_abs / psf_flux_err) > SNT {
                    let (magpsf, sigmapsf) = flux2mag(psf_flux_abs, psf_flux_err, magzpsci);
                    (
                        Some(magpsf),
                        Some(sigmapsf),
                        Some(psf_flux > 0.0),
                        Some(psf_flux_abs / psf_flux_err),
                    )
                } else {
                    (None, None, None, None)
                }
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
    pub nmtchps: i32,
    pub dsnrms: Option<f32>,
    pub ssnrms: Option<f32>,
    pub dsdiff: Option<f32>,
    pub magzpsci: Option<f32>,
    pub magzpsciunc: Option<f32>,
    pub magzpscirms: Option<f32>,
    pub zpmed: Option<f32>,
    pub exptime: Option<f32>,
    pub drb: Option<f32>,

    pub clrcoeff: Option<f32>,
    pub clrcounc: Option<f32>,
    pub neargaia: Option<f32>,
    pub neargaiabright: Option<f32>,
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
        .filter_map(|fp| ForcedPhot::try_from(fp).ok())
        .collect();

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
    pub cutout_science: Vec<u8>,
    #[serde(
        rename = "cutoutTemplate",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    pub cutout_template: Vec<u8>,
    #[serde(
        rename = "cutoutDifference",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    pub cutout_difference: Vec<u8>,
}

fn deserialize_cutout_as_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let cutout: Option<Cutout> = Option::deserialize(deserializer)?;
    // if cutout is None, return an error
    match cutout {
        None => Err(serde::de::Error::custom("Missing cutout data")),
        Some(cutout) => Ok(cutout.stamp_data),
    }
}

impl Alert for ZtfAlert {
    fn object_id(&self) -> String {
        self.object_id.clone()
    }
    fn ra(&self) -> f64 {
        self.candidate.ra
    }
    fn dec(&self) -> f64 {
        self.candidate.dec
    }
    fn candid(&self) -> i64 {
        self.candid
    }
}

pub struct ZtfAlertWorker {
    stream_name: String,
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<Document>,
    alert_aux_collection: mongodb::Collection<Document>,
    alert_cutout_collection: mongodb::Collection<Document>,
    schema_cache: SchemaCache,
    lsst_alert_aux_collection: mongodb::Collection<Document>,
    decam_alert_aux_collection: mongodb::Collection<Document>,
}

impl ZtfAlertWorker {
    #[instrument(skip(self), err)]
    async fn get_survey_matches(&self, ra: f64, dec: f64) -> Result<Document, AlertError> {
        let lsst_matches = self
            .get_matches(
                ra,
                dec,
                lsst::LSST_DEC_RANGE,
                ZTF_LSST_XMATCH_RADIUS,
                &self.lsst_alert_aux_collection,
            )
            .await?;

        let decam_matches = self
            .get_matches(
                ra,
                dec,
                decam::DECAM_DEC_RANGE,
                ZTF_DECAM_XMATCH_RADIUS,
                &self.decam_alert_aux_collection,
            )
            .await?;

        Ok(doc! {
            "LSST": lsst_matches,
            "DECAM": decam_matches,
        })
    }

    #[instrument(
        skip(
            self,
            prv_candidates_doc,
            prv_nondetections_doc,
            fp_hist_doc,
            xmatches,
            survey_matches
        ),
        err
    )]
    async fn insert_alert_aux(
        &self,
        object_id: String,
        ra: f64,
        dec: f64,
        prv_candidates_doc: &Vec<Document>,
        prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        xmatches: Document,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError> {
        let alert_aux_doc = doc! {
            "_id": object_id,
            "prv_candidates": prv_candidates_doc,
            "prv_nondetections": prv_nondetections_doc,
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
            },
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
    fn format_prv_candidates_and_fp_hist(
        &self,
        prv_candidates: &Vec<PrvCandidate>,
        candidate_doc: Document,
        fp_hist: &Vec<ForcedPhot>,
    ) -> (Vec<Document>, Vec<Document>, Vec<Document>) {
        // we split the prv_candidates into detections and non-detections
        let mut prv_candidates_doc = vec![];
        let mut prv_nondetections_doc = vec![];

        for prv_candidate in prv_candidates {
            if prv_candidate.magpsf.is_some() {
                prv_candidates_doc.push(mongify(&prv_candidate));
            } else {
                prv_nondetections_doc.push(mongify(&prv_candidate));
            }
        }
        prv_candidates_doc.push(candidate_doc);

        let fp_hist_doc = fp_hist.into_iter().map(|x| mongify(&x)).collect::<Vec<_>>();
        (prv_candidates_doc, prv_nondetections_doc, fp_hist_doc)
    }
}

#[async_trait::async_trait]
impl AlertWorker for ZtfAlertWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<ZtfAlertWorker, AlertWorkerError> {
        let config_file =
            conf::load_config(&config_path).inspect_err(as_error!("failed to load config"))?;

        let xmatch_configs = conf::build_xmatch_configs(&config_file, STREAM_NAME)
            .inspect_err(as_error!("failed to load xmatch config"))?;

        let db: mongodb::Database = conf::build_db(&config_file)
            .await
            .inspect_err(as_error!("failed to create mongo client"))?;

        let alert_collection = db.collection(&ALERT_COLLECTION);
        let alert_aux_collection = db.collection(&ALERT_AUX_COLLECTION);
        let alert_cutout_collection = db.collection(&ALERT_CUTOUT_COLLECTION);

        let lsst_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&lsst::ALERT_AUX_COLLECTION);

        let decam_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&decam::ALERT_AUX_COLLECTION);

        let worker = ZtfAlertWorker {
            stream_name: STREAM_NAME.to_string(),
            xmatch_configs,
            db,
            alert_collection,
            alert_aux_collection,
            alert_cutout_collection,
            schema_cache: SchemaCache::default(),
            lsst_alert_aux_collection,
            decam_alert_aux_collection,
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
            prv_candidates_doc,
            prv_nondetections_doc,
            fp_hist_doc,
            survey_matches
        ),
        err
    )]
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
    ) -> Result<(), AlertError> {
        let xmatches = xmatch(ra, dec, &self.xmatch_configs, &self.db).await?;
        self.insert_alert_aux(
            object_id.into(),
            ra,
            dec,
            prv_candidates_doc,
            prv_nondetections_doc,
            fp_hist_doc,
            xmatches,
            survey_matches,
            now,
        )
        .await?;
        Ok(())
    }

    #[instrument(
        skip(
            self,
            prv_candidates_doc,
            prv_nondetections_doc,
            fp_hist_doc,
            survey_matches
        ),
        err
    )]
    async fn update_aux(
        self: &mut Self,
        object_id: &str,
        prv_candidates_doc: &Vec<Document>,
        prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError> {
        let update_pipeline = vec![doc! {
            "$set": {
                "prv_candidates": update_timeseries_op("prv_candidates", "jd", prv_candidates_doc),
                "prv_nondetections": update_timeseries_op("prv_nondetections", "jd", prv_nondetections_doc),
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

    #[instrument(skip_all, err)]
    async fn process_alert(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();
        let mut alert: ZtfAlert = self
            .schema_cache
            .alert_from_avro_bytes(avro_bytes)
            .inspect_err(as_error!())?;

        let candid = alert.candid();
        let object_id = alert.object_id();
        let ra = alert.ra();
        let dec = alert.dec();

        let prv_candidates = match alert.prv_candidates.take() {
            Some(candidates) => candidates,
            None => Vec::new(),
        };
        let fp_hist = match alert.fp_hists.take() {
            Some(hist) => hist,
            None => Vec::new(),
        };

        let candidate_doc = mongify(&alert.candidate);

        // add the cutouts, skip processing if the cutouts already exist
        let cutout_status = self
            .format_and_insert_cutouts(
                candid,
                alert.cutout_science,
                alert.cutout_template,
                alert.cutout_difference,
                &self.alert_cutout_collection,
            )
            .await
            .inspect_err(as_error!())?;

        if let ProcessAlertStatus::Exists(_) = cutout_status {
            return Ok(cutout_status);
        }

        let alert_aux_exists = self
            .check_alert_aux_exists(&object_id)
            .await
            .inspect_err(as_error!())?;

        let (prv_candidates_doc, prv_nondetections_doc, fp_hist_doc) = self
            .format_prv_candidates_and_fp_hist(&prv_candidates, candidate_doc.clone(), &fp_hist);

        let survey_matches = Some(
            self.get_survey_matches(ra, dec)
                .await
                .inspect_err(as_error!())?,
        );

        if !alert_aux_exists {
            let result = self
                .insert_aux(
                    &object_id,
                    ra,
                    dec,
                    &prv_candidates_doc,
                    &prv_nondetections_doc,
                    &fp_hist_doc,
                    &survey_matches,
                    now,
                )
                .await;
            if let Err(AlertError::AlertAuxExists) = result {
                self.update_aux(
                    &object_id,
                    &prv_candidates_doc,
                    &prv_nondetections_doc,
                    &fp_hist_doc,
                    &survey_matches,
                    now,
                )
                .await
                .inspect_err(as_error!())?;
            } else {
                result.inspect_err(as_error!())?;
            }
        } else {
            self.update_aux(
                &object_id,
                &prv_candidates_doc,
                &prv_nondetections_doc,
                &fp_hist_doc,
                &survey_matches,
                now,
            )
            .await
            .inspect_err(as_error!())?;
        }

        // insert the alert
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

        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{
        enums::Survey,
        testing::{ztf_alert_worker, AlertRandomizer},
    };

    #[tokio::test]
    async fn test_ztf_alert_from_avro_bytes() {
        let mut alert_worker = ztf_alert_worker().await;

        let (candid, object_id, ra, dec, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Ztf).get().await;
        let alert = alert_worker
            .schema_cache
            .alert_from_avro_bytes(&bytes_content);
        assert!(alert.is_ok());

        // validate the alert
        let alert: ZtfAlert = alert.unwrap();
        assert_eq!(alert.schemavsn, "4.02");
        assert_eq!(alert.publisher, "ZTF (www.ztf.caltech.edu)");
        assert_eq!(alert.object_id, object_id);
        assert_eq!(alert.candid, candid);
        assert_eq!(alert.candidate.ra, ra);
        assert_eq!(alert.candidate.dec, dec);

        // validate the prv_candidates
        let prv_candidates = alert.clone().prv_candidates;
        assert!(!prv_candidates.is_none());

        let prv_candidates = prv_candidates.unwrap();
        assert_eq!(prv_candidates.len(), 10);

        let non_detection = prv_candidates.get(0).unwrap();
        assert_eq!(non_detection.magpsf.is_none(), true);
        assert_eq!(non_detection.diffmaglim.is_some(), true);

        let detection = prv_candidates.get(1).unwrap();
        assert_eq!(detection.magpsf.is_some(), true);
        assert_eq!(detection.sigmapsf.is_some(), true);
        assert_eq!(detection.diffmaglim.is_some(), true);
        assert_eq!(detection.isdiffpos.is_some(), true);

        // validate the fp_hists
        let fp_hists = alert.clone().fp_hists;
        assert!(fp_hists.is_some());

        let fp_hists = fp_hists.unwrap();
        assert_eq!(fp_hists.len(), 10);

        // at the moment, negative fluxes should yield detections,
        // but with isdiffpos = false
        let fp_negative_det = fp_hists.get(0).unwrap();
        println!("{:?}", fp_negative_det);
        assert!((fp_negative_det.magpsf.unwrap() - 15.949999).abs() < 1e-6);
        assert!((fp_negative_det.sigmapsf.unwrap() - 0.002316).abs() < 1e-6);
        assert!((fp_negative_det.diffmaglim - 20.879942).abs() < 1e-6);
        assert_eq!(fp_negative_det.isdiffpos.unwrap(), false);
        assert!((fp_negative_det.snr.unwrap() - 468.75623).abs() < 1e-6);
        assert!((fp_negative_det.fp_hist.jd - 2460447.920278).abs() < 1e-6);

        let fp_positive_det = fp_hists.get(9).unwrap();
        assert!((fp_positive_det.magpsf.unwrap() - 20.801506).abs() < 1e-6);
        assert!((fp_positive_det.sigmapsf.unwrap() - 0.3616859).abs() < 1e-6);
        assert!((fp_positive_det.diffmaglim - 20.247562).abs() < 1e-6);
        assert_eq!(fp_positive_det.isdiffpos.is_some(), true);
        assert!((fp_positive_det.snr.unwrap() - 3.0018756).abs() < 1e-6);
        assert!((fp_positive_det.fp_hist.jd - 2460420.9637616).abs() < 1e-6);

        // validate the cutouts
        assert_eq!(alert.cutout_science.len(), 13107);
        assert_eq!(alert.cutout_template.len(), 12410);
        assert_eq!(alert.cutout_difference.len(), 14878);
    }
}
