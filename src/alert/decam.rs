use std::collections::HashMap;

use crate::{
    alert::{
        base::{
            AlertCutout, AlertError, AlertWorker, AlertWorkerError, ProcessAlertStatus, SchemaCache,
        },
        lsst, ztf,
    },
    conf::{self, AppConfig},
    utils::{
        db::{mongify, update_timeseries_op},
        enums::Survey,
        lightcurves::Band,
        o11y::logging::as_error,
        spatial::{xmatch, Coordinates},
    },
};
use constcat::concat;
use flare::Time;
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use tracing::instrument;

pub const STREAM_NAME: &str = "DECAM";
pub const DECAM_DEC_RANGE: (f64, f64) = (-90.0, 33.5);
// Position uncertainty in arcsec (median FHWM from Table 1 in https://iopscience.iop.org/article/10.3847/1538-4365/ac78eb)
pub const DECAM_POSITION_UNCERTAINTY: f64 = 1.24;
pub const ALERT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts");
pub const ALERT_AUX_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_aux");
pub const ALERT_CUTOUT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_cutouts");

pub const DECAM_ZTF_XMATCH_RADIUS: f64 =
    (DECAM_POSITION_UNCERTAINTY.max(ztf::ZTF_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();
pub const DECAM_LSST_XMATCH_RADIUS: f64 =
    (DECAM_POSITION_UNCERTAINTY.max(lsst::LSST_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct FpHist {
    pub mjd: f64,
    pub forcediffimflux: f64,
    pub forcediffimfluxunc: f64,
    #[serde(rename(deserialize = "forcediffimmag"))]
    pub magap: f64,
    #[serde(rename(deserialize = "forcediffimmagunc"))]
    pub sigmagap: f64,
    pub band: Band,
    pub diffmaglim: f64,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct Candidate {
    pub mjd: f64,
    pub forcediffimflux: f64,
    pub forcediffimfluxunc: f64,
    #[serde(rename(deserialize = "forcediffimmag"))]
    pub magap: f64,
    #[serde(rename(deserialize = "forcediffimmagunc"))]
    pub sigmagap: f64,
    pub band: Band,
    pub diffmaglim: f64,
    pub ra: f64,
    pub dec: f64,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DecamCandidate {
    #[serde(flatten)]
    pub candidate: Candidate,
    pub jd: f64,
}

impl TryFrom<Candidate> for DecamCandidate {
    type Error = AlertError;

    fn try_from(candidate: Candidate) -> Result<Self, Self::Error> {
        Ok(DecamCandidate {
            jd: candidate.mjd + 2400000.5,
            candidate,
        })
    }
}

fn deserialize_candidate<'de, D>(deserializer: D) -> Result<DecamCandidate, D::Error>
where
    D: Deserializer<'de>,
{
    let candidate = <Candidate as Deserialize>::deserialize(deserializer)?;
    DecamCandidate::try_from(candidate).map_err(serde::de::Error::custom)
}

fn deserialize_fp_hists<'de, D>(deserializer: D) -> Result<Vec<DecamForcedPhot>, D::Error>
where
    D: Deserializer<'de>,
{
    let fp_hists = <Vec<FpHist> as Deserialize>::deserialize(deserializer)?;
    fp_hists
        .into_iter()
        .map(DecamForcedPhot::try_from)
        .collect::<Result<Vec<DecamForcedPhot>, _>>()
        .map_err(serde::de::Error::custom)
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DecamForcedPhot {
    #[serde(flatten)]
    pub fp_hist: FpHist,
    pub jd: f64,
}

impl TryFrom<FpHist> for DecamForcedPhot {
    type Error = AlertError;

    fn try_from(fp_hist: FpHist) -> Result<Self, Self::Error> {
        Ok(DecamForcedPhot {
            jd: fp_hist.mjd + 2400000.5,
            fp_hist,
        })
    }
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DecamRawAvroAlert {
    pub publisher: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candid: i64,
    #[serde(deserialize_with = "deserialize_candidate")]
    pub candidate: DecamCandidate,
    #[serde(deserialize_with = "deserialize_fp_hists")]
    pub fp_hists: Vec<DecamForcedPhot>,
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

#[derive(Debug, Deserialize, Serialize)]
pub struct DecamAliases {
    #[serde(rename = "ZTF")]
    pub ztf: Vec<String>,
    #[serde(rename = "LSST")]
    pub lsst: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DecamObject {
    #[serde(rename = "_id")]
    pub object_id: String,
    pub prv_candidates: Vec<DecamCandidate>,
    pub fp_hists: Vec<DecamForcedPhot>,
    pub cross_matches: Option<HashMap<String, Vec<Document>>>,
    pub aliases: Option<DecamAliases>,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct DecamAlert {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: DecamCandidate,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

pub struct DecamAlertWorker {
    stream_name: String,
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<DecamAlert>,
    alert_aux_collection: mongodb::Collection<DecamObject>,
    alert_cutout_collection: mongodb::Collection<AlertCutout>,
    ztf_alert_aux_collection: mongodb::Collection<Document>,
    lsst_alert_aux_collection: mongodb::Collection<Document>,
    schema_cache: SchemaCache,
}

impl DecamAlertWorker {
    #[instrument(skip(self), err)]
    async fn get_survey_matches(&self, ra: f64, dec: f64) -> Result<DecamAliases, AlertError> {
        let ztf_matches = self
            .get_matches(
                ra,
                dec,
                ztf::ZTF_DEC_RANGE,
                DECAM_ZTF_XMATCH_RADIUS,
                &self.ztf_alert_aux_collection,
            )
            .await?;

        let lsst_matches = self
            .get_matches(
                ra,
                dec,
                lsst::LSST_DEC_RANGE,
                DECAM_LSST_XMATCH_RADIUS,
                &self.lsst_alert_aux_collection,
            )
            .await?;
        Ok(DecamAliases {
            ztf: ztf_matches,
            lsst: lsst_matches,
        })
    }
    #[instrument(skip(self, prv_candidates, fp_hists, survey_matches), err)]
    async fn update_aux(
        self: &mut Self,
        object_id: &str,
        prv_candidates: &Vec<DecamCandidate>,
        fp_hists: &Vec<DecamForcedPhot>,
        survey_matches: &Option<DecamAliases>,
        now: f64,
    ) -> Result<(), AlertError> {
        let update_pipeline = vec![doc! {
            "$set": {
                "prv_candidates": update_timeseries_op("prv_candidates", "jd", &prv_candidates.iter().map(|pc| mongify(pc)).collect::<Vec<Document>>()),
                "fp_hists": update_timeseries_op("fp_hists", "jd", &fp_hists.iter().map(|pc| mongify(pc)).collect::<Vec<Document>>()),
                "aliases": mongify(survey_matches),
                "updated_at": now,
            }
        }];
        self.alert_aux_collection
            .update_one(doc! { "_id": object_id }, update_pipeline)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AlertWorker for DecamAlertWorker {
    async fn new(config_path: &str) -> Result<DecamAlertWorker, AlertWorkerError> {
        let config = AppConfig::from_path(config_path)?;

        let xmatch_configs = config
            .crossmatch
            .get(&Survey::Decam)
            .cloned()
            .unwrap_or_default();

        let db: mongodb::Database = config
            .build_db()
            .await
            .inspect_err(as_error!("failed to create mongo client"))?;

        let alert_collection = db.collection(&ALERT_COLLECTION);
        let alert_aux_collection = db.collection(&ALERT_AUX_COLLECTION);
        let alert_cutout_collection = db.collection(&ALERT_CUTOUT_COLLECTION);

        let ztf_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&ztf::ALERT_AUX_COLLECTION);

        let lsst_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&lsst::ALERT_AUX_COLLECTION);

        let worker = DecamAlertWorker {
            stream_name: STREAM_NAME.to_string(),
            xmatch_configs,
            db,
            alert_collection,
            alert_aux_collection,
            alert_cutout_collection,
            ztf_alert_aux_collection,
            lsst_alert_aux_collection,
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

    async fn process_alert(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();
        let avro_alert: DecamRawAvroAlert = self
            .schema_cache
            .alert_from_avro_bytes(avro_bytes)
            .inspect_err(as_error!())?;

        let candid = avro_alert.candid;
        let object_id = avro_alert.object_id;
        let ra = avro_alert.candidate.candidate.ra;
        let dec = avro_alert.candidate.candidate.dec;

        let prv_candidates = vec![avro_alert.candidate.clone()];
        let fp_hists = avro_alert.fp_hists;

        let status = self
            .format_and_insert_cutouts(
                candid,
                avro_alert.cutout_science,
                avro_alert.cutout_template,
                avro_alert.cutout_difference,
                &self.alert_cutout_collection,
            )
            .await
            .inspect_err(as_error!())?;

        if let ProcessAlertStatus::Exists(_) = status {
            return Ok(status);
        }

        let alert_aux_exists = self
            .check_alert_aux_exists(&object_id, &self.alert_aux_collection)
            .await
            .inspect_err(as_error!())?;

        let survey_matches = Some(
            self.get_survey_matches(ra, dec)
                .await
                .inspect_err(as_error!())?,
        );

        if !alert_aux_exists {
            let xmatches = xmatch(ra, dec, &self.xmatch_configs, &self.db).await?;
            let obj = DecamObject {
                object_id: object_id.clone(),
                prv_candidates,
                fp_hists,
                cross_matches: Some(xmatches),
                aliases: survey_matches,
                coordinates: Coordinates::new(ra, dec),
                created_at: now,
                updated_at: now,
            };
            let result = self.insert_aux(&obj, &self.alert_aux_collection).await;
            if let Err(AlertError::AlertAuxExists) = result {
                self.update_aux(
                    &object_id,
                    &obj.prv_candidates,
                    &obj.fp_hists,
                    &obj.aliases,
                    now,
                )
                .await
                .inspect_err(as_error!())?;
            } else {
                result.inspect_err(as_error!())?;
            }
        } else {
            self.update_aux(&object_id, &prv_candidates, &fp_hists, &survey_matches, now)
                .await
                .inspect_err(as_error!())?;
        }

        let alert = DecamAlert {
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
        let alert: DecamRawAvroAlert = alert.unwrap();
        assert_eq!(alert.publisher, "DESIRT");
        assert_eq!(alert.object_id, object_id);
        assert_eq!(alert.candid, candid);
        assert_eq!(alert.candidate.candidate.ra, ra);
        assert_eq!(alert.candidate.candidate.dec, dec);

        // validate the fp_hists
        let fp_hists = alert.clone().fp_hists;
        assert_eq!(fp_hists.len(), 61);

        let fp_positive_det = fp_hists.get(0).unwrap();
        assert!((fp_positive_det.fp_hist.magap - 22.595936).abs() < 1e-6);
        assert!((fp_positive_det.fp_hist.sigmagap - 0.093660).abs() < 1e-6);
        assert!((fp_positive_det.jd - 2460709.838387).abs() < 1e-6);
        assert_eq!(fp_positive_det.fp_hist.band, Band::G);

        // validate the cutouts
        assert_eq!(alert.cutout_science.clone().len(), 54561);
        assert_eq!(alert.cutout_template.clone().len(), 49810);
        assert_eq!(alert.cutout_difference.clone().len(), 54569);
    }
}
