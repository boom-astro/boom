use std::collections::HashMap;

use crate::{
    alert::{
        base::{
            AlertCutout, AlertError, AlertWorker, AlertWorkerError, LightcurveJdOnly,
            ProcessAlertStatus, SchemaCache,
        },
        lsst, ztf, TimeSeries,
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
use tracing::{instrument, warn};

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

impl TimeSeries for DecamCandidate {
    fn time(&self) -> f64 {
        self.jd
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

impl TimeSeries for DecamForcedPhot {
    fn time(&self) -> f64 {
        self.jd
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

#[derive(Deserialize, Serialize)]
struct AlertAuxForUpdate {
    #[serde(default)]
    pub prv_candidates: Vec<LightcurveJdOnly>,
    #[serde(default)]
    pub fp_hists: Vec<LightcurveJdOnly>,
    pub version: Option<i32>,
}

pub struct DecamAlertWorker {
    stream_name: String,
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<DecamAlert>,
    alert_aux_collection: mongodb::Collection<DecamObject>,
    alert_aux_collection_update: mongodb::Collection<AlertAuxForUpdate>,
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

    async fn get_existing_aux(
        &self,
        object_id: String,
    ) -> Result<Option<AlertAuxForUpdate>, AlertError> {
        let result = self
            .alert_aux_collection_update
            .find_one(doc! { "_id": &object_id })
            .projection(doc! { "prv_candidates.jd": 1, "fp_hists.jd": 1, "version": 1 })
            .await
            .inspect_err(as_error!())?;
        Ok(result)
    }

    #[instrument(skip(self, prv_candidates, fp_hists, survey_matches), err)]
    async fn update_aux_fallback(
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
                // we still want to increment the version even in the fallback,
                // to prevent concurrency issues between a fallback update and a normal update from another thread
                "version": doc! { "$add": [ { "$ifNull": [ "$version", 0 ] }, 1 ] },
            }
        }];
        self.alert_aux_collection
            .update_one(doc! { "_id": object_id }, update_pipeline)
            .await?;
        Ok(())
    }

    #[instrument(
        skip(self, prv_candidates, fp_hists, survey_matches, existing_alert_aux),
        err
    )]
    async fn update_aux(
        self: &mut Self,
        object_id: &str,
        prv_candidates: &Vec<DecamCandidate>,
        fp_hists: &Vec<DecamForcedPhot>,
        survey_matches: &Option<DecamAliases>,
        now: f64,
        existing_alert_aux: &AlertAuxForUpdate,
    ) -> Result<(), AlertError> {
        let current_version = existing_alert_aux.version;

        let Ok((new_prv_candidates_docs, need_sort_prv_candidates)) =
            DecamCandidate::prepare_timeseries_update(
                prv_candidates,
                &existing_alert_aux.prv_candidates,
                "prv_candidates",
            )
        else {
            return self
                .update_aux_fallback(object_id, prv_candidates, fp_hists, survey_matches, now)
                .await;
        };

        let Ok((new_fp_hists_docs, need_sort_fp_hists)) =
            DecamForcedPhot::prepare_timeseries_update(
                fp_hists,
                &existing_alert_aux.fp_hists,
                "fp_hists",
            )
        else {
            return self
                .update_aux_fallback(object_id, prv_candidates, fp_hists, survey_matches, now)
                .await;
        };

        let mut push_updates = Document::new();
        if !new_prv_candidates_docs.is_empty() {
            if need_sort_prv_candidates {
                push_updates.insert(
                    "prv_candidates",
                    doc! { "$each": new_prv_candidates_docs, "$sort": { "jd": 1 } },
                );
            } else {
                push_updates.insert("prv_candidates", doc! { "$each": new_prv_candidates_docs });
            }
        }
        if !new_fp_hists_docs.is_empty() {
            if need_sort_fp_hists {
                push_updates.insert(
                    "fp_hists",
                    doc! { "$each": new_fp_hists_docs, "$sort": { "jd": 1 } },
                );
            } else {
                push_updates.insert("fp_hists", doc! { "$each": new_fp_hists_docs });
            }
        }

        let update_doc = if push_updates.is_empty() {
            doc! {
                "$set": {
                    "aliases": mongify(survey_matches),
                    "updated_at": now,
                    "version": current_version.unwrap_or(0) + 1,
                }
            }
        } else {
            doc! {
                "$push": push_updates,
                "$set": {
                    "aliases": mongify(survey_matches),
                    "updated_at": now,
                    "version": current_version.unwrap_or(0) + 1,
                }
            }
        };
        let find_doc = if let Some(version) = current_version {
            doc! { "_id": object_id, "version": version }
        } else {
            doc! {
                 "_id": object_id,
                 "$or": [
                     doc! { "version": { "$exists": false } },
                     doc! { "version": mongodb::bson::Bson::Null },
                 ]
            }
        };
        let update_result = self
            .alert_aux_collection
            .update_one(find_doc, update_doc)
            .await?;
        if update_result.matched_count == 0 {
            warn!(
                "Concurrent modification detected for object_id {}. Using DB-only update.",
                object_id
            );
            return self
                .update_aux_fallback(object_id, prv_candidates, fp_hists, survey_matches, now)
                .await;
        }
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
        let alert_aux_collection_update = db.collection(&ALERT_AUX_COLLECTION);
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
            alert_aux_collection_update,
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
        let mut fp_hists = avro_alert.fp_hists;

        // Sort and deduplicate time series data by jd
        DecamForcedPhot::sanitize_timeseries(&mut fp_hists);

        let cutout_status = self
            .format_and_insert_cutouts(
                candid,
                avro_alert.cutout_science,
                avro_alert.cutout_template,
                avro_alert.cutout_difference,
                &self.alert_cutout_collection,
            )
            .await
            .inspect_err(as_error!())?;

        if let ProcessAlertStatus::Exists(_) = cutout_status {
            return Ok(cutout_status);
        }

        let existing_alert_aux = self.get_existing_aux(object_id.clone()).await?;

        let survey_matches = Some(
            self.get_survey_matches(ra, dec)
                .await
                .inspect_err(as_error!())?,
        );

        if existing_alert_aux.is_none() {
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
                // use the race-condition free fallback update
                warn!(
                    "Alert aux document for object_id {} already exists. Using fallback update.",
                    object_id
                );
                self.update_aux_fallback(
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
            self.update_aux(
                &object_id,
                &prv_candidates,
                &fp_hists,
                &survey_matches,
                now,
                &existing_alert_aux.unwrap(),
            )
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
        testing::{decam_alert_worker, drop_alert_from_collections, AlertRandomizer},
    };

    struct DecamPrvLightcurveGen {
        template: DecamCandidate,
    }

    impl DecamPrvLightcurveGen {
        fn new(template: DecamCandidate) -> Self {
            Self { template }
        }

        fn at_jd(&self, jd: f64) -> DecamCandidate {
            let mut candidate = self.template.clone();
            candidate.jd = jd;
            candidate.candidate.mjd = jd - 2400000.5;
            candidate
        }
    }

    struct DecamFpLightcurveGen {
        template: DecamForcedPhot,
    }

    impl DecamFpLightcurveGen {
        fn new(template: DecamForcedPhot) -> Self {
            Self { template }
        }

        fn at_jd(&self, jd: f64) -> DecamForcedPhot {
            let mut fp = self.template.clone();
            fp.jd = jd;
            fp.fp_hist.mjd = jd - 2400000.5;
            fp
        }
    }

    fn assert_strictly_increasing_unique(points: &[LightcurveJdOnly]) {
        assert!(points.iter().all(|point| point.jd.is_finite()));
        assert!(points.windows(2).all(|window| window[0].jd < window[1].jd));
    }

    async fn seed_decam_alert(worker: &mut DecamAlertWorker) -> (i64, String, Vec<u8>) {
        let (candid, object_id, _ra, _dec, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Decam).get().await;
        let status = worker.process_alert(&bytes_content).await.unwrap();
        assert_eq!(status, ProcessAlertStatus::Added(candid));
        (candid, object_id, bytes_content)
    }

    async fn load_aux(worker: &DecamAlertWorker, object_id: &str) -> AlertAuxForUpdate {
        worker
            .get_existing_aux(object_id.to_string())
            .await
            .unwrap()
            .unwrap()
    }

    async fn set_aux_fields(worker: &DecamAlertWorker, object_id: &str, set_doc: Document) {
        worker
            .alert_aux_collection
            .update_one(doc! { "_id": object_id }, doc! { "$set": set_doc })
            .await
            .unwrap();
    }

    async fn apply_update(
        worker: &mut DecamAlertWorker,
        object_id: &str,
        prv_candidates: Vec<DecamCandidate>,
        fp_hists: Vec<DecamForcedPhot>,
        survey_matches: &Option<DecamAliases>,
        existing_aux: &AlertAuxForUpdate,
    ) {
        worker
            .update_aux(
                object_id,
                &prv_candidates,
                &fp_hists,
                survey_matches,
                Time::now().to_jd(),
                existing_aux,
            )
            .await
            .unwrap();
    }

    fn empty_aliases() -> DecamAliases {
        DecamAliases {
            ztf: vec![],
            lsst: vec![],
        }
    }

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

    #[tokio::test]
    async fn test_update_aux_branches_and_fallback() {
        let mut worker = decam_alert_worker().await;

        let (candid, object_id, bytes_content) = seed_decam_alert(&mut worker).await;

        let parsed_alert: DecamRawAvroAlert = worker
            .schema_cache
            .alert_from_avro_bytes(&bytes_content)
            .unwrap();
        let candidate_template = parsed_alert.candidate;
        let fp_template = parsed_alert
            .fp_hists
            .first()
            .cloned()
            .expect("test data should include at least one DECAM forced photometry point");

        let prv_gen = DecamPrvLightcurveGen::new(candidate_template);
        let fp_gen = DecamFpLightcurveGen::new(fp_template);
        let survey_matches = Some(empty_aliases());

        // Branch: empty push updates => update uses $set only.
        let existing_before = load_aux(&worker, &object_id).await;
        let prv_len_before = existing_before.prv_candidates.len();
        let fp_len_before = existing_before.fp_hists.len();
        apply_update(
            &mut worker,
            &object_id,
            vec![],
            vec![],
            &survey_matches,
            &existing_before,
        )
        .await;

        let after_empty = load_aux(&worker, &object_id).await;
        assert_eq!(after_empty.prv_candidates.len(), prv_len_before);
        assert_eq!(after_empty.fp_hists.len(), fp_len_before);
        assert_eq!(
            after_empty.version,
            Some(existing_before.version.unwrap_or(0) + 1)
        );

        // Branch: append-only updates without sort.
        let append_prv_jd = after_empty.prv_candidates.last().unwrap().jd + 100.0;
        let append_fp_jd = after_empty.fp_hists.last().unwrap().jd + 100.0;

        apply_update(
            &mut worker,
            &object_id,
            vec![prv_gen.at_jd(append_prv_jd)],
            vec![fp_gen.at_jd(append_fp_jd)],
            &survey_matches,
            &after_empty,
        )
        .await;

        let after_append = load_aux(&worker, &object_id).await;
        assert_eq!(after_append.prv_candidates.len(), prv_len_before + 1);
        assert_eq!(after_append.fp_hists.len(), fp_len_before + 1);
        assert!((after_append.prv_candidates.last().unwrap().jd - append_prv_jd).abs() < 1e-9);
        assert!((after_append.fp_hists.last().unwrap().jd - append_fp_jd).abs() < 1e-9);

        // Branch: overlap requires full update with sort.
        let sort_prv_jd = after_append.prv_candidates.first().unwrap().jd - 50.0;
        let sort_fp_jd = after_append.fp_hists.first().unwrap().jd - 50.0;

        apply_update(
            &mut worker,
            &object_id,
            vec![prv_gen.at_jd(sort_prv_jd)],
            vec![fp_gen.at_jd(sort_fp_jd)],
            &survey_matches,
            &after_append,
        )
        .await;

        let after_sort = load_aux(&worker, &object_id).await;
        assert_eq!(after_sort.prv_candidates.len(), prv_len_before + 2);
        assert_eq!(after_sort.fp_hists.len(), fp_len_before + 2);
        assert!(after_sort
            .prv_candidates
            .windows(2)
            .all(|window| window[0].jd < window[1].jd));
        assert!(after_sort
            .fp_hists
            .windows(2)
            .all(|window| window[0].jd < window[1].jd));

        // Branch: optimistic-lock miss triggers fallback update.
        let stale_aux = load_aux(&worker, &object_id).await;
        let fresh_aux = load_aux(&worker, &object_id).await;

        let concurrent_prv_jd = after_sort.prv_candidates.last().unwrap().jd + 10.0;
        let concurrent_fp_jd = after_sort.fp_hists.last().unwrap().jd + 10.0;
        apply_update(
            &mut worker,
            &object_id,
            vec![prv_gen.at_jd(concurrent_prv_jd)],
            vec![fp_gen.at_jd(concurrent_fp_jd)],
            &survey_matches,
            &fresh_aux,
        )
        .await;

        let fallback_prv_jd = concurrent_prv_jd + 1.0;
        let fallback_fp_jd = concurrent_fp_jd + 1.0;
        apply_update(
            &mut worker,
            &object_id,
            vec![prv_gen.at_jd(fallback_prv_jd)],
            vec![fp_gen.at_jd(fallback_fp_jd)],
            &survey_matches,
            &stale_aux,
        )
        .await;

        let after_fallback = load_aux(&worker, &object_id).await;
        assert!(after_fallback
            .prv_candidates
            .iter()
            .any(|point| (point.jd - fallback_prv_jd).abs() < 1e-9));
        assert!(after_fallback
            .fp_hists
            .iter()
            .any(|point| (point.jd - fallback_fp_jd).abs() < 1e-9));
        assert_eq!(
            after_fallback.version,
            Some(stale_aux.version.unwrap_or(0) + 2)
        );

        drop_alert_from_collections(candid, "DECAM").await.unwrap();
    }

    #[tokio::test]
    async fn test_update_aux_repairs_corrupted_existing_lightcurves() {
        let mut worker = decam_alert_worker().await;

        let (candid, object_id, _bytes_content) = seed_decam_alert(&mut worker).await;

        set_aux_fields(
            &worker,
            &object_id,
            doc! {
                "prv_candidates": vec![
                    doc! { "jd": 2.0 },
                    doc! { "jd": 1.0 },
                    doc! { "jd": 1.0 },
                ],
                "fp_hists": vec![
                    doc! { "jd": 3.0 },
                    doc! { "jd": 2.0 },
                    doc! { "jd": 2.0 },
                ],
            },
        )
        .await;

        let corrupted = load_aux(&worker, &object_id).await;
        let version_before = corrupted.version.unwrap_or(0);

        apply_update(
            &mut worker,
            &object_id,
            vec![],
            vec![],
            &Some(empty_aliases()),
            &corrupted,
        )
        .await;

        let repaired = load_aux(&worker, &object_id).await;
        assert_strictly_increasing_unique(&repaired.prv_candidates);
        assert_strictly_increasing_unique(&repaired.fp_hists);
        assert_eq!(
            repaired
                .prv_candidates
                .iter()
                .map(|point| point.jd)
                .collect::<Vec<_>>(),
            vec![1.0, 2.0]
        );
        assert_eq!(
            repaired
                .fp_hists
                .iter()
                .map(|point| point.jd)
                .collect::<Vec<_>>(),
            vec![2.0, 3.0]
        );
        assert_eq!(repaired.version, Some(version_before + 1));

        drop_alert_from_collections(candid, "DECAM").await.unwrap();
    }

    #[tokio::test]
    async fn test_get_existing_aux_fails_on_malformed_jd_type() {
        let mut worker = decam_alert_worker().await;

        let (candid, object_id, _bytes_content) = seed_decam_alert(&mut worker).await;

        set_aux_fields(
            &worker,
            &object_id,
            doc! {
                "prv_candidates": vec![doc! { "jd": "not-a-number" }],
            },
        )
        .await;

        let result = worker.get_existing_aux(object_id.clone()).await;
        assert!(matches!(result, Err(AlertError::Mongodb(_))));

        drop_alert_from_collections(candid, "DECAM").await.unwrap();
    }

    #[tokio::test]
    async fn test_update_aux_with_non_finite_existing_jd_hits_failure_mode() {
        let mut worker = decam_alert_worker().await;

        let (candid, object_id, _bytes_content) = seed_decam_alert(&mut worker).await;

        set_aux_fields(
            &worker,
            &object_id,
            doc! {
                "prv_candidates": vec![doc! { "jd": f64::NAN }],
            },
        )
        .await;

        let corrupted = load_aux(&worker, &object_id).await;
        let version_before = corrupted.version.unwrap_or(0);

        apply_update(
            &mut worker,
            &object_id,
            vec![],
            vec![],
            &Some(empty_aliases()),
            &corrupted,
        )
        .await;

        let after = load_aux(&worker, &object_id).await;
        assert_eq!(after.version, Some(version_before + 1));
        assert!(LightcurveJdOnly::validate_strictly_increasing(
            &after.prv_candidates,
            "prv_candidates"
        )
        .is_err());

        drop_alert_from_collections(candid, "DECAM").await.unwrap();
    }
}
