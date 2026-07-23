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
        o11y::logging::as_error,
        spatial::{xmatch, Coordinates},
    },
};
use constcat::concat;
use flare::Time;
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use tracing::{debug, error, instrument, warn};

pub const STREAM_NAME: &str = "ASKAP";
// VAST/ASKAP observes the southern sky (dec <~ +41).
pub const ASKAP_DEC_RANGE: (f64, f64) = (-90.0, 41.0);
// Typical VAST combined astrometric uncertainty in arcsec.
pub const ASKAP_POSITION_UNCERTAINTY: f64 = 2.0;
pub const ALERT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts");
pub const ALERT_AUX_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_aux");

pub const ASKAP_ZTF_XMATCH_RADIUS: f64 =
    (ASKAP_POSITION_UNCERTAINTY.max(ztf::ZTF_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();
pub const ASKAP_LSST_XMATCH_RADIUS: f64 =
    (ASKAP_POSITION_UNCERTAINTY.max(lsst::LSST_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();

/// One VAST measurement (blind or forced); carries `jd` natively.
#[apache_avro_macros::serdavro]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct AskapCandidate {
    pub candid: i64,
    pub jd: f64,
    pub image_id: i32,
    pub frequency: Option<f32>,
    pub name: Option<String>,
    pub ra: f64,
    pub dec: f64,
    pub ra_err: f32,
    pub dec_err: f32,
    pub uncertainty_ew: f32,
    pub uncertainty_ns: f32,
    pub ew_sys_err: f32,
    pub ns_sys_err: f32,
    pub error_radius: f32,
    pub weight_ew: Option<f32>,
    pub weight_ns: Option<f32>,
    pub flux_peak: f32,
    pub flux_peak_err: f32,
    pub flux_int: f32,
    pub flux_int_err: f32,
    pub local_rms: f32,
    pub snr: f32,
    pub compactness: f32,
    #[serde(default)]
    pub forced: bool,
    pub island_id: Option<String>,
    pub component_id: Option<String>,
    #[serde(default)]
    pub has_siblings: bool,
    pub bmaj: Option<f32>,
    pub bmaj_err: Option<f32>,
    pub bmin: Option<f32>,
    pub bmin_err: Option<f32>,
    pub pa: Option<f32>,
    pub pa_err: Option<f32>,
    pub psf_bmaj: Option<f32>,
    pub psf_bmin: Option<f32>,
    pub psf_pa: Option<f32>,
    pub spectral_index: Option<f32>,
    #[serde(rename = "spectral_index_from_TT")]
    pub spectral_index_from_tt: Option<bool>,
    pub chi_squared_fit: Option<f32>,
    pub flag_c4: Option<bool>,
    pub flux_peak_isl_ratio: Option<f32>,
    pub flux_int_isl_ratio: Option<f32>,
}

impl TimeSeries for AskapCandidate {
    fn time(&self) -> f64 {
        self.jd
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct AskapCutout {
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "stampData")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub stamp_data: Vec<u8>,
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct AskapRawAvroAlert {
    pub schemavsn: String,
    pub publisher: String,
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: AskapCandidate,
    #[serde(default)]
    pub prv_candidates: Option<Vec<AskapCandidate>>,
    #[serde(default)]
    pub fp_hists: Option<Vec<AskapCandidate>>,
    #[serde(rename = "cutoutScience", default)]
    pub cutout_science: Option<AskapCutout>,
}

#[apache_avro_macros::serdavro]
#[derive(Debug, Deserialize, Serialize)]
pub struct AskapAliases {
    #[serde(rename = "ZTF")]
    pub ztf: Vec<String>,
    #[serde(rename = "LSST")]
    pub lsst: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AskapObject {
    #[serde(rename = "_id")]
    pub object_id: String,
    pub prv_candidates: Vec<AskapCandidate>,
    pub fp_hists: Vec<AskapCandidate>,
    pub cross_matches: Option<HashMap<String, Vec<Document>>>,
    pub aliases: Option<AskapAliases>,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct AskapAlert {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: AskapCandidate,
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

pub struct AskapAlertWorker {
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<AskapAlert>,
    alert_aux_collection: mongodb::Collection<AskapObject>,
    alert_cutout_storage: CutoutStorage,
    alert_aux_collection_update: mongodb::Collection<AlertAuxForUpdate>,
    ztf_alert_aux_collection: mongodb::Collection<Document>,
    lsst_alert_aux_collection: mongodb::Collection<Document>,
    schema_cache: SchemaCache,
}

impl AskapAlertWorker {
    #[instrument(skip(self), err)]
    async fn get_survey_matches(&self, ra: f64, dec: f64) -> Result<AskapAliases, AlertError> {
        let ztf_matches = self
            .get_matches(
                ra,
                dec,
                ztf::ZTF_DEC_RANGE,
                ASKAP_ZTF_XMATCH_RADIUS,
                &self.ztf_alert_aux_collection,
            )
            .await?;

        let lsst_matches = self
            .get_matches(
                ra,
                dec,
                lsst::LSST_DEC_RANGE,
                ASKAP_LSST_XMATCH_RADIUS,
                &self.lsst_alert_aux_collection,
            )
            .await?;
        Ok(AskapAliases {
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
            .projection(doc! { "prv_candidates.jd": 1, "fp_hists.jd": 1, "version": 1 })
            .await
            .inspect_err(as_error!())?;
        Ok(result)
    }

    #[instrument(skip(self, prv_candidates, fp_hists, survey_matches), err)]
    async fn update_aux_fallback(
        &mut self,
        object_id: &str,
        prv_candidates: &Vec<AskapCandidate>,
        fp_hists: &Vec<AskapCandidate>,
        survey_matches: &Option<AskapAliases>,
        now: f64,
    ) -> Result<(), AlertError> {
        Self::db_only_aux_update(
            object_id,
            doc! {
                "prv_candidates": update_timeseries_op("prv_candidates", "jd", &mongify_vec(prv_candidates)),
                "fp_hists": update_timeseries_op("fp_hists", "jd", &mongify_vec(fp_hists)),
            },
            survey_matches,
            now,
            &self.alert_aux_collection,
        )
        .await
    }

    #[instrument(skip(self, prv_candidates, fp_hists, survey_matches, existing_alert_aux))]
    async fn update_aux_inner(
        &mut self,
        object_id: &str,
        prv_candidates: &Vec<AskapCandidate>,
        fp_hists: &Vec<AskapCandidate>,
        survey_matches: &Option<AskapAliases>,
        now: f64,
        existing_alert_aux: &AlertAuxForUpdate,
    ) -> Result<(), AlertError> {
        let current_version = existing_alert_aux.version;

        let prepared_prv_candidates = AskapCandidate::prepare_timeseries_update(
            prv_candidates,
            &existing_alert_aux.prv_candidates,
            "prv_candidates",
        )?;

        let prepared_fp_hists = AskapCandidate::prepare_timeseries_update(
            fp_hists,
            &existing_alert_aux.fp_hists,
            "fp_hists",
        )?;

        let mut push_updates = Document::new();
        Self::add_to_push_aux_update(&mut push_updates, "prv_candidates", prepared_prv_candidates);
        Self::add_to_push_aux_update(&mut push_updates, "fp_hists", prepared_fp_hists);

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
        prv_candidates: &Vec<AskapCandidate>,
        fp_hists: &Vec<AskapCandidate>,
        survey_matches: &Option<AskapAliases>,
        now: f64,
        existing_alert_aux: &AlertAuxForUpdate,
    ) -> Result<(), AlertError> {
        match self
            .update_aux_inner(
                object_id,
                prv_candidates,
                fp_hists,
                survey_matches,
                now,
                existing_alert_aux,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                // fall back to the concurrency-safe, self-healing in-DB update
                match &e {
                    AlertError::ConcurrentAuxUpdate(_) => debug!(error = %e),
                    _ => error!(error = %e),
                }
                self.update_aux_fallback(object_id, prv_candidates, fp_hists, survey_matches, now)
                    .await
            }
        }
    }
}

#[async_trait::async_trait]
impl AlertWorker for AskapAlertWorker {
    async fn new(config_path: &str) -> Result<AskapAlertWorker, AlertWorkerError> {
        let config = AppConfig::from_path(config_path)?;

        let xmatch_configs = config
            .crossmatch
            .get(&Survey::Askap)
            .cloned()
            .unwrap_or_default();

        let db: mongodb::Database = config
            .build_db()
            .await
            .inspect_err(as_error!("failed to create mongo client"))?;

        let alert_collection = db.collection(&ALERT_COLLECTION);
        let alert_aux_collection = db.collection(&ALERT_AUX_COLLECTION);
        let alert_cutout_storage = config
            .build_cutout_storage(&Survey::Askap)
            .await
            .inspect_err(as_error!("failed to create cutout storage"))?;
        let alert_aux_collection_update = db.collection(&ALERT_AUX_COLLECTION);

        let ztf_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&ztf::ALERT_AUX_COLLECTION);

        let lsst_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&lsst::ALERT_AUX_COLLECTION);

        let worker = AskapAlertWorker {
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
        Survey::Askap
    }

    fn input_queue_name(&self) -> String {
        format!("{}_alerts_packets_queue", AskapAlertWorker::survey())
    }

    fn output_queue_name(&self) -> String {
        format!("{}_alerts_enrichment_queue", AskapAlertWorker::survey())
    }

    async fn process_alert(&mut self, avro_bytes: &[u8]) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();
        let avro_alert: AskapRawAvroAlert = self
            .schema_cache
            .alert_from_avro_bytes(avro_bytes)
            .inspect_err(as_error!())?;

        let candid = avro_alert.candid;
        let object_id = avro_alert.object_id;
        let ra = avro_alert.candidate.ra;
        let dec = avro_alert.candidate.dec;

        let mut prv_candidates = avro_alert.prv_candidates.unwrap_or_default();
        prv_candidates.push(avro_alert.candidate.clone());
        let mut fp_hists = avro_alert.fp_hists.unwrap_or_default();

        AskapCandidate::sanitize_timeseries(&mut prv_candidates);
        AskapCandidate::sanitize_timeseries(&mut fp_hists);

        let alert = AskapAlert {
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
            self.update_aux(
                &object_id,
                &prv_candidates,
                &fp_hists,
                &survey_matches,
                now,
                &existing,
            )
            .await
            .inspect_err(as_error!())?;
        } else {
            let xmatches = xmatch(ra, dec, &self.xmatch_configs, &self.db).await?;
            let obj = AskapObject {
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
        }

        // Radio alerts carry at most a science stamp; template/difference are absent.
        let cutout_science = avro_alert
            .cutout_science
            .map(|c| c.stamp_data)
            .unwrap_or_default();
        let status = self
            .format_and_insert_cutouts(
                candid,
                &object_id,
                cutout_science,
                None,
                None,
                &self.alert_cutout_storage,
            )
            .await
            .inspect_err(as_error!())?;

        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{enums::Survey, testing::AlertRandomizer};

    #[tokio::test]
    async fn test_askap_alert_from_avro_bytes() {
        let mut schema_cache = SchemaCache::default();

        let (candid, object_id, ra, dec, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Askap).get().await;
        let alert: AskapRawAvroAlert = schema_cache.alert_from_avro_bytes(&bytes_content).unwrap();

        assert_eq!(alert.publisher, "VAST");
        assert_eq!(alert.object_id, object_id);
        assert_eq!(alert.candid, candid);
        assert_eq!(alert.candidate.ra, ra);
        assert_eq!(alert.candidate.dec, dec);
        assert!(!alert.candidate.forced);

        let prv = alert.prv_candidates.clone().unwrap_or_default();
        let fp = alert.fp_hists.clone().unwrap_or_default();
        assert!(!fp.is_empty());
        assert!(fp.iter().all(|c| c.forced));
        assert!(prv.iter().all(|c| !c.forced));
    }
}
