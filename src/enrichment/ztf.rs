use crate::alert::ZtfCandidate;
use crate::conf::AppConfig;
use crate::enrichment::{
    babamul::{Babamul, BabamulZtfAlert},
    fetch_alerts,
    models::{AcaiModel, BtsBotModel, FusionModel, Model, ModelError, SharedModels},
    EnrichmentWorker, EnrichmentWorkerError, LsstMatch,
};
use crate::utils::cutouts::{AlertCutout, CutoutStorage};
use crate::utils::db::mongify;
use crate::utils::enums::Survey;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, AllBandsProperties, Band, PerBandProperties,
    PhotometryMag, ZTF_ZP,
};
use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use serde::{Deserialize, Deserializer};
use std::sync::Arc;
use tracing::{info, instrument, trace, warn};
#[cfg(all(feature = "gpu", target_os = "linux"))]
use villar_pso::gpu::{GpuBatchData, SourceData};
#[cfg(all(feature = "gpu", target_os = "macos"))]
use villar_pso::gpu_metal::{GpuBatchData, SourceData};

#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// Represents ZTF alert photometry data we retrieve from the database
/// (e.g. prv_candidates, prv_nondetections) and later convert to `ZtfPhotometry`
pub struct ZtfAlertPhotometry {
    pub jd: f64,
    pub magpsf: Option<f64>,
    pub sigmapsf: Option<f64>,
    pub diffmaglim: f64,
    #[serde(rename = "psfFlux")]
    pub flux: Option<f64>, // in nJy
    #[serde(rename = "psfFluxErr")]
    pub flux_err: f64, // in nJy
    pub band: Band,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub snr_psf: Option<f64>,
    /// Legacy fallback: populated from the `snr` field for un-migrated documents that pre-date the snr migration.
    #[allow(dead_code)]
    #[serde(rename = "snr", default, skip_serializing)]
    pub snr_legacy: Option<f64>,
    pub programid: i32,
}

#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// Represents ZTF forced photometry data we retrieve from the database
/// (e.g. prv_candidates, prv_nondetections) and later convert to `ZtfPhotometry`
pub struct ZtfForcedPhotometry {
    pub jd: f64,
    pub magpsf: Option<f64>,
    pub sigmapsf: Option<f64>,
    pub diffmaglim: f64,
    // TODO: read from psfFlux once that is moved to a fixed ZP in the database
    #[serde(rename = "forcediffimflux")]
    pub flux: Option<f64>,
    // TODO: read from psfFlux once that is moved to a fixed ZP in the database
    #[serde(rename = "forcediffimfluxunc")]
    pub flux_err: f64,
    pub band: Band,
    pub magzpsci: Option<f64>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub snr_psf: Option<f64>,
    /// Legacy fallback: populated from the `snr` field for un-migrated documents that pre-date the snr migration.
    #[allow(dead_code)]
    #[serde(rename = "snr", default, skip_serializing)]
    pub snr_legacy: Option<f64>,
    pub programid: i32,
    pub procstatus: Option<String>,
}

#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// Represents ZTF photometry data we retrieved from the database
/// (from alert or forced photometry)
pub struct ZtfPhotometry {
    pub jd: f64,
    pub magpsf: Option<f64>,
    pub sigmapsf: Option<f64>,
    pub diffmaglim: f64,
    #[serde(rename = "psfFlux")]
    pub flux: Option<f64>, // in nJy
    #[serde(rename = "psfFluxErr")]
    pub flux_err: f64, // in nJy
    pub band: Band,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub snr_psf: Option<f64>,
    pub programid: i32,
}

impl TryFrom<ZtfAlertPhotometry> for ZtfPhotometry {
    type Error = EnrichmentWorkerError;
    fn try_from(phot: ZtfAlertPhotometry) -> Result<Self, Self::Error> {
        Ok(ZtfPhotometry {
            jd: phot.jd,
            magpsf: phot.magpsf,
            sigmapsf: phot.sigmapsf,
            diffmaglim: phot.diffmaglim,
            flux: phot.flux,
            flux_err: phot.flux_err,
            ra: phot.ra,
            dec: phot.dec,
            band: phot.band,
            snr_psf: phot.snr_psf.or(phot.snr_legacy),
            programid: phot.programid,
        })
    }
}

impl TryFrom<ZtfForcedPhotometry> for ZtfPhotometry {
    type Error = EnrichmentWorkerError;
    fn try_from(phot: ZtfForcedPhotometry) -> Result<Self, Self::Error> {
        let procstatus = phot.procstatus.ok_or(EnrichmentWorkerError::Serialization(
            "missing procstatus".to_string(),
        ))?;
        // TODO: accept all "acceptable" procstatus (if not just "0")
        if procstatus != "0" {
            return Err(EnrichmentWorkerError::BadProcstatus(procstatus));
        }

        // TODO: remove this conversion once we read flux and flux_err from the database with a fixed ZP
        let zp_scaling_factor = if let Some(magzpsci) = phot.magzpsci {
            10f64.powf((ZTF_ZP as f64 - magzpsci) / 2.5)
        } else {
            return Err(EnrichmentWorkerError::MissingMagZPSci);
        };

        let flux = if phot.flux != Some(-99999.0) && phot.flux.map_or(false, |f| !f.is_nan()) {
            phot.flux.map(|f| f * 1e9_f64 * zp_scaling_factor) // convert to a fixed ZP and nJy
        } else {
            None
        };
        let flux_err = if phot.flux_err != -99999.0 && !phot.flux_err.is_nan() {
            phot.flux_err * 1e9_f64 * zp_scaling_factor // convert to a fixed ZP and nJy
        } else {
            return Err(EnrichmentWorkerError::MissingFluxPSF);
        };

        Ok(ZtfPhotometry {
            jd: phot.jd,
            magpsf: phot.magpsf,
            sigmapsf: phot.sigmapsf,
            diffmaglim: phot.diffmaglim,
            flux,
            flux_err,
            ra: phot.ra,
            dec: phot.dec,
            band: phot.band,
            snr_psf: phot.snr_psf.or(phot.snr_legacy),
            programid: phot.programid,
        })
    }
}

pub fn deserialize_ztf_alert_lightcurve<'de, D>(
    deserializer: D,
) -> Result<Vec<ZtfPhotometry>, D::Error>
where
    D: Deserializer<'de>,
{
    let lightcurve = <Option<Vec<ZtfAlertPhotometry>> as Deserialize>::deserialize(deserializer)?;
    match lightcurve {
        Some(lightcurve) => {
            let converted_lightcurve = lightcurve
                .into_iter()
                .filter_map(|p| {
                    ZtfPhotometry::try_from(p)
                        .map_err(|e| {
                            warn!(
                                "Failed to convert ZtfAlertPhotometry to ZtfPhotometry: {}",
                                e
                            );
                        })
                        .ok()
                })
                .collect();
            Ok(converted_lightcurve)
        }
        None => Ok(vec![]),
    }
}

pub fn deserialize_ztf_forced_lightcurve<'de, D>(
    deserializer: D,
) -> Result<Vec<ZtfPhotometry>, D::Error>
where
    D: Deserializer<'de>,
{
    let lightcurve = <Option<Vec<ZtfForcedPhotometry>> as Deserialize>::deserialize(deserializer)?;
    match lightcurve {
        Some(lightcurve) => {
            let converted_lightcurve = lightcurve
                .into_iter()
                .filter_map(|p| {
                    ZtfPhotometry::try_from(p)
                        .map_err(|e| {
                            // log badprocstatus at trace level to avoid flooding logs
                            if let EnrichmentWorkerError::BadProcstatus(_) = e {
                                trace!(
                                    "Failed to convert ZtfForcedPhotometry to ZtfPhotometry: {}",
                                    e
                                );
                            } else {
                                warn!(
                                    "Failed to convert ZtfForcedPhotometry to ZtfPhotometry: {}",
                                    e
                                );
                            }
                        })
                        .ok()
                })
                .collect();
            Ok(converted_lightcurve)
        }
        None => Ok(vec![]),
    }
}

// it should return an optional PhotometryMag
impl ZtfPhotometry {
    pub fn to_photometry_mag(&self, min_snr: Option<f64>) -> Option<PhotometryMag> {
        // If snr, magpsf, and sigmapsf are all present, this returns Some(PhotometryMag)
        // optionally applying an SNR filter: when min_snr is None, no SNR filtering is
        // applied; when it is Some(thresh), points with |snr| below thresh are filtered out.
        match (self.snr_psf, self.magpsf, self.sigmapsf) {
            (Some(snr), Some(mag), Some(sig)) => match min_snr {
                Some(thresh) if snr.abs() < thresh => None,
                _ => Some(PhotometryMag {
                    time: self.jd,
                    mag: mag as f32,
                    mag_err: sig as f32,
                    band: self.band.clone(),
                }),
            },
            _ => None,
        }
    }
}

pub fn create_ztf_alert_pipeline(include_classifications: bool) -> Vec<Document> {
    let mut pipeline = vec![
        doc! {
            "$match": {
                "_id": {"$in": []}
            }
        },
        doc! {
            "$lookup": {
                "from": "ZTF_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$unwind": {
                "path": "$aux",
                "preserveNullAndEmptyArrays": false
            }
        },
        doc! {
            "$lookup": {
                "from": "LSST_alerts_aux",
                "localField": "aux.aliases.LSST.0",
                "foreignField": "_id",
                "as": "lsst_aux"
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates": "$aux.prv_candidates",
                "prv_nondetections": "$aux.prv_nondetections",
                "fp_hists": "$aux.fp_hists",
                "survey_matches": {
                    "lsst": {
                        "$cond": {
                            "if": { "$gt": [ { "$size": "$lsst_aux" }, 0 ] },
                            "then": {
                                "objectId": { "$arrayElemAt": [ "$lsst_aux._id", 0 ] },
                                "prv_candidates": { "$arrayElemAt": [ "$lsst_aux.prv_candidates", 0 ] },
                                "fp_hists": { "$arrayElemAt": [ "$lsst_aux.fp_hists", 0 ] },
                                "ra": { "$add": [
                                    { "$arrayElemAt": [{ "$arrayElemAt": [ "$lsst_aux.coordinates.radec_geojson.coordinates", 0 ] }, 0]},
                                    180
                                ]},
                                "dec": { "$arrayElemAt": [{ "$arrayElemAt": [ "$lsst_aux.coordinates.radec_geojson.coordinates", 0 ] }, 1]},
                            },
                            "else": null
                        }
                    }
                }
            }
        },
    ];

    if include_classifications {
        // we want to add classifications: 1 in the final project stage only
        if let Some(project_stage) = pipeline.last_mut() {
            if let Some(project_doc) = project_stage.get_document_mut("$project").ok() {
                project_doc.insert("classifications", 1);
            }
        }
    }

    pipeline
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, AvroSchema)]
pub struct ZtfSurveyMatches {
    pub lsst: Option<LsstMatch>,
}

#[serdavro]
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
pub struct ZtfMatch {
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub ra: f64,
    pub dec: f64,
    #[serde(deserialize_with = "deserialize_ztf_alert_lightcurve")]
    pub prv_candidates: Vec<ZtfPhotometry>,
    #[serde(deserialize_with = "deserialize_ztf_alert_lightcurve")]
    pub prv_nondetections: Vec<ZtfPhotometry>,
    #[serde(deserialize_with = "deserialize_ztf_forced_lightcurve")]
    pub fp_hists: Vec<ZtfPhotometry>,
}

/// ZTF alert structure used to deserialize alerts
/// from the database, used by the enrichment worker
/// to compute features and ML scores
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ZtfAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: ZtfCandidate,
    #[serde(deserialize_with = "deserialize_ztf_alert_lightcurve")]
    pub prv_candidates: Vec<ZtfPhotometry>,
    #[serde(deserialize_with = "deserialize_ztf_alert_lightcurve")]
    pub prv_nondetections: Vec<ZtfPhotometry>,
    #[serde(deserialize_with = "deserialize_ztf_forced_lightcurve")]
    pub fp_hists: Vec<ZtfPhotometry>,
    pub survey_matches: Option<ZtfSurveyMatches>,
}

/// ZTF alert properties computed during enrichment and inserted back into the alert document
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct ZtfAlertProperties {
    pub rock: bool,
    pub star: bool,
    pub near_brightstar: bool,
    pub stationary: bool,
    pub photstats: PerBandProperties,
    pub multisurvey_photstats: Option<PerBandProperties>,
}

/// Class probability output of the CIDER fusion model.
/// Fields are ordered to match the ONNX model's output index (0–7).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct CiderClassProbs {
    #[serde(rename = "AGN-like")]
    pub agn_like: f32,
    #[serde(rename = "Accreting WD Var")]
    pub accreting_wd_var: f32,
    #[serde(rename = "Other Stellar Var")]
    pub other_stellar_var: f32,
    #[serde(rename = "TDE")]
    pub tde: f32,
    #[serde(rename = "Ia-like SN")]
    pub ia_like_sn: f32,
    #[serde(rename = "Stripped Envelope SN")]
    pub stripped_envelope_sn: f32,
    #[serde(rename = "H-rich CCSN")]
    pub h_rich_ccsn: f32,
    #[serde(rename = "Superluminous SN")]
    pub superluminous_sn: f32,
}

impl CiderClassProbs {
    fn from_probs(p: &[f32]) -> Option<Self> {
        if p.len() < 8 {
            return None;
        }
        Some(Self {
            agn_like: p[0],
            accreting_wd_var: p[1],
            other_stellar_var: p[2],
            tde: p[3],
            ia_like_sn: p[4],
            stripped_envelope_sn: p[5],
            h_rich_ccsn: p[6],
            superluminous_sn: p[7],
        })
    }
}

/// ZTF alert ML classifier scores
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct ZtfAlertClassifications {
    pub acai_h: f32,
    pub acai_n: f32,
    pub acai_v: f32,
    pub acai_o: f32,
    pub acai_b: f32,
    pub btsbot: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cider_fusion: Option<CiderClassProbs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fusion_embedding: Option<Vec<f32>>,
}

/// Per-alert intermediate data used during enrichment processing.
struct AlertWork {
    candid: i64,
    programid: i32,
    properties: ZtfAlertProperties,
    all_bands_properties: AllBandsProperties,
    cutouts: AlertCutout,
    alert: ZtfAlertForEnrichment,
    lightcurve: Vec<PhotometryMag>,
}

pub struct ZtfEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_cutout_storage: CutoutStorage,
    alert_pipeline: Vec<Document>,
    /// Shared ONNX models (loaded once, shared across all enrichment workers
    /// via Arc). On Linux+`gpu` this also owns the per-device CUDA stream and
    /// villar-pso `GpuContext` so that PSO and ONNX inference share a stream.
    models: Option<Arc<SharedModels>>,
    babamul: Option<Babamul>,
    gpu_enabled: bool,
}

#[cfg(feature = "gpu")]
fn to_villar_photometry(p: &PhotometryMag) -> Option<villar_pso::PhotometryMag> {
    let band = match p.band {
        Band::G => villar_pso::Band::G,
        Band::R => villar_pso::Band::R,
        _ => return None,
    };
    Some(villar_pso::PhotometryMag {
        time: p.time,
        mag: p.mag,
        mag_err: p.mag_err,
        band,
    })
}

#[async_trait::async_trait]
impl EnrichmentWorker for ZtfEnrichmentWorker {
    #[instrument(skip(shared_models), err)]
    async fn new(
        config_path: &str,
        shared_models: Option<Arc<SharedModels>>,
    ) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("ZTF_alerts");
        let alert_cutout_storage = config.build_cutout_storage(&Survey::Ztf).await?;

        let input_queue = "ZTF_alerts_enrichment_queue".to_string();
        let output_queue = "ZTF_alerts_filter_queue".to_string();

        // Detect if Babamul is enabled from the config
        let babamul: Option<Babamul> = if config.babamul.enabled {
            Some(Babamul::new(&config))
        } else {
            None
        };

        // Use shared models if provided (GPU path), otherwise load per-worker
        // models on CPU. Per-worker models avoid mutex contention when multiple
        // enrichment workers run in parallel on CPU.
        let models = match shared_models {
            Some(m) => Some(m),
            None => Some(SharedModels::load(None)?),
        };

        Ok(ZtfEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_cutout_storage,
            alert_pipeline: create_ztf_alert_pipeline(false),
            models,
            babamul,
            gpu_enabled: config.gpu.enabled,
        })
    }

    fn survey() -> Survey {
        Survey::Ztf
    }

    fn disable_babamul(&mut self) {
        self.babamul = None;
    }

    fn input_queue_name(&self) -> String {
        self.input_queue.clone()
    }

    fn output_queue_name(&self) -> String {
        self.output_queue.clone()
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(
        &mut self,
        candids: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError> {
        let alerts: Vec<ZtfAlertForEnrichment> =
            fetch_alerts(&candids, &self.alert_pipeline, &self.alert_collection).await?;

        if alerts.len() != candids.len() {
            warn!(
                "only {} alerts fetched from {} candids",
                alerts.len(),
                candids.len()
            );
        }
        if alerts.is_empty() {
            return Ok(vec![]);
        }

        let mut candid_to_cutouts = self
            .alert_cutout_storage
            .retrieve_multiple_cutouts(candids, true)
            .await?;

        if candid_to_cutouts.len() != alerts.len() {
            warn!(
                "only {} cutouts fetched from {} candids",
                candid_to_cutouts.len(),
                alerts.len()
            );
        }

        let now = flare::Time::now().to_jd();

        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        let mut enriched_alerts: Vec<BabamulZtfAlert> = Vec::new();

        let mut work_items: Vec<AlertWork> = Vec::with_capacity(alerts.len());
        #[cfg(feature = "gpu")]
        let mut villar_inputs: Vec<(i64, Vec<PhotometryMag>)> = Vec::new();
        for alert in alerts {
            let candid = alert.candid;
            let cutouts = candid_to_cutouts
                .remove(&candid)
                .ok_or_else(|| EnrichmentWorkerError::MissingCutouts(candid))?;

            // Compute numerical and boolean features from lightcurve and candidate analysis
            let (properties, all_bands_properties, programid, lightcurve) =
                self.get_alert_properties(&alert).await?;
            #[cfg(feature = "gpu")]
            if self
                .models
                .as_ref()
                .and_then(|m| m.gpu_ctx.as_ref())
                .is_some()
            {
                villar_inputs.push((candid, lightcurve));
            }

            work_items.push(AlertWork {
                candid,
                programid,
                properties,
                all_bands_properties,
                cutouts,
                alert,
                lightcurve,
            });
        }

        // Run ML classification using shared models
        let classify_start = std::time::Instant::now();
        let classifications_list: Vec<Option<ZtfAlertClassifications>> =
            if let Some(ref models) = self.models {
                self.classify(&models, &work_items)?
            } else {
                vec![None; work_items.len()]
            };
        info!(
            batch_size = work_items.len(),
            classify_ms = classify_start.elapsed().as_millis() as u64,
            "ML classification complete"
        );

        for (item, classifications) in work_items.into_iter().zip(classifications_list) {
            let update_alert_document = if let Some(ref cls) = classifications {
                doc! { "$set": {
                    "classifications": mongify(cls),
                    "properties": mongify(&item.properties),
                    "updated_at": now,
                }}
            } else {
                doc! { "$set": {
                    "properties": mongify(&item.properties),
                    "updated_at": now,
                }}
            };

            let update = WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(doc! {"_id": item.candid})
                    .update(update_alert_document)
                    .build(),
            );

            updates.push(update);
            processed_alerts.push(format!("{},{}", item.programid, item.candid));

            if self.babamul.is_some() {
                let enriched_alert =
                    BabamulZtfAlert::from_alert_and_properties(item.alert, item.properties);
                enriched_alerts.push(enriched_alert);
            }
        }

        let _ = self.client.bulk_write(updates).await?.modified_count;

        // GPU batch Villar light curve fitting — only when SharedModels was
        // loaded with a GPU device (i.e. config.gpu.enabled is true).
        // Otherwise `villar_inputs` is empty and we skip the entire block.
        #[cfg(feature = "gpu")]
        if let Some(gpu_ctx) = self.models.as_ref().and_then(|m| m.gpu_ctx.as_ref()) {
            // Document whose keys match a successful fit's keys but with all values NaN.
            // Written whenever a fit can't be produced (bad photometry or GPU failure).
            let nan_set_doc = {
                let mut d = doc! { "villar_fit.reduced_chi2": f64::NAN };
                for filt in villar_pso::FILTERS {
                    for pname in villar_pso::PARAM_NAMES {
                        d.insert(format!("villar_fit.{}_{}", pname, filt), f64::NAN);
                    }
                }
                d
            };

            let alert_collection = &self.alert_collection;
            let build_update = |candid: i64, set_doc: Document| {
                WriteModel::UpdateOne(
                    UpdateOneModel::builder()
                        .namespace(alert_collection.namespace())
                        .filter(doc! { "_id": candid })
                        .update(doc! { "$set": set_doc })
                        .build(),
                )
            };

            // Preprocess each lightcurve; split into fittable sources and
            // NaN-update-only candids that failed preprocessing.
            let mut villar_updates: Vec<WriteModel> = Vec::new();
            let mut fittable: Vec<(i64, SourceData)> = Vec::new();
            for (candid, lc) in &villar_inputs {
                let villar_lc: Vec<villar_pso::PhotometryMag> =
                    lc.iter().filter_map(to_villar_photometry).collect();
                match villar_pso::preprocess_from_photometry(&villar_lc) {
                    Ok(preproc) => fittable.push((
                        *candid,
                        SourceData {
                            name: candid.to_string(),
                            data: preproc,
                        },
                    )),
                    Err(e) => {
                        trace!(candid, "skipping Villar fit: {}", e);
                        villar_updates.push(build_update(*candid, nan_set_doc.clone()));
                    }
                }
            }

            // Run GPU batch fit on the fittable sources.
            if !fittable.is_empty() {
                let (candids, sources): (Vec<i64>, Vec<SourceData>) = fittable.into_iter().unzip();
                let source_refs: Vec<&SourceData> = sources.iter().collect();
                let pso_config = villar_pso::PsoConfig::default();

                let villar_start = std::time::Instant::now();
                let batch_result = GpuBatchData::new(gpu_ctx, &source_refs);

                let fit_result = batch_result.and_then(|batch| {
                    gpu_ctx.batch_pso_multi_seed(&batch, &source_refs, &pso_config)
                });
                info!(
                    fittable = source_refs.len(),
                    villar_ms = villar_start.elapsed().as_millis() as u64,
                    "Villar PSO batch complete"
                );

                match fit_result {
                    Ok(results) => {
                        for (result, candid) in results.iter().zip(candids) {
                            let mut set_doc = doc! {
                                "villar_fit.reduced_chi2": result.reduced_chi2,
                            };
                            for (key, val) in &result.params_unnorm.to_named_map() {
                                set_doc.insert(format!("villar_fit.{}", key), *val);
                            }
                            villar_updates.push(build_update(candid, set_doc));
                        }
                    }
                    Err(e) => {
                        warn!("GPU Villar batch fitting failed: {}", e);
                        villar_updates.extend(
                            candids
                                .into_iter()
                                .map(|c| build_update(c, nan_set_doc.clone())),
                        );
                    }
                }
            }

            if !villar_updates.is_empty() {
                if let Err(e) = self.client.bulk_write(villar_updates).await {
                    warn!("failed to write Villar fit results: {}", e);
                }
            }
        }

        // Send to Babamul for batch processing
        if let Some(babamul) = self.babamul.as_ref() {
            babamul.process_ztf_alerts(enriched_alerts).await?;
        }

        Ok(processed_alerts)
    }
}

impl ZtfEnrichmentWorker {
    async fn get_alert_properties(
        &self,
        alert: &ZtfAlertForEnrichment,
    ) -> Result<
        (
            ZtfAlertProperties,
            AllBandsProperties,
            i32,
            Vec<PhotometryMag>,
        ),
        EnrichmentWorkerError,
    > {
        let candidate = &alert.candidate.candidate;
        let programid = candidate.programid;
        let ssdistnr = candidate.ssdistnr.unwrap_or(f32::INFINITY);
        let ssmagnr = candidate.ssmagnr.unwrap_or(f32::INFINITY);
        let is_rock = ssdistnr >= 0.0 && ssdistnr < 12.0 && ssmagnr >= 0.0;

        let sgscore1 = candidate.sgscore1.unwrap_or(0.0);
        let sgscore2 = candidate.sgscore2.unwrap_or(0.0);
        let sgscore3 = candidate.sgscore3.unwrap_or(0.0);
        let distpsnr1 = candidate.distpsnr1.unwrap_or(f32::INFINITY);
        let distpsnr2 = candidate.distpsnr2.unwrap_or(f32::INFINITY);
        let distpsnr3 = candidate.distpsnr3.unwrap_or(f32::INFINITY);

        let srmag1 = candidate.srmag1.unwrap_or(f32::INFINITY);
        let srmag2 = candidate.srmag2.unwrap_or(f32::INFINITY);
        let srmag3 = candidate.srmag3.unwrap_or(f32::INFINITY);
        let sgmag1 = candidate.sgmag1.unwrap_or(f32::INFINITY);
        let simag1 = candidate.simag1.unwrap_or(f32::INFINITY);
        let szmag1 = candidate.szmag1.unwrap_or(f32::INFINITY);

        let neargaiabright = candidate.neargaiabright.unwrap_or(f32::INFINITY);
        let maggaiabright = candidate.maggaiabright.unwrap_or(f32::INFINITY);

        let is_star = (sgscore1 > 0.76 && distpsnr1 >= 0.0 && distpsnr1 <= 2.0)
            || (sgscore1 > 0.2
                && distpsnr1 >= 0.0
                && distpsnr1 <= 1.0
                && srmag1 > 0.0
                && ((szmag1 > 0.0 && srmag1 - szmag1 > 3.0)
                    || (simag1 > 0.0 && srmag1 - simag1 > 3.0)));

        let is_near_brightstar = (neargaiabright >= 0.0
            && neargaiabright <= 20.0
            && maggaiabright > 0.0
            && maggaiabright <= 12.0)
            || (sgscore1 > 0.49 && distpsnr1 <= 20.0 && srmag1 > 0.0 && srmag1 <= 15.0)
            || (sgscore2 > 0.49 && distpsnr2 <= 20.0 && srmag2 > 0.0 && srmag2 <= 15.0)
            || (sgscore3 > 0.49 && distpsnr3 <= 20.0 && srmag3 > 0.0 && srmag3 <= 15.0)
            || (sgscore1 == 0.5
                && distpsnr1 < 0.5
                && (sgmag1 < 17.0 || srmag1 < 17.0 || simag1 < 17.0));

        let prv_candidates: Vec<PhotometryMag> = alert
            .prv_candidates
            .iter()
            .filter(|p| p.jd <= alert.candidate.candidate.jd)
            .filter_map(|p| p.to_photometry_mag(None))
            .collect();
        let fp_hists: Vec<PhotometryMag> = alert
            .fp_hists
            .iter()
            .filter(|p| p.jd <= alert.candidate.candidate.jd)
            .filter_map(|p| p.to_photometry_mag(Some(3.0)))
            .collect();

        // lightcurve is prv_candidates + fp_hists, no need for parse_photometry here
        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);
        let (photstats, all_bands_properties, stationary) = analyze_photometry(&lightcurve);
        // Snapshot ZTF-only lightcurve before any cross-survey extension; cider was trained on ZTF only
        let ztf_lightcurve = lightcurve.clone();

        // Compute multisurvey photstats (including LSST if available, other surveys can be added later)
        let mut has_matches = false;
        if let Some(survey_matches) = &alert.survey_matches {
            if let Some(lsst_match) = &survey_matches.lsst {
                let lsst_prv_candidates: Vec<PhotometryMag> = lsst_match
                    .prv_candidates
                    .iter()
                    .filter(|p| p.jd <= alert.candidate.candidate.jd)
                    .filter_map(|p| p.to_photometry_mag(None))
                    .collect();
                let lsst_fp_hists: Vec<PhotometryMag> = lsst_match
                    .fp_hists
                    .iter()
                    .filter(|p| p.jd <= alert.candidate.candidate.jd)
                    .filter_map(|p| p.to_photometry_mag(Some(3.0)))
                    .collect();
                let mut lsst_lightcurve = [lsst_prv_candidates, lsst_fp_hists].concat();
                prepare_photometry(&mut lsst_lightcurve);
                lightcurve.extend(lsst_lightcurve);
                has_matches = true;
            }
        }
        let multisurvey_photstats = if has_matches {
            analyze_photometry(&lightcurve).0
        } else {
            photstats.clone()
        };

        Ok((
            ZtfAlertProperties {
                rock: is_rock,
                star: is_star,
                near_brightstar: is_near_brightstar,
                stationary,
                photstats,
                multisurvey_photstats: Some(multisurvey_photstats),
            },
            all_bands_properties,
            programid,
            ztf_lightcurve,
        ))
    }

    /// Run ONNX classification using shared models.
    /// Each model is locked individually to minimize contention.
    fn classify(
        &self,
        models: &SharedModels,
        work_items: &[AlertWork],
    ) -> Result<Vec<Option<ZtfAlertClassifications>>, EnrichmentWorkerError> {
        if self.gpu_enabled {
            return Self::classify_gpu_batch(models, work_items);
        }

        Self::classify_per_item(models, work_items)
    }

    fn classify_per_item(
        models: &SharedModels,
        work_items: &[AlertWork],
    ) -> Result<Vec<Option<ZtfAlertClassifications>>, EnrichmentWorkerError> {
        let mut results = Vec::with_capacity(work_items.len());
        for item in work_items {
            let triplet = match AcaiModel::get_triplet(&[&item.cutouts]) {
                Ok(triplet) => triplet,
                Err(err) => {
                    warn!(
                        "Skipping ML inference for candid {} due to invalid cutouts: {}",
                        item.candid, err
                    );
                    results.push(None);
                    continue;
                }
            };
            let metadata_result = AcaiModel::get_metadata(&[&item.alert]);
            let btsbot_metadata_result =
                BtsBotModel::get_metadata(&[&item.alert], &[item.all_bands_properties.clone()]);

            let cls = if let (Ok(metadata), Ok(btsbot_metadata)) =
                (metadata_result, btsbot_metadata_result)
            {
                let acai_h_scores = models.acai_h.lock().unwrap().predict(&metadata, &triplet)?;
                let acai_n_scores = models.acai_n.lock().unwrap().predict(&metadata, &triplet)?;
                let acai_v_scores = models.acai_v.lock().unwrap().predict(&metadata, &triplet)?;
                let acai_o_scores = models.acai_o.lock().unwrap().predict(&metadata, &triplet)?;
                let acai_b_scores = models.acai_b.lock().unwrap().predict(&metadata, &triplet)?;
                let btsbot_scores = models
                    .btsbot
                    .lock()
                    .unwrap()
                    .predict(&btsbot_metadata, &triplet)?;

                let cider_result = (|| -> Result<(CiderClassProbs, Vec<f32>), ModelError> {
                    let mut m = models.cider.lock().unwrap();
                    let meta = m.get_metadata(&[&item.alert], &[&item.all_bands_properties])?;
                    let img = m.get_triplet(&[&item.cutouts])?;
                    let (tx, tpm, tg) = m.photometry_inputs(item.lightcurve.clone())?;
                    let (probs, embedding) = m.predict(&tx, &tpm, &tg, &meta, &img)?;
                    let cls = CiderClassProbs::from_probs(&probs).ok_or(
                        ModelError::MissingFeature("cider: unexpected output length"),
                    )?;
                    Ok((cls, embedding))
                })()
                .map_err(|e| {
                    warn!("cider inference failed for candid {}: {}", item.candid, e);
                })
                .ok();

                Some(ZtfAlertClassifications {
                    acai_h: acai_h_scores[0],
                    acai_n: acai_n_scores[0],
                    acai_v: acai_v_scores[0],
                    acai_o: acai_o_scores[0],
                    acai_b: acai_b_scores[0],
                    btsbot: btsbot_scores[0],
                    cider_fusion: cider_result.as_ref().map(|(cls, _)| cls.clone()),
                    fusion_embedding: cider_result.map(|(_, emb)| emb),
                })
            } else {
                warn!(
                    "Skipping ML inference for candid {} due to missing features",
                    item.candid
                );
                None
            };
            results.push(cls);
        }
        Ok(results)
    }

    /// Fixed ONNX batch dimension for GPU inference. ORT builds — and never
    /// releases — a separate memory plan per distinct input shape, so a
    /// varying batch size makes GPU memory spike. Every inference is run at
    /// exactly this many rows: partial batches are zero-padded and the padding
    /// outputs discarded, so ORT only ever sees one shape and the arena is
    /// stable. An RPOP batch larger than this is split into several fixed-size
    /// chunks; only the final chunk carries any zero-padding.
    // 1000 was tried and OOMs (~15.7 GB footprint exceeds the 16 GB card);
    // 750 is stable at ~10.3 GB.
    // const GPU_INFER_SHAPE: usize = 1000;
    const GPU_INFER_SHAPE: usize = 750;

    fn classify_gpu_batch(
        models: &SharedModels,
        work_items: &[AlertWork],
    ) -> Result<Vec<Option<ZtfAlertClassifications>>, EnrichmentWorkerError> {
        let mut results = vec![None; work_items.len()];

        let all_alerts: Vec<&ZtfAlertForEnrichment> = work_items.iter().map(|w| &w.alert).collect();
        let all_cutouts: Vec<&AlertCutout> = work_items.iter().map(|w| &w.cutouts).collect();
        let all_band_props: Vec<AllBandsProperties> = work_items
            .iter()
            .map(|w| w.all_bands_properties.clone())
            .collect();

        let (triplet_indices, triplet_all) = AcaiModel::get_triplet_indexed(&all_cutouts)?;
        let (acai_indices, acai_metadata_all) = AcaiModel::get_metadata_indexed(&all_alerts)?;
        let (bts_indices, bts_metadata_all) =
            BtsBotModel::get_metadata_indexed(&all_alerts, &all_band_props)?;

        let triplet_pos: std::collections::HashMap<usize, usize> = triplet_indices
            .iter()
            .enumerate()
            .map(|(pos, idx)| (*idx, pos))
            .collect();
        let acai_pos: std::collections::HashMap<usize, usize> = acai_indices
            .iter()
            .enumerate()
            .map(|(pos, idx)| (*idx, pos))
            .collect();
        let bts_pos: std::collections::HashMap<usize, usize> = bts_indices
            .iter()
            .enumerate()
            .map(|(pos, idx)| (*idx, pos))
            .collect();

        let mut selected_indices: Vec<usize> = Vec::new();
        for idx in 0..work_items.len() {
            if triplet_pos.contains_key(&idx)
                && acai_pos.contains_key(&idx)
                && bts_pos.contains_key(&idx)
            {
                selected_indices.push(idx);
            } else {
                warn!(
                    "Skipping ML inference for candid {} due to missing features",
                    work_items[idx].candid
                );
            }
        }

        if selected_indices.is_empty() {
            return Ok(results);
        }

        // CIDER batch on ALL selected alerts at once — failure is non-fatal.
        let n_sel = selected_indices.len();
        let cider_batch: Option<(Vec<f32>, Vec<f32>)> =
            (|| -> Result<(Vec<f32>, Vec<f32>), ModelError> {
                let cider_alerts: Vec<&ZtfAlertForEnrichment> = selected_indices
                    .iter()
                    .map(|&i| &work_items[i].alert)
                    .collect();
                let cider_cutouts: Vec<&AlertCutout> = selected_indices
                    .iter()
                    .map(|&i| &work_items[i].cutouts)
                    .collect();
                let cider_props: Vec<&AllBandsProperties> = selected_indices
                    .iter()
                    .map(|&i| &work_items[i].all_bands_properties)
                    .collect();

                let mut cider = models.cider.lock().unwrap();
                let cider_meta = cider.get_metadata(&cider_alerts, &cider_props)?;
                let cider_image = cider.get_triplet(&cider_cutouts)?;

                let phot: Vec<_> = selected_indices
                    .iter()
                    .map(|&i| cider.photometry_inputs(work_items[i].lightcurve.clone()))
                    .collect::<Result<Vec<_>, _>>()?;

                let tx_views: Vec<_> = phot.iter().map(|(x, _, _)| x.view()).collect();
                let tpm_views: Vec<_> = phot.iter().map(|(_, m, _)| m.view()).collect();
                let tg_views: Vec<_> = phot.iter().map(|(_, _, g)| g.view()).collect();

                let tx = ndarray::concatenate(ndarray::Axis(0), &tx_views)?;
                let tpm = ndarray::concatenate(ndarray::Axis(0), &tpm_views)?;
                let tg = ndarray::concatenate(ndarray::Axis(0), &tg_views)?;

                cider.predict(&tx, &tpm, &tg, &cider_meta, &cider_image)
            })()
            .map_err(|e| {
                warn!("cider batch inference failed: {}", e);
            })
            .ok();

        let cider_n_cls = cider_batch
            .as_ref()
            .map(|(p, _)| p.len() / n_sel)
            .unwrap_or(0);
        let cider_emb_dim = cider_batch
            .as_ref()
            .map(|(_, e)| e.len() / n_sel)
            .unwrap_or(0);

        // ACAI/BTSBot in fixed-size padded chunks so ORT always sees the same
        // input shape (see `GPU_INFER_SHAPE`). Padding rows produce scores that are ignored.
        for (chunk_idx, chunk) in selected_indices.chunks(Self::GPU_INFER_SHAPE).enumerate() {
            let mut triplet = ndarray::Array::zeros((Self::GPU_INFER_SHAPE, 63, 63, 3));
            let mut metadata = ndarray::Array::zeros((Self::GPU_INFER_SHAPE, 25));
            let mut btsbot_metadata = ndarray::Array::zeros((Self::GPU_INFER_SHAPE, 25));

            for (row, idx) in chunk.iter().enumerate() {
                let tpos = *triplet_pos.get(idx).expect("triplet position missing");
                let apos = *acai_pos.get(idx).expect("acai position missing");
                let bpos = *bts_pos.get(idx).expect("bts position missing");

                triplet
                    .slice_mut(ndarray::s![row, .., .., ..])
                    .assign(&triplet_all.slice(ndarray::s![tpos, .., .., ..]));
                metadata.row_mut(row).assign(&acai_metadata_all.row(apos));
                btsbot_metadata
                    .row_mut(row)
                    .assign(&bts_metadata_all.row(bpos));
            }

            let acai_h_scores = models.acai_h.lock().unwrap().predict(&metadata, &triplet)?;
            let acai_n_scores = models.acai_n.lock().unwrap().predict(&metadata, &triplet)?;
            let acai_v_scores = models.acai_v.lock().unwrap().predict(&metadata, &triplet)?;
            let acai_o_scores = models.acai_o.lock().unwrap().predict(&metadata, &triplet)?;
            let acai_b_scores = models.acai_b.lock().unwrap().predict(&metadata, &triplet)?;
            let btsbot_scores = models
                .btsbot
                .lock()
                .unwrap()
                .predict(&btsbot_metadata, &triplet)?;

            for (name, got) in [
                ("acai_h", acai_h_scores.len()),
                ("acai_n", acai_n_scores.len()),
                ("acai_v", acai_v_scores.len()),
                ("acai_o", acai_o_scores.len()),
                ("acai_b", acai_b_scores.len()),
                ("btsbot", btsbot_scores.len()),
            ] {
                if got != Self::GPU_INFER_SHAPE {
                    return Err(EnrichmentWorkerError::ConfigurationError(format!(
                        "model {} returned {} scores for {} padded inputs",
                        name,
                        got,
                        Self::GPU_INFER_SHAPE
                    )));
                }
            }

            let chunk_start = chunk_idx * Self::GPU_INFER_SHAPE;
            for (batch_idx, &item_idx) in chunk.iter().enumerate() {
                let sel_idx = chunk_start + batch_idx;
                let cider_fusion = cider_batch.as_ref().and_then(|(probs, _)| {
                    CiderClassProbs::from_probs(
                        &probs[sel_idx * cider_n_cls..(sel_idx + 1) * cider_n_cls],
                    )
                });
                let fusion_embedding = cider_batch.as_ref().map(|(_, emb)| {
                    emb[sel_idx * cider_emb_dim..(sel_idx + 1) * cider_emb_dim].to_vec()
                });
                results[item_idx] = Some(ZtfAlertClassifications {
                    acai_h: acai_h_scores[batch_idx],
                    acai_n: acai_n_scores[batch_idx],
                    acai_v: acai_v_scores[batch_idx],
                    acai_o: acai_o_scores[batch_idx],
                    acai_b: acai_b_scores[batch_idx],
                    btsbot: btsbot_scores[batch_idx],
                    cider_fusion,
                    fusion_embedding,
                });
            }
        }

        Ok(results)
    }
}
