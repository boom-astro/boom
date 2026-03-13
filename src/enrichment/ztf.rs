use crate::conf::AppConfig;
use crate::enrichment::babamul::{Babamul, BabamulZtfAlert};
use crate::enrichment::LsstMatch;
use crate::gpu::{
    gpu_inference_queue_name, gpu_result_key, GpuInferenceRequest, GpuInferenceResponse,
};
use crate::utils::db::mongify;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, AllBandsProperties, Band, PerBandProperties,
    PhotometryMag, ZTF_ZP,
};
use crate::{
    alert::ZtfCandidate,
    enrichment::{
        fetch_alert_cutouts, fetch_alerts,
        models::{AcaiModel, BtsBotModel, Model},
        EnrichmentWorker, EnrichmentWorkerError,
    },
};
use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use redis::AsyncCommands;
use serde::{Deserialize, Deserializer};
use tracing::{instrument, trace, warn};

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

/// ZTF alert ML classifier scores
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct ZtfAlertClassifications {
    pub acai_h: f32,
    pub acai_n: f32,
    pub acai_v: f32,
    pub acai_o: f32,
    pub acai_b: f32,
    pub btsbot: f32,
}

/// Per-alert intermediate data used during enrichment processing.
struct AlertWork {
    candid: i64,
    programid: i32,
    properties: ZtfAlertProperties,
    all_bands_properties: AllBandsProperties,
    cutouts: crate::alert::AlertCutout,
    alert: ZtfAlertForEnrichment,
}

/// Holds ONNX models when running in CPU-inline mode (gpu.enabled = false).
/// When gpu.enabled = true, models are owned by the GPU worker instead.
struct InlineModels {
    acai_h: AcaiModel,
    acai_n: AcaiModel,
    acai_v: AcaiModel,
    acai_o: AcaiModel,
    acai_b: AcaiModel,
    btsbot: BtsBotModel,
}

pub struct ZtfEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_cutout_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
    /// None when gpu.enabled = true (GPU worker owns models).
    models: Option<InlineModels>,
    /// Redis connection for GPU inference requests (only used when gpu.enabled).
    redis_con: Option<redis::aio::MultiplexedConnection>,
    /// GPU inference queue name (only used when gpu.enabled).
    gpu_queue: Option<String>,
    babamul: Option<Babamul>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for ZtfEnrichmentWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("ZTF_alerts");
        let alert_cutout_collection = db.collection("ZTF_alerts_cutouts");

        let input_queue = "ZTF_alerts_enrichment_queue".to_string();
        let output_queue = "ZTF_alerts_filter_queue".to_string();

        // When GPU mode is enabled, models are owned by the GPU worker.
        // The enrichment worker only needs a Redis connection to push requests.
        let (models, redis_con, gpu_queue) = if config.gpu.enabled {
            let con = config.build_redis().await?;
            let queue = gpu_inference_queue_name("ZTF");
            (None, Some(con), Some(queue))
        } else {
            // CPU-inline mode: load ONNX models in this worker
            let models = InlineModels {
                acai_h: AcaiModel::new("data/models/acai_h.d1_dnn_20201130.onnx")?,
                acai_n: AcaiModel::new("data/models/acai_n.d1_dnn_20201130.onnx")?,
                acai_v: AcaiModel::new("data/models/acai_v.d1_dnn_20201130.onnx")?,
                acai_o: AcaiModel::new("data/models/acai_o.d1_dnn_20201130.onnx")?,
                acai_b: AcaiModel::new("data/models/acai_b.d1_dnn_20201130.onnx")?,
                btsbot: BtsBotModel::new("data/models/btsbot-v1.0.1.onnx")?,
            };
            (Some(models), None, None)
        };

        // Detect if Babamul is enabled from the config
        let babamul: Option<Babamul> = if config.babamul.enabled {
            Some(Babamul::new(&config))
        } else {
            None
        };

        Ok(ZtfEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_cutout_collection,
            alert_pipeline: create_ztf_alert_pipeline(false),
            models,
            redis_con,
            gpu_queue,
            babamul,
        })
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

        let mut candid_to_cutouts =
            fetch_alert_cutouts(&candids, &self.alert_cutout_collection).await?;

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
        for alert in alerts {
            let candid = alert.candid;
            let cutouts = candid_to_cutouts
                .remove(&candid)
                .ok_or_else(|| EnrichmentWorkerError::MissingCutouts(candid))?;
            let (properties, all_bands_properties, programid, _lightcurve) =
                self.get_alert_properties(&alert).await?;
            work_items.push(AlertWork {
                candid,
                programid,
                properties,
                all_bands_properties,
                cutouts,
                alert,
            });
        }

        // Determine classifications: GPU-deferred or CPU-inline
        let classifications_list: Vec<Option<ZtfAlertClassifications>> =
            if self.redis_con.is_some() && self.gpu_queue.is_some() {
                // GPU path: push inference requests and poll for results
                let con = self.redis_con.as_mut().unwrap();
                let gpu_queue = self.gpu_queue.as_ref().unwrap();
                Self::classify_via_gpu(&work_items, con, gpu_queue).await?
            } else if let Some(ref mut models) = self.models {
                // CPU-inline path: run inference directly
                Self::classify_inline(models, &work_items)?
            } else {
                // No models and no GPU — skip classification
                vec![None; work_items.len()]
            };

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
            lightcurve,
        ))
    }

    /// CPU-inline classification: run ONNX models directly in this worker thread.
    fn classify_inline(
        models: &mut InlineModels,
        work_items: &[AlertWork],
    ) -> Result<Vec<Option<ZtfAlertClassifications>>, EnrichmentWorkerError> {
        let mut results = Vec::with_capacity(work_items.len());
        for item in work_items {
            let triplet = models.acai_h.get_triplet(&[&item.cutouts])?;
            let metadata_result = models.acai_h.get_metadata(&[&item.alert]);
            let btsbot_metadata_result = models
                .btsbot
                .get_metadata(&[&item.alert], &[item.all_bands_properties.clone()]);

            let cls = if let (Ok(metadata), Ok(btsbot_metadata)) =
                (metadata_result, btsbot_metadata_result)
            {
                let acai_h_scores = models.acai_h.predict(&metadata, &triplet)?;
                let acai_n_scores = models.acai_n.predict(&metadata, &triplet)?;
                let acai_v_scores = models.acai_v.predict(&metadata, &triplet)?;
                let acai_o_scores = models.acai_o.predict(&metadata, &triplet)?;
                let acai_b_scores = models.acai_b.predict(&metadata, &triplet)?;
                let btsbot_scores = models.btsbot.predict(&btsbot_metadata, &triplet)?;
                Some(ZtfAlertClassifications {
                    acai_h: acai_h_scores[0],
                    acai_n: acai_n_scores[0],
                    acai_v: acai_v_scores[0],
                    acai_o: acai_o_scores[0],
                    acai_b: acai_b_scores[0],
                    btsbot: btsbot_scores[0],
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

    /// GPU-deferred classification: push inference requests to the GPU worker
    /// via Redis, then poll for results.
    async fn classify_via_gpu(
        work_items: &[AlertWork],
        con: &mut redis::aio::MultiplexedConnection,
        gpu_queue: &str,
    ) -> Result<Vec<Option<ZtfAlertClassifications>>, EnrichmentWorkerError> {
        // We need a temporary AcaiModel just for get_metadata/get_triplet (CPU-only operations).
        // These are lightweight — they don't load ONNX. We use a dummy to access the trait methods.
        // Actually, get_triplet and get_metadata are on the Model trait impl, not the Session.
        // For the GPU path, we pre-extract features and triplets here, then send them.
        //
        // Since get_triplet and get_metadata need a Model instance, and we don't have one
        // in GPU mode, we compute the features manually. The triplet preparation uses
        // prepare_triplet from utils::fits which doesn't need a model.

        use crate::utils::fits::prepare_triplet;

        let mut request_ids: Vec<Option<String>> = Vec::with_capacity(work_items.len());
        let mut gpu_requests: Vec<String> = Vec::new();

        for item in work_items {
            // Prepare triplet (CPU operation, no model needed)
            let triplet_result = prepare_triplet(&item.cutouts);
            let triplet_flat = match triplet_result {
                Ok((sci, tmpl, diff)) => {
                    let mut flat = Vec::with_capacity(63 * 63 * 3);
                    // Interleave as [row][col][channel] matching the model's expected layout
                    for row in 0..63 {
                        for col in 0..63 {
                            flat.push(sci[row * 63 + col]);
                            flat.push(tmpl[row * 63 + col]);
                            flat.push(diff[row * 63 + col]);
                        }
                    }
                    flat
                }
                Err(e) => {
                    warn!("Failed to prepare triplet for candid {}: {}", item.candid, e);
                    request_ids.push(None);
                    continue;
                }
            };

            // Extract ACAI metadata (25 features)
            let candidate = &item.alert.candidate.candidate;
            let acai_metadata = match Self::extract_acai_metadata(candidate) {
                Some(m) => m,
                None => {
                    warn!(
                        "Skipping GPU inference for candid {} due to missing ACAI features",
                        item.candid
                    );
                    request_ids.push(None);
                    continue;
                }
            };

            // Extract BTSBot metadata (25 features)
            let btsbot_metadata =
                match Self::extract_btsbot_metadata(candidate, &item.all_bands_properties) {
                    Some(m) => m,
                    None => {
                        warn!(
                            "Skipping GPU inference for candid {} due to missing BTSBot features",
                            item.candid
                        );
                        request_ids.push(None);
                        continue;
                    }
                };

            let request_id = uuid::Uuid::new_v4().to_string();
            let req = GpuInferenceRequest {
                request_id: request_id.clone(),
                candid: item.candid,
                acai_metadata,
                btsbot_metadata,
                triplet: triplet_flat,
            };

            match serde_json::to_string(&req) {
                Ok(json) => {
                    gpu_requests.push(json);
                    request_ids.push(Some(request_id));
                }
                Err(e) => {
                    warn!("Failed to serialize GPU request for candid {}: {}", item.candid, e);
                    request_ids.push(None);
                }
            }
        }

        // Push all requests to the GPU queue
        if !gpu_requests.is_empty() {
            let _: usize = con.lpush(gpu_queue, &gpu_requests).await.map_err(|e| {
                EnrichmentWorkerError::Redis(e)
            })?;
        }

        // Poll for results with timeout
        let poll_interval = std::time::Duration::from_millis(10);
        let max_wait = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();

        let mut results: Vec<Option<ZtfAlertClassifications>> = vec![None; work_items.len()];
        let mut pending: Vec<(usize, String)> = request_ids
            .iter()
            .enumerate()
            .filter_map(|(i, rid)| rid.as_ref().map(|r| (i, r.clone())))
            .collect();

        while !pending.is_empty() && start.elapsed() < max_wait {
            let mut still_pending = Vec::new();
            for (idx, rid) in pending {
                let key = gpu_result_key(&rid);
                let result: Option<String> = con.get(&key).await.unwrap_or(None);
                if let Some(json) = result {
                    // Clean up the result key
                    let _: Result<(), _> = con.del::<&str, ()>(&key).await;
                    match serde_json::from_str::<GpuInferenceResponse>(&json) {
                        Ok(resp) => {
                            results[idx] = Some(ZtfAlertClassifications {
                                acai_h: resp.acai_h,
                                acai_n: resp.acai_n,
                                acai_v: resp.acai_v,
                                acai_o: resp.acai_o,
                                acai_b: resp.acai_b,
                                btsbot: resp.btsbot,
                            });
                        }
                        Err(e) => {
                            warn!("Failed to parse GPU result for request {}: {}", rid, e);
                        }
                    }
                } else {
                    still_pending.push((idx, rid));
                }
            }
            pending = still_pending;
            if !pending.is_empty() {
                tokio::time::sleep(poll_interval).await;
            }
        }

        if !pending.is_empty() {
            warn!(
                "{} GPU inference requests timed out after {:?}",
                pending.len(),
                max_wait
            );
        }

        Ok(results)
    }

    /// Extract 25 ACAI metadata features from a candidate. Returns None if any required field is missing.
    fn extract_acai_metadata(candidate: &crate::alert::Candidate) -> Option<Vec<f32>> {
        Some(vec![
            candidate.drb? as f32,
            candidate.diffmaglim? as f32,
            candidate.ra as f32,
            candidate.dec as f32,
            candidate.magpsf,
            candidate.sigmapsf,
            candidate.chipsf? as f32,
            candidate.fwhm? as f32,
            candidate.sky? as f32,
            candidate.chinr? as f32,
            candidate.sharpnr? as f32,
            candidate.sgscore1? as f32,
            candidate.distpsnr1? as f32,
            candidate.sgscore2? as f32,
            candidate.distpsnr2? as f32,
            candidate.sgscore3? as f32,
            candidate.distpsnr3? as f32,
            candidate.ndethist as f32,
            candidate.ncovhist as f32,
            candidate.scorr? as f32,
            candidate.nmtchps as f32,
            candidate.clrcoeff? as f32,
            candidate.clrcounc? as f32,
            candidate.neargaia? as f32,
            candidate.neargaiabright? as f32,
        ])
    }

    /// Extract 25 BTSBot metadata features. Returns None if any required field is missing.
    fn extract_btsbot_metadata(
        candidate: &crate::alert::Candidate,
        all_bands: &AllBandsProperties,
    ) -> Option<Vec<f32>> {
        let ndethist = candidate.ndethist as f32;
        let ncovhist = candidate.ncovhist as f32;
        let nnondet = ncovhist - ndethist;
        let days_since_peak = (all_bands.last_jd - all_bands.peak_jd) as f32;
        let days_to_peak = (all_bands.peak_jd - all_bands.first_jd) as f32;
        let age = (all_bands.first_jd - all_bands.last_jd) as f32;

        Some(vec![
            candidate.sgscore1? as f32,
            candidate.distpsnr1? as f32,
            candidate.sgscore2? as f32,
            candidate.distpsnr2? as f32,
            candidate.fwhm? as f32,
            candidate.magpsf as f32,
            candidate.sigmapsf,
            candidate.chipsf? as f32,
            candidate.ra as f32,
            candidate.dec as f32,
            candidate.diffmaglim? as f32,
            ndethist,
            candidate.nmtchps as f32,
            age,
            days_since_peak,
            days_to_peak,
            all_bands.peak_mag as f32,
            candidate.drb? as f32,
            ncovhist,
            nnondet,
            candidate.chinr? as f32,
            candidate.sharpnr? as f32,
            candidate.scorr? as f32,
            candidate.sky? as f32,
            all_bands.faintest_mag as f32,
        ])
    }
}
