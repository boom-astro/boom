use crate::conf::AppConfig;
use crate::enrichment::babamul::{Babamul, BabamulZtfAlert};
use crate::enrichment::LsstMatch;
use crate::utils::db::mongify;
use crate::utils::enums::Survey;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, AllBandsProperties, Band, PerBandProperties,
    PhotometryMag, ZTF_ZP,
};
use crate::{
    alert::ZtfCandidate,
    enrichment::{
        fetch_alert_cutouts, fetch_alerts,
        models::{AcaiModel, BtsBotModel, Model, SharedModels},
        EnrichmentWorker, EnrichmentWorkerError,
    },
};
use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use serde::{Deserialize, Deserializer};
use std::sync::Arc;
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

pub struct ZtfEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_cutout_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
    /// Shared ONNX models (loaded once, shared across all enrichment workers via Arc).
    models: Option<Arc<SharedModels>>,
    babamul: Option<Babamul>,
    gpu_enabled: bool,
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
        let alert_cutout_collection = db.collection("ZTF_alerts_cutouts");

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
            alert_cutout_collection,
            alert_pipeline: create_ztf_alert_pipeline(false),
            models,
            babamul,
            gpu_enabled: config.gpu.enabled,
        })
    }

    fn survey() -> Survey {
        Survey::Ztf
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

        // Run ML classification using shared models
        let classifications_list: Vec<Option<ZtfAlertClassifications>> =
            if let Some(ref models) = self.models {
                self.classify(&models, &work_items)?
            } else {
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

    fn classify_gpu_batch(
        models: &SharedModels,
        work_items: &[AlertWork],
    ) -> Result<Vec<Option<ZtfAlertClassifications>>, EnrichmentWorkerError> {
        let mut results = vec![None; work_items.len()];

        let all_alerts: Vec<&ZtfAlertForEnrichment> = work_items.iter().map(|w| &w.alert).collect();
        let all_cutouts: Vec<&crate::alert::AlertCutout> =
            work_items.iter().map(|w| &w.cutouts).collect();
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

        let mut triplet = ndarray::Array::zeros((selected_indices.len(), 63, 63, 3));
        let mut metadata = ndarray::Array::zeros((selected_indices.len(), 25));
        let mut btsbot_metadata = ndarray::Array::zeros((selected_indices.len(), 25));

        for (row, idx) in selected_indices.iter().enumerate() {
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

        let expected = selected_indices.len();
        for (name, got) in [
            ("acai_h", acai_h_scores.len()),
            ("acai_n", acai_n_scores.len()),
            ("acai_v", acai_v_scores.len()),
            ("acai_o", acai_o_scores.len()),
            ("acai_b", acai_b_scores.len()),
            ("btsbot", btsbot_scores.len()),
        ] {
            if got != expected {
                return Err(EnrichmentWorkerError::ConfigurationError(format!(
                    "model {} returned {} scores for {} inputs",
                    name, got, expected
                )));
            }
        }

        for (batch_idx, &item_idx) in selected_indices.iter().enumerate() {
            results[item_idx] = Some(ZtfAlertClassifications {
                acai_h: acai_h_scores[batch_idx],
                acai_n: acai_n_scores[batch_idx],
                acai_v: acai_v_scores[batch_idx],
                acai_o: acai_o_scores[batch_idx],
                acai_b: acai_b_scores[batch_idx],
                btsbot: btsbot_scores[batch_idx],
            });
        }

        Ok(results)
    }
}
