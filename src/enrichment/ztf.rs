use crate::conf::AppConfig;
use crate::enrichment::babamul::{Babamul, BabamulZtfAlert};
use crate::enrichment::LsstMatch;
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
use lightcurve_fitting::{
    build_mag_bands, fit_nonparametric, fit_thermal, LightcurveFittingResult,
};
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
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
    #[serde(alias = "snr")]
    pub snr_psf: Option<f64>,
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
    #[serde(alias = "snr")]
    pub snr_psf: Option<f64>,
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
    #[serde(alias = "snr")]
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
            snr_psf: phot.snr_psf,
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
            snr_psf: phot.snr_psf,
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

pub fn create_ztf_alert_pipeline() -> Vec<Document> {
    vec![
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
    ]
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

pub struct ZtfEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_cutout_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
    acai_h_model: AcaiModel,
    acai_n_model: AcaiModel,
    acai_v_model: AcaiModel,
    acai_o_model: AcaiModel,
    acai_b_model: AcaiModel,
    btsbot_model: BtsBotModel,
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

        // we load the ACAI models (same architecture, same input/output)
        let acai_h_model = AcaiModel::new("data/models/acai_h.d1_dnn_20201130.onnx")?;
        let acai_n_model = AcaiModel::new("data/models/acai_n.d1_dnn_20201130.onnx")?;
        let acai_v_model = AcaiModel::new("data/models/acai_v.d1_dnn_20201130.onnx")?;
        let acai_o_model = AcaiModel::new("data/models/acai_o.d1_dnn_20201130.onnx")?;
        let acai_b_model = AcaiModel::new("data/models/acai_b.d1_dnn_20201130.onnx")?;

        // we load the btsbot model (different architecture, and input/output then ACAI)
        let btsbot_model = BtsBotModel::new("data/models/btsbot-v1.0.1.onnx")?;

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
            alert_pipeline: create_ztf_alert_pipeline(),
            acai_h_model,
            acai_n_model,
            acai_v_model,
            acai_o_model,
            acai_b_model,
            btsbot_model,
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

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        let mut enriched_alerts: Vec<BabamulZtfAlert> = Vec::new();
        for alert in alerts {
            let candid = alert.candid;
            let cutouts = candid_to_cutouts
                .remove(&candid)
                .ok_or_else(|| EnrichmentWorkerError::MissingCutouts(candid))?;

            // Compute numerical and boolean features from lightcurve and candidate analysis
            let (properties, all_bands_properties, programid, lightcurve) =
                self.get_alert_properties(&alert).await?;

            // Lightcurve fitting (GP nonparametric)
            let lc = lightcurve.clone();
            let fitting_result = match tokio::time::timeout(
                std::time::Duration::from_secs(60),
                tokio::task::spawn_blocking(move || {
                    let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
                    let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
                    let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
                    let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();
                    let mag_bands = build_mag_bands(&times, &mags, &mag_errs, &bands);
                    let (nonparametric, trained_gps) = fit_nonparametric(&mag_bands);
                    let thermal = fit_thermal(&mag_bands, Some(&trained_gps));
                    LightcurveFittingResult {
                        nonparametric,
                        parametric: vec![],
                        thermal,
                    }
                }),
            )
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => {
                    warn!("Lightcurve fitting panicked for candid {}: {}", candid, e);
                    LightcurveFittingResult {
                        nonparametric: vec![],
                        parametric: vec![],
                        thermal: None,
                    }
                }
                Err(_) => {
                    warn!("Lightcurve fitting timed out (60s) for candid {}", candid);
                    LightcurveFittingResult {
                        nonparametric: vec![],
                        parametric: vec![],
                        thermal: None,
                    }
                }
            };

            // Now, prepare inputs for ML models and run inference
            // (we skip ML inference if features cannot be computed, e.g. missing required features)
            let triplet = self.acai_h_model.get_triplet(&[&cutouts])?;
            let metadata_result = self.acai_h_model.get_metadata(&[&alert]);
            let btsbot_metadata_result = self
                .btsbot_model
                .get_metadata(&[&alert], &[all_bands_properties.clone()]);

            let classifications = if let (Ok(metadata), Ok(btsbot_metadata)) =
                (metadata_result, btsbot_metadata_result)
            {
                let acai_h_scores = self.acai_h_model.predict(&metadata, &triplet)?;
                let acai_n_scores = self.acai_n_model.predict(&metadata, &triplet)?;
                let acai_v_scores = self.acai_v_model.predict(&metadata, &triplet)?;
                let acai_o_scores = self.acai_o_model.predict(&metadata, &triplet)?;
                let acai_b_scores = self.acai_b_model.predict(&metadata, &triplet)?;
                let btsbot_scores = self.btsbot_model.predict(&btsbot_metadata, &triplet)?;
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
                    candid
                );
                None
            };

            let update_alert_document = if let Some(classifications) = classifications {
                doc! { "$set": {
                    "classifications": mongify(&classifications),
                    "properties": mongify(&properties),
                    "lightcurve_fitting": mongify(&fitting_result),
                    "updated_at": now,
                }}
            } else {
                doc! { "$set": {
                    "properties": mongify(&properties),
                    "lightcurve_fitting": mongify(&fitting_result),
                    "updated_at": now,
                }}
            };

            let update = WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(doc! {"_id": candid})
                    .update(update_alert_document)
                    .build(),
            );

            updates.push(update);
            processed_alerts.push(format!("{},{}", programid, candid));

            // If Babamul is enabled, add the enriched alert to the batch
            if self.babamul.is_some() {
                let enriched_alert = BabamulZtfAlert::from_alert_and_properties(alert, properties);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightcurve_fitting::{
        build_mag_bands, fit_nonparametric, fit_thermal, LightcurveFittingResult,
    };

    /// Helper: build a synthetic lightcurve with a transient-like shape in g and r bands.
    fn make_synthetic_lightcurve() -> Vec<PhotometryMag> {
        let mut lc = Vec::new();
        let jd_start = 2460000.0;
        // g-band: 20 points with a bump peaking around day 15
        for i in 0..20 {
            let t = jd_start + i as f64 * 2.0;
            let phase = (i as f64 - 7.0) / 5.0;
            let mag = 19.0 - 1.5 * (-phase * phase).exp();
            lc.push(PhotometryMag {
                time: t,
                mag: mag as f32,
                mag_err: 0.05,
                band: Band::G,
            });
        }
        // r-band: 15 points with a dimmer bump
        for i in 0..15 {
            let t = jd_start + i as f64 * 2.5 + 1.0;
            let phase = (i as f64 - 6.0) / 5.0;
            let mag = 19.3 - 1.2 * (-phase * phase).exp();
            lc.push(PhotometryMag {
                time: t,
                mag: mag as f32,
                mag_err: 0.06,
                band: Band::R,
            });
        }
        lc
    }

    #[test]
    fn test_photometry_to_raw_arrays() {
        let lc = make_synthetic_lightcurve();
        let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
        let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
        let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
        let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();

        assert_eq!(times.len(), 35);
        assert_eq!(mags.len(), 35);
        assert_eq!(mag_errs.len(), 35);
        assert_eq!(bands.len(), 35);

        // Check band names are lowercase
        assert!(bands.iter().all(|b| b == "g" || b == "r"));
        assert_eq!(bands.iter().filter(|b| *b == "g").count(), 20);
        assert_eq!(bands.iter().filter(|b| *b == "r").count(), 15);
    }

    #[test]
    fn test_build_mag_bands_from_photometry() {
        let lc = make_synthetic_lightcurve();
        let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
        let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
        let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
        let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();

        let mag_bands = build_mag_bands(&times, &mags, &mag_errs, &bands);

        assert!(mag_bands.contains_key("g"));
        assert!(mag_bands.contains_key("r"));
        assert_eq!(mag_bands["g"].times.len(), 20);
        assert_eq!(mag_bands["r"].times.len(), 15);

        // Times should be relative (starting near 0)
        let g_t_min = mag_bands["g"]
            .times
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        assert!(
            g_t_min < 2.0,
            "g-band times should be relative, got min={g_t_min}"
        );
    }

    #[test]
    fn test_build_mag_bands_empty() {
        let mag_bands = build_mag_bands(&[], &[], &[], &[]);
        assert!(mag_bands.is_empty());
    }

    #[test]
    fn test_nonparametric_fitting_synthetic() {
        let lc = make_synthetic_lightcurve();
        let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
        let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
        let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
        let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();

        let mag_bands = build_mag_bands(&times, &mags, &mag_errs, &bands);
        let (nonparametric, trained_gps) = fit_nonparametric(&mag_bands);

        // Should produce results for both bands
        assert_eq!(nonparametric.len(), 2);

        let band_names: Vec<&str> = nonparametric.iter().map(|r| r.band.as_str()).collect();
        assert!(band_names.contains(&"g"));
        assert!(band_names.contains(&"r"));

        // Peak magnitude should be brighter than 19.0 (our synthetic peak is ~17.5)
        for result in &nonparametric {
            assert!(
                result.peak_mag.is_some(),
                "peak_mag should be Some for {}",
                result.band
            );
            assert!(result.n_obs >= 5);
        }

        // Trained GPs should be returned for reuse by thermal fitting
        assert!(!trained_gps.is_empty());
    }

    #[test]
    fn test_thermal_fitting_synthetic() {
        let lc = make_synthetic_lightcurve();
        let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
        let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
        let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
        let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();

        let mag_bands = build_mag_bands(&times, &mags, &mag_errs, &bands);
        let (_, trained_gps) = fit_nonparametric(&mag_bands);
        let thermal = fit_thermal(&mag_bands, Some(&trained_gps));

        // With g and r bands, thermal fitting should produce a result
        assert!(
            thermal.is_some(),
            "thermal fitting should return Some with g+r bands"
        );
        let thermal = thermal.unwrap();
        assert!(thermal.n_bands_used >= 1);
        assert!(thermal.ref_band == "g" || thermal.ref_band == "r");
    }

    #[test]
    fn test_full_fitting_pipeline_synthetic() {
        let lc = make_synthetic_lightcurve();
        let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
        let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
        let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
        let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();

        let mag_bands = build_mag_bands(&times, &mags, &mag_errs, &bands);
        let (nonparametric, trained_gps) = fit_nonparametric(&mag_bands);
        let thermal = fit_thermal(&mag_bands, Some(&trained_gps));

        let result = LightcurveFittingResult {
            nonparametric,
            parametric: vec![],
            thermal,
        };

        // Nonparametric should have 2 bands
        assert_eq!(result.nonparametric.len(), 2);

        // Result should serialize to JSON (needed for BSON/MongoDB)
        let json = serde_json::to_string(&result)
            .expect("LightcurveFittingResult should serialize to JSON");
        assert!(json.contains("nonparametric"));
        assert!(json.contains("thermal"));

        // Round-trip: deserialize back
        let deserialized: LightcurveFittingResult = serde_json::from_str(&json)
            .expect("LightcurveFittingResult should deserialize from JSON");
        assert_eq!(deserialized.nonparametric.len(), result.nonparametric.len());
    }

    #[test]
    fn test_fitting_empty_lightcurve() {
        let mag_bands = build_mag_bands(&[], &[], &[], &[]);
        let (nonparametric, trained_gps) = fit_nonparametric(&mag_bands);
        let thermal = fit_thermal(&mag_bands, Some(&trained_gps));

        assert!(nonparametric.is_empty());
        assert!(thermal.is_none());
    }

    #[test]
    fn test_fitting_single_band() {
        let mut lc = Vec::new();
        let jd_start = 2460000.0;
        for i in 0..15 {
            let t = jd_start + i as f64 * 3.0;
            let phase = (i as f64 - 5.0) / 4.0;
            let mag = 18.5 - 2.0 * (-phase * phase).exp();
            lc.push(PhotometryMag {
                time: t,
                mag: mag as f32,
                mag_err: 0.04,
                band: Band::G,
            });
        }

        let times: Vec<f64> = lc.iter().map(|p| p.time).collect();
        let mags: Vec<f64> = lc.iter().map(|p| p.mag as f64).collect();
        let mag_errs: Vec<f64> = lc.iter().map(|p| p.mag_err as f64).collect();
        let bands: Vec<String> = lc.iter().map(|p| p.band.to_string()).collect();

        let mag_bands = build_mag_bands(&times, &mags, &mag_errs, &bands);
        let (nonparametric, trained_gps) = fit_nonparametric(&mag_bands);
        let thermal = fit_thermal(&mag_bands, Some(&trained_gps));

        assert_eq!(nonparametric.len(), 1);
        assert_eq!(nonparametric[0].band, "g");

        // Thermal with a single band: either None or a result with no useful temperature
        if let Some(ref t) = thermal {
            assert!(
                t.log_temp_peak.is_none() || t.n_bands_used == 0,
                "thermal with single band should have no temperature estimate"
            );
        }
    }

    #[test]
    fn test_band_display_matches_fitting_expectations() {
        // Verify Band::Display produces the lowercase names the fitting crate expects
        assert_eq!(Band::G.to_string(), "g");
        assert_eq!(Band::R.to_string(), "r");
        assert_eq!(Band::I.to_string(), "i");
        assert_eq!(Band::Z.to_string(), "z");
        assert_eq!(Band::Y.to_string(), "y");
        assert_eq!(Band::U.to_string(), "u");
    }
}
