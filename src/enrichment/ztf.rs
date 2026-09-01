use crate::alert::ZtfCandidate;
use crate::conf::AppConfig;
use crate::enrichment::{
    babamul::{Babamul, BabamulZtfAlert},
    fetch_alerts,
    models::{AcaiModel, BtsBotModel, FusionModel, Model, ModelError, SharedModels},
    EnrichmentWorker, EnrichmentWorkerError, LsstMatch, LsstPhotometry,
};
use crate::utils::cutouts::{AlertCutout, CutoutStorage};
use crate::utils::db::mongify;
use crate::utils::enums::Survey;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, ActivityMetrics, AllBandsProperties, Band,
    PerBandProperties, PhotometryMag, ZTF_ZP,
};
use crate::utils::mpcorb::{elements_from_document, normalize_ztf_ssnamenr, ORBITS_COLLECTION};
use crate::utils::sso_geometry::{geometry_at, OrbitalElements};
use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use serde::{Deserialize, Deserializer};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, instrument, trace, warn};
#[cfg(all(feature = "gpu", target_os = "linux"))]
use villar_pso::gpu::{GpuBatchData, SourceData};
#[cfg(all(feature = "gpu", target_os = "macos"))]
use villar_pso::gpu_metal::{GpuBatchData, SourceData};

#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    #[allow(dead_code)]
    #[serde(rename = "snr", default, skip_serializing)]
    pub snr_legacy: Option<f64>,
    pub programid: i32,
}

#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    #[allow(dead_code)]
    #[serde(rename = "snr", default, skip_serializing)]
    pub snr_legacy: Option<f64>,
    pub programid: i32,
    pub procstatus: Option<String>,
}

#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

        let flux = if phot.flux != Some(-99999.0) && phot.flux.is_some_and(|f| !f.is_nan()) {
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

fn convert_photometry<T>(points: Option<Vec<T>>, kind: &str) -> Vec<ZtfPhotometry>
where
    T: TryInto<ZtfPhotometry, Error = EnrichmentWorkerError>,
{
    points
        .unwrap_or_default()
        .into_iter()
        .filter_map(|p| {
            p.try_into()
                .map_err(|e| {
                    if matches!(e, EnrichmentWorkerError::BadProcstatus(_)) {
                        trace!("Failed to convert {} to ZtfPhotometry: {}", kind, e);
                    } else {
                        warn!("Failed to convert {} to ZtfPhotometry: {}", kind, e);
                    }
                })
                .ok()
        })
        .collect()
}

pub fn deserialize_ztf_alert_lightcurve<'de, D>(
    deserializer: D,
) -> Result<Vec<ZtfPhotometry>, D::Error>
where
    D: Deserializer<'de>,
{
    let points = <Option<Vec<ZtfAlertPhotometry>> as Deserialize>::deserialize(deserializer)?;
    Ok(convert_photometry(points, "ZtfAlertPhotometry"))
}

pub fn deserialize_ztf_forced_lightcurve<'de, D>(
    deserializer: D,
) -> Result<Vec<ZtfPhotometry>, D::Error>
where
    D: Deserializer<'de>,
{
    let points = <Option<Vec<ZtfForcedPhotometry>> as Deserialize>::deserialize(deserializer)?;
    Ok(convert_photometry(points, "ZtfForcedPhotometry"))
}

impl ZtfPhotometry {
    pub fn to_photometry_mag(&self, min_snr: Option<f64>) -> Option<PhotometryMag> {
        let (Some(snr), Some(mag), Some(sig)) = (self.snr_psf, self.magpsf, self.sigmapsf) else {
            return None;
        };
        if min_snr.is_some_and(|thresh| snr.abs() < thresh) {
            return None;
        }
        Some(PhotometryMag {
            time: self.jd,
            mag: mag as f32,
            mag_err: sig as f32,
            band: self.band.clone(),
        })
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
        if let Some(project) = pipeline
            .last_mut()
            .and_then(|stage| stage.get_document_mut("$project").ok())
        {
            project.insert("classifications", 1);
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

/// Solar system association for one ZTF detection. Group light curves on
/// `designation`, never on the positional `objectId`.
#[derive(
    Debug, Clone, Default, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema,
)]
#[serde(default)]
pub struct ZtfSsoAssociation {
    /// Known solar system object identified here. Not thresholded on separation.
    pub is_sso: bool,
    /// MPC designation (ZTF `ssnamenr`), e.g. `"9816"`.
    pub designation: Option<String>,
    /// Arcseconds to the predicted position (ZTF `ssdistnr`); `None` if unmatched.
    pub separation_arcsec: Option<f32>,
    /// Catalogued magnitude predicted for the object (ZTF `ssmagnr`).
    pub predicted_mag: Option<f32>,
    /// Who made the association, `"ipac"` for the one carried in the alert.
    pub source: Option<String>,
    /// Sun-to-object distance at the alert epoch, au. `None` if absent from `MPC_orbits`.
    #[serde(default)]
    pub helio_dist: Option<f32>,
    /// Observer-to-object distance at the alert epoch, au.
    #[serde(default)]
    pub topo_dist: Option<f32>,
    /// Sun-object-observer angle at the alert epoch, degrees.
    #[serde(default)]
    pub phase_angle: Option<f32>,
}

impl ZtfSsoAssociation {
    /// Negative upstream values are "no match" sentinels, normalised to `None`.
    pub fn from_ipac(
        designation: Option<String>,
        ssdistnr: Option<f32>,
        ssmagnr: Option<f32>,
    ) -> Self {
        let is_sso = designation.is_some();
        ZtfSsoAssociation {
            is_sso,
            source: is_sso.then(|| "ipac".to_string()),
            designation,
            separation_arcsec: ssdistnr.filter(|d| *d >= 0.0),
            predicted_mag: ssmagnr.filter(|m| *m >= 0.0),
            helio_dist: None,
            topo_dist: None,
            phase_angle: None,
        }
    }

    pub fn with_geometry(mut self, elements: Option<&OrbitalElements>, jd: f64) -> Self {
        if let Some(elements) = elements {
            let geometry = geometry_at(elements, jd);
            self.helio_dist = Some(geometry.helio_dist as f32);
            self.topo_dist = Some(geometry.topo_dist as f32);
            self.phase_angle = Some(geometry.phase_angle as f32);
        }
        self
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct ZtfAlertProperties {
    /// Deprecated, thresholded at a hardcoded 12". Prefer `sso.is_sso`.
    pub rock: bool,
    pub star: bool,
    pub near_brightstar: bool,
    pub stationary: bool,
    pub photstats: PerBandProperties,
    pub multisurvey_photstats: Option<PerBandProperties>,
    /// `None` means never evaluated, not "not an asteroid".
    #[serde(default)]
    pub sso: Option<ZtfSsoAssociation>,
    #[serde(default)]
    pub activity: Option<ActivityMetrics>,
}

/// Field order matches the ONNX output index (0-7).
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
    #[serde(skip_serializing)]
    pub fusion_embedding: Option<Vec<f32>>,
}

const CIDER_MAX_LC_SPAN_DAYS: f64 = 100.0;
const CIDER_MIN_PHOTOMETRY_POINTS: usize = 2;

const ACAI_BTS_NAMES: [&str; 6] = ["acai_h", "acai_n", "acai_v", "acai_o", "acai_b", "btsbot"];

struct AlertWork {
    candid: i64,
    programid: i32,
    properties: ZtfAlertProperties,
    all_bands_properties: AllBandsProperties,
    cutouts: AlertCutout,
    alert: ZtfAlertForEnrichment,
    ztf_lightcurve: Vec<PhotometryMag>,
}

impl AlertWork {
    fn cider_eligible(&self) -> bool {
        let lc = &self.ztf_lightcurve;
        lc.len() >= CIDER_MIN_PHOTOMETRY_POINTS
            && lc
                .last()
                .and_then(|last| lc.first().map(|first| last.time - first.time))
                .unwrap_or(f64::MAX)
                <= CIDER_MAX_LC_SPAN_DAYS
    }
}

pub struct ZtfEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    mpc_orbits: mongodb::Collection<Document>,
    alert_cutout_storage: CutoutStorage,
    alert_pipeline: Vec<Document>,
    models: Option<Arc<SharedModels>>,
    babamul: Option<Babamul>,
    gpu_enabled: bool,
    batch_size: usize,
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
        let mpc_orbits = db.collection(ORBITS_COLLECTION);
        let alert_cutout_storage = config.build_cutout_storage(&Survey::Ztf).await?;

        let input_queue = "ZTF_alerts_enrichment_queue".to_string();
        let output_queue = "ZTF_alerts_filter_queue".to_string();

        let babamul: Option<Babamul> = if config.babamul.enabled {
            Some(Babamul::new(&config))
        } else {
            None
        };

        let models = match shared_models {
            Some(m) => Some(m),
            None => Some(SharedModels::load(None)?),
        };

        let batch_size = config
            .workers
            .get(&Survey::Ztf)
            .ok_or(EnrichmentWorkerError::WorkerConfigMissing(Survey::Ztf))?
            .enrichment
            .batch_size;

        Ok(ZtfEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            mpc_orbits,
            alert_cutout_storage,
            alert_pipeline: create_ztf_alert_pipeline(false),
            models,
            babamul,
            gpu_enabled: config.gpu.enabled,
            batch_size,
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
            fetch_alerts(candids, &self.alert_pipeline, &self.alert_collection).await?;

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

        let orbits = self.fetch_orbits(&alerts).await;

        let batch_size = alerts.len();
        let mut skipped_empty_lightcurve = 0usize;
        let mut work_items: Vec<AlertWork> = Vec::with_capacity(alerts.len());
        #[cfg(feature = "gpu")]
        let mut villar_inputs: Vec<(i64, Vec<PhotometryMag>)> = Vec::new();
        #[cfg(feature = "gpu")]
        let villar_enabled = self.models.as_ref().is_some_and(|m| m.gpu_ctx.is_some());
        for alert in alerts {
            let candid = alert.candid;
            let cutouts = candid_to_cutouts
                .remove(&candid)
                .ok_or_else(|| EnrichmentWorkerError::MissingCutouts(candid))?;
            #[cfg_attr(not(feature = "gpu"), allow(unused_variables))]
            let (
                properties,
                all_bands_properties,
                programid,
                multisurvey_lightcurve,
                ztf_lightcurve,
            ) = match self.get_alert_properties(&alert, &orbits).await {
                Ok(v) => v,
                // Skipping keeps the queue draining; the batch total is warned below.
                Err(EnrichmentWorkerError::EmptyLightcurve(_)) => {
                    skipped_empty_lightcurve += 1;
                    debug!(candid, "skipping alert: empty lightcurve after filtering");
                    continue;
                }
                Err(e) => return Err(e),
            };
            #[cfg(feature = "gpu")]
            if villar_enabled {
                villar_inputs.push((candid, multisurvey_lightcurve));
            }

            work_items.push(AlertWork {
                candid,
                programid,
                properties,
                all_bands_properties,
                cutouts,
                alert,
                ztf_lightcurve,
            });
        }

        if skipped_empty_lightcurve > 0 {
            warn!(
                skipped = skipped_empty_lightcurve,
                enriched = work_items.len(),
                batch_size,
                "skipped alerts with empty lightcurves during enrichment"
            );
        }

        let classifications_list: Vec<Option<ZtfAlertClassifications>> =
            if let Some(ref models) = self.models {
                self.classify(models, &work_items)?
            } else {
                vec![None; work_items.len()]
            };

        for (item, classifications) in work_items.into_iter().zip(classifications_list) {
            let mut set_doc = doc! {
                "properties": mongify(&item.properties),
                "updated_at": now,
            };
            if let Some(cls) = &classifications {
                set_doc.insert("classifications", mongify(cls));
            }

            updates.push(WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(doc! {"_id": item.candid})
                    .update(doc! { "$set": set_doc })
                    .build(),
            ));
            processed_alerts.push(format!("{},{}", item.programid, item.candid));

            if self.babamul.is_some() {
                enriched_alerts.push(BabamulZtfAlert::from_alert_and_properties(
                    item.alert,
                    item.properties,
                ));
            }
        }

        // bulk_write rejects an empty operation list.
        if !updates.is_empty() {
            self.client.bulk_write(updates).await?;
        }

        #[cfg(feature = "gpu")]
        if let Some(gpu_ctx) = self.models.as_ref().and_then(|m| m.gpu_ctx.as_ref()) {
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

            if !fittable.is_empty() {
                let (candids, sources): (Vec<i64>, Vec<SourceData>) = fittable.into_iter().unzip();
                let source_refs: Vec<&SourceData> = sources.iter().collect();
                let pso_config = villar_pso::PsoConfig::default();

                let batch_result = GpuBatchData::new(gpu_ctx, &source_refs);

                match batch_result.and_then(|batch| {
                    gpu_ctx.batch_pso_multi_seed(&batch, &source_refs, &pso_config)
                }) {
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

        if let Some(babamul) = self.babamul.as_ref() {
            babamul.process_ztf_alerts(enriched_alerts).await?;
        }

        Ok(processed_alerts)
    }
}

impl ZtfEnrichmentWorker {
    /// Keyed by `ssnamenr` as the alert carries it, not by the MPCORB key.
    async fn fetch_orbits(
        &self,
        alerts: &[ZtfAlertForEnrichment],
    ) -> HashMap<String, OrbitalElements> {
        let key_by_name: HashMap<&str, String> = alerts
            .iter()
            .filter_map(|a| a.candidate.candidate.ssnamenr.as_deref())
            .collect::<HashSet<_>>()
            .into_iter()
            .filter_map(|name| normalize_ztf_ssnamenr(name).map(|key| (name, key)))
            .collect();

        if key_by_name.is_empty() {
            return HashMap::new();
        }

        // Several names reduce to the same key, so query the distinct keys.
        let keys: Vec<&String> = key_by_name
            .values()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let cursor = match self.mpc_orbits.find(doc! { "_id": { "$in": &keys } }).await {
            Ok(cursor) => cursor,
            Err(e) => {
                warn!("failed to query {}: {}", ORBITS_COLLECTION, e);
                return HashMap::new();
            }
        };

        let docs: Vec<Document> = match cursor.try_collect().await {
            Ok(docs) => docs,
            Err(e) => {
                warn!("failed to read {}: {}", ORBITS_COLLECTION, e);
                return HashMap::new();
            }
        };

        let by_key: HashMap<&str, OrbitalElements> = docs
            .iter()
            .filter_map(|doc| Some((doc.get_str("_id").ok()?, elements_from_document(doc)?)))
            .collect();

        if by_key.is_empty() {
            warn!(
                "no elements found in {} for any of {} objects in this batch",
                ORBITS_COLLECTION,
                keys.len()
            );
        }

        key_by_name
            .into_iter()
            .filter_map(|(name, key)| Some((name.to_string(), *by_key.get(key.as_str())?)))
            .collect()
    }

    async fn get_alert_properties(
        &self,
        alert: &ZtfAlertForEnrichment,
        orbits: &HashMap<String, OrbitalElements>,
    ) -> Result<
        (
            ZtfAlertProperties,
            AllBandsProperties,
            i32,
            Vec<PhotometryMag>,
            Vec<PhotometryMag>,
        ),
        EnrichmentWorkerError,
    > {
        let candidate = &alert.candidate.candidate;
        let programid = candidate.programid;
        let ssdistnr = candidate.ssdistnr.unwrap_or(f32::INFINITY);
        let ssmagnr = candidate.ssmagnr.unwrap_or(f32::INFINITY);
        let is_rock = ssdistnr >= 0.0 && ssdistnr < 12.0 && ssmagnr >= 0.0;

        let activity = ActivityMetrics::from_magnitudes(Some(candidate.magpsf), candidate.magap);

        let elements = candidate
            .ssnamenr
            .as_deref()
            .and_then(|name| orbits.get(name));

        let sso = ZtfSsoAssociation::from_ipac(
            candidate.ssnamenr.clone(),
            candidate.ssdistnr,
            candidate.ssmagnr,
        )
        .with_geometry(elements, candidate.jd);

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

        let ztf_mags = |points: &[ZtfPhotometry], min_snr| -> Vec<PhotometryMag> {
            points
                .iter()
                .filter(|p| p.jd <= candidate.jd)
                .filter_map(|p| p.to_photometry_mag(min_snr))
                .collect()
        };

        let mut lightcurve = [
            ztf_mags(&alert.prv_candidates, None),
            ztf_mags(&alert.fp_hists, Some(3.0)),
        ]
        .concat();

        prepare_photometry(&mut lightcurve);

        if lightcurve.is_empty() {
            return Err(EnrichmentWorkerError::EmptyLightcurve(alert.candid));
        }
        let (photstats, all_bands_properties, stationary) = analyze_photometry(&lightcurve);
        // cider was trained on ZTF only, so snapshot before the cross-survey extend.
        let ztf_lightcurve = lightcurve.clone();

        let mut has_matches = false;
        if let Some(survey_matches) = &alert.survey_matches {
            if let Some(lsst_match) = &survey_matches.lsst {
                let lsst_mags = |points: &[LsstPhotometry], min_snr| -> Vec<PhotometryMag> {
                    points
                        .iter()
                        .filter(|p| p.jd <= candidate.jd)
                        .filter_map(|p| p.to_photometry_mag(min_snr))
                        .collect()
                };
                let mut lsst_lightcurve = [
                    lsst_mags(&lsst_match.prv_candidates, None),
                    lsst_mags(&lsst_match.fp_hists, Some(3.0)),
                ]
                .concat();
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
                sso: Some(sso),
                activity: Some(activity),
            },
            all_bands_properties,
            programid,
            lightcurve,
            ztf_lightcurve,
        ))
    }

    fn predict_acai_btsbot(
        models: &SharedModels,
        metadata: &ndarray::Array2<f32>,
        btsbot_metadata: &ndarray::Array2<f32>,
        triplet: &ndarray::Array4<f32>,
    ) -> Result<[Vec<f32>; 6], ModelError> {
        Ok([
            models.acai_h.lock().unwrap().predict(metadata, triplet)?,
            models.acai_n.lock().unwrap().predict(metadata, triplet)?,
            models.acai_v.lock().unwrap().predict(metadata, triplet)?,
            models.acai_o.lock().unwrap().predict(metadata, triplet)?,
            models.acai_b.lock().unwrap().predict(metadata, triplet)?,
            models
                .btsbot
                .lock()
                .unwrap()
                .predict(btsbot_metadata, triplet)?,
        ])
    }

    fn classify(
        &self,
        models: &SharedModels,
        work_items: &[AlertWork],
    ) -> Result<Vec<Option<ZtfAlertClassifications>>, EnrichmentWorkerError> {
        if self.gpu_enabled {
            return self.classify_gpu_batch(models, work_items);
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
            let btsbot_metadata_result = BtsBotModel::get_metadata(
                &[&item.alert],
                std::slice::from_ref(&item.all_bands_properties),
            );

            let cls = if let (Ok(metadata), Ok(btsbot_metadata)) =
                (metadata_result, btsbot_metadata_result)
            {
                let [acai_h, acai_n, acai_v, acai_o, acai_b, btsbot] =
                    Self::predict_acai_btsbot(models, &metadata, &btsbot_metadata, &triplet)?;

                let cider_result = if item.cider_eligible() {
                    (|| -> Result<(CiderClassProbs, Vec<f32>), ModelError> {
                        let mut m = models.cider.lock().unwrap();
                        let meta = m.get_metadata(&[&item.alert], &[&item.all_bands_properties])?;
                        let img = m.get_triplet(&[&item.cutouts])?;
                        let (tx, tpm, tg) = m.photometry_inputs(item.ztf_lightcurve.clone())?;
                        let (probs, embedding) = m.predict(&tx, &tpm, &tg, &meta, &img)?;
                        let cls = CiderClassProbs::from_probs(&probs).ok_or(
                            ModelError::MissingFeature("cider: unexpected output length"),
                        )?;
                        Ok((cls, embedding))
                    })()
                    .map_err(|e| {
                        warn!("cider inference failed for candid {}: {}", item.candid, e);
                    })
                    .ok()
                } else {
                    None
                };

                Some(ZtfAlertClassifications {
                    acai_h: acai_h[0],
                    acai_n: acai_n[0],
                    acai_v: acai_v[0],
                    acai_o: acai_o[0],
                    acai_b: acai_b[0],
                    btsbot: btsbot[0],
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

    fn classify_gpu_batch(
        &self,
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

        let row_of = |indices: &[usize]| -> HashMap<usize, usize> {
            indices
                .iter()
                .enumerate()
                .map(|(pos, i)| (*i, pos))
                .collect()
        };
        let triplet_pos = row_of(&triplet_indices);
        let acai_pos = row_of(&acai_indices);
        let bts_pos = row_of(&bts_indices);

        let selected_indices: Vec<usize> = (0..work_items.len())
            .filter(|idx| {
                let complete = triplet_pos.contains_key(idx)
                    && acai_pos.contains_key(idx)
                    && bts_pos.contains_key(idx);
                if !complete {
                    warn!(
                        "Skipping ML inference for candid {} due to missing features",
                        work_items[*idx].candid
                    );
                }
                complete
            })
            .collect();

        if selected_indices.is_empty() {
            return Ok(results);
        }

        let cider_indices: Vec<usize> = selected_indices
            .iter()
            .copied()
            .filter(|&i| work_items[i].cider_eligible())
            .collect();
        let cider_pos = row_of(&cider_indices);
        let cider_batch: Option<(Vec<f32>, Vec<f32>)> = (!cider_indices.is_empty())
            .then(|| -> Result<(Vec<f32>, Vec<f32>), ModelError> {
                let cider_alerts: Vec<&ZtfAlertForEnrichment> = cider_indices
                    .iter()
                    .map(|&i| &work_items[i].alert)
                    .collect();
                let cider_cutouts: Vec<&AlertCutout> = cider_indices
                    .iter()
                    .map(|&i| &work_items[i].cutouts)
                    .collect();
                let cider_props: Vec<&AllBandsProperties> = cider_indices
                    .iter()
                    .map(|&i| &work_items[i].all_bands_properties)
                    .collect();

                let mut cider = models.cider.lock().unwrap();
                let cider_meta = cider.get_metadata(&cider_alerts, &cider_props)?;
                let cider_image = cider.get_triplet(&cider_cutouts)?;

                let phot: Vec<_> = cider_indices
                    .iter()
                    .map(|&i| cider.photometry_inputs(work_items[i].ztf_lightcurve.clone()))
                    .collect::<Result<Vec<_>, _>>()?;

                let tx_views: Vec<_> = phot.iter().map(|(x, _, _)| x.view()).collect();
                let tpm_views: Vec<_> = phot.iter().map(|(_, m, _)| m.view()).collect();
                let tg_views: Vec<_> = phot.iter().map(|(_, _, g)| g.view()).collect();

                let tx = ndarray::concatenate(ndarray::Axis(0), &tx_views)?;
                let tpm = ndarray::concatenate(ndarray::Axis(0), &tpm_views)?;
                let tg = ndarray::concatenate(ndarray::Axis(0), &tg_views)?;

                cider.predict(&tx, &tpm, &tg, &cider_meta, &cider_image)
            })
            .and_then(|r| {
                r.map_err(|e| {
                    warn!("cider batch inference failed: {}", e);
                })
                .ok()
            });

        let cider_n_cls = cider_batch
            .as_ref()
            .map(|(p, _)| p.len() / cider_indices.len())
            .unwrap_or(0);
        let cider_emb_dim = cider_batch
            .as_ref()
            .map(|(_, e)| e.len() / cider_indices.len())
            .unwrap_or(0);

        // ORT needs one fixed input shape, so pad the last chunk and drop the pad rows.
        for chunk in selected_indices.chunks(self.batch_size) {
            let mut triplet = ndarray::Array::zeros((self.batch_size, 63, 63, 3));
            let mut metadata = ndarray::Array::zeros((self.batch_size, 25));
            let mut btsbot_metadata = ndarray::Array::zeros((self.batch_size, 25));

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

            let scores = Self::predict_acai_btsbot(models, &metadata, &btsbot_metadata, &triplet)?;
            for (name, got) in ACAI_BTS_NAMES.iter().zip(scores.iter().map(Vec::len)) {
                if got != self.batch_size {
                    return Err(EnrichmentWorkerError::ConfigurationError(format!(
                        "model {} returned {} scores for {} padded inputs",
                        name, got, self.batch_size
                    )));
                }
            }
            let [acai_h, acai_n, acai_v, acai_o, acai_b, btsbot] = scores;

            for (batch_idx, &item_idx) in chunk.iter().enumerate() {
                let cider = cider_pos.get(&item_idx).copied().zip(cider_batch.as_ref());
                results[item_idx] = Some(ZtfAlertClassifications {
                    acai_h: acai_h[batch_idx],
                    acai_n: acai_n[batch_idx],
                    acai_v: acai_v[batch_idx],
                    acai_o: acai_o[batch_idx],
                    acai_b: acai_b[batch_idx],
                    btsbot: btsbot[batch_idx],
                    cider_fusion: cider.and_then(|(row, (probs, _))| {
                        CiderClassProbs::from_probs(
                            &probs[row * cider_n_cls..(row + 1) * cider_n_cls],
                        )
                    }),
                    fusion_embedding: cider.map(|(row, (_, emb))| {
                        emb[row * cider_emb_dim..(row + 1) * cider_emb_dim].to_vec()
                    }),
                });
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sso_association_populated_when_identified() {
        let sso = ZtfSsoAssociation::from_ipac(Some("9816".to_string()), Some(1.0), Some(18.1));
        assert!(sso.is_sso);
        assert_eq!(sso.designation.as_deref(), Some("9816"));
        assert_eq!(sso.separation_arcsec, Some(1.0));
        assert_eq!(sso.predicted_mag, Some(18.1));
        assert_eq!(sso.source.as_deref(), Some("ipac"));
    }

    #[test]
    fn test_sso_association_absent_when_unidentified() {
        let sso = ZtfSsoAssociation::from_ipac(None, None, None);
        assert!(!sso.is_sso);
        assert!(sso.designation.is_none());
        assert!(sso.separation_arcsec.is_none());
        assert!(
            sso.source.is_none(),
            "source is only set when a match was made"
        );
    }

    fn ceres() -> OrbitalElements {
        OrbitalElements {
            epoch_jd: 2_461_200.5,
            a: 2.7655526,
            e: 0.0796923,
            incl: 10.58803,
            node: 80.24863,
            peri: 73.29420,
            mean_anomaly: 274.41935,
        }
    }

    // Tolerances are loose because of f32 storage, not the propagation.
    #[test]
    fn test_geometry_populated_when_elements_are_available() {
        let sso = ZtfSsoAssociation::from_ipac(Some("1".to_string()), Some(0.4), Some(9.2))
            .with_geometry(Some(&ceres()), 2_461_272.5);

        let helio = sso.helio_dist.expect("heliocentric distance");
        let topo = sso.topo_dist.expect("topocentric distance");
        let phase = sso.phase_angle.expect("phase angle");
        assert!(
            (helio - 2.706853).abs() < 1e-3,
            "heliocentric distance was {helio}"
        );
        assert!(
            (topo - 3.168905).abs() < 1e-3,
            "topocentric distance was {topo}"
        );
        assert!((phase - 17.6824).abs() < 0.01, "phase angle was {phase}");
    }

    #[test]
    fn test_geometry_absent_when_elements_are_missing() {
        let sso = ZtfSsoAssociation::from_ipac(Some("9816".to_string()), Some(1.0), Some(18.1))
            .with_geometry(None, 2_461_272.5);
        assert!(sso.is_sso, "the association itself still stands");
        assert!(sso.helio_dist.is_none());
        assert!(sso.topo_dist.is_none());
        assert!(sso.phase_angle.is_none());
    }

    #[test]
    fn test_ipac_designations_resolve_to_orbit_keys() {
        let by_key = HashMap::from([("1", ceres())]);
        let orbits: HashMap<String, OrbitalElements> = ["1", "(1)Ceres", "C/2026O1"]
            .into_iter()
            .filter_map(|name| normalize_ztf_ssnamenr(name).map(|key| (name, key)))
            .filter_map(|(name, key)| Some((name.to_string(), *by_key.get(key.as_str())?)))
            .collect();

        for ssnamenr in ["1", "(1)Ceres"] {
            assert!(
                orbits.contains_key(ssnamenr),
                "ssnamenr {ssnamenr} did not resolve to an orbit"
            );
        }
        assert!(!orbits.contains_key("C/2026O1"));
        assert!(normalize_ztf_ssnamenr("C/2026O1").is_none());
    }

    #[test]
    fn test_negative_sentinels_are_normalised_to_none() {
        let sso = ZtfSsoAssociation::from_ipac(None, Some(-999.0), Some(-999.0));
        assert!(sso.separation_arcsec.is_none());
        assert!(sso.predicted_mag.is_none());
    }

    #[test]
    fn test_properties_without_sso_still_deserialize() {
        let legacy = serde_json::json!({
            "rock": false,
            "star": false,
            "near_brightstar": false,
            "stationary": true,
            "photstats": PerBandProperties::default(),
            "multisurvey_photstats": null,
        });

        let props: ZtfAlertProperties =
            serde_json::from_value(legacy).expect("legacy properties must still deserialize");
        assert!(
            props.sso.is_none(),
            "absent means never evaluated, not evaluated-and-negative"
        );
    }

    #[test]
    fn test_partial_sso_block_deserializes() {
        let sso: ZtfSsoAssociation =
            serde_json::from_value(serde_json::json!({"designation": "9816"}))
                .expect("partial sso block must deserialize");
        assert_eq!(sso.designation.as_deref(), Some("9816"));
        assert!(!sso.is_sso);
        assert!(sso.separation_arcsec.is_none());
    }

    // Regression guard: `rock` is thresholded at 12", `is_sso` must not be.
    #[test]
    fn test_is_sso_is_not_thresholded_on_separation() {
        let far = ZtfSsoAssociation::from_ipac(Some("407033".to_string()), Some(18.0), Some(21.6));
        let rock = 18.0f32 >= 0.0 && 18.0f32 < 12.0 && 21.6f32 >= 0.0;

        assert!(!rock, "the deprecated rock flag drops this object");
        assert!(
            far.is_sso,
            "but it is still an identified solar system object"
        );
        assert_eq!(far.separation_arcsec, Some(18.0));
        assert_eq!(
            far.designation.as_deref(),
            Some("407033"),
            "the grouping key survives, which is what downstream light curves need"
        );
    }
}
