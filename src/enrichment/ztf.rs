use crate::alert::{Candidate, ZtfCandidate};
use crate::conf::AppConfig;
use crate::enrichment::{
    babamul::{Babamul, BabamulZtfAlert},
    fetch_alerts,
    models::{AcaiModel, BtsBotModel, Model, SharedModels},
    EnrichmentWorker, EnrichmentWorkerError, LsstMatch,
};
use crate::utils::cutouts::{AlertCutout, CutoutStorage};
use crate::utils::db::mongify;
use crate::utils::enums::Survey;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, ActivityMetrics, AllBandsProperties, Band, Outburst,
    PerBandProperties, PhotometryMag, ZTF_ZP,
};
use crate::utils::mpcorb::{elements_from_document, normalize_ztf_ssnamenr, ORBITS_COLLECTION};
use crate::utils::outburst::{Point, MAX_SEPARATION_ARCSEC};
use crate::utils::phase_curve::{curves_from_document, PhaseCurve, BASELINES_COLLECTION};
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
    /// Legacy fallback for documents that pre-date the snr migration.
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
    /// Legacy fallback for documents that pre-date the snr migration.
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

        let flux = phot
            .flux
            .filter(|f| *f != -99999.0 && !f.is_nan())
            .map(|f| f * 1e9_f64 * zp_scaling_factor); // convert to a fixed ZP and nJy
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

impl ZtfPhotometry {
    /// `min_snr` of `None` applies no SNR cut.
    pub fn to_photometry_mag(&self, min_snr: Option<f64>) -> Option<PhotometryMag> {
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
        if let Some(project_stage) = pipeline.last_mut() {
            if let Ok(project_doc) = project_stage.get_document_mut("$project") {
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

/// Longest window `Outburst` scores over, so also how far back history is read.
const HISTORY_WINDOW_DAYS: f64 = 30.0;

/// One historical detection as the outburst statistic needs it, or `None` when
/// the document is missing photometry or geometry.
fn history_point(doc: &Document) -> Option<(String, f64, Point)> {
    let candidate = doc.get_document("candidate").ok()?;
    let sso = doc
        .get_document("properties")
        .ok()?
        .get_document("sso")
        .ok()?;
    let number = |d: &Document, key: &str| d.get(key).and_then(crate::utils::bson_number);
    Some((
        candidate.get_str("ssnamenr").ok()?.to_string(),
        number(candidate, "jd")?,
        Point {
            rh: number(sso, "helio_dist")?,
            delta: number(sso, "topo_dist")?,
            phase: number(sso, "phase_angle")?,
            mag: number(candidate, "magpsf")?,
            mag_err: number(candidate, "sigmapsf")?,
            band: number(candidate, "fid")? as u8,
        },
    ))
}

/// Score this detection against the object's own earlier photometry.
///
/// `None` unless the alert is a mover with geometry and at least one earlier
/// detection that also has geometry. Nearly every mover is seen more than once
/// in a month, so the limiting factor is geometry on the earlier detection.
fn outburst_for(
    sso: &ZtfSsoAssociation,
    candidate: &Candidate,
    sso_history: &HashMap<String, Vec<(f64, Point)>>,
    baselines: &HashMap<String, HashMap<u8, PhaseCurve>>,
) -> Option<Outburst> {
    // A detection away from the object's position is not a measurement of it.
    let separation = sso.separation_arcsec? as f64;
    if separation >= MAX_SEPARATION_ARCSEC {
        return None;
    }
    let test = Point {
        rh: sso.helio_dist? as f64,
        delta: sso.topo_dist? as f64,
        phase: sso.phase_angle? as f64,
        mag: candidate.magpsf as f64,
        mag_err: candidate.sigmapsf as f64,
        band: candidate.fid as u8,
    };
    // A redelivery is already stored, and comparing a point to itself is a zero.
    let designation = sso.designation.as_deref()?;
    let history: Vec<(f64, Point)> = sso_history
        .get(designation)
        .map(|points| {
            points
                .iter()
                .filter(|(jd, _)| *jd < candidate.jd)
                .copied()
                .collect()
        })
        .unwrap_or_default();
    let curve = baselines.get(designation).and_then(|b| b.get(&test.band));
    Outburst::from_history(&history, test, candidate.jd, curve)
}

/// Solar system object association for a single ZTF detection.
///
/// ZTF `objectId`s are positional, so a moving object is given a new one on very
/// nearly every detection (measured over a week of alerts: 1.03 detections per
/// `objectId`). `designation` is therefore the only stable key across a moving
/// object's detections — downstream consumers building light curves must group on
/// it, never on `objectId`. For the same reason the alert's `prv_candidates` and
/// `fp_hists` describe whatever else has occupied that sky position, not this
/// object.
#[derive(
    Debug, Clone, Default, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema,
)]
#[serde(default)]
pub struct ZtfSsoAssociation {
    /// Whether a known solar system object was identified at this position.
    ///
    /// Deliberately not thresholded on separation, unlike the deprecated `rock`
    /// flag: when the upstream ephemeris degrades, that shows up as a growing
    /// `separation_arcsec` the consumer can see, rather than silently flipping a
    /// boolean they cannot.
    pub is_sso: bool,
    /// MPC designation of the matched object (ZTF `ssnamenr`), e.g. `"9816"`.
    pub designation: Option<String>,
    /// Separation between the detection and the object's predicted position
    /// (ZTF `ssdistnr`), in arcseconds. Negative upstream sentinels are stored as
    /// `None`. This is a quality indicator for the upstream ephemeris, and worth
    /// monitoring in aggregate: a drifting distribution means stale orbits.
    pub separation_arcsec: Option<f32>,
    /// Catalogued magnitude predicted for the object (ZTF `ssmagnr`). Compared
    /// against the measured `magpsf` this gives predicted-minus-measured per
    /// detection at no extra cost.
    pub predicted_mag: Option<f32>,
    /// Who made the association. `"ipac"` for the identification carried in the
    /// ZTF alert itself; an independent association computed by BOOM would
    /// identify itself differently here.
    pub source: Option<String>,
    /// Sun-to-object distance at the alert epoch, au. Derived by propagating MPC
    /// elements, since the ZTF packet carries no state vectors, so it is `None`
    /// whenever the object is missing from `MPC_orbits`.
    ///
    /// Named to match the LSST association, which reads the same quantity straight
    /// from its own `ssSource` vectors, so a filter is identical on both surveys.
    #[serde(default)]
    pub helio_dist: Option<f32>,
    /// Observer-to-object distance at the alert epoch, au. Same provenance.
    #[serde(default)]
    pub topo_dist: Option<f32>,
    /// Sun-object-observer angle at the alert epoch, degrees. Same provenance.
    #[serde(default)]
    pub phase_angle: Option<f32>,
}

impl ZtfSsoAssociation {
    /// Build the association from the solar system fields IPAC puts in the ZTF
    /// alert. Negative values are upstream "no match" sentinels rather than
    /// measurements, so they are normalised to `None`.
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

    /// Fill in observing geometry from MPC elements propagated to `jd`.
    ///
    /// Left untouched when the object has no elements available: an absent
    /// geometry is reported as absent rather than as a default, since a
    /// plausible-looking wrong distance is worse here than a missing one.
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

/// ZTF alert properties computed during enrichment and inserted back into the alert document
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct ZtfAlertProperties {
    /// Deprecated alias for `sso.is_sso`, retained so existing filters keep
    /// working. Unlike `sso.is_sso` this is thresholded at a hardcoded 12", so it
    /// silently loses objects as the upstream ephemeris degrades. Prefer
    /// `sso.is_sso`, optionally with an explicit `sso.separation_arcsec` cut.
    pub rock: bool,
    pub star: bool,
    pub near_brightstar: bool,
    pub stationary: bool,
    pub photstats: PerBandProperties,
    pub multisurvey_photstats: Option<PerBandProperties>,
    /// `None` on alerts enriched before this field existed — those were never
    /// evaluated for a solar system association, which is different from having
    /// been evaluated and found not to be one (`Some` with `is_sso: false`).
    /// Consumers must not read `None` as "not an asteroid".
    #[serde(default)]
    pub sso: Option<ZtfSsoAssociation>,
    /// `None` on alerts enriched before this existed.
    #[serde(default)]
    pub activity: Option<ActivityMetrics>,
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
    cutouts: AlertCutout,
    alert: ZtfAlertForEnrichment,
}

pub struct ZtfEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    /// MPC orbital elements, refreshed nightly by `mpcorb_ingest`.
    mpc_orbits: mongodb::Collection<Document>,
    /// Fitted per-object phase curves, rebuilt by `sso_baselines`.
    sso_baselines: mongodb::Collection<Document>,
    alert_cutout_storage: CutoutStorage,
    alert_pipeline: Vec<Document>,
    /// Shared ONNX models (loaded once, shared across all enrichment workers
    /// via Arc). On Linux+`gpu` this also owns the per-device CUDA stream and
    /// villar-pso `GpuContext` so that PSO and ONNX inference share a stream.
    models: Arc<SharedModels>,
    babamul: Option<Babamul>,
    gpu_enabled: bool,
    /// Alerts per batch — also the fixed ONNX inference shape (see
    /// [`EnrichmentWorkerConfig::batch_size`] in `conf.rs`).
    batch_size: usize,
}

fn position_index(indices: &[usize]) -> HashMap<usize, usize> {
    indices
        .iter()
        .enumerate()
        .map(|(pos, idx)| (*idx, pos))
        .collect()
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
        let sso_baselines = db.collection(BASELINES_COLLECTION);
        let alert_cutout_storage = config.build_cutout_storage(&Survey::Ztf).await?;

        let input_queue = "ZTF_alerts_enrichment_queue".to_string();
        let output_queue = "ZTF_alerts_filter_queue".to_string();

        let babamul: Option<Babamul> = if config.babamul.enabled {
            Some(Babamul::new(&config))
        } else {
            None
        };

        // CPU workers each load their own models: no mutex contention.
        let models = match shared_models {
            Some(models) => models,
            None => SharedModels::load(None)?,
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
            sso_baselines,
            alert_cutout_storage,
            alert_pipeline: create_ztf_alert_pipeline(false),
            models,
            babamul,
            gpu_enabled: config.gpu.is_active(),
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

        // Independent reads: awaiting them in turn pays each round trip.
        let (orbits, sso_history, baselines) = tokio::join!(
            self.fetch_orbits(&alerts),
            self.fetch_sso_history(&alerts),
            self.fetch_baselines(&alerts),
        );

        let batch_size = alerts.len();
        let mut skipped_empty_lightcurve = 0usize;
        let mut work_items: Vec<AlertWork> = Vec::with_capacity(alerts.len());
        #[cfg(feature = "gpu")]
        let mut villar_inputs: Vec<(i64, Vec<PhotometryMag>)> = Vec::new();
        for alert in alerts {
            let candid = alert.candid;
            let cutouts = candid_to_cutouts
                .remove(&candid)
                .ok_or_else(|| EnrichmentWorkerError::MissingCutouts(candid))?;
            #[cfg_attr(not(feature = "gpu"), allow(unused_variables))]
            let (properties, all_bands_properties, programid, lightcurve) = match self
                .get_alert_properties(&alert, &orbits, &sso_history, &baselines)
                .await
            {
                Ok(v) => v,
                // Skip the alert instead of aborting the batch: the queue keeps draining.
                Err(EnrichmentWorkerError::EmptyLightcurve(_)) => {
                    skipped_empty_lightcurve += 1;
                    debug!(candid, "skipping alert: empty lightcurve after filtering");
                    continue;
                }
                Err(e) => return Err(e),
            };
            #[cfg(feature = "gpu")]
            if self.models.gpu_ctx.is_some() {
                villar_inputs.push((candid, lightcurve));
            }

            work_items.push(AlertWork {
                candid,
                programid,
                properties,
                all_bands_properties,
                cutouts,
                alert,
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

        let classifications_list = self.classify(&self.models, &work_items)?;

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

        // bulk_write rejects an empty operation list.
        if !updates.is_empty() {
            let _ = self.client.bulk_write(updates).await?.modified_count;
        }

        // Villar fitting needs SharedModels loaded on a GPU device.
        #[cfg(feature = "gpu")]
        if let Some(gpu_ctx) = self.models.gpu_ctx.as_ref() {
            // Same keys as a successful fit, all NaN, so consumers see one schema.
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
    /// Look up MPC elements for every object named in this batch, in one query.
    ///
    /// Per-alert lookups would put a round trip in the hot enrichment path for
    /// every asteroid detection; a batch has at most a few hundred distinct
    /// objects, so one `$in` covers all of them.
    ///
    /// A failure here is not fatal: geometry is an enrichment, and dropping it
    /// for one batch is better than refusing to enrich the batch at all.
    /// Keyed by `ssnamenr` as the alert carries it, not by the MPCORB key, so
    /// each distinct name is normalised once here rather than again per alert.
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

        // A number and its "(number)Name" form reduce to the same key.
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

        // An empty catalogue looks like every object missing, so say which it is.
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

    /// Fitted phase curves for the batch's objects, keyed by `ssnamenr` then band.
    ///
    /// One `$in` per batch, for the same reason `fetch_orbits` batches. An object
    /// with no entry is scored against its window alone.
    async fn fetch_baselines(
        &self,
        alerts: &[ZtfAlertForEnrichment],
    ) -> HashMap<String, HashMap<u8, PhaseCurve>> {
        let names: Vec<&str> = alerts
            .iter()
            .filter_map(|a| a.candidate.candidate.ssnamenr.as_deref())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if names.is_empty() {
            return HashMap::new();
        }

        let cursor = match self
            .sso_baselines
            .find(doc! { "_id": { "$in": &names } })
            .await
        {
            Ok(cursor) => cursor,
            Err(e) => {
                warn!("failed to query {}: {}", BASELINES_COLLECTION, e);
                return HashMap::new();
            }
        };
        let docs: Vec<Document> = match cursor.try_collect().await {
            Ok(docs) => docs,
            Err(e) => {
                warn!("failed to read {}: {}", BASELINES_COLLECTION, e);
                return HashMap::new();
            }
        };

        docs.iter()
            .filter_map(|doc| {
                Some((
                    doc.get_str("_id").ok()?.to_string(),
                    curves_from_document(doc),
                ))
            })
            .collect()
    }

    /// A moving object's own recent photometry, keyed by `ssnamenr`.
    ///
    /// `objectId` cannot be used to join a mover's detections, so this reads the
    /// alert collection directly on the `ssnamenr`/`jd` index. One `$in` per
    /// batch, for the same reason `fetch_orbits` batches. Points without geometry
    /// are dropped: the statistic scales every point to the test epoch and
    /// cannot place one whose distances are unknown.
    ///
    /// A failure is not fatal, matching `fetch_orbits`.
    async fn fetch_sso_history(
        &self,
        alerts: &[ZtfAlertForEnrichment],
    ) -> HashMap<String, Vec<(f64, Point)>> {
        let names: Vec<&str> = alerts
            .iter()
            .filter_map(|a| a.candidate.candidate.ssnamenr.as_deref())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if names.is_empty() {
            return HashMap::new();
        }

        let earliest = alerts
            .iter()
            .map(|a| a.candidate.candidate.jd)
            .fold(f64::INFINITY, f64::min)
            - HISTORY_WINDOW_DAYS;

        let filter = doc! {
            "candidate.ssnamenr": { "$in": &names },
            "candidate.jd": { "$gte": earliest },
            "candidate.ssdistnr": { "$gte": 0.0, "$lt": MAX_SEPARATION_ARCSEC },
        };
        let projection = doc! {
            "_id": 0,
            "candidate.ssnamenr": 1,
            "candidate.jd": 1,
            "candidate.fid": 1,
            "candidate.magpsf": 1,
            "candidate.sigmapsf": 1,
            "candidate.ssdistnr": 1,
            "properties.sso.helio_dist": 1,
            "properties.sso.topo_dist": 1,
            "properties.sso.phase_angle": 1,
        };

        let cursor = match self
            .alert_collection
            .find(filter)
            .projection(projection)
            .await
        {
            Ok(cursor) => cursor,
            Err(e) => {
                warn!("failed to query solar system history: {}", e);
                return HashMap::new();
            }
        };
        let docs: Vec<Document> = match cursor.try_collect().await {
            Ok(docs) => docs,
            Err(e) => {
                warn!("failed to read solar system history: {}", e);
                return HashMap::new();
            }
        };

        let mut history: HashMap<String, Vec<(f64, Point)>> = HashMap::new();
        for doc in &docs {
            let Some((name, jd, point)) = history_point(doc) else {
                continue;
            };
            history.entry(name).or_default().push((jd, point));
        }
        for points in history.values_mut() {
            points.sort_by(|a, b| a.0.total_cmp(&b.0));
        }
        history
    }

    async fn get_alert_properties(
        &self,
        alert: &ZtfAlertForEnrichment,
        orbits: &HashMap<String, OrbitalElements>,
        sso_history: &HashMap<String, Vec<(f64, Point)>>,
        baselines: &HashMap<String, HashMap<u8, PhaseCurve>>,
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
        let is_rock = (0.0..12.0).contains(&ssdistnr) && ssmagnr >= 0.0;

        let activity = ActivityMetrics::from_magnitudes(Some(candidate.magpsf), candidate.magap);

        // Evaluated at the observation epoch, so a stale MPCORB degrades gradually.
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

        let activity = ActivityMetrics {
            outburst: outburst_for(&sso, candidate, sso_history, baselines),
            ..activity
        };

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

        let is_star = (sgscore1 > 0.76 && (0.0..=2.0).contains(&distpsnr1))
            || (sgscore1 > 0.2
                && (0.0..=1.0).contains(&distpsnr1)
                && srmag1 > 0.0
                && ((szmag1 > 0.0 && srmag1 - szmag1 > 3.0)
                    || (simag1 > 0.0 && srmag1 - simag1 > 3.0)));

        let is_near_brightstar = ((0.0..=20.0).contains(&neargaiabright)
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

        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);

        // No usable photometry: every feature would come from placeholder zeros.
        if lightcurve.is_empty() {
            return Err(EnrichmentWorkerError::EmptyLightcurve(alert.candid));
        }
        let (photstats, all_bands_properties, stationary) = analyze_photometry(&lightcurve);

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
                sso: Some(sso),
                activity: Some(activity),
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

        let triplet_pos = position_index(&triplet_indices);
        let acai_pos = position_index(&acai_indices);
        let bts_pos = position_index(&bts_indices);

        let mut selected: Vec<(usize, usize, usize, usize)> = Vec::new();
        for (idx, item) in work_items.iter().enumerate() {
            match (triplet_pos.get(&idx), acai_pos.get(&idx), bts_pos.get(&idx)) {
                (Some(&tpos), Some(&apos), Some(&bpos)) => selected.push((idx, tpos, apos, bpos)),
                _ => warn!(
                    "Skipping ML inference for candid {} due to missing features",
                    item.candid
                ),
            }
        }

        if selected.is_empty() {
            return Ok(results);
        }

        // Fixed-size chunks: ORT needs one input shape, so the last is zero-padded.
        for chunk in selected.chunks(self.batch_size) {
            let mut triplet = ndarray::Array::zeros((self.batch_size, 63, 63, 3));
            let mut metadata = ndarray::Array::zeros((self.batch_size, 25));
            let mut btsbot_metadata = ndarray::Array::zeros((self.batch_size, 25));

            for (row, &(_, tpos, apos, bpos)) in chunk.iter().enumerate() {
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
                if got != self.batch_size {
                    return Err(EnrichmentWorkerError::ConfigurationError(format!(
                        "model {} returned {} scores for {} padded inputs",
                        name, got, self.batch_size
                    )));
                }
            }

            for (batch_idx, &(item_idx, ..)) in chunk.iter().enumerate() {
                results[item_idx] = Some(ZtfAlertClassifications {
                    acai_h: acai_h_scores[batch_idx],
                    acai_n: acai_n_scores[batch_idx],
                    acai_v: acai_v_scores[batch_idx],
                    acai_o: acai_o_scores[batch_idx],
                    acai_b: acai_b_scores[batch_idx],
                    btsbot: btsbot_scores[batch_idx],
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

    /// 1 Ceres, the MPCORB elements checked against JPL Horizons in
    /// `sso_geometry::tests`. Values there are the reference for the numbers below.
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

    // An IPAC designation has to reach the geometry; f32 storage sets the tolerance.
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

    // A default would be indistinguishable from a real measurement downstream.
    #[test]
    fn test_geometry_absent_when_elements_are_missing() {
        let sso = ZtfSsoAssociation::from_ipac(Some("9816".to_string()), Some(1.0), Some(18.1))
            .with_geometry(None, 2_461_272.5);
        assert!(sso.is_sso, "the association itself still stands");
        assert!(sso.helio_dist.is_none());
        assert!(sso.topo_dist.is_none());
        assert!(sso.phase_angle.is_none());
    }

    // IPAC does not write designations the way MPCORB does: guard the join key.
    #[test]
    fn test_ipac_designations_resolve_to_orbit_keys() {
        // Mirrors fetch_orbits: keyed by ssnamenr as the alert carries it.
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
        // Comets are not in MPCORB; missing beats matching the wrong object.
        assert!(!orbits.contains_key("C/2026O1"));
        assert!(normalize_ztf_ssnamenr("C/2026O1").is_none());
    }

    // Upstream uses -999 for "no match"; stored verbatim it reads as a close match.
    #[test]
    fn test_negative_sentinels_are_normalised_to_none() {
        let sso = ZtfSsoAssociation::from_ipac(None, Some(-999.0), Some(-999.0));
        assert!(sso.separation_arcsec.is_none());
        assert!(sso.predicted_mag.is_none());
    }

    // Alerts enriched before `properties.sso` must read back as None, not 500.
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

    // A partially-written block should not fail either.
    #[test]
    fn test_partial_sso_block_deserializes() {
        let sso: ZtfSsoAssociation =
            serde_json::from_value(serde_json::json!({"designation": "9816"}))
                .expect("partial sso block must deserialize");
        assert_eq!(sso.designation.as_deref(), Some("9816"));
        assert!(!sso.is_sso);
        assert!(sso.separation_arcsec.is_none());
    }

    // Regression: 12" identifications fell 98.2% to 82.4%; `is_sso` must ignore it.
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

    /// A detection far from the predicted position belongs to something else,
    /// and its brightness would read as a large outburst.
    #[test]
    fn test_misassociated_detections_are_not_scored() {
        let history = HashMap::new();
        let baselines = HashMap::new();
        let candidate = Candidate {
            jd: 2_460_000.0,
            magpsf: 18.0,
            sigmapsf: 0.05,
            fid: 1,
            ..Default::default()
        };

        let near = ZtfSsoAssociation::from_ipac(Some("9816".into()), Some(0.5), Some(18.1))
            .with_geometry(None, candidate.jd);
        assert!(near.separation_arcsec.is_some());

        let far = ZtfSsoAssociation::from_ipac(
            Some("9816".into()),
            Some(MAX_SEPARATION_ARCSEC as f32 + 1.0),
            Some(18.1),
        );
        assert!(outburst_for(&far, &candidate, &history, &baselines).is_none());

        let unmeasured = ZtfSsoAssociation::from_ipac(Some("9816".into()), None, Some(18.1));
        assert!(outburst_for(&unmeasured, &candidate, &history, &baselines).is_none());
    }

    /// Geometry is what lets a point be scaled to the test epoch, so a detection
    /// enriched before geometry existed cannot join the window.
    #[test]
    fn test_history_point_requires_geometry() {
        let complete = doc! {
            "candidate": { "ssnamenr": "9816", "jd": 2_460_000.0, "fid": 1,
                           "magpsf": 18.5, "sigmapsf": 0.04 },
            "properties": { "sso": { "helio_dist": 2.5, "topo_dist": 1.6, "phase_angle": 12.0 } },
        };
        let (name, jd, point) = history_point(&complete).expect("complete document");
        assert_eq!(name, "9816");
        assert_eq!(jd, 2_460_000.0);
        assert_eq!(point.band, 1);
        assert_eq!(point.rh, 2.5);

        let mut without_geometry = complete.clone();
        without_geometry.insert("properties", doc! { "sso": { "helio_dist": 2.5 } });
        assert!(history_point(&without_geometry).is_none());

        let mut unenriched = complete.clone();
        unenriched.insert("properties", doc! {});
        assert!(history_point(&unenriched).is_none());
    }

    /// The geometry fields are f32 in the association but reach BSON as either
    /// double or int depending on the writer, and an integer phase angle is a
    /// value the archive really holds.
    #[test]
    fn test_history_point_accepts_integer_valued_geometry() {
        let doc = doc! {
            "candidate": { "ssnamenr": "9816", "jd": 2_460_000.0, "fid": 2,
                           "magpsf": 18.5, "sigmapsf": 0.04 },
            "properties": { "sso": { "helio_dist": 2.5, "topo_dist": 1.6, "phase_angle": 12i32 } },
        };
        let (_, _, point) = history_point(&doc).expect("integer phase angle");
        assert_eq!(point.phase, 12.0);
    }
}
