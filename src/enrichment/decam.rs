use crate::alert::DecamCandidate;
use crate::conf::AppConfig;
use crate::enrichment::lsst::{lsst_detections, lsst_nondetections, LsstMatch};
use crate::enrichment::ztf::{ztf_detections, ztf_nondetections, ZtfMatch};
use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::{fetch_timeseries_op, mongify};
use crate::utils::enums::Survey;
use crate::utils::host::HostGalaxyAssociation;
use crate::utils::identify::{closest, IdentifyConfig, NightPaths, OrbitEntry};
use crate::utils::lightcurves::{
    analyze_photometry, is_stationary, prepare_photometry, summarise_detections, Band,
    DetectionHistory, EpisodeHistory, MultisurveyDetectionHistory, PerBandProperties,
    PhotometryMag, EPISODE_GAP_DAYS, SNT,
};
use crate::utils::linking::{night_start, Detection};
use crate::utils::mpcorb::{load_catalogue, MAX_EPOCH_GAP_DAYS};
use crate::utils::o11y::logging::{log_error, WARN};
use crate::utils::sso_geometry::{geometry_at, DECAM};
use apache_avro_derive::AvroSchema;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};
use tracing::{instrument, warn};

const SSO_MATCH_RADIUS_ARCSEC: f64 = 8.0;
const CATALOGUE_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const CATALOGUE_MIN_RELOAD: Duration = Duration::from_secs(10 * 60);

pub fn create_decam_alert_pipeline() -> Vec<Document> {
    vec![
        doc! {
            "$match": {
                "_id": {"$in": []}
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "candidate": 1,
            }
        },
        doc! {
            "$lookup": {
                "from": "DECAM_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$lookup": {
                "from": "ZTF_alerts_aux",
                "localField": "aux.aliases.ZTF.0",
                "foreignField": "_id",
                "as": "ztf_aux"
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
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "survey_matches": {
                    "ztf": {
                        "$cond": {
                            "if": { "$gt": [ { "$size": "$ztf_aux" }, 0 ] },
                            "then": {
                                "objectId": { "$arrayElemAt": [ "$ztf_aux._id", 0 ] },
                                "prv_candidates": { "$arrayElemAt": [ "$ztf_aux.prv_candidates", 0 ] },
                                "prv_nondetections": { "$arrayElemAt": [ "$ztf_aux.prv_nondetections", 0 ] },
                                "fp_hists": { "$arrayElemAt": [ "$ztf_aux.fp_hists", 0 ] },
                                "ra": { "$add": [
                                    { "$arrayElemAt": [{ "$arrayElemAt": [ "$ztf_aux.coordinates.radec_geojson.coordinates", 0 ] }, 0]},
                                    180
                                ]},
                                "dec": { "$arrayElemAt": [{ "$arrayElemAt": [ "$ztf_aux.coordinates.radec_geojson.coordinates", 0 ] }, 1]},
                            },
                            "else": null
                        }
                    },
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
                },
                "prv_candidates": fetch_timeseries_op(
                    "aux.prv_candidates",
                    "candidate.jd",
                    365,
                    None
                ),
                "fp_hists": fetch_timeseries_op(
                    "aux.fp_hists",
                    "candidate.jd",
                    365,
                    Some(vec![doc! {
                        "$gte": [
                            "$$x.snr",
                            SNT
                        ]
                    }]),
                ),
                "fp_nondetections": fetch_timeseries_op(
                    "aux.fp_hists",
                    "candidate.jd",
                    365,
                    Some(below_threshold()),
                ),
                "host_galaxy": {"$arrayElemAt": ["$aux.host_galaxy", 0]},
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates.jd": 1,
                "prv_candidates.magap": 1,
                "prv_candidates.sigmagap": 1,
                "prv_candidates.band": 1,
                "prv_candidates.snr": 1,
                "fp_hists.jd": 1,
                "fp_hists.magap": 1,
                "fp_hists.sigmagap": 1,
                "fp_hists.band": 1,
                "fp_hists.snr": 1,
                "fp_nondetections.jd": 1,
                "fp_nondetections.band": 1,
                "fp_nondetections.diffmaglim": 1,
                "host_galaxy": 1,
                "survey_matches": 1,
            }
        },
    ]
}

// Null and NaN sort below every number, so the lower bound also drops unmeasured epochs.
fn below_threshold() -> Vec<Document> {
    vec![
        doc! { "$gt": [ "$$x.snr", -SNT ] },
        doc! { "$lt": [ "$$x.snr", SNT ] },
    ]
}

/// DECAM prv_candidate for enrichment: the magnitude fields feed the light-curve
/// stats (mirrors `PhotometryMag`, also accepting DECam's `magap`/`sigmagap`
/// keys), the signed `snr` feeds the detection-history sign.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DecamPhotometry {
    #[serde(alias = "jd")]
    pub time: f64,
    #[serde(alias = "magpsf", alias = "magap")]
    pub mag: f32,
    #[serde(alias = "sigmapsf", alias = "sigmagap")]
    pub mag_err: f32,
    pub band: Band,
    #[serde(default)]
    pub snr: Option<f64>,
}

impl DecamPhotometry {
    fn to_positive_mag(&self) -> Option<PhotometryMag> {
        if self.snr.is_some_and(|s| s < 0.0) {
            return None;
        }
        Some(PhotometryMag {
            time: self.time,
            mag: self.mag,
            mag_err: self.mag_err,
            band: self.band.clone(),
        })
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DecamNonDetection {
    pub jd: f64,
    pub diffmaglim: f64,
    pub band: Band,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DecamMatch {
    pub prv_candidates: Vec<DecamPhotometry>,
    pub fp_hists: Vec<DecamPhotometry>,
    #[serde(default)]
    pub fp_nondetections: Vec<DecamNonDetection>,
}

impl DecamMatch {
    pub fn lightcurve(&self, jd: f64) -> Vec<PhotometryMag> {
        self.prv_candidates
            .iter()
            .chain(&self.fp_hists)
            .filter(|p| p.time <= jd)
            .filter_map(DecamPhotometry::to_positive_mag)
            .collect()
    }

    pub fn detections(&self) -> impl Iterator<Item = (f64, Survey, &Band)> {
        decam_detections(&self.prv_candidates)
    }

    pub fn nondetections(&self) -> impl Iterator<Item = (f64, f32, Survey, &Band)> {
        decam_nondetections(&self.fp_nondetections)
    }
}

fn decam_detections(
    prv_candidates: &[DecamPhotometry],
) -> impl Iterator<Item = (f64, Survey, &Band)> {
    prv_candidates
        .iter()
        .filter(|p| p.snr.is_some_and(|s| s >= 0.0))
        .map(|p| (p.time, Survey::Decam, &p.band))
}

fn decam_nondetections(
    fp_nondetections: &[DecamNonDetection],
) -> impl Iterator<Item = (f64, f32, Survey, &Band)> {
    fp_nondetections
        .iter()
        .map(|p| (p.jd, p.diffmaglim as f32, Survey::Decam, &p.band))
}

/// The pipeline must end with its `$project` stage.
pub fn add_decam_match(mut pipeline: Vec<Document>) -> Vec<Document> {
    let project_index = pipeline.len() - 1;
    if let Ok(project) = pipeline[project_index].get_document_mut("$project") {
        project.insert(
            "decam_match",
            doc! {
                "$cond": {
                    "if": { "$gt": [ { "$size": "$decam_aux" }, 0 ] },
                    "then": {
                        "prv_candidates": {
                            "$ifNull": [{ "$arrayElemAt": [ "$decam_aux.prv_candidates", 0 ] }, []]
                        },
                        "fp_hists": {
                            "$filter": {
                                "input": {
                                    "$ifNull": [{ "$arrayElemAt": [ "$decam_aux.fp_hists", 0 ] }, []]
                                },
                                "as": "x",
                                "cond": { "$gte": [ "$$x.snr", SNT ] }
                            }
                        },
                        "fp_nondetections": {
                            "$filter": {
                                "input": {
                                    "$ifNull": [{ "$arrayElemAt": [ "$decam_aux.fp_hists", 0 ] }, []]
                                },
                                "as": "x",
                                "cond": { "$and": below_threshold() }
                            }
                        },
                    },
                    "else": null
                }
            },
        );
    }
    pipeline.insert(
        project_index,
        doc! {
            "$lookup": {
                "from": "DECAM_alerts_aux",
                "localField": "aux.aliases.DECAM.0",
                "foreignField": "_id",
                "as": "decam_aux"
            }
        },
    );
    pipeline
}

/// DECAM alert structure used to deserialize alerts
/// from the database, used by the enrichment worker
/// to compute features and ML scores
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct DecamAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: DecamCandidate,
    // Signed SNR is only needed here: detection history counts detections.
    pub prv_candidates: Vec<DecamPhotometry>,
    pub fp_hists: Vec<DecamPhotometry>,
    #[serde(default)]
    pub fp_nondetections: Vec<DecamNonDetection>,
    #[serde(default)]
    pub host_galaxy: Option<HostGalaxyAssociation>,
    #[serde(default)]
    pub survey_matches: Option<DecamSurveyMatches>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct DecamSurveyMatches {
    pub ztf: Option<ZtfMatch>,
    pub lsst: Option<LsstMatch>,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize, AvroSchema)]
#[serde(default)]
pub struct DecamSsoAssociation {
    pub is_sso: bool,
    pub designation: Option<String>,
    pub separation_arcsec: Option<f32>,
    pub source: Option<String>,
    pub helio_dist: Option<f32>,
    pub topo_dist: Option<f32>,
    pub phase_angle: Option<f32>,
    pub a: Option<f32>,
    pub e: Option<f32>,
    pub incl: Option<f32>,
    pub q: Option<f32>,
    pub tisserand_jupiter: Option<f32>,
}

/// DECAM alert properties computed during enrichment
/// and inserted back into the alert document
#[derive(Debug, serde::Deserialize, serde::Serialize, AvroSchema)]
pub struct DecamAlertProperties {
    #[serde(default)]
    pub rock: bool,
    pub stationary: bool,
    /// Absent means never evaluated for a host, not evaluated and hostless.
    #[serde(default)]
    pub hosted: Option<bool>,
    pub photstats: PerBandProperties,
    #[serde(default)]
    pub multisurvey_photstats: Option<PerBandProperties>,
    /// Per-object detection-history summary for history-aware filters.
    /// `None` on alerts enriched before this field existed.
    #[serde(default)]
    pub detection_history: Option<DetectionHistory>,
    #[serde(default)]
    pub multisurvey_detection_history: Option<MultisurveyDetectionHistory>,
    /// Detection episodes, for finding sources that outburst more than once.
    /// `None` on alerts enriched before this field existed.
    pub episode_history: Option<EpisodeHistory>,
    #[serde(default)]
    pub sso: Option<DecamSsoAssociation>,
}

struct KnownObjects {
    orbits: Vec<OrbitEntry>,
    loaded: Instant,
    jd: f64,
    paths: Option<(i64, NightPaths)>,
}

fn known_associations(
    known: &mut KnownObjects,
    alerts: &[DecamAlertForEnrichment],
) -> HashMap<i64, DecamSsoAssociation> {
    let cfg = IdentifyConfig {
        site: DECAM,
        match_radius_arcsec: SSO_MATCH_RADIUS_ARCSEC,
        ..IdentifyConfig::default()
    };
    let mut by_night: BTreeMap<i64, Vec<Detection>> = BTreeMap::new();
    for alert in alerts {
        let detection = Detection {
            id: alert.candid,
            jd: alert.candidate.jd,
            ra: alert.candidate.candidate.ra,
            dec: alert.candidate.candidate.dec,
            mag: None,
            mag_err: None,
            band: None,
            site: Some(DECAM),
        };
        by_night
            .entry(detection.night())
            .or_default()
            .push(detection);
    }

    let mut associations = HashMap::new();
    for (night, detections) in by_night {
        if !matches!(&known.paths, Some((n, _)) if *n == night) {
            let paths = NightPaths::new(
                &known.orbits,
                night_start(night, Some(&DECAM)),
                night_start(night + 1, Some(&DECAM)),
                cfg.coarse_radius_deg,
            );
            known.paths = Some((night, paths));
        }
        let Some((_, paths)) = &known.paths else {
            continue;
        };
        let orbits = &known.orbits;
        let found: Vec<(i64, DecamSsoAssociation)> = detections
            .par_iter()
            .map(|d| {
                let sso = match closest(d, orbits, paths, &cfg) {
                    Some((idx, separation)) => {
                        let elements = &orbits[idx].elements;
                        let geometry = geometry_at(elements, d.jd, &DECAM);
                        DecamSsoAssociation {
                            is_sso: true,
                            designation: Some(orbits[idx].designation.clone()),
                            separation_arcsec: Some(separation as f32),
                            source: Some("boom".to_string()),
                            helio_dist: Some(geometry.helio_dist as f32),
                            topo_dist: Some(geometry.topo_dist as f32),
                            phase_angle: Some(geometry.phase_angle as f32),
                            a: (elements.e < 1.0).then_some(elements.a as f32),
                            e: Some(elements.e as f32),
                            incl: Some(elements.incl as f32),
                            q: Some(elements.q as f32),
                            tisserand_jupiter: elements.tisserand_jupiter().map(|t| t as f32),
                        }
                    }
                    None => DecamSsoAssociation::default(),
                };
                (d.id, sso)
            })
            .collect();
        associations.extend(found);
    }
    associations
}

pub struct DecamEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
    known: Option<KnownObjects>,
    only_properties: Option<Vec<String>>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for DecamEnrichmentWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        _shared_models: Option<std::sync::Arc<crate::enrichment::models::SharedModels>>,
    ) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("DECAM_alerts");

        let input_queue = "DECAM_alerts_enrichment_queue".to_string();
        let output_queue = "DECAM_alerts_filter_queue".to_string();

        Ok(DecamEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            db,
            alert_collection,
            alert_pipeline: create_decam_alert_pipeline(),
            known: None,
            only_properties: None,
        })
    }

    fn survey() -> Survey {
        Survey::Decam
    }

    fn disable_babamul(&mut self) {}

    fn write_only(&mut self, properties: Vec<String>, gp_fit: bool) -> bool {
        if gp_fit {
            return false;
        }
        self.only_properties = Some(properties);
        true
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
        let alerts: Vec<DecamAlertForEnrichment> =
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

        let now = flare::Time::now().to_jd();

        let (first, last) = alerts
            .iter()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), a| {
                (lo.min(a.candidate.jd), hi.max(a.candidate.jd))
            });
        let match_known = self
            .only_properties
            .as_ref()
            .is_none_or(|keys| keys.iter().any(|k| k == "rock" || k == "sso"));
        let mut associations = if match_known {
            self.known_objects((first + last) / 2.0)
                .await
                .map(|known| known_associations(known, &alerts))
        } else {
            None
        };

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for alert in alerts {
            let candid = alert.candid;

            let sso = associations.as_mut().and_then(|a| a.remove(&candid));
            let properties = mongify(&Self::alert_properties(&alert, sso)?);
            let mut set = match &self.only_properties {
                None => doc! { "properties": properties },
                Some(keys) => keys
                    .iter()
                    .filter_map(|k| {
                        properties
                            .get(k)
                            .map(|v| (format!("properties.{k}"), v.clone()))
                    })
                    .collect(),
            };
            set.insert("updated_at", now);
            let update_alert_document = doc! { "$set": set };

            let update = WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(doc! {"_id": candid})
                    .update(update_alert_document)
                    .build(),
            );

            updates.push(update);
            processed_alerts.push(format!("{}", candid));
        }

        let _ = self.client.bulk_write(updates).await?.modified_count;

        Ok(processed_alerts)
    }
}

impl DecamEnrichmentWorker {
    async fn known_objects(&mut self, jd: f64) -> Option<&mut KnownObjects> {
        if self.known.as_ref().is_none_or(|k| {
            k.loaded.elapsed() >= CATALOGUE_MAX_AGE
                || ((k.jd - jd).abs() > MAX_EPOCH_GAP_DAYS
                    && k.loaded.elapsed() >= CATALOGUE_MIN_RELOAD)
        }) {
            match load_catalogue(&self.db, jd).await {
                Ok(orbits) if !orbits.is_empty() => {
                    self.known = Some(KnownObjects {
                        orbits,
                        loaded: Instant::now(),
                        jd,
                        paths: None,
                    });
                }
                Ok(_) => {
                    warn!("MPC_orbits is empty, DECam alerts are not matched to known objects")
                }
                Err(error) => log_error!(WARN, error, "could not load MPC_orbits"),
            }
        }
        self.known.as_mut()
    }

    fn alert_properties(
        alert: &DecamAlertForEnrichment,
        sso: Option<DecamSsoAssociation>,
    ) -> Result<DecamAlertProperties, EnrichmentWorkerError> {
        let prv_candidates: Vec<PhotometryMag> = alert
            .prv_candidates
            .iter()
            .filter_map(DecamPhotometry::to_positive_mag)
            .collect();
        let fp_hists: Vec<PhotometryMag> = alert
            .fp_hists
            .iter()
            .filter_map(DecamPhotometry::to_positive_mag)
            .collect();

        // lightcurve is prv_candidates + fp_hists, no need for parse_photometry here
        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);
        let (photstats, _, _) = analyze_photometry(&lightcurve);
        let stationary = is_stationary(
            alert
                .prv_candidates
                .iter()
                .map(|p| p.time)
                .chain(alert.fp_hists.iter().map(|p| p.time)),
        );

        let jd = alert.candidate.jd;
        let decam_points = lightcurve.len();
        if let Some(matches) = &alert.survey_matches {
            if let Some(ztf) = &matches.ztf {
                lightcurve.extend(
                    ztf.prv_candidates
                        .iter()
                        .chain(&ztf.fp_hists)
                        .filter(|p| p.jd <= jd)
                        .filter_map(|p| p.to_positive_mag()),
                );
            }
            if let Some(lsst) = &matches.lsst {
                lightcurve.extend(
                    lsst.prv_candidates
                        .iter()
                        .chain(&lsst.fp_hists)
                        .filter(|p| p.jd <= jd)
                        .filter_map(|p| p.to_positive_mag()),
                );
            }
        }
        let multisurvey_photstats = if lightcurve.len() == decam_points {
            photstats.clone()
        } else {
            prepare_photometry(&mut lightcurve);
            analyze_photometry(&lightcurve).0
        };

        // Per-object detection history for history-aware filters (sign from signed snr).
        let (mut detection_history, episode_history) = summarise_detections(
            alert
                .prv_candidates
                .iter()
                .map(|p| (p.time, p.snr.filter(|s| !s.is_nan()).map(|s| s < 0.0))),
            // Every stored forced epoch carries a magnitude, so it is a detection.
            alert.fp_hists.iter().map(|p| (p.time, None)),
            alert.candidate.jd,
            EPISODE_GAP_DAYS,
        );
        detection_history.add_nondetections(
            alert
                .fp_nondetections
                .iter()
                .map(|p| (p.jd, p.diffmaglim as f32)),
            alert.candidate.jd,
        );

        let ztf_match = alert.survey_matches.as_ref().and_then(|m| m.ztf.as_ref());
        let lsst_match = alert.survey_matches.as_ref().and_then(|m| m.lsst.as_ref());
        let multisurvey_detection_history = MultisurveyDetectionHistory::from_points(
            decam_detections(&alert.prv_candidates)
                .chain(
                    ztf_match
                        .into_iter()
                        .flat_map(|m| ztf_detections(&m.prv_candidates)),
                )
                .chain(
                    lsst_match
                        .into_iter()
                        .flat_map(|m| lsst_detections(&m.prv_candidates)),
                ),
            decam_nondetections(&alert.fp_nondetections)
                .chain(
                    ztf_match
                        .into_iter()
                        .flat_map(|m| ztf_nondetections(&m.prv_nondetections, &m.fp_hists)),
                )
                .chain(
                    lsst_match
                        .into_iter()
                        .flat_map(|m| lsst_nondetections(&m.fp_hists)),
                ),
            alert.candidate.jd,
        );

        Ok(DecamAlertProperties {
            rock: sso.as_ref().is_some_and(|s| s.is_sso),
            stationary,
            hosted: alert.host_galaxy.as_ref().map(|h| h.best_host.is_some()),
            photstats,
            multisurvey_photstats: Some(multisurvey_photstats),
            detection_history: Some(detection_history),
            multisurvey_detection_history: Some(multisurvey_detection_history),
            episode_history: Some(episode_history),
            sso,
        })
    }
}

pub fn decam_alert_properties_without_sso(
    alert: &DecamAlertForEnrichment,
) -> Result<DecamAlertProperties, EnrichmentWorkerError> {
    DecamEnrichmentWorker::alert_properties(alert, None)
}
