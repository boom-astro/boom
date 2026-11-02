use crate::alert::DecamCandidate;
use crate::conf::AppConfig;
use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::{fetch_timeseries_op, mongify};
use crate::utils::enums::Survey;
use crate::utils::host::HostGalaxyAssociation;
use crate::utils::identify::{closest, IdentifyConfig, NightPaths, OrbitEntry};
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, summarise_detections, Band, DetectionHistory,
    EpisodeHistory, PerBandProperties, PhotometryMag, EPISODE_GAP_DAYS, SNT,
};
use crate::utils::linking::{night_start, Detection};
use crate::utils::mpcorb::load_catalogue;
use crate::utils::o11y::logging::{log_error, WARN};
use crate::utils::sso_geometry::{geometry_at, DECAM};
use apache_avro_derive::AvroSchema;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};
use tracing::{instrument, warn};

const SSO_MATCH_RADIUS_ARCSEC: f64 = 30.0;
const CATALOGUE_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

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
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
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
                "host_galaxy": 1,
            }
        },
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
    fn to_photometry_mag(&self) -> PhotometryMag {
        PhotometryMag {
            time: self.time,
            mag: self.mag,
            mag_err: self.mag_err,
            band: self.band.clone(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DecamMatch {
    pub prv_candidates: Vec<DecamPhotometry>,
    pub fp_hists: Vec<DecamPhotometry>,
}

impl DecamMatch {
    pub fn lightcurve(&self, jd: f64) -> Vec<PhotometryMag> {
        self.prv_candidates
            .iter()
            .chain(&self.fp_hists)
            .filter(|p| p.time <= jd)
            .map(DecamPhotometry::to_photometry_mag)
            .collect()
    }
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
    pub fp_hists: Vec<PhotometryMag>,
    #[serde(default)]
    pub host_galaxy: Option<HostGalaxyAssociation>,
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
    /// Per-object detection-history summary for history-aware filters.
    /// `None` on alerts enriched before this field existed.
    #[serde(default)]
    pub detection_history: Option<DetectionHistory>,
    /// Detection episodes, for finding sources that outburst more than once.
    /// `None` on alerts enriched before this field existed.
    pub episode_history: Option<EpisodeHistory>,
    #[serde(default)]
    pub sso: Option<DecamSsoAssociation>,
}

struct KnownObjects {
    orbits: Vec<OrbitEntry>,
    loaded: Instant,
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
                        let geometry = geometry_at(&orbits[idx].elements, d.jd, &DECAM);
                        DecamSsoAssociation {
                            is_sso: true,
                            designation: Some(orbits[idx].designation.clone()),
                            separation_arcsec: Some(separation as f32),
                            source: Some("boom".to_string()),
                            helio_dist: Some(geometry.helio_dist as f32),
                            topo_dist: Some(geometry.topo_dist as f32),
                            phase_angle: Some(geometry.phase_angle as f32),
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
        })
    }

    fn survey() -> Survey {
        Survey::Decam
    }

    fn disable_babamul(&mut self) {}

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

        let mut associations = self
            .known_objects()
            .await
            .map(|known| known_associations(known, &alerts));

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for alert in alerts {
            let candid = alert.candid;

            let sso = associations.as_mut().and_then(|a| a.remove(&candid));
            let properties = self.get_alert_properties(&alert, sso).await?;

            let update_alert_document = doc! {
                "$set": {
                    "properties": mongify(&properties),
                    "updated_at": now,
                }
            };

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
    async fn known_objects(&mut self) -> Option<&mut KnownObjects> {
        if self
            .known
            .as_ref()
            .is_none_or(|k| k.loaded.elapsed() >= CATALOGUE_MAX_AGE)
        {
            match load_catalogue(&self.db).await {
                Ok(orbits) if !orbits.is_empty() => {
                    self.known = Some(KnownObjects {
                        orbits,
                        loaded: Instant::now(),
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

    async fn get_alert_properties(
        &self,
        alert: &DecamAlertForEnrichment,
        sso: Option<DecamSsoAssociation>,
    ) -> Result<DecamAlertProperties, EnrichmentWorkerError> {
        let prv_candidates: Vec<PhotometryMag> = alert
            .prv_candidates
            .iter()
            .map(DecamPhotometry::to_photometry_mag)
            .collect();
        let fp_hists = alert.fp_hists.clone();

        // lightcurve is prv_candidates + fp_hists, no need for parse_photometry here
        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);
        let (photstats, _, stationary) = analyze_photometry(&lightcurve);

        // Per-object detection history for history-aware filters (sign from signed snr).
        let (detection_history, episode_history) = summarise_detections(
            alert
                .prv_candidates
                .iter()
                .map(|p| (p.time, p.snr.filter(|s| !s.is_nan()).map(|s| s < 0.0))),
            // Every stored forced epoch carries a magnitude, so it is a detection.
            alert.fp_hists.iter().map(|p| (p.time, None)),
            alert.candidate.jd,
            EPISODE_GAP_DAYS,
        );

        Ok(DecamAlertProperties {
            rock: sso.as_ref().is_some_and(|s| s.is_sso),
            stationary,
            hosted: alert.host_galaxy.as_ref().map(|h| h.best_host.is_some()),
            photstats,
            detection_history: Some(detection_history),
            episode_history: Some(episode_history),
            sso,
        })
    }
}
