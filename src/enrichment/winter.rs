use crate::alert::WinterCandidate;
use crate::conf::AppConfig;
use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::{fetch_timeseries_op, mongify};
use crate::utils::enums::Survey;
use crate::utils::host::HostGalaxyAssociation;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, summarise_detections, Band, DetectionHistory,
    EpisodeHistory, PerBandProperties, PhotometryMag, EPISODE_GAP_DAYS,
};
use apache_avro_derive::AvroSchema;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

pub fn create_winter_alert_pipeline() -> Vec<Document> {
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
                "from": "WINTER_alerts_aux",
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
                    1000,
                    None
                ),
                "host_galaxy": {"$arrayElemAt": ["$aux.host_galaxy", 0]},
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates.jd": 1,
                "prv_candidates.magpsf": 1,
                "prv_candidates.sigmapsf": 1,
                "prv_candidates.band": 1,
                "prv_candidates.isdiffpos": 1,
                "host_galaxy": 1,
            }
        },
    ]
}

/// WINTER prv_candidate for enrichment: the magnitude fields feed the light-curve
/// stats, `isdiffpos` feeds the detection-history sign.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct WinterPhotometry {
    #[serde(alias = "jd")]
    pub time: f64,
    #[serde(alias = "magpsf")]
    pub mag: f32,
    #[serde(alias = "sigmapsf")]
    pub mag_err: f32,
    pub band: Band,
    #[serde(default)]
    pub isdiffpos: Option<bool>,
}

impl WinterPhotometry {
    fn to_photometry_mag(&self) -> PhotometryMag {
        PhotometryMag {
            time: self.time,
            mag: self.mag,
            mag_err: self.mag_err,
            band: self.band.clone(),
        }
    }
}

/// WINTER alert structure used to deserialize alerts from the database, used by
/// the enrichment worker to compute lightcurve features.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct WinterAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: WinterCandidate,
    pub prv_candidates: Vec<WinterPhotometry>,
    #[serde(default)]
    pub host_galaxy: Option<HostGalaxyAssociation>,
}

/// WINTER alert properties computed during enrichment and inserted back into the
/// alert document.
#[derive(Debug, serde::Deserialize, serde::Serialize, AvroSchema)]
pub struct WinterAlertProperties {
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
    /// ZTF's PS1 star rule, or a Gaia 5-sigma parallax within 2". `None` if not crossmatched.
    #[serde(default)]
    pub star: Option<bool>,
    /// Within 20" of a Gaia G < 14 star or ZTF's PS1 bright star. `None` if not crossmatched.
    #[serde(default)]
    pub near_brightstar: Option<bool>,
}

pub struct WinterEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for WinterEnrichmentWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        _shared_models: Option<std::sync::Arc<crate::enrichment::models::SharedModels>>,
    ) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("WINTER_alerts");

        let input_queue = "WINTER_alerts_enrichment_queue".to_string();
        let output_queue = "WINTER_alerts_filter_queue".to_string();

        Ok(WinterEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_pipeline: create_winter_alert_pipeline(),
        })
    }

    fn survey() -> Survey {
        Survey::Winter
    }

    fn input_queue_name(&self) -> String {
        self.input_queue.clone()
    }

    fn output_queue_name(&self) -> String {
        self.output_queue.clone()
    }

    /// No-op: WINTER enrichment has no Babamul integration, so there is
    /// nothing to disable.
    fn disable_babamul(&mut self) {}

    #[instrument(skip_all, err)]
    async fn process_alerts(
        &mut self,
        candids: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError> {
        let alerts: Vec<WinterAlertForEnrichment> =
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

        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for alert in alerts {
            let candid = alert.candid;

            let properties = self.get_alert_properties(&alert).await?;

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

impl WinterEnrichmentWorker {
    async fn get_alert_properties(
        &self,
        alert: &WinterAlertForEnrichment,
    ) -> Result<WinterAlertProperties, EnrichmentWorkerError> {
        let mut lightcurve: Vec<PhotometryMag> = alert
            .prv_candidates
            .iter()
            .map(WinterPhotometry::to_photometry_mag)
            .collect();

        prepare_photometry(&mut lightcurve);
        let (photstats, _, stationary) = analyze_photometry(&lightcurve);

        // Per-object detection history for history-aware filters (sign from isdiffpos).
        let (detection_history, episode_history) = summarise_detections(
            alert
                .prv_candidates
                .iter()
                .map(|p| (p.time, p.isdiffpos.map(|d| !d))),
            alert.candidate.jd,
            EPISODE_GAP_DAYS,
        );

        Ok(WinterAlertProperties {
            stationary,
            hosted: alert.host_galaxy.as_ref().map(|h| h.best_host.is_some()),
            photstats,
            detection_history: Some(detection_history),
            episode_history: Some(episode_history),
            star: has_stellar_crossmatch(&alert.candidate).then(|| is_star(&alert.candidate)),
            near_brightstar: has_stellar_crossmatch(&alert.candidate)
                .then(|| is_near_brightstar(&alert.candidate)),
        })
    }
}

// Schema v0.1 packets carry neither the PS1 nor the Gaia crossmatch.
fn has_stellar_crossmatch(candidate: &WinterCandidate) -> bool {
    candidate.sgscore1.is_some() || candidate.distgaia.is_some()
}

// ZTF's rule, except that a missing PS1 magnitude fails its cut instead of passing it.
fn is_star(candidate: &WinterCandidate) -> bool {
    let sgscore1 = candidate.sgscore1.unwrap_or(0.0);
    let distpsnr1 = candidate.distpsnr1.unwrap_or(f32::INFINITY);
    let red_color = |redder_band: Option<f32>| {
        candidate
            .srmag1
            .zip(redder_band)
            .is_some_and(|(r_band, redder_band)| {
                r_band > 0.0 && redder_band > 0.0 && r_band - redder_band > 3.0
            })
    };
    let gaia_parallax = candidate
        .distgaia
        .is_some_and(|distance| (0.0..=2.0).contains(&distance))
        && candidate
            .plxgaia
            .is_some_and(|significance| significance >= 5.0)
        && candidate.ruwegaia.is_some_and(|ruwe| ruwe < 1.4);
    (sgscore1 > 0.76 && (0.0..=2.0).contains(&distpsnr1))
        || (sgscore1 > 0.2
            && (0.0..=1.0).contains(&distpsnr1)
            && (red_color(candidate.szmag1) || red_color(candidate.simag1)))
        || gaia_parallax
}

// ZTF's PS1 terms. The Gaia term uses `distgaiabright`, as v0.4 has no Gaia magnitude.
fn is_near_brightstar(candidate: &WinterCandidate) -> bool {
    let bright_ps1_star = |sgscore: Option<f32>, distpsnr: Option<f32>, srmag: Option<f32>| {
        sgscore.unwrap_or(0.0) > 0.49
            && distpsnr.unwrap_or(f32::INFINITY) <= 20.0
            && srmag.is_some_and(|srmag| srmag > 0.0 && srmag <= 15.0)
    };
    let saturated_ps1_star = candidate.sgscore1 == Some(0.5)
        && candidate.distpsnr1.is_some_and(|distance| distance < 0.5)
        && [candidate.sgmag1, candidate.srmag1, candidate.simag1]
            .into_iter()
            .flatten()
            .any(|magnitude| magnitude < 17.0);
    candidate
        .distgaiabright
        .is_some_and(|distance| (0.0..=20.0).contains(&distance))
        || bright_ps1_star(candidate.sgscore1, candidate.distpsnr1, candidate.srmag1)
        || bright_ps1_star(candidate.sgscore2, candidate.distpsnr2, candidate.srmag2)
        || bright_ps1_star(candidate.sgscore3, candidate.distpsnr3, candidate.srmag3)
        || saturated_ps1_star
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alert::{sanitize_winter_avro, WinterRawAvroAlert};

    fn packet_candidate() -> WinterCandidate {
        read_candidate("tests/data/alerts/winter/alert.avro")
    }

    fn read_candidate(path: &str) -> WinterCandidate {
        let raw = std::fs::read(path).unwrap();
        let fixed = sanitize_winter_avro(&raw).unwrap();
        let reader = apache_avro::Reader::new(&fixed[..]).unwrap();
        let value = reader.into_iter().next().unwrap().unwrap();
        apache_avro::from_value::<WinterRawAvroAlert>(&value)
            .unwrap()
            .candidate
    }

    #[test]
    fn test_packet_is_not_a_star_nor_near_a_bright_star() {
        let candidate = packet_candidate();
        assert!(!is_star(&candidate));
        assert!(!is_near_brightstar(&candidate));
    }

    #[test]
    fn test_schema_v0_1_packet_has_no_stellar_crossmatch() {
        assert!(has_stellar_crossmatch(&packet_candidate()));
        let candidate = read_candidate("tests/data/alerts/winter/alert_schemavsn_0.1.avro");
        assert!(!has_stellar_crossmatch(&candidate));
    }

    #[test]
    fn test_is_star() {
        let mut candidate = packet_candidate();
        candidate.sgscore1 = Some(0.9);
        assert!(is_star(&candidate));

        let mut candidate = packet_candidate();
        candidate.plxgaia = Some(8.0);
        candidate.ruwegaia = Some(1.0);
        assert!(is_star(&candidate));
        candidate.ruwegaia = Some(2.0);
        assert!(
            !is_star(&candidate),
            "a poor astrometric fit is not a parallax"
        );

        let mut candidate = packet_candidate();
        candidate.szmag1 = Some(17.0);
        assert!(is_star(&candidate));
        candidate.srmag1 = None;
        assert!(
            !is_star(&candidate),
            "a missing r magnitude is not a red color"
        );
    }

    #[test]
    fn test_is_near_brightstar() {
        let mut candidate = packet_candidate();
        candidate.distgaiabright = Some(15.0);
        assert!(is_near_brightstar(&candidate));

        let mut candidate = packet_candidate();
        candidate.srmag2 = Some(14.0);
        assert!(is_near_brightstar(&candidate));

        let mut candidate = packet_candidate();
        candidate.sgscore1 = Some(0.5);
        candidate.distpsnr1 = Some(0.2);
        candidate.simag1 = Some(16.0);
        assert!(is_near_brightstar(&candidate));
    }
}
