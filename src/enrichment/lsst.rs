use crate::alert::LsstCandidate;
use crate::conf::AppConfig;
use crate::enrichment::babamul::{Babamul, EnrichedLsstAlert};
use crate::enrichment::{
    fetch_alert_cutouts, fetch_alerts, EnrichmentWorker, EnrichmentWorkerError, ZtfMatch,
};
use crate::utils::db::mongify;
use crate::utils::enums::Survey;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, Band, PerBandProperties, PhotometryMag,
};
use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use cdshealpix::nested::get;
use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType, MocType};
use moc::moc::range::RangeMOC;
use moc::moc::{CellMOCIntoIterator, CellMOCIterator, HasMaxDepth};
use moc::qty::Hpx;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use std::collections::HashMap;
use std::sync::OnceLock;
use tracing::{debug, error, instrument, warn};

pub const IS_STELLAR_DISTANCE_THRESH_ARCSEC: f64 = 1.0;
pub const IS_NEAR_BRIGHTSTAR_DISTANCE_THRESH_ARCSEC: f64 = 20.0;
pub const IS_NEAR_BRIGHTSTAR_MAG_THRESH: f64 = 15.0;
pub const IS_HOSTED_SCORE_THRESH: f64 = 0.5;
const MOC_FOOTPRINT_PATH: &str = "./data/ls_footprint_moc.fits";
const MOC_DEPTH: u8 = 11;

// Lazy-loaded footprint MOC
static FOOTPRINT_MOC: OnceLock<RangeMOC<u64, Hpx<u64>>> = OnceLock::new();

fn load_footprint_moc() -> RangeMOC<u64, Hpx<u64>> {
    let file = std::fs::File::open(MOC_FOOTPRINT_PATH).expect("Failed to open footprint MOC file");

    let reader = std::io::BufReader::new(file);
    match from_fits_ivoa(reader) {
        Ok(MocIdxType::U64(MocQtyType::Hpx(MocType::Ranges(moc)))) => {
            RangeMOC::new(moc.depth_max(), moc.collect())
        }
        Ok(MocIdxType::U64(MocQtyType::Hpx(MocType::Cells(cell_moc)))) => {
            let depth = cell_moc.depth_max();
            let ranges = cell_moc.into_cell_moc_iter().ranges().collect();
            RangeMOC::new(depth, ranges)
        }
        Ok(_) => {
            panic!("Unexpected MOC type in footprint MOC file");
        }
        Err(e) => {
            panic!("Failed to parse footprint MOC: {}", e);
        }
    }
}

pub fn is_in_footprint(ra_deg: f64, dec_deg: f64) -> bool {
    let moc = FOOTPRINT_MOC.get_or_init(load_footprint_moc);
    let ra_rad = ra_deg.to_radians();
    let dec_rad = dec_deg.to_radians();
    let layer = get(MOC_DEPTH);
    let cell = layer.hash(ra_rad, dec_rad);
    moc.contains_cell(MOC_DEPTH, cell)
}

fn default_lsst_zp() -> Option<f64> {
    Some(8.9)
}

#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LsstPhotometry {
    pub jd: f64,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub diffmaglim: f32,
    #[serde(rename = "psfFlux")]
    pub flux: Option<f64>, // in nJy
    #[serde(rename = "psfFluxErr")]
    pub flux_err: f64, // in nJy
    pub band: Band,
    // Set a default if missing
    #[serde(default = "default_lsst_zp")]
    pub zp: Option<f64>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub snr: Option<f64>,
}

impl LsstPhotometry {
    pub fn to_photometry_mag(&self, min_snr: Option<f64>) -> Option<PhotometryMag> {
        match (self.snr, self.magpsf, self.sigmapsf) {
            (Some(snr), Some(mag), Some(sig)) => match min_snr {
                Some(thresh) if snr.abs() < thresh => None,
                _ => Some(PhotometryMag {
                    time: self.jd,
                    mag,
                    mag_err: sig,
                    band: self.band.clone(),
                }),
            },
            _ => None,
        }
    }
}

pub fn create_lsst_alert_pipeline() -> Vec<Document> {
    vec![
        doc! {
            "$match": {
                "_id": {"$in": []}
            }
        },
        doc! {
            "$lookup": {
                "from": "LSST_alerts_aux",
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
                "from": "ZTF_alerts_aux",
                "localField": "aux.aliases.ZTF.0",
                "foreignField": "_id",
                "as": "ztf_aux"
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "ssObjectId": 1,
                "candidate": 1,
                "prv_candidates": "$aux.prv_candidates",
                "fp_hists": "$aux.fp_hists",
                "cross_matches": "$aux.cross_matches",
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
                    }
                }
            }
        },
    ]
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, AvroSchema)]
pub struct LsstSurveyMatches {
    pub ztf: Option<ZtfMatch>,
}

#[serdavro]
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
pub struct LsstMatch {
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub ra: f64,
    pub dec: f64,
    pub prv_candidates: Vec<LsstPhotometry>,
    pub fp_hists: Vec<LsstPhotometry>,
}

/// LSST alert structure used to deserialize alerts
/// from the database, used by the enrichment worker
/// to compute features and ML scores
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct LsstAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    #[serde(rename = "ssObjectId")]
    pub ss_object_id: Option<String>,
    pub candidate: LsstCandidate,
    pub prv_candidates: Vec<LsstPhotometry>,
    pub fp_hists: Vec<LsstPhotometry>,
    pub cross_matches: Option<HashMap<String, Vec<serde_json::Value>>>,
    pub survey_matches: Option<LsstSurveyMatches>,
}

/// LSST alert properties computed during enrichment and inserted back into the alert document
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct LsstAlertProperties {
    pub rock: bool,
    pub stationary: bool,
    pub star: Option<bool>,
    pub near_brightstar: Option<bool>,
    pub photstats: PerBandProperties,
    pub multisurvey_photstats: PerBandProperties,
}

pub struct LsstEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_cutout_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
    babamul: Option<Babamul>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for LsstEnrichmentWorker {
    #[instrument(err, skip(config_path))]
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("LSST_alerts");
        let alert_cutout_collection = db.collection("LSST_alerts_cutouts");

        let input_queue = "LSST_alerts_enrichment_queue".to_string();
        let output_queue = "LSST_alerts_filter_queue".to_string();

        // Detect if Babamul is enabled from the config
        let babamul_enabled = config.babamul.enabled;
        // If enabled, we need to ensure we have LSPSC cross-matches configured
        // and that the catalog exists in the database
        if babamul_enabled {
            // Require LSST cross-match config to include LSPSC
            let Some(lsst_crossmatch_config) = config.crossmatch.get(&Survey::Lsst) else {
                return Err(EnrichmentWorkerError::ConfigurationError(
                    "Babamul is enabled but no LSST cross-match configuration is present"
                        .to_string(),
                ));
            };
            let lspsc_found = lsst_crossmatch_config
                .iter()
                .any(|xmatch_config| xmatch_config.catalog == "LSPSC");
            if !lspsc_found {
                return Err(EnrichmentWorkerError::ConfigurationError(
                    "Babamul is enabled but LSPSC cross-match is not configured for LSST alerts"
                        .to_string(),
                ));
            }
            // Also require the LSPSC catalog collection to exist in the database
            let collections = db.list_collection_names().await?;
            if !collections.contains(&"LSPSC".to_string()) {
                return Err(EnrichmentWorkerError::ConfigurationError(
                    "Babamul is enabled but the LSPSC catalog does not exist in the database"
                        .to_string(),
                ));
            }
        }
        let babamul: Option<Babamul> = if babamul_enabled {
            Some(Babamul::new(&config))
        } else {
            None
        };
        debug!(input_queue = %input_queue, output_queue = %output_queue, "LSST Enrichment Worker configuration");

        Ok(LsstEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_cutout_collection,
            alert_pipeline: create_lsst_alert_pipeline(),
            babamul,
        })
    }

    fn input_queue_name(&self) -> String {
        self.input_queue.clone()
    }

    fn output_queue_name(&self) -> String {
        self.output_queue.clone()
    }

    #[instrument(skip_all, fields(batch_size = candids.len()), err)]
    async fn process_alerts(
        &mut self,
        candids: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError> {
        let alerts: Vec<LsstAlertForEnrichment> =
            fetch_alerts(&candids, &self.alert_pipeline, &self.alert_collection).await?;

        debug!(
            alerts = alerts.len(),
            candids = candids.len(),
            "fetched alerts"
        );
        if alerts.len() != candids.len() {
            warn!(
                alerts = alerts.len(),
                candids = candids.len(),
                "alert/candid mismatch"
            );
        }

        if alerts.is_empty() {
            return Ok(vec![]);
        }

        let mut candid_to_cutouts = if self.babamul.is_some() {
            fetch_alert_cutouts(&candids, &self.alert_cutout_collection).await?
        } else {
            HashMap::new()
        };

        if self.babamul.is_some() && candid_to_cutouts.len() != alerts.len() {
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
        let mut enriched_alerts: Vec<(
            EnrichedLsstAlert,
            std::collections::HashMap<String, Vec<serde_json::Value>>,
        )> = Vec::new();
        for alert in alerts {
            let candid = alert.candid;

            // Compute numerical and boolean features from lightcurve and candidate analysis
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

            // If Babamul is enabled, add the enriched alert to the batch
            if self.babamul.is_some() {
                let cutouts = candid_to_cutouts
                    .remove(&candid)
                    .ok_or_else(|| EnrichmentWorkerError::MissingCutouts(candid))?;
                let (enriched_alert, cross_matches) =
                    EnrichedLsstAlert::from_alert_properties_and_cutouts(
                        alert,
                        Some(cutouts.cutout_science),
                        Some(cutouts.cutout_template),
                        Some(cutouts.cutout_difference),
                        properties,
                    );
                enriched_alerts.push((enriched_alert, cross_matches));
            }
        }

        let _ = self.client.bulk_write(updates).await?.modified_count;

        // Send to Babamul for batch processing
        match self.babamul.as_ref() {
            Some(babamul) => {
                if let Err(e) = babamul.process_lsst_alerts(enriched_alerts).await {
                    error!("Failed to process enriched alerts in Babamul: {}", e);
                }
            }
            None => {}
        }

        debug!("processed {} alerts", processed_alerts.len());
        Ok(processed_alerts)
    }
}

impl LsstEnrichmentWorker {
    #[instrument(skip(self, alert), fields(candid = alert.candid), err)]
    async fn get_alert_properties(
        &self,
        alert: &LsstAlertForEnrichment,
    ) -> Result<LsstAlertProperties, EnrichmentWorkerError> {
        // Compute numerical and boolean features from lightcurve and candidate analysis
        let is_rock = alert.ss_object_id.is_some();

        // Determine if this is a star based on LSPSC cross-matches
        let mut is_star = Some(false);
        let mut is_near_brightstar = Some(false);

        let empty_vec = vec![];
        let lspsc_matches = alert
            .cross_matches
            .as_ref()
            .and_then(|xmatches| xmatches.get("LSPSC"))
            .unwrap_or(&empty_vec);
        if lspsc_matches.is_empty() {
            if !is_in_footprint(
                alert.candidate.dia_source.ra,
                alert.candidate.dia_source.dec,
            ) {
                is_star = None;
                is_near_brightstar = None;
            }
        } else {
            // Check each LSPSC match for a nearby stellar-like object
            // and for bright stars within a larger radius
            for m in lspsc_matches {
                let distance = match m.get("distance_arcsec").and_then(|v| v.as_f64()) {
                    Some(d) => d,
                    None => continue,
                };
                let score = match m.get("score").and_then(|v| v.as_f64()) {
                    Some(s) => s,
                    None => continue,
                };
                if distance <= IS_STELLAR_DISTANCE_THRESH_ARCSEC && score > IS_HOSTED_SCORE_THRESH {
                    is_star = Some(true);
                }
                let mag_white = match m.get("mag_white").and_then(|v| v.as_f64()) {
                    Some(m) => m,
                    None => continue,
                };
                if distance <= IS_NEAR_BRIGHTSTAR_DISTANCE_THRESH_ARCSEC
                    && score > IS_HOSTED_SCORE_THRESH
                    && mag_white <= IS_NEAR_BRIGHTSTAR_MAG_THRESH
                {
                    is_near_brightstar = Some(true);
                }

                // if the 2 properties we are evaluating are true, we can stop checking
                if is_star == Some(true) && is_near_brightstar == Some(true) {
                    break;
                }
            }
        }

        let prv_candidates: Vec<PhotometryMag> = alert
            .prv_candidates
            .iter()
            .filter(|p| p.jd <= alert.candidate.jd)
            .filter_map(|p| p.to_photometry_mag(None))
            .collect();
        let fp_hists: Vec<PhotometryMag> = alert
            .fp_hists
            .iter()
            .filter(|p| p.jd <= alert.candidate.jd)
            .filter_map(|p| p.to_photometry_mag(Some(3.0)))
            .collect();

        // lightcurve is prv_candidates + fp_hists, no need for parse_photometry here
        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);
        let (photstats, _, stationary) = analyze_photometry(&lightcurve);

        // Compute multisurvey photstats (including ZTF if available, other surveys can be added later)
        let mut has_matches = false;
        if let Some(survey_matches) = &alert.survey_matches {
            if let Some(ztf_match) = &survey_matches.ztf {
                let ztf_prv_candidates: Vec<PhotometryMag> = ztf_match
                    .prv_candidates
                    .iter()
                    .filter(|p| p.jd <= alert.candidate.jd)
                    .filter_map(|p| p.to_photometry_mag(None))
                    .collect();
                let ztf_fp_hists: Vec<PhotometryMag> = ztf_match
                    .fp_hists
                    .iter()
                    .filter(|p| p.jd <= alert.candidate.jd)
                    .filter_map(|p| p.to_photometry_mag(Some(3.0)))
                    .collect();
                let mut ztf_lightcurve = [ztf_prv_candidates, ztf_fp_hists].concat();
                prepare_photometry(&mut ztf_lightcurve);
                lightcurve.extend(ztf_lightcurve);
                has_matches = true;
            }
        }
        let multisurvey_photstats = if has_matches {
            analyze_photometry(&lightcurve).0
        } else {
            photstats.clone()
        };

        Ok(LsstAlertProperties {
            rock: is_rock,
            star: is_star,
            near_brightstar: is_near_brightstar,
            stationary,
            photstats,
            multisurvey_photstats,
        })
    }
}
