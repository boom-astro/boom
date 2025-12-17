use crate::alert::LsstCandidate;
use crate::conf::AppConfig;
use crate::enrichment::babamul::{Babamul, EnrichedLsstAlert};
use crate::enrichment::{
    fetch_alert_cutouts, fetch_alerts, EnrichmentWorker, EnrichmentWorkerError, ZtfMatch,
};
use crate::utils::db::{get_array_dict_element, get_array_element, mongify};
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, Band, PerBandProperties, PhotometryMag,
};
use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use schemars::JsonSchema;
use std::collections::HashMap;
use tracing::{error, instrument, warn};

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
    // set a default of 23.9 for zp if missing
    #[serde(default = "default_lsst_zp")]
    pub zp: Option<f64>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub snr: Option<f64>,
}

// same for LsstPhotometry
impl LsstPhotometry {
    pub fn to_photometry_mag(&self) -> Option<PhotometryMag> {
        // if the abs value of the snr > 3 and magpsf is Some, we return Some(PhotometryMag)
        match (self.snr, self.magpsf, self.sigmapsf) {
            (Some(snr), Some(mag), Some(sig)) if snr.abs() > 3.0 => Some(PhotometryMag {
                time: self.jd,
                mag,
                mag_err: sig,
                band: self.band.clone(),
            }),
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
            "$project": {
                "objectId": 1,
                "candidate": 1,
            }
        },
        doc! {
            "$lookup": {
                "from": "LSST_alerts_aux",
                "let": { "obj_id": "$objectId" },
                "pipeline": [
                    doc! {
                        "$match": {
                            "$expr": {
                                "$eq": [ "$_id", "$$obj_id" ]
                            }
                        }
                    },
                    doc! {
                        "$project": {
                            // prv_candidates
                            "prv_candidates.jd": 1,
                            "prv_candidates.magpsf": 1,
                            "prv_candidates.sigmapsf": 1,
                            "prv_candidates.diffmaglim": 1,
                            "prv_candidates.band": 1,
                            "prv_candidates.psfFlux": 1,
                            "prv_candidates.psfFluxErr": 1,
                            "prv_candidates.ra": 1,
                            "prv_candidates.dec": 1,
                            "prv_candidates.snr": 1,
                            // fp_hists
                            "fp_hists.jd": 1,
                            "fp_hists.magpsf": 1,
                            "fp_hists.sigmapsf": 1,
                            "fp_hists.diffmaglim": 1,
                            "fp_hists.band": 1,
                            "fp_hists.psfFlux": 1,
                            "fp_hists.psfFluxErr": 1,
                            "fp_hists.zp": 1,
                            "fp_hists.snr": 1,
                            // aliases
                            "aliases": 1,
                        }
                    }
                ],
                "as": "aux"
            }
        },
        doc! {
            "$addFields": {
                "prv_candidates": get_array_element("aux.prv_candidates"),
                "fp_hists": get_array_element("aux.fp_hists"),
                "aliases": get_array_dict_element("aux.aliases"),
                "aux": mongodb::bson::Bson::Null,
            }
        },
        doc! {
            // same here, we do a pipeline to be more efficient
            "$lookup": {
                "from": "ZTF_alerts_aux",
                "let": { "ztf_obj_id": { "$arrayElemAt": [ "$aliases.ZTF", 0 ] } },
                "pipeline": [
                    doc! {
                        "$match": {
                            "$expr": {
                                "$eq": [ "$_id", "$$ztf_obj_id" ]
                            }
                        }
                    },
                    doc! {
                        "$project": {
                            // prv_candidates
                            "prv_candidates.jd": 1,
                            "prv_candidates.magpsf": 1,
                            "prv_candidates.sigmapsf": 1,
                            "prv_candidates.diffmaglim": 1,
                            "prv_candidates.band": 1,
                            "prv_candidates.psfFlux": 1,
                            "prv_candidates.psfFluxErr": 1,
                            "prv_candidates.ra": 1,
                            "prv_candidates.dec": 1,
                            "prv_candidates.programid": 1,
                            "prv_candidates.snr": 1,
                            // prv_nondetections
                            "prv_nondetections.jd": 1,
                            "prv_nondetections.diffmaglim": 1,
                            "prv_nondetections.band": 1,
                            "prv_nondetections.psfFluxErr": 1,
                            "prv_nondetections.programid": 1,
                            // fp_hists
                            "fp_hists.jd": 1,
                            "fp_hists.magpsf": 1,
                            "fp_hists.sigmapsf": 1,
                            "fp_hists.diffmaglim": 1,
                            "fp_hists.band": 1,
                            "fp_hists.psfFlux": 1,
                            "fp_hists.psfFluxErr": 1,
                            "fp_hists.zp": 1,
                            "fp_hists.programid": 1,
                            "fp_hists.snr": 1,
                            // grab the ra from coordinates.radec_geojson
                            "ra": { "$add": [
                                { "$arrayElemAt": [ "$coordinates.radec_geojson.coordinates", 0 ] },
                                180
                            ] },
                            "dec": { "$arrayElemAt": [ "$coordinates.radec_geojson.coordinates", 1 ] },
                        }
                    }
                ],
                "as": "ztf_aux"
            }
        },
        doc! {
            "$addFields": {
                "survey_matches.ztf": {
                    "$cond": {
                        "if": { "$gt": [ { "$size": "$ztf_aux" }, 0 ] },
                        "then": {
                            "object_id": { "$arrayElemAt": [ "$ztf_aux._id", 0 ] },
                            "prv_candidates": get_array_element("ztf_aux.prv_candidates"),
                            "prv_nondetections": get_array_element("ztf_aux.prv_nondetections"),
                            "fp_hists": get_array_element("ztf_aux.fp_hists"),
                            "ra": { "$arrayElemAt": [ "$ztf_aux.ra", 0 ] },
                            "dec": { "$arrayElemAt": [ "$ztf_aux.dec", 0 ] },
                        },
                        "else": null
                    }
                },
                "ztf_aux": mongodb::bson::Bson::Null,
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates":  1,
                "fp_hists": 1,
                "survey_matches": 1,
            }
        },
    ]
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, AvroSchema)]
pub struct LsstSurveyMatches {
    pub ztf: Option<ZtfMatch>,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, AvroSchema)]
pub struct LsstMatch {
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
    pub candidate: LsstCandidate,
    pub prv_candidates: Vec<LsstPhotometry>,
    pub fp_hists: Vec<LsstPhotometry>,
    pub survey_matches: Option<LsstSurveyMatches>,
}

/// LSST alert properties computed during enrichment and inserted back into the alert document
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, JsonSchema)]
pub struct LsstAlertProperties {
    pub rock: bool,
    pub stationary: bool,
    pub photstats: PerBandProperties,
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
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("LSST_alerts");
        let alert_cutout_collection = db.collection("LSST_alerts_cutouts");

        let input_queue = "LSST_alerts_enrichment_queue".to_string();
        let output_queue = "LSST_alerts_filter_queue".to_string();

        // Detect if Babamul is enabled from the config
        let babamul: Option<Babamul> = if config.babamul.enabled {
            Some(Babamul::new(&config))
        } else {
            None
        };

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

    #[instrument(skip_all, err)]
    async fn process_alerts(
        &mut self,
        candids: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError> {
        let alerts: Vec<LsstAlertForEnrichment> =
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

        let mut candid_to_cutouts = if self.babamul.is_some() {
            fetch_alert_cutouts(&candids, &self.alert_cutout_collection).await?
        } else {
            HashMap::new()
        };

        if candid_to_cutouts.len() != alerts.len() {
            warn!(
                "only {} cutouts fetched from {} candids",
                candid_to_cutouts.len(),
                alerts.len()
            );
        }

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        let mut enriched_alerts: Vec<EnrichedLsstAlert> = Vec::new();
        for alert in alerts {
            let candid = alert.candid;

            // Compute numerical and boolean features from lightcurve and candidate analysis
            let properties = self.get_alert_properties(&alert).await?;

            let update_alert_document = doc! {
                "$set": {
                    "properties": mongify(&properties),
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
                let enriched_alert = EnrichedLsstAlert::from_alert_properties_and_cutouts(
                    alert,
                    Some(cutouts.cutout_science),
                    Some(cutouts.cutout_template),
                    Some(cutouts.cutout_difference),
                    properties,
                );
                enriched_alerts.push(enriched_alert);
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

        Ok(processed_alerts)
    }
}

impl LsstEnrichmentWorker {
    async fn get_alert_properties(
        &self,
        alert: &LsstAlertForEnrichment,
    ) -> Result<LsstAlertProperties, EnrichmentWorkerError> {
        // Compute numerical and boolean features from lightcurve and candidate analysis
        let candidate = &alert.candidate;

        let is_rock = candidate.is_sso;

        let prv_candidates: Vec<PhotometryMag> = alert
            .prv_candidates
            .iter()
            .filter_map(|p| p.to_photometry_mag())
            .collect();
        let fp_hists: Vec<PhotometryMag> = alert
            .fp_hists
            .iter()
            .filter_map(|p| p.to_photometry_mag())
            .collect();

        // lightcurve is prv_candidates + fp_hists, no need for parse_photometry here
        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);
        let (photstats, _, stationary) = analyze_photometry(&lightcurve);

        Ok(LsstAlertProperties {
            rock: is_rock,
            stationary,
            photstats,
        })
    }
}
