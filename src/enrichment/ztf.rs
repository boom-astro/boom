use crate::conf::AppConfig;
use crate::enrichment::babamul::{Babamul, EnrichedZtfAlert};
use crate::enrichment::LsstMatch;
use crate::utils::db::{get_array_dict_element, get_array_element, mongify};
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, AllBandsProperties, Band, PerBandProperties,
    PhotometryMag,
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
use schemars::JsonSchema;
use tracing::{instrument, warn};

fn default_ztf_zp() -> Option<f64> {
    Some(23.9)
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
    // set a default of 23.9 for zp if missing
    #[serde(default = "default_ztf_zp")]
    pub zp: Option<f64>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub snr: Option<f64>,
    pub programid: i32,
}

// it should return an optional PhotometryMag
impl ZtfPhotometry {
    pub fn to_photometry_mag(&self) -> Option<PhotometryMag> {
        // if the abs value of the snr > 3 and magpsf is Some, we return Some(PhotometryMag)
        println!(
            "ZtfPhotometry to_photometry_mag: jd={}, magpsf={:?}, sigmapsf={:?}, snr={:?}",
            self.jd, self.magpsf, self.sigmapsf, self.snr
        );
        match (self.snr, self.magpsf, self.sigmapsf) {
            (Some(snr), Some(mag), Some(sig)) if snr.abs() > 3.0 => Some(PhotometryMag {
                time: self.jd,
                mag: mag as f32,
                mag_err: sig as f32,
                band: self.band.clone(),
            }),
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
            "$project": {
                "objectId": 1,
                "candidate": 1,
            }
        },
        doc! {
            "$lookup": {
                "from": "ZTF_alerts_aux",
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
                "prv_nondetections": get_array_element("aux.prv_nondetections"),
                "fp_hists": get_array_element("aux.fp_hists"),
                "aliases": get_array_dict_element("aux.aliases"),
                "aux": mongodb::bson::Bson::Null,
            }
        },
        doc! {
            // same here, we do a pipeline to be more efficient
            "$lookup": {
                "from": "LSST_alerts_aux",
                "let": { "lsst_obj_id": { "$arrayElemAt": [ "$aliases.LSST", 0 ] } },
                "pipeline": [
                    doc! {
                        "$match": {
                            "$expr": {
                                "$eq": [ "$_id", "$$lsst_obj_id" ]
                            }
                        }
                    },
                    doc! {
                        "$project": {
                            // prv_candidates up to 365 days
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
                            // fp_hists up to 365 days
                            "fp_hists.jd": 1,
                            "fp_hists.magpsf": 1,
                            "fp_hists.sigmapsf": 1,
                            "fp_hists.diffmaglim": 1,
                            "fp_hists.band": 1,
                            "fp_hists.psfFlux": 1,
                            "fp_hists.psfFluxErr": 1,
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
                "as": "lsst_aux"
            }
        },
        doc! {
            "$addFields": {
                "survey_matches.lsst": {
                    "$cond": {
                        "if": { "$gt": [ { "$size": "$lsst_aux" }, 0 ] },
                        "then": {
                            "object_id": { "$arrayElemAt": [ "$lsst_aux._id", 0 ] },
                            "prv_candidates": get_array_element("lsst_aux.prv_candidates"),
                            "prv_nondetections": get_array_element("lsst_aux.prv_nondetections"),
                            "fp_hists": get_array_element("lsst_aux.fp_hists"),
                            "ra": { "$arrayElemAt": [ "$lsst_aux.ra", 0 ] },
                            "dec": { "$arrayElemAt": [ "$lsst_aux.dec", 0 ] },
                        },
                        "else": null
                    }
                },
                "lsst_aux": mongodb::bson::Bson::Null,
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates":  1,
                "prv_nondetections": 1,
                "fp_hists": 1,
                "survey_matches": 1,
            }
        },
    ]
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, AvroSchema)]
pub struct ZtfSurveyMatches {
    pub lsst: Option<LsstMatch>,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, AvroSchema)]
pub struct ZtfMatch {
    // #[serde(rename = "_id")]
    pub object_id: String,
    pub ra: f64,
    pub dec: f64,
    pub prv_candidates: Vec<ZtfPhotometry>,
    pub prv_nondetections: Vec<ZtfPhotometry>,
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
    pub prv_candidates: Vec<ZtfPhotometry>,
    pub prv_nondetections: Vec<ZtfPhotometry>,
    pub fp_hists: Vec<ZtfPhotometry>,
    pub survey_matches: Option<ZtfSurveyMatches>,
}

/// ZTF alert properties computed during enrichment and inserted back into the alert document
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, JsonSchema)]
pub struct ZtfAlertProperties {
    pub rock: bool,
    pub star: bool,
    pub near_brightstar: bool,
    pub stationary: bool,
    pub photstats: PerBandProperties,
    pub multisurvey_photstats: PerBandProperties,
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

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        let mut enriched_alerts: Vec<EnrichedZtfAlert> = Vec::new();
        for alert in alerts {
            let candid = alert.candid;
            let cutouts = candid_to_cutouts
                .remove(&candid)
                .ok_or_else(|| EnrichmentWorkerError::MissingCutouts(candid))?;

            // Compute numerical and boolean features from lightcurve and candidate analysis
            let (properties, all_bands_properties, programid, _lightcurve) =
                self.get_alert_properties(&alert).await?;

            // Now, prepare inputs for ML models and run inference
            let metadata = self.acai_h_model.get_metadata(&[&alert])?;
            let triplet = self.acai_h_model.get_triplet(&[&cutouts])?;

            let acai_h_scores = self.acai_h_model.predict(&metadata, &triplet)?;
            let acai_n_scores = self.acai_n_model.predict(&metadata, &triplet)?;
            let acai_v_scores = self.acai_v_model.predict(&metadata, &triplet)?;
            let acai_o_scores = self.acai_o_model.predict(&metadata, &triplet)?;
            let acai_b_scores = self.acai_b_model.predict(&metadata, &triplet)?;

            let metadata_btsbot = self
                .btsbot_model
                .get_metadata(&[&alert], &[all_bands_properties])?;
            let btsbot_scores = self.btsbot_model.predict(&metadata_btsbot, &triplet)?;

            let update_alert_document = doc! {
                "$set": {
                    // ML scores
                    "classifications.acai_h": acai_h_scores[0],
                    "classifications.acai_n": acai_n_scores[0],
                    "classifications.acai_v": acai_v_scores[0],
                    "classifications.acai_o": acai_o_scores[0],
                    "classifications.acai_b": acai_b_scores[0],
                    "classifications.btsbot": btsbot_scores[0],
                    // properties
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
            processed_alerts.push(format!("{},{}", programid, candid));

            // If Babamul is enabled, add the enriched alert to the batch
            if self.babamul.is_some() {
                let enriched_alert = EnrichedZtfAlert::from_alert_properties_and_cutouts(
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
        let (photstats, all_bands_properties, stationary) = analyze_photometry(&lightcurve);

        // make a multisurvey lightcurve if we have LSST matches
        let multisurvey_photstats = if let Some(survey_matches) = &alert.survey_matches {
            if let Some(lsst_match) = &survey_matches.lsst {
                let lsst_prv_candidates: Vec<PhotometryMag> = lsst_match
                    .prv_candidates
                    .iter()
                    .filter_map(|p| p.to_photometry_mag())
                    .collect();
                let lsst_fp_hists: Vec<PhotometryMag> = lsst_match
                    .fp_hists
                    .iter()
                    .filter_map(|p| p.to_photometry_mag())
                    .collect();
                let mut lsst_lightcurve = [lsst_prv_candidates, lsst_fp_hists].concat();
                prepare_photometry(&mut lsst_lightcurve);
                lightcurve.extend(lsst_lightcurve);
            }
            analyze_photometry(&lightcurve).0
        } else {
            PerBandProperties::default()
        };

        Ok((
            ZtfAlertProperties {
                rock: is_rock,
                star: is_star,
                near_brightstar: is_near_brightstar,
                stationary,
                photstats,
                multisurvey_photstats,
            },
            all_bands_properties,
            programid,
            lightcurve,
        ))
    }
}
