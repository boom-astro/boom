use std::collections::HashMap;

use crate::conf::AppConfig;
use crate::enrichment::babamul::{Babamul, EnrichedZtfAlert};
use crate::enrichment::base::CrossMatch;
use crate::utils::db::{fetch_timeseries_op, get_array_element, mongify};
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, AllBandsProperties, PerBandProperties, PhotometryMag,
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
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use schemars::JsonSchema;
use tracing::{instrument, warn};

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
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        // Lookup LSST cross-matches
        doc! {
            "$lookup": {
                "from": "LSST_alerts_aux",
                "let": { "lsst_ids": doc! { "$arrayElemAt": ["$aux.cross_matches.LSST", 0] } },
                "pipeline": [
                    doc! { "$match": { "$expr": { "$in": ["$_id", "$$lsst_ids"] } } },
                    doc! {
                        "$project": {
                            "_id": 1,
                            "prv_candidates": 1,
                            "fp_hists": 1,
                        }
                    }
                ],
                "as": "lsst_xmatches"
            }
        },
        // Lookup DECAM cross-matches
        doc! {
            "$lookup": {
                "from": "DECAM_alerts_aux",
                "let": { "decam_ids": doc! { "$arrayElemAt": ["$aux.cross_matches.DECAM", 0] } },
                "pipeline": [
                    doc! { "$match": { "$expr": { "$in": ["$_id", "$$decam_ids"] } } },
                    doc! {
                        "$project": {
                            "_id": 1,
                            "prv_candidates": 1,
                            "fp_hists": 1,
                        }
                    }
                ],
                "as": "decam_xmatches"
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
                            3.0
                        ]
                    }]),
                ),
                "aliases": get_array_element("aux.aliases"),
                "cross_matches": doc! {
                    "LSST": doc! {
                        "$map": {
                            "input": "$lsst_xmatches",
                            "as": "obj",
                            "in": doc! {
                                "survey": "LSST",
                                "object_id": "$$obj._id",
                                "prv_candidates": fetch_timeseries_op(
                                    "$$obj.prv_candidates",
                                    "jd",
                                    365,
                                    None
                                ),
                                "fp_hists": fetch_timeseries_op(
                                    "$$obj.fp_hists",
                                    "jd",
                                    365,
                                    Some(vec![doc! {
                                        "$gte": [
                                            "$$x.snr",
                                            3.0
                                        ]
                                    }]),
                                ),
                            }
                        }
                    },
                    "DECAM": doc! {
                        "$map": {
                            "input": "$decam_xmatches",
                            "as": "obj",
                            "in": doc! {
                                "survey": "DECAM",
                                "object_id": "$$obj._id",
                                "prv_candidates": fetch_timeseries_op(
                                    "$$obj.prv_candidates",
                                    "jd",
                                    365,
                                    None
                                ),
                                "fp_hists": fetch_timeseries_op(
                                    "$$obj.fp_hists",
                                    "jd",
                                    365,
                                    Some(vec![doc! {
                                        "$gte": [
                                            "$$x.snr",
                                            3.0
                                        ]
                                    }]),
                                ),
                            }
                        }
                    },
                }
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
                "fp_hists.jd": 1,
                "fp_hists.magpsf": 1,
                "fp_hists.sigmapsf": 1,
                "fp_hists.band": 1,
                "cross_matches": 1,
            }
        },
    ]
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
    pub prv_candidates: Vec<PhotometryMag>,
    pub fp_hists: Vec<PhotometryMag>,
    pub cross_matches: Option<HashMap<String, Vec<CrossMatch>>>,
}

/// ZTF alert properties computed during enrichment and inserted back into the alert document
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, JsonSchema)]
pub struct ZtfAlertProperties {
    pub rock: bool,
    pub star: bool,
    pub near_brightstar: bool,
    pub stationary: bool,
    pub photstats: PerBandProperties,
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

        let prv_candidates = alert.prv_candidates.clone();
        let fp_hists = alert.fp_hists.clone();

        // lightcurve is prv_candidates + fp_hists, no need for parse_photometry here
        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);
        let (photstats, all_bands_properties, stationary) = analyze_photometry(&lightcurve);

        Ok((
            ZtfAlertProperties {
                rock: is_rock,
                star: is_star,
                near_brightstar: is_near_brightstar,
                stationary,
                photstats,
            },
            all_bands_properties,
            programid,
            lightcurve,
        ))
    }
}
