use crate::enrichment::models::{AcaiModel, BtsBotModel, Model};
use crate::enrichment::{EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::fetch_timeseries_op;
use crate::utils::lightcurves::{analyze_photometry, parse_photometry};
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

pub struct ZtfEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_cutout_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_pipeline: Vec<Document>,
    acai_h_model: AcaiModel,
    acai_n_model: AcaiModel,
    acai_v_model: AcaiModel,
    acai_o_model: AcaiModel,
    acai_b_model: AcaiModel,
    btsbot_model: BtsBotModel,
}

#[async_trait::async_trait]
impl EnrichmentWorker for ZtfEnrichmentWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError> {
        let config_file = crate::conf::load_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let client = db.client().clone();
        let alert_collection = db.collection("ZTF_alerts");
        let alert_cutout_collection = db.collection("ZTF_alerts_cutouts");

        let alert_pipeline = vec![
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
            doc! {
                "$project": doc! {
                    "objectId": 1,
                    "candidate": 1,
                    "prv_candidates": fetch_timeseries_op(
                        "aux.prv_candidates",
                        "candidate.jd",
                        365,
                        None
                    )
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
                }
            },
        ];

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

        Ok(ZtfEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_cutout_collection,
            alert_pipeline,
            acai_h_model,
            acai_n_model,
            acai_v_model,
            acai_o_model,
            acai_b_model,
            btsbot_model,
        })
    }

    fn input_queue_name(&self) -> String {
        self.input_queue.clone()
    }

    fn output_queue_name(&self) -> String {
        self.output_queue.clone()
    }

    #[instrument(skip_all, err)]
    async fn fetch_alerts(
        &self,
        candids: &[i64], // this is a slice of candids to process
    ) -> Result<Vec<Document>, EnrichmentWorkerError> {
        let mut alert_pipeline = self.alert_pipeline.clone();
        if let Some(first_stage) = alert_pipeline.first_mut() {
            *first_stage = doc! {
                "$match": {
                    "_id": {"$in": candids}
                }
            };
        }
        let mut alert_cursor = self.alert_collection.aggregate(alert_pipeline).await?;

        let mut alerts: Vec<Document> = Vec::new();
        let mut candid_to_idx = std::collections::HashMap::new();
        let mut count = 0;
        while let Some(result) = alert_cursor.next().await {
            match result {
                Ok(document) => {
                    alerts.push(document);
                    let candid = alerts[count].get_i64("_id")?;
                    candid_to_idx.insert(candid, count);
                    count += 1;
                }
                _ => {
                    continue;
                }
            }
        }

        // next we fetch cutouts from the cutout collection
        let mut cutout_cursor = self
            .alert_cutout_collection
            .find(doc! {
                "_id": {"$in": candids}
            })
            .await?;
        while let Some(result) = cutout_cursor.next().await {
            match result {
                Ok(cutout_doc) => {
                    let candid = cutout_doc.get_i64("_id")?;
                    if let Some(idx) = candid_to_idx.get(&candid) {
                        alerts[*idx]
                            .insert("cutoutScience", cutout_doc.get("cutoutScience").unwrap());
                        alerts[*idx]
                            .insert("cutoutTemplate", cutout_doc.get("cutoutTemplate").unwrap());
                        alerts[*idx].insert(
                            "cutoutDifference",
                            cutout_doc.get("cutoutDifference").unwrap(),
                        );
                    }
                }
                _ => {
                    continue;
                }
            }
        }

        Ok(alerts)
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(
        &mut self,
        candids: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError> {
        let alerts = self.fetch_alerts(&candids).await?;

        if alerts.len() != candids.len() {
            warn!(
                "FEATURE WORKER: only {} alerts fetched from {} candids",
                alerts.len(),
                candids.len()
            );
        }

        if alerts.is_empty() {
            return Ok(vec![]);
        }

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for i in 0..alerts.len() {
            let candid = alerts[i].get_i64("_id")?;

            // Compute numerical and boolean features from lightcurve and candidate analysis
            let candidate = alerts[i].get_document("candidate")?;

            let jd = candidate.get_f64("jd")?;
            let ssdistnr = candidate.get_f64("ssdistnr").unwrap_or(-999.0);
            let ssmagnr = candidate.get_f64("ssmagnr").unwrap_or(-999.0);

            let is_rock = ssdistnr >= 0.0 && ssdistnr < 12.0 && ssmagnr >= 0.0;

            let sgscore1 = candidate.get_f64("sgscore1").unwrap_or(0.0);
            let sgscore2 = candidate.get_f64("sgscore2").unwrap_or(0.0);
            let sgscore3 = candidate.get_f64("sgscore3").unwrap_or(0.0);
            let distpsnr1 = candidate.get_f64("distpsnr1").unwrap_or(f64::INFINITY);
            let distpsnr2 = candidate.get_f64("distpsnr2").unwrap_or(f64::INFINITY);
            let distpsnr3 = candidate.get_f64("distpsnr3").unwrap_or(f64::INFINITY);

            let srmag1 = candidate.get_f64("srmag1").unwrap_or(f64::INFINITY);
            let srmag2 = candidate.get_f64("srmag2").unwrap_or(f64::INFINITY);
            let srmag3 = candidate.get_f64("srmag3").unwrap_or(f64::INFINITY);
            let sgmag1 = candidate.get_f64("sgmag1").unwrap_or(f64::INFINITY);
            let simag1 = candidate.get_f64("simag1").unwrap_or(f64::INFINITY);

            let is_star = sgscore1 > 0.76 && distpsnr1 >= 0.0 && distpsnr1 <= 2.0;

            let is_near_brightstar =
                (sgscore1 > 0.49 && distpsnr1 <= 20.0 && srmag1 > 0.0 && srmag1 <= 15.0)
                    || (sgscore2 > 0.49 && distpsnr2 <= 20.0 && srmag2 > 0.0 && srmag2 <= 15.0)
                    || (sgscore3 > 0.49 && distpsnr3 <= 20.0 && srmag3 > 0.0 && srmag3 <= 15.0)
                    || (sgscore1 == 0.5
                        && distpsnr1 < 0.5
                        && (sgmag1 < 17.0 || srmag1 < 17.0 || simag1 < 17.0));

            let prv_candidates = alerts[i].get_array("prv_candidates")?;

            let lightcurve =
                parse_photometry(prv_candidates, "jd", "magpsf", "sigmapsf", "band", jd);

            let (photstats, all_bands_properties, stationary) = analyze_photometry(lightcurve, jd);

            let programid = candidate.get_i32("programid")?;

            // Now, prepare inputs for ML models and run inference
            let metadata = self.acai_h_model.get_metadata(&alerts[i..i + 1])?;
            let triplet = self.acai_h_model.get_triplet(&alerts[i..i + 1])?;

            let acai_h_scores = self.acai_h_model.predict(&metadata, &triplet)?;
            let acai_n_scores = self.acai_n_model.predict(&metadata, &triplet)?;
            let acai_v_scores = self.acai_v_model.predict(&metadata, &triplet)?;
            let acai_o_scores = self.acai_o_model.predict(&metadata, &triplet)?;
            let acai_b_scores = self.acai_b_model.predict(&metadata, &triplet)?;

            let metadata_btsbot = self
                .btsbot_model
                .get_metadata(&alerts[i..i + 1], &[all_bands_properties])?;
            let btsbot_scores = self.btsbot_model.predict(&metadata_btsbot, &triplet)?;

            let find_document = doc! {
                "_id": candid
            };

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
                    "properties.rock": is_rock,
                    "properties.star": is_star,
                    "properties.near_brightstar": is_near_brightstar,
                    "properties.stationary": stationary,
                    "properties.photstats": photstats,
                }
            };

            let update = WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(find_document)
                    .update(update_alert_document)
                    .build(),
            );

            updates.push(update);
            processed_alerts.push(format!("{},{}", programid, candid));
        }

        let _ = self.client.bulk_write(updates).await?.modified_count;

        Ok(processed_alerts)
    }
}
