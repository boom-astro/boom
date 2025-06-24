use crate::ml::models::{AcaiModel, BtsBotModel, Model};
use crate::ml::{MLWorker, MLWorkerError};
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

pub struct ZtfMLWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    acai_h_model: AcaiModel,
    acai_n_model: AcaiModel,
    acai_v_model: AcaiModel,
    acai_o_model: AcaiModel,
    acai_b_model: AcaiModel,
    btsbot_model: BtsBotModel,
}

#[async_trait::async_trait]
impl MLWorker for ZtfMLWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<Self, MLWorkerError> {
        let config_file = crate::conf::load_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let client = db.client().clone();
        let alert_collection = db.collection("ZTF_alerts");

        let input_queue = "ZTF_alerts_classifier_queue".to_string();
        let output_queue = "ZTF_alerts_filter_queue".to_string();

        // we load the ACAI models (same architecture, same input/output)
        let acai_h_model = AcaiModel::new("data/models/acai_h.d1_dnn_20201130.onnx")?;
        let acai_n_model = AcaiModel::new("data/models/acai_n.d1_dnn_20201130.onnx")?;
        let acai_v_model = AcaiModel::new("data/models/acai_v.d1_dnn_20201130.onnx")?;
        let acai_o_model = AcaiModel::new("data/models/acai_o.d1_dnn_20201130.onnx")?;
        let acai_b_model = AcaiModel::new("data/models/acai_b.d1_dnn_20201130.onnx")?;

        // we load the btsbot model (different architecture, and input/output then ACAI)
        let btsbot_model = BtsBotModel::new("data/models/btsbot-v1.0.1.onnx")?;

        Ok(ZtfMLWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
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
    ) -> Result<Vec<Document>, MLWorkerError> {
        let mut alert_cursor = self
            .alert_collection
            .aggregate(vec![
                doc! {
                    "$match": {
                        "_id": {"$in": candids}
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
                    "$lookup": {
                        "from": "ZTF_alerts_cutouts",
                        "localField": "_id",
                        "foreignField": "_id",
                        "as": "object"
                    }
                },
                doc! {
                    "$project": doc! {
                        "objectId": 1,
                        "candidate": 1,
                        "prv_candidates": doc! {
                            "$filter": doc! {
                                "input": doc! {
                                    "$arrayElemAt": [
                                        "$aux.prv_candidates",
                                        0
                                    ]
                                },
                                "as": "x",
                                "cond": doc! {
                                    "$and": [
                                        {
                                            "$lt": [
                                                {
                                                    "$subtract": [
                                                        "$candidate.jd",
                                                        "$$x.jd"
                                                    ]
                                                },
                                                365
                                            ]
                                        },
                                        {
                                            "$gte": [
                                                {
                                                    "$subtract": [
                                                        "$candidate.jd",
                                                        "$$x.jd"
                                                    ]
                                                },
                                                0
                                            ]
                                        },

                                    ]
                                }
                            }
                        },
                        "cutoutScience": doc! {
                            "$arrayElemAt": [
                                "$object.cutoutScience",
                                0
                            ]
                        },
                        "cutoutTemplate": doc! {
                            "$arrayElemAt": [
                                "$object.cutoutTemplate",
                                0
                            ]
                        },
                        "cutoutDifference": doc! {
                            "$arrayElemAt": [
                                "$object.cutoutDifference",
                                0
                            ]
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
                        "cutoutScience": 1,
                        "cutoutTemplate": 1,
                        "cutoutDifference": 1
                    }
                },
            ])
            .await?;

        let mut alerts: Vec<Document> = Vec::new();
        while let Some(result) = alert_cursor.next().await {
            match result {
                Ok(document) => {
                    alerts.push(document);
                }
                _ => {
                    continue;
                }
            }
        }

        Ok(alerts)
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(&mut self, candids: &[i64]) -> Result<Vec<String>, MLWorkerError> {
        let alerts = self.fetch_alerts(&candids).await?;

        if alerts.len() != candids.len() {
            warn!(
                "ML WORKER: only {} alerts fetched from {} candids",
                alerts.len(),
                candids.len()
            );
        }

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for i in 0..alerts.len() {
            let candid = alerts[i].get_i64("_id")?;
            let programid = alerts[i].get_document("candidate")?.get_i32("programid")?;

            let metadata = self.acai_h_model.get_metadata(&alerts[i..i + 1])?;
            let triplet = self.acai_h_model.get_triplet(&alerts[i..i + 1])?;

            let acai_h_scores = self.acai_h_model.predict(&metadata, &triplet)?;
            let acai_n_scores = self.acai_n_model.predict(&metadata, &triplet)?;
            let acai_v_scores = self.acai_v_model.predict(&metadata, &triplet)?;
            let acai_o_scores = self.acai_o_model.predict(&metadata, &triplet)?;
            let acai_b_scores = self.acai_b_model.predict(&metadata, &triplet)?;

            let metadata_btsbot = self.btsbot_model.get_metadata(&alerts[i..i + 1])?;
            let btsbot_scores = self.btsbot_model.predict(&metadata_btsbot, &triplet)?;

            let find_document = doc! {
                "_id": candid
            };

            let update_alert_document = doc! {
                "$set": {
                    "classifications.acai_h": acai_h_scores[0],
                    "classifications.acai_n": acai_n_scores[0],
                    "classifications.acai_v": acai_v_scores[0],
                    "classifications.acai_o": acai_o_scores[0],
                    "classifications.acai_b": acai_b_scores[0],
                    "classifications.btsbot": btsbot_scores[0]
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
