use crate::utils::worker::WorkerCmd;
use mongodb::options::{UpdateOneModel, WriteModel};
use redis::AsyncCommands;
use std::num::NonZero;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{info, warn};

use crate::ml::models::{AcaiModel, BtsBotModel, Model};
use crate::ml::{MLWorker, MLWorkerError};
use futures::StreamExt;
use mongodb::bson::{doc, Document};

pub struct ZtfMLWorker {
    id: String,
    receiver: mpsc::Receiver<WorkerCmd>,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
}

#[async_trait::async_trait]
impl MLWorker for ZtfMLWorker {
    async fn new(
        id: String,
        receiver: mpsc::Receiver<WorkerCmd>,
        config_path: &str,
    ) -> Result<Self, MLWorkerError> {
        let config_file = crate::conf::load_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let client = db.client().clone();
        let alert_collection = db.collection("ZTF_alerts");
        Ok(ZtfMLWorker {
            id,
            receiver,
            client,
            alert_collection,
        })
    }

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
            ])
            .await
            .unwrap();

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

    async fn run(&mut self) -> Result<(), MLWorkerError> {
        let client_redis = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
        let mut con = client_redis
            .get_multiplexed_async_connection()
            .await
            .map_err(MLWorkerError::ConnectRedisError)?;

        let queue = "ZTF_alerts_classifier_queue".to_string();
        let output_queue = "ZTF_alerts_filter_queue".to_string();

        let command_interval: i64 = 500;
        let mut command_check_countdown = command_interval;
        let mut count = 0;

        // we load the ACAI models (same architecture, same input/output)
        let acai_h_model = AcaiModel::new("data/models/acai_h.d1_dnn_20201130.onnx");
        let acai_n_model = AcaiModel::new("data/models/acai_n.d1_dnn_20201130.onnx");
        let acai_v_model = AcaiModel::new("data/models/acai_v.d1_dnn_20201130.onnx");
        let acai_o_model = AcaiModel::new("data/models/acai_o.d1_dnn_20201130.onnx");
        let acai_b_model = AcaiModel::new("data/models/acai_b.d1_dnn_20201130.onnx");

        // we load the btsbot model (different architecture, and input/output then ACAI)
        let btsbot_model = BtsBotModel::new("data/models/btsbot-v1.0.1.onnx");

        let start = std::time::Instant::now();
        loop {
            // check for command from threadpool
            if command_check_countdown == 0 {
                match self.receiver.try_recv() {
                    Ok(WorkerCmd::TERM) => {
                        info!("alert worker {} received termination command", self.id);
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        warn!(
                            "alert worker {} receiver disconnected, terminating",
                            self.id
                        );
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        command_check_countdown = command_interval;
                    }
                }
            }

            let candids = con
                .rpop::<&str, Vec<i64>>(&queue, NonZero::new(1000))
                .await
                .unwrap();

            if candids.is_empty() {
                info!("ML WORKER {}: Queue is empty", self.id);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                command_check_countdown = 0;
                continue;
            }

            let alerts = self.fetch_alerts(&candids).await.unwrap();

            if alerts.len() != candids.len() {
                warn!(
                    "ML WORKER {}: only {} alerts fetched from {} candids",
                    self.id,
                    alerts.len(),
                    candids.len()
                );
            }

            // we keep it very simple for now, let's run on 1 alert at a time
            // we will move to batch processing later
            let mut updates = Vec::new();
            let mut processed_candids = Vec::new();
            for i in 0..alerts.len() {
                let candid = alerts[i].get_i64("_id").unwrap();
                let programid = alerts[i]
                    .get_document("candidate")
                    .unwrap()
                    .get_i32("programid")
                    .unwrap();

                let metadata = acai_h_model.get_metadata(&alerts[i..i + 1]);
                let triplet = acai_h_model.get_triplet(&alerts[i..i + 1]);

                let acai_h_scores = acai_h_model.predict(&metadata, &triplet);
                let acai_n_scores = acai_n_model.predict(&metadata, &triplet);
                let acai_v_scores = acai_v_model.predict(&metadata, &triplet);
                let acai_o_scores = acai_o_model.predict(&metadata, &triplet);
                let acai_b_scores = acai_b_model.predict(&metadata, &triplet);

                let metadata_btsbot = btsbot_model.get_metadata(&alerts[i..i + 1]);
                let btsbot_scores = btsbot_model.predict(&metadata_btsbot, &triplet);

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
                processed_candids.push((candid, programid));
            }

            if count % 1000 == 0 {
                let elapsed = start.elapsed().as_secs();
                info!(
                    "\nRan ML models for {} ZTF alerts in {} seconds, avg: {:.4} alerts/s\n",
                    count,
                    elapsed,
                    count as f64 / elapsed as f64
                );
            }

            let nb_modified = self.client.bulk_write(updates).await?.modified_count;

            // TODO: once PR https://github.com/boom-astro/boom/pull/97
            // is merged, push a string tuple and not a tuple,
            // which isn't valid and just pushes 2 individual values to redis
            con.lpush::<&str, Vec<(i64, i32)>, usize>(&output_queue, processed_candids)
                .await?;

            count += nb_modified;
            command_check_countdown -= nb_modified;
        }

        Ok(())
    }
}
