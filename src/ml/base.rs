use crate::{
    conf,
    utils::fits::prepare_triplet,
    utils::worker::{get_check_command_interval, WorkerCmd},
};
use core::time;
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use redis::AsyncCommands;
use std::{num::NonZero, thread};
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(thiserror::Error, Debug)]
pub enum MLWorkerError {
    #[error("failed to connect to database")]
    ConnectMongoError(#[from] mongodb::error::Error),
    #[error("failed to connect to redis")]
    ConnectRedisError(#[from] redis::RedisError),
    #[error("failed to read config")]
    ReadConfigError(#[from] conf::BoomConfigError),
}

// fake ml worker which for now does not run any models
#[tokio::main]
pub async fn run_ml_worker(
    id: String,
    mut receiver: mpsc::Receiver<WorkerCmd>,
    stream_name: &str,
    config_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog: String = stream_name.to_string();
    let queue = format!("{}_alerts_classifier_queue", catalog);
    let output_queue = format!("{}_alerts_filter_queue", stream_name);

    let config_file = conf::load_config(&config_path).unwrap();
    let db = conf::build_db(&config_file).await?;
    let client_redis = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection()
        .await
        .map_err(MLWorkerError::ConnectRedisError)?;

    let mut alert_counter = 0;
    let command_interval = get_check_command_interval(config_file, &stream_name);

    loop {
        // check for interrupt from thread pool
        if alert_counter - command_interval > 0 {
            alert_counter = 0;
            if let Ok(command) = receiver.try_recv() {
                match command {
                    WorkerCmd::TERM => {
                        warn!("alert worker {} received termination command", id);
                        return Ok(());
                    }
                }
            }
        }

        let candids = con
            .rpop::<&str, Vec<i64>>(queue.as_str(), NonZero::new(1000))
            .await
            .unwrap();

        let mut alert_cursor = db
            .collection::<Document>(format!("{}_alerts", catalog).as_str())
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
                        "from": format!("{}_alerts_aux", catalog),
                        "localField": "objectId",
                        "foreignField": "_id",
                        "as": "aux"
                    }
                },
                doc! {
                    "$lookup": {
                        "from": format!("{}_alerts_cutouts", catalog),
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

        if alerts.len() == 0 {
            info!("ML WORKER {}: queue empty", id);
            thread::sleep(time::Duration::from_secs(1));
            alert_counter = 0;
            if let Ok(command) = receiver.try_recv() {
                match command {
                    WorkerCmd::TERM => {
                        warn!("alert worker {} received termination command", id);
                        return Ok(());
                    }
                }
            }
            continue;
        } else {
            alert_counter += alerts.len() as i64;
        }

        // TODO: run ML models and add the results to the alerts
        // For now, we just just perform some of the preprocessing operations, like reading the images
        // Ideally, we want to prepare the data for the whole batch of alerts at once, so we can
        // run the models on batches of alerts instead of one by one
        let mut processed_candids = Vec::new();
        for alert in &alerts {
            let candid = alert.get_i64("_id").unwrap();
            let programid = alert
                .get_document("candidate")
                .unwrap()
                .get_i32("programid")
                .unwrap();
            let obj_id = alert.get_str("objectId").unwrap();
            let result = prepare_triplet(&alert);
            if result.is_err() {
                warn!(
                    "ML WORKER {}: error preparing triplet for alert {}",
                    id, obj_id
                );
                continue;
            }
            let (_cutout_science, _cutout_template, _cutout_difference) = result.unwrap();

            processed_candids.push((candid, programid));
        }

        con.lpush::<&str, Vec<(i64, i32)>, usize>(output_queue.as_str(), processed_candids)
            .await
            .unwrap();
    }
}
