use crate::{
    conf,
    worker_util::{self, WorkerCmd},
};
use core::time;
use futures::StreamExt;
use mongodb::bson::{doc, Document};
use redis::AsyncCommands;
use std::{collections::HashMap, num::NonZero, sync::mpsc, thread};
use tracing::{info, warn};

// fake ml worker which, like the ML worker, receives alerts from the alert worker and sends them
//      to streams
#[tokio::main]
pub async fn fake_ml_worker(
    id: String,
    receiver: mpsc::Receiver<WorkerCmd>,
    stream_name: String,
    config_path: String,
) {
    let ztf_allowed_permissions = vec![1, 2, 3];
    let catalog = stream_name.clone();
    let queue = format!("{}_alerts_classifier_queue", catalog);

    let config_file = conf::load_config(&config_path).unwrap();
    let db = conf::build_db(&config_file).await;
    let client_redis = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    let mut alert_counter = 0;
    let command_interval = worker_util::get_check_command_interval(config_file, &stream_name);

    loop {
        // check for interrupt from thread pool
        if alert_counter - command_interval > 0 {
            alert_counter = 0;
            if let Ok(command) = receiver.try_recv() {
                match command {
                    WorkerCmd::TERM => {
                        warn!("alert worker {} received termination command", id);
                        return;
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
            .find(doc! {"candid": {"$in": candids}})
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
            thread::sleep(time::Duration::from_secs(5));
            alert_counter = 0;
            if let Ok(command) = receiver.try_recv() {
                match command {
                    WorkerCmd::TERM => {
                        warn!("alert worker {} received termination command", id);
                        return;
                    }
                }
            }
            continue;
        } else {
            info!("ML WORKER {}: received alerts len: {}", id, alerts.len());
            alert_counter += alerts.len() as i64;
        }

        let mut candids_grouped: HashMap<i32, Vec<i64>> = HashMap::new();

        for alert in alerts {
            let candidate = alert.get("candidate").unwrap();
            let programid = mongodb::bson::to_document(candidate)
                .unwrap()
                .get("programid")
                .unwrap()
                .as_i32()
                .unwrap();
            let candid = alert.get("candid").unwrap().as_i64().unwrap();
            if !candids_grouped.contains_key(&programid) {
                candids_grouped.insert(programid, Vec::new());
            } else {
                candids_grouped
                    .entry(programid)
                    .and_modify(|candids| candids.push(candid));
            }
        }

        for (programid, candids) in candids_grouped {
            for candid in candids {
                for permission in &ztf_allowed_permissions {
                    if programid <= *permission {
                        let _ = con
                            .xadd::<&str, &str, &str, i64, ()>(
                                format!(
                                    "{}_alerts_programid_{}_filter_stream",
                                    &catalog, permission
                                )
                                .as_str(),
                                "*",
                                &[("candid", candid)],
                            )
                            .await;
                    }
                }
            }
        }
    }
}
