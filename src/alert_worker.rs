use crate::{
    alert, conf,
    types::ztf_alert_schema,
    worker_util::{self, WorkerCmd},
};
use redis::AsyncCommands;
use std::sync::{mpsc, Arc, Mutex};
use tracing::{error, info, warn};

// alert worker as a standalone function which is run by the scheduler
#[tokio::main]
pub async fn alert_worker(
    id: String,
    receiver: Arc<Mutex<mpsc::Receiver<WorkerCmd>>>,
    stream_name: String,
    config_path: String,
) {
    let config_file = conf::load_config(&config_path).unwrap();

    // XMATCH CONFIGS
    let xmatch_configs = conf::build_xmatch_configs(&config_file, &stream_name);

    // DATABASE
    let db: mongodb::Database = conf::build_db(&config_file).await;
    if let Err(e) = db.list_collection_names().await {
        error!("Error connecting to the database: {}", e);
        return;
    }

    // create alert and alert-auxillary collections
    let alert_collection = db.collection(&format!("{}_alerts", stream_name));
    let alert_aux_collection = db.collection(&format!("{}_alerts_aux", stream_name));

    // create index for alert collection
    let alert_candid_index = mongodb::IndexModel::builder()
        .keys(mongodb::bson::doc! { "candid": -1 })
        .options(
            mongodb::options::IndexOptions::builder()
                .unique(true)
                .build(),
        )
        .build();
    match alert_collection.create_index(alert_candid_index).await {
        Err(e) => {
            error!(
                "Error when creating index for candidate.candid in collection {}: {}",
                format!("{}_alerts", stream_name),
                e
            );
        }
        Ok(_x) => {}
    }

    // REDIS
    let client_redis = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let queue_name = format!("{}_alerts_packet_queue", stream_name);
    let queue_temp_name = format!("{}_alerts_packet_queuetemp", stream_name);
    let classifer_queue_name = format!("{}_alerts_classifier_queue", stream_name);

    let mut alert_counter = 0;
    let command_interval = worker_util::get_check_command_interval(config_file, &stream_name);
    // ALERT SCHEMA (for fast avro decoding)
    let schema = ztf_alert_schema().unwrap();
    let mut count = 0;
    let start = std::time::Instant::now();
    loop {
        // check for command from threadpool
        if alert_counter - command_interval > 0 {
            alert_counter = 0;
            if let Ok(command) = receiver.lock().unwrap().try_recv() {
                match command {
                    WorkerCmd::TERM => {
                        warn!("alert worker {} received termination command", id);
                        return;
                    }
                }
            }
        }
        // retrieve candids from redis
        let result: Option<Vec<Vec<u8>>> =
            con.rpoplpush(&queue_name, &queue_temp_name).await.unwrap();
        match result {
            Some(value) => {
                let candid = alert::process_alert(
                    value[0].clone(),
                    &xmatch_configs,
                    &db,
                    &alert_collection,
                    &alert_aux_collection,
                    &schema,
                )
                .await;
                alert_counter += 1;
                match candid {
                    Ok(Some(candid)) => {
                        info!(
                            "Processed alert with candid: {}, queueing for classification",
                            candid
                        );
                        // queue the candid for processing by the classifier
                        con.lpush::<&str, i64, isize>(&classifer_queue_name, candid)
                            .await
                            .unwrap();
                        con.lrem::<&str, Vec<u8>, isize>(&queue_temp_name, 1, value[0].clone())
                            .await
                            .unwrap();
                    }
                    Ok(None) => {
                        info!("Alert already exists");
                        // remove the alert from the queue
                        con.lrem::<&str, Vec<u8>, isize>(&queue_temp_name, 1, value[0].clone())
                            .await
                            .unwrap();
                    }
                    Err(e) => {
                        error!("Error processing alert: {}, requeueing", e);
                        // put it back in the alertpacketqueue, to the left (pop from the right, push to the left)
                        con.lrem::<&str, Vec<u8>, isize>(&queue_temp_name, 1, value[0].clone())
                            .await
                            .unwrap();
                        con.lpush::<&str, Vec<u8>, isize>(&queue_name, value[0].clone())
                            .await
                            .unwrap();
                    }
                }
                if count > 1 && count % 100 == 0 {
                    let elapsed = start.elapsed().as_secs();
                    info!(
                        "\nProcessed {} {} alerts in {} seconds, avg: {:.4} alerts/s\n",
                        count,
                        stream_name,
                        elapsed,
                        count as f64 / elapsed as f64
                    );
                }
                count += 1;
            }
            None => {
                info!("ALERT WORKER {}: Queue is empty", id);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                alert_counter = 0;
                // check for command from threadpool
                if let Ok(command) = receiver.lock().unwrap().try_recv() {
                    match command {
                        WorkerCmd::TERM => {
                            warn!("alert worker {} received termination command", id);
                            return;
                        }
                    }
                }
            }
        }
    }
}
