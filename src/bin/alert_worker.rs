use boom::{alert, conf, types::ztf_alert_schema, worker_util};
use redis::AsyncCommands;
use std::env;
use std::sync::{Arc, Mutex};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args: Vec<String> = env::args().collect();
    // user can pass the path to a config file, but it is optional.
    // if not provided, we use the default config.default.yaml
    // the user also needs to pass the name of the alert stream to process
    // stream name comes first, optional config file comes second
    if args.len() < 2 {
        error!("Usage: alert_worker <stream_name> <config_file>, where config_file is optional");
        return;
    }

    let interrupt_flag = Arc::new(Mutex::new(false));
    worker_util::sig_int_handler(Arc::clone(&interrupt_flag)).await;

    let stream_name = &args[1];

    let config_file = if args.len() > 2 {
        conf::load_config(&args[2]).unwrap()
    } else {
        warn!("No config file provided, using config.yaml");
        conf::load_config("./config.yaml").unwrap()
    };

    // XMATCH CONFIGS
    let xmatch_configs = conf::build_xmatch_configs(&config_file, stream_name);

    // DATABASE
    let db: mongodb::Database = conf::build_db(&config_file).await;
    if let Err(e) = db.list_collection_names().await {
        error!("Error connecting to the database: {}", e);
        return;
    }

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

    // ALERT SCHEMA (for fast avro decoding)
    let schema = ztf_alert_schema().unwrap();

    let mut count = 0;
    let start = std::time::Instant::now();
    loop {
        // check for interruption signal
        worker_util::check_exit(Arc::clone(&interrupt_flag));
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
                info!("Queue is empty");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }
}
