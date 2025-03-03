use anyhow::{anyhow, Context};
use boom::{alert, conf, types::ztf_alert_schema, worker_util};
use clap::Parser;
use redis::AsyncCommands;
use std::sync::{Arc, Mutex};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
struct Cli {
    #[arg(required = true, help = "Name of stream to ingest")]
    stream: String,

    #[arg(long, value_name = "FILE", help = "Path to the configuration file")]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let args = Cli::parse();

    let stream_name = &args.stream;

    let config_path = args.config.unwrap_or_else(|| {
        info!("No config file provided, using config.yaml");
        String::from("./config.yaml")
    });
    let config_file = conf::load_config(&config_path).context("failed to load config")?;

    // TODO: There may be a better way to accomplish termination with ctrl-c
    let interrupt_flag = Arc::new(Mutex::new(false));
    worker_util::sig_int_handler(Arc::clone(&interrupt_flag)).await;

    // XMATCH CONFIGS
    let xmatch_configs = conf::build_xmatch_configs(&config_file, stream_name);

    // DATABASE
    let db: mongodb::Database = conf::build_db(&config_file).await;
    db.list_collection_names().await?;

    let alert_collection_name = format!("{}_alerts", stream_name);
    let alert_collection = db.collection(&alert_collection_name);
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
    if let Err(error) = alert_collection.create_index(alert_candid_index).await {
        warn!(
            error = %error,
            "Error when creating index for candidate.candid in collection {}",
            alert_collection_name,
        );
    }

    // REDIS
    let client_redis = redis::Client::open("redis://localhost:6379".to_string())?;
    let mut con = client_redis.get_multiplexed_async_connection().await?;
    let queue_name = format!("{}_alerts_packets_queue", stream_name);
    let queue_temp_name = format!("{}_alerts_packets_queuetemp", stream_name);
    let classifer_queue_name = format!("{}_alerts_classifier_queue", stream_name);

    // ALERT SCHEMA (for fast avro decoding)
    let schema = ztf_alert_schema().ok_or_else(|| anyhow!("failed to get alert schema"))?;

    let mut count = 0;
    let start = std::time::Instant::now();
    loop {
        // check for interruption signal
        worker_util::check_exit(Arc::clone(&interrupt_flag));
        // retrieve candids from redis
        let Some(value): Option<Vec<Vec<u8>>> =
            con.rpoplpush(&queue_name, &queue_temp_name).await?
        else {
            info!("Queue is empty");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            continue;
        };
        match alert::process_alert(
            &value[0],
            &xmatch_configs,
            &db,
            &alert_collection,
            &alert_aux_collection,
            &schema,
        )
        .await
        {
            Ok(Some(candid)) => {
                info!(
                    "Processed alert with candid: {}, queueing for classification",
                    candid
                );
                // queue the candid for processing by the classifier
                con.lpush::<&str, i64, isize>(&classifer_queue_name, candid)
                    .await?;
                con.lrem::<&str, Vec<u8>, isize>(&queue_temp_name, 1, value[0].clone())
                    .await?;
            }
            Ok(None) => {
                info!("Alert already exists");
                // remove the alert from the queue
                con.lrem::<&str, Vec<u8>, isize>(&queue_temp_name, 1, value[0].clone())
                    .await?;
            }
            Err(error) => {
                error!(error = %error, "Error processing alert, requeueing");
                // put it back in the ZTF_alerts_packets_queue, to the left (pop from the right, push to the left)
                con.lrem::<&str, Vec<u8>, isize>(&queue_temp_name, 1, value[0].clone())
                    .await?;
                con.lpush::<&str, Vec<u8>, isize>(&queue_name, value[0].clone())
                    .await?;
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
}
