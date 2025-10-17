//! This app pulls alerts from the Babamul queue in Valkey, does some filtering,
//! and if any alerts passed they are sent to Babamul-specific Kafka output
//! streams

use boom::conf;
use boom::filter::Alert;
use redis::AsyncCommands;
use serde_json;
use std::num::NonZero;
use tokio::time;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to Valkey
    let config_path = "config.yaml";
    let config = conf::load_config(config_path)?;
    let mut valkey_con = conf::build_redis(&config).await?;

    // Some general parameters/constants
    let input_queue = "babamul";
    let batch_size = NonZero::new(1000);

    loop {
        // First, pull some alerts from the Babamul queue in Valkey
        // These are JSON strings that we need to deserialize into Alerts
        // Pull a batch of raw JSON strings from Redis.
        let raw_alerts: Vec<String> = match valkey_con
            .rpop::<&str, Vec<String>>(&input_queue, batch_size)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!("redis rpop error: {}", e);
                continue;
            }
        };

        // Deserialize JSON strings into Alert structs; log and skip malformed messages
        let mut alerts: Vec<Alert> = Vec::new();
        for s in raw_alerts.into_iter() {
            match serde_json::from_str::<Alert>(&s) {
                Ok(alert) => alerts.push(alert),
                Err(e) => {
                    error!("failed to deserialize babamul alert: {}", e);
                    // Drop this malformed message and continue
                }
            }
        }

        // If no alerts in queue, sleep
        if alerts.is_empty() {
            info!("no alerts in queue, sleeping");
            // use tokio sleep in the async runtime
            time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        }

        // No sg_score available on the Alert struct in this binary; for now
        // send all alerts to the 'none' topic. This keeps behavior simple and
        // compiles. Partitioning logic can be restored if the Alert type is
        // extended with sg_score later.
        let star_alerts: Vec<Alert> = Vec::new();
        let galaxy_alerts: Vec<Alert> = Vec::new();
        let none_alerts: Vec<Alert> = alerts;

        // Finally, send to the appropriate Babamul Kafka topics. Compute
        // counts first because send_to_kafka takes ownership of the Vecs.
        // TODO: This should be 8 different topics
        let star_count = star_alerts.len();
        let galaxy_count = galaxy_alerts.len();
        let none_count = none_alerts.len();

        if !star_alerts.is_empty() {
            send_to_kafka("babamul.stars", star_alerts);
        }
        if !galaxy_alerts.is_empty() {
            send_to_kafka("babamul.galaxies", galaxy_alerts);
        }
        if !none_alerts.is_empty() {
            send_to_kafka("babamul.none", none_alerts);
        }

        // record a simple log line instead of emitting metrics
        info!(
            "processed batch: stars={} galaxies={} none={}",
            star_count, galaxy_count, none_count
        );
    }
    // infinite loop above; no return is necessary
}

// Minimal stub for sending alerts to Kafka. The real implementation should
// use the project's Kafka producer utilities; this stub keeps the binary
// compilable and logs the outgoing batch sizes.
fn send_to_kafka(topic: &str, alerts: Vec<Alert>) {
    info!("would send {} alerts to topic {}", alerts.len(), topic);
}
