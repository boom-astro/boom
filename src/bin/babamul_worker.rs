//! This app pulls alerts from the Babamul queue in Valkey, does some filtering,
//! and if any alerts passed they are sent to Babamul-specific Kafka output
//! streams

use crate::conf;

// Add instrumentation
// UpDownCounter for the number of alert batches currently being processed by the enrichment workers.
static ACTIVE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .i64_up_down_counter("enrichment_worker.active")
        .with_unit("{batch}")
        .with_description(
            "Number of alert batches currently being processed by the Babamul worker.",
        )
        .build()
});
// Counter for the number of alert batches processed by the Babamul workers.
static BATCH_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("babamul_worker.batch.processed")
        .with_unit("{batch}")
        .with_description("Number of alert batches processed by the Babamul worker.")
        .build()
});
// Counter for the number of alerts processed by the Babamul workers.
static ALERT_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("enrichment_worker.alert.processed")
        .with_unit("{alert}")
        .with_description("Number of alerts processed by the Babamul worker.")
        .build()
});

fn main() {
    // Connect to Valkey
    let config_path = "config.yaml";
    let config = conf::load_config(config_path)?;
    let mut valkey_con = conf::build_redis(&config).await?;

    // Some general parameters/constants
    let input_queue = "babamul";
    let batch_size = NonZero::new(1000).unwrap();

    // Filter criteria
    // TODO: Get these from config?
    let min_reliability = 0.5;
    let pixel_flags = vec![1, 2, 3];
    let sso = false;
    let sg_star_thresh = 0.5;

    loop {
        // First, pull some alerts from the Babamul queue in Valkey
        ACTIVE.add(1, &active_attrs);
        let alerts: Vec<Alert> = valkey_con
            .rpop::<&str, Vec<Alert>>(&input_queue, batch_size)
            .inspect_err(|_| {
                ACTIVE.add(-1, &active_attrs);
                BATCH_PROCESSED.add(1, &input_error_attrs);
            })?;

        // If no alerts in queue, sleep
        if alerts.is_empty() {
            ACTIVE.add(-1, &active_attrs);
            time::sleep(std::time::Duration::from_millis(500));
            command_check_countdown = 0;
            continue;
        }

        // Next, filter the alerts based on the criteria
        // TODO: Move into enrichment worker
        let filtered_alerts: Vec<Alert> = alerts
            .into_iter()
            .filter(|alert| {
                alert.reliability >= min_reliability
                    && !pixel_flags.contains(&alert.pixel_flag)
                    && alert.sso == sso
            })
            .collect();

        // Now, split into categories for ZTF match: star, galaxy, or none
        let (star_alerts, galaxy_alerts, none_alerts): (Vec<Alert>, Vec<Alert>, Vec<Alert>) =
            filtered_alerts.into_iter().partition_map(|alert| {
                if alert.sg_score > sg_star_thresh {
                    Either::Left(alert)
                } else if alert.sg_score <= (1.0 - sg_star_thresh) {
                    Either::Right(Either::Left(alert))
                } else {
                    Either::Right(Either::Right(alert))
                }
            });

        // Finally, send to the appropriate Babamul Kafka topics
        if !star_alerts.is_empty() {
            send_to_kafka("babamul_stars", star_alerts);
        }
        if !galaxy_alerts.is_empty() {
            send_to_kafka("babamul_galaxies", galaxy_alerts);
        }
        if !none_alerts.is_empty() {
            send_to_kafka("babamul_none", none_alerts);
        }

        let attributes = &ok_attrs;
        ACTIVE.add(-1, &active_attrs);
        BATCH_PROCESSED.add(1, attributes);
        ALERT_PROCESSED.add(alerts.len() as u64, attributes);
    }
}
