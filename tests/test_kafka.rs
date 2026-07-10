use boom::{
    kafka::{
        count_messages, delete_topic, AlertConsumer, AlertProducer, ZtfAlertConsumer,
        ZtfAlertProducer,
    },
    utils::{data::count_files_in_dir, enums::ProgramId, testing::TEST_CONFIG_FILE},
};
use redis::AsyncCommands;
use std::path::{Path, PathBuf};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

fn naive(date: &str) -> chrono::NaiveDateTime {
    chrono::NaiveDate::parse_from_str(date, "%Y%m%d")
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

#[tokio::test]
async fn test_download_from_archive() {
    let date_str = "20231118";
    let expected_count = 271u32;

    let producer = ZtfAlertProducer::new(
        naive(date_str).date(),
        0,
        ProgramId::Public,
        "localhost:9092",
        false,
    );
    let result = producer.download_alerts_from_archive().await;

    // Verify the producer succeeded and reports the expected count
    // Sometimes this is a little flaky and is one off; handle that case too
    assert!(result.is_ok());
    let downloaded_count = result.unwrap();
    assert!(
        downloaded_count.abs_diff(expected_count as i64) <= 2,
        "expected {} ± 2, got {}",
        expected_count,
        downloaded_count
    );

    // Verify the data directory exists and has the right number of avro files:
    let data_directory = Path::new("data/alerts/ztf/public").join(date_str);
    assert!(data_directory.exists());
    let avro_count = count_files_in_dir(data_directory.to_str().unwrap(), Some(&["avro"])).unwrap();
    assert!(
        avro_count.abs_diff(expected_count as usize) <= 2,
        "expected {} ± 2, got {}",
        expected_count,
        avro_count
    );
}

#[tokio::test]
async fn test_produce_and_consume_from_archive() {
    let date_str = "20240617";
    let topic = uuid::Uuid::new_v4().to_string();
    let output_queue = uuid::Uuid::new_v4().to_string();
    let expected_count = 710u32;

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    /* Part 1: Producer */

    let datetime = naive(date_str);
    let producer = ZtfAlertProducer::new(
        datetime.date(),
        0,
        ProgramId::Public,
        "localhost:9092",
        false,
    );

    // Verify that the producer runs and reports the correct count:
    let result = producer.produce(Some(topic.clone())).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().unwrap(), expected_count as i64);

    // Verify that the messages were actually produced:
    let message_count = count_messages(&producer.server_url(), &topic)
        .unwrap()
        .unwrap();
    assert_eq!(message_count, expected_count);

    // Verify that the correct number of avro files have been downloaded
    // (test_download_from_archive does a more detailed check of this):
    let avro_count = count_files_in_dir(&producer.data_directory(), Some(&["avro"])).unwrap();
    assert_eq!(avro_count, expected_count as usize);

    /* Part 2: Consumer */

    let ztf_alert_consumer =
        ZtfAlertConsumer::new(Some(&output_queue), Some(vec![ProgramId::Public]));

    ztf_alert_consumer
        .clear_output_queue(TEST_CONFIG_FILE)
        .await
        .unwrap();

    let timestamp = datetime.and_utc().timestamp();
    ztf_alert_consumer
        .consume(
            Some(vec![topic]),
            timestamp,
            None,
            Some(1),
            None,
            true,
            TEST_CONFIG_FILE,
        )
        .await
        .unwrap();

    // Verify that the output queue has the expected number of messages:
    let config = boom::conf::load_config(Some(TEST_CONFIG_FILE)).unwrap();
    let mut con = config.build_redis().await.unwrap();

    let queue_len: usize = con.llen(&output_queue).await.unwrap();

    assert_eq!(queue_len, expected_count as usize);
    // delete the queue to clean up
    let _: () = con.del(&output_queue).await.unwrap();
}

async fn produce_ztf_in_dir(
    date_str: &str,
    working_dir: &str,
    topic: &str,
    limit: u32,
) -> ZtfAlertProducer {
    // Cache data for the given date as usual:
    let producer = ZtfAlertProducer::new(
        naive(date_str).date(),
        0,
        ProgramId::Public,
        "localhost:9092",
        false,
    );
    producer.download_alerts_from_archive().await.unwrap();
    let src_dir = PathBuf::from(producer.data_directory());

    // Create a *new* producer that uses given working directory:
    let producer = producer.with_working_dir(working_dir);
    let dst_dir = PathBuf::from(producer.data_directory());

    // Copy the downloaded alerts to the working directory:
    eprintln!("creating destination directory {:?}", dst_dir);
    std::fs::create_dir_all(dst_dir.clone()).expect("failed to create destination directory");
    eprintln!("reading source directory {:?}", src_dir);
    let mut n_copied = 0;
    for entry in src_dir
        .read_dir()
        .expect(&format!("failed to read source directory"))
    {
        // Can't simply use Iterator::take(limit) because not all entries are
        // guaranteed to be avro files, so we use a counter instead:
        if n_copied >= limit {
            break;
        }
        eprintln!("got entry {:?}", entry);
        let entry = entry.expect("entry error");
        let src_path = entry.path();
        if !(src_path.is_file() && src_path.extension().is_some_and(|ext| ext == "avro")) {
            eprintln!("ignoring {:?}", src_path);
            continue;
        }
        let dst_path = dst_dir.join(entry.file_name());
        eprintln!("copying {:?} to {:?}", src_path, dst_path);
        _ = std::fs::copy(src_path, dst_path).expect("failed to copy");
        n_copied += 1;
    }

    // Produce the alerts and verify the message count
    // (test_produce_and_consume_from_archive does a more detailed check):
    let message_count = producer
        .produce(Some(topic.to_string()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message_count, limit as i64);

    // Verify the file count equals the message count:
    let avro_count = count_files_in_dir(&producer.data_directory(), Some(&["avro"])).unwrap();
    assert_eq!(avro_count, message_count as usize);

    producer
}

#[tokio::test]
async fn test_skip_producing_when_counts_match() {
    let date_str = "20231118";
    let topic = uuid::Uuid::new_v4().to_string();
    let limit = 10u32;
    let tmp_dir = tempfile::tempdir().unwrap();

    // Produce:
    let producer =
        produce_ztf_in_dir(date_str, tmp_dir.path().to_str().unwrap(), &topic, limit).await;

    // Try again: the message count matches the avro count, so no more messages
    // will be produced:
    let option = producer.produce(Some(topic.clone())).await.unwrap();
    assert!(option.is_none()); // Reported count is None, i.e., no messages were produced

    // Verify the topic still has the correct number of messages:
    let message_count = count_messages(&producer.server_url(), &topic)
        .unwrap()
        .unwrap();
    assert_eq!(message_count, limit);
}

#[tokio::test]
async fn test_produce_when_counts_do_not_match() {
    let date_str = "20231118";
    let topic = uuid::Uuid::new_v4().to_string();
    let limit = 10u32;
    let tmp_dir = tempfile::tempdir().unwrap();

    // Produce:
    let producer =
        produce_ztf_in_dir(date_str, tmp_dir.path().to_str().unwrap(), &topic, limit).await;

    // Remove a file:
    let first_file = PathBuf::from(producer.data_directory())
        .read_dir()
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(first_file).unwrap();

    // Try again: the message count does not match the avro count, so we should
    // produce again. The missing file won't be redownloaded; the download logic
    // just recognizes that the directory exists and is non-empty. The producer
    // produces whatever it finds in the data directory, and now there is one
    // fewer alert than before:
    let message_count = producer
        .produce(Some(topic.clone()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message_count, (limit - 1) as i64);

    // Verify the topic now has one fewer message:
    let message_count = count_messages(&producer.server_url(), &topic)
        .unwrap()
        .unwrap();
    assert_eq!(message_count, limit - 1);
}

#[tokio::test]
async fn test_produce_when_topic_does_not_exist() {
    let date_str = "20231118";
    let topic = uuid::Uuid::new_v4().to_string();
    let limit = 10u32;
    let tmp_dir = tempfile::tempdir().unwrap();

    // Produce:
    let producer =
        produce_ztf_in_dir(date_str, tmp_dir.path().to_str().unwrap(), &topic, limit).await;

    // Delete the topic:
    delete_topic(&producer.server_url(), &topic).await.unwrap();

    // Try again: the topic doesn't exist, so should produce as usual:
    let message_count = producer
        .produce(Some(topic.clone()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message_count, limit as i64);

    // Verify the topic has the correct number of messages:
    let message_count = count_messages(&producer.server_url(), &topic)
        .unwrap()
        .unwrap();
    assert_eq!(message_count, limit);
}

// Ignored because it *always* downloads ZTF alerts and is therefore too
// expensive to run during normal development.
#[tokio::test]
#[ignore]
async fn test_produce_when_data_does_not_exist() {
    let date_str = "20231118";
    let topic = uuid::Uuid::new_v4().to_string();
    let limit = 10u32;
    let tmp_dir = tempfile::tempdir().unwrap();

    // Produce:
    let producer =
        produce_ztf_in_dir(date_str, tmp_dir.path().to_str().unwrap(), &topic, limit).await;

    // Delete the data directory:
    std::fs::remove_dir_all(PathBuf::from(producer.data_directory())).unwrap();

    // Try again: the data doesn't exist, so there's no avro count to verify
    // that the message count is correct. Should produce as usual:
    let message_count = producer
        .produce(Some(topic.clone()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message_count, limit as i64);

    // Verify the topic has the correct number of messages:
    let message_count = count_messages(&producer.server_url(), &topic)
        .unwrap()
        .unwrap();
    assert_eq!(message_count, limit);
}

/// Poll the redis output queue until every payload in `expected` has been seen,
/// returning the full set of payloads observed (so callers can also assert that
/// unwanted payloads are absent). Panics on timeout.
async fn wait_for_payloads(
    con: &mut redis::aio::MultiplexedConnection,
    queue: &str,
    expected: &[String],
    timeout: std::time::Duration,
) -> std::collections::HashSet<String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let items: Vec<Vec<u8>> = con.lrange(queue, 0, -1).await.unwrap_or_default();
        let seen: std::collections::HashSet<String> = items
            .iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect();
        if expected.iter().all(|e| seen.contains(e)) {
            return seen;
        }
        if std::time::Instant::now() > deadline {
            panic!("timed out waiting for {expected:?}; saw {seen:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

// End-to-end test of the self-rollover consumer: the long-running consumer
// subscribes to a topic *pattern* and must (a) skip an old retained topic's
// messages on cold start, (b) consume the current day's messages, and (c)
// auto-discover and consume the *next* day's topic created while it is running,
// all without restarting. Requires a local Kafka broker + redis.
//
// The consumer runs on its own dedicated OS thread + runtime (see below) so its
// blocking rdkafka `poll` can't starve this test driver's runtime.
#[tokio::test]
async fn test_consumer_rolls_over_and_skips_old() {
    use boom::conf::{AppConfig, KafkaConsumerConfig};
    use boom::kafka::{consumer, delete_topic, initialize_topic};
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use std::time::Duration;

    let server = "localhost:9092";
    let now_ms = chrono::Utc::now().timestamp_millis();
    // Unique, regex-safe prefix so the pattern matches only this test's topics.
    let prefix = format!("rollovertest{now_ms}");
    let pattern = format!("^{prefix}_[0-9]+$");
    let output_queue = format!("{prefix}_queue");
    let group_id = format!("{prefix}_group");
    let topic_day1 = format!("{prefix}_20260628");
    let topic_day2 = format!("{prefix}_20260629");

    let app_config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let mut con = app_config.build_redis().await.unwrap();
    let _: () = con.del(&output_queue).await.unwrap_or(());

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", server)
        .create()
        .unwrap();

    // Day 1 topic: one stale message (10 days old, lowest offset) then fresh ones.
    initialize_topic(server, &topic_day1, 1).await.unwrap();
    let stale_ms = now_ms - 10 * 24 * 60 * 60 * 1000;
    producer
        .send(
            FutureRecord::to(topic_day1.as_str())
                .payload("stale")
                .key("k")
                .timestamp(stale_ms),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    let fresh: Vec<String> = (0..5).map(|i| format!("fresh-{i}")).collect();
    for p in &fresh {
        producer
            .send(
                FutureRecord::to(topic_day1.as_str())
                    .payload(p.as_str())
                    .key("k")
                    .timestamp(now_ms),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }

    // Start the long-running consumer. Cold-start timestamp = 1h ago, so the
    // 10-day-old "stale" message is skipped while the fresh ones are consumed.
    let cold_start_ts = now_ms / 1000 - 3600;
    let kafka_cfg = KafkaConsumerConfig {
        server: server.to_string(),
        group_id,
        schema_registry: None,
        schema_github_fallback_url: None,
        username: None,
        password: None,
    };
    // Run the consumer on its own OS thread + runtime so its blocking rdkafka
    // `poll` doesn't starve the test driver's runtime (this mirrors prod, where
    // each consumer process owns its runtime). Dropping the runtime when the
    // stop signal arrives aborts the consumer.
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let consumer_thread = {
        let config = app_config.clone();
        let oq = output_queue.clone();
        let pat = pattern.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.spawn(async move {
                let _ = consumer(
                    "0",
                    vec![pat],
                    &oq,
                    0,
                    cold_start_ts,
                    &config,
                    &kafka_cfg,
                    false,
                    "WINTER",
                )
                .await;
            });
            let _ = stop_rx.recv();
            // Force shutdown rather than waiting on the consumer's in-flight
            // blocking `poll` (its worker thread is abandoned and dies with the
            // process).
            rt.shutdown_timeout(std::time::Duration::from_secs(1));
        })
    };

    // (b)+(a): fresh messages arrive; the stale one is skipped.
    let seen = wait_for_payloads(&mut con, &output_queue, &fresh, Duration::from_secs(40)).await;
    assert!(
        !seen.contains("stale"),
        "stale (pre-window) message should have been skipped"
    );

    // (c): create the next day's topic *after* the consumer is running and
    // produce to it. The consumer must auto-discover it without a restart.
    initialize_topic(server, &topic_day2, 1).await.unwrap();
    let newday: Vec<String> = (0..4).map(|i| format!("newday-{i}")).collect();
    for p in &newday {
        producer
            .send(
                FutureRecord::to(topic_day2.as_str())
                    .payload(p.as_str())
                    .key("k")
                    .timestamp(now_ms),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }

    let expected: Vec<String> = fresh.iter().chain(newday.iter()).cloned().collect();
    let seen = wait_for_payloads(&mut con, &output_queue, &expected, Duration::from_secs(60)).await;
    assert!(
        !seen.contains("stale"),
        "stale message must never be consumed"
    );

    let _ = stop_tx.send(());
    let _ = consumer_thread.join();
    let _: () = con.del(&output_queue).await.unwrap_or(());
    let _ = delete_topic(server, &topic_day1).await;
    let _ = delete_topic(server, &topic_day2).await;
}
