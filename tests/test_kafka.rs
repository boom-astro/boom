use boom::{
    kafka::{
        count_messages, delete_topic, AlertConsumer, AlertProducer, StartDate, ZtfAlertConsumer,
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
            StartDate::Pinned(timestamp),
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
// subscribes to the bounded window of concrete daily topic names and must
// (a) skip an old retained topic's messages on cold start, (b) consume the
// current day's messages, and (c) consume the *next* day's topic, which does
// not yet exist at subscribe time and is created while the consumer is
// running, all without restarting. (c) also covers the regression this window
// replaced a pattern subscription to avoid: a subscribed-but-absent topic must
// not stall the poll loop. Requires a local Kafka broker + redis.
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
    // Unique prefix so this test's topics can't collide with another run's.
    let prefix = format!("rollovertest{now_ms}");
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
        subscription_window_days: 1,
    };
    // Run the consumer on its own OS thread + runtime so its blocking rdkafka
    // `poll` doesn't starve the test driver's runtime (this mirrors prod, where
    // each consumer process owns its runtime). Dropping the runtime when the
    // stop signal arrives aborts the consumer.
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let consumer_thread = {
        let config = app_config.clone();
        let oq = output_queue.clone();
        // The rollover window as the survey impls build it: both days are
        // subscribed up front, and day 2 does not exist yet.
        let window = vec![topic_day1.clone(), topic_day2.clone()];
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.spawn(async move {
                let _ = consumer(
                    "0",
                    std::sync::Arc::new(move |_, _| window.clone()),
                    &oq,
                    0,
                    boom::kafka::StartDate::From(cold_start_ts).plan(1),
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

// The rollover window is what keeps a daily-topic subscription bounded. A
// pattern subscription matched every night the cluster still advertised, so an
// unbounded set of expired partitions accumulated in the assignment and starved
// the poll loop; these assert the replacement stays small and concrete.
#[test]
fn test_subscription_window_is_today_and_yesterday() {
    use boom::kafka::subscription_window;

    // 2026-08-09 00:00:00 UTC
    let today = chrono::NaiveDate::from_ymd_opt(2026, 8, 9).unwrap();
    let ts = today.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();

    let window = subscription_window(ts, 1);
    assert_eq!(window.len(), 2);
    assert_eq!(
        window,
        vec![chrono::NaiveDate::from_ymd_opt(2026, 8, 8).unwrap(), today,],
        "window should be oldest-first and cover the night straddling UTC midnight"
    );
}

#[test]
fn test_subscription_window_crosses_month_boundary() {
    use boom::kafka::subscription_window;

    let ts = chrono::NaiveDate::from_ymd_opt(2026, 9, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();

    assert_eq!(
        subscription_window(ts, 1),
        vec![
            chrono::NaiveDate::from_ymd_opt(2026, 8, 31).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 9, 1).unwrap(),
        ]
    );
}

#[test]
fn test_ztf_subscription_topics_are_concrete_and_bounded() {
    use boom::kafka::AlertConsumer;
    use boom::kafka::ZtfAlertConsumer;
    use boom::utils::enums::ProgramId;

    let ts = chrono::NaiveDate::from_ymd_opt(2026, 8, 9)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();

    let consumer = ZtfAlertConsumer::new(None, Some(vec![ProgramId::Public]));
    let topics = consumer.subscription_topics(ts, 1);

    assert_eq!(
        topics,
        vec![
            "ztf_20260808_programid1".to_string(),
            "ztf_20260809_programid1".to_string(),
        ]
    );
    assert!(
        topics.iter().all(|t| !t.starts_with('^')),
        "topics must be literal names; a `^…` entry is treated by librdkafka as \
         a regex and would match every past night"
    );
}

#[test]
fn test_ztf_subscription_topics_cover_every_program_id() {
    use boom::kafka::AlertConsumer;
    use boom::kafka::ZtfAlertConsumer;
    use boom::utils::enums::ProgramId;

    let ts = chrono::NaiveDate::from_ymd_opt(2026, 8, 9)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();

    let consumer =
        ZtfAlertConsumer::new(None, Some(vec![ProgramId::Public, ProgramId::Partnership]));
    let topics = consumer.subscription_topics(ts, 1);

    assert_eq!(topics.len(), 4, "2 days x 2 program ids");
    for expected in [
        "ztf_20260808_programid1",
        "ztf_20260808_programid2",
        "ztf_20260809_programid1",
        "ztf_20260809_programid2",
    ] {
        assert!(topics.contains(&expected.to_string()), "missing {expected}");
    }
}

// An empty selection would otherwise subscribe to nothing at all, silently
// consuming no alerts.
#[test]
fn test_ztf_empty_program_ids_falls_back_to_public() {
    use boom::kafka::AlertConsumer;
    use boom::kafka::ZtfAlertConsumer;

    let ts = chrono::NaiveDate::from_ymd_opt(2026, 8, 9)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();

    let topics = ZtfAlertConsumer::new(None, Some(vec![])).subscription_topics(ts, 1);
    assert_eq!(
        topics,
        vec![
            "ztf_20260808_programid1".to_string(),
            "ztf_20260809_programid1".to_string(),
        ]
    );
}

// LSST is not date-partitioned: its single static topic must be returned
// unchanged whatever the date, and must never be windowed.
#[test]
fn test_lsst_subscription_topic_is_static() {
    use boom::kafka::AlertConsumer;
    use boom::kafka::LsstAlertConsumer;

    let consumer = LsstAlertConsumer::new(None, false);
    let at_epoch = consumer.subscription_topics(0, 1);
    let much_later = consumer.subscription_topics(1_800_000_000, 9);

    assert_eq!(at_epoch.len(), 1);
    assert_eq!(at_epoch, much_later);
}

// Rollover intent is stated by the caller, not inferred from the timestamp.
// The throughput benchmark runs `kafka_consumer ztf --on 20250311`, and chasing the
// wall clock would unsubscribe it from the only topic it was started to read.
#[test]
fn test_start_date_signals_rollover_intent() {
    use boom::kafka::StartDate;

    assert!(StartDate::Current.follows_clock());
    assert!(!StartDate::Pinned(1_741_651_200).follows_clock());

    // Pinned to *today* is still pinned -- the case an inferred check, comparing
    // the start date against the current date, would get wrong.
    let today_midnight = chrono::Utc::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();
    assert!(!StartDate::Pinned(today_midnight).follows_clock());

    // Pinned reports exactly what it was given; Current resolves to a UTC midnight.
    assert_eq!(StartDate::Pinned(1_741_651_200).timestamp(), 1_741_651_200);
    let current = StartDate::Current.timestamp();
    assert_eq!(current % 86_400, 0, "should resolve to a UTC midnight");
    let now = chrono::Utc::now().timestamp();
    assert!(
        (0..86_400).contains(&(now - current)),
        "should resolve to the current UTC day"
    );
}

// Catching up from a past date means two different instants: the window ends at
// today, so every night since is subscribed, while partitions are still
// positioned at the requested date. Positioning them at today instead would find
// nothing at or after it in the older topics and skip every one of them.
#[test]
fn test_from_date_catches_up_every_night_since() {
    use boom::kafka::{AlertConsumer, StartDate, ZtfAlertConsumer};
    use boom::utils::enums::ProgramId;

    let today = chrono::Utc::now().date_naive();
    let start = today.checked_sub_days(chrono::Days::new(10)).unwrap();
    let start_ts = start.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();

    let plan = StartDate::From(start_ts).plan(1);
    assert_eq!(plan.position_timestamp, start_ts);
    assert_eq!(
        plan.window_days, 10,
        "the window must reach back to the start date"
    );
    assert!(
        plan.follows_clock,
        "catch-up still rolls onto each new night"
    );
    assert!(
        !plan.positions_at_rolled_day,
        "rolling must not move the position past the nights being caught up"
    );
    assert!(!plan.replay, "a catch-up is the production path");

    let topics = ZtfAlertConsumer::new(None, Some(vec![ProgramId::Public]))
        .subscription_topics(plan.subscription_timestamp, plan.window_days);
    assert_eq!(topics.len(), 11, "10 nights back plus today");
    assert!(topics.contains(&format!("ztf_{}_programid1", start.format("%Y%m%d"))));
    assert!(topics.contains(&format!("ztf_{}_programid1", today.format("%Y%m%d"))));
}

// Catching up is bounded: a date further back than any survey retains would
// subscribe to hundreds of topics that no longer exist, each of which fails
// every poll.
#[test]
fn test_catch_up_is_bounded_and_only_applies_to_from() {
    use boom::kafka::{StartDate, MAX_CATCH_UP_DAYS};

    let old = chrono::Utc::now()
        .date_naive()
        .checked_sub_days(chrono::Days::new(MAX_CATCH_UP_DAYS + 500))
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();

    assert!(StartDate::From(old).catch_up_days() > MAX_CATCH_UP_DAYS);
    assert_eq!(
        StartDate::Pinned(old).catch_up_days(),
        0,
        "a pinned replay reaches back over nothing"
    );
    assert_eq!(StartDate::Current.catch_up_days(), 0);
}

// The other two modes read from the instant they subscribe to, over the window
// the survey is configured with.
#[test]
fn test_current_and_pinned_plans_keep_the_configured_window() {
    use boom::kafka::StartDate;

    let current = StartDate::Current.plan(1);
    assert_eq!(current.window_days, 1);
    assert_eq!(current.position_timestamp, current.subscription_timestamp);
    assert!(current.follows_clock);
    assert!(current.positions_at_rolled_day);
    assert!(!current.replay);

    let pinned = StartDate::Pinned(1_741_651_200).plan(1);
    assert_eq!(pinned.window_days, 1);
    assert_eq!(pinned.position_timestamp, 1_741_651_200);
    assert_eq!(pinned.subscription_timestamp, 1_741_651_200);
    assert!(!pinned.follows_clock);
    assert!(!pinned.positions_at_rolled_day);
    assert!(
        pinned.replay,
        "pinning to one night is what puts the run on the replay path"
    );
}

// A pinned replay still has to subscribe to the topic it was asked for.
#[test]
fn test_pinned_date_window_contains_that_date() {
    use boom::kafka::AlertConsumer;
    use boom::kafka::ZtfAlertConsumer;
    use boom::utils::enums::ProgramId;

    let pinned = chrono::NaiveDate::from_ymd_opt(2025, 3, 11)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();

    let topics =
        ZtfAlertConsumer::new(None, Some(vec![ProgramId::Public])).subscription_topics(pinned, 1);
    assert!(
        topics.contains(&"ztf_20250311_programid1".to_string()),
        "the pinned date's own topic must be in the window, got {topics:?}"
    );
}

// After an upstream outage the window can be widened to catch up on the nights
// that were missed, rather than skipping them (see issue #569). Recovery is
// still bounded by upstream retention -- ZTF keeps roughly 7 days.
#[test]
fn test_widened_window_reaches_back_over_an_outage() {
    use boom::kafka::AlertConsumer;
    use boom::kafka::ZtfAlertConsumer;
    use boom::utils::enums::ProgramId;

    let ts = chrono::NaiveDate::from_ymd_opt(2026, 8, 10)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();

    let consumer = ZtfAlertConsumer::new(None, Some(vec![ProgramId::Public]));

    // Default: only the current night and the one before it.
    let narrow = consumer.subscription_topics(ts, 1);
    assert_eq!(narrow.len(), 2);
    assert!(!narrow.contains(&"ztf_20260806_programid1".to_string()));

    // Widened to cover a 5-day gap: every intervening night is subscribed.
    let wide = consumer.subscription_topics(ts, 5);
    assert_eq!(wide.len(), 6, "5 days back plus the current day");
    for day in 5..=10 {
        let expected = format!("ztf_202608{:02}_programid1", day);
        assert!(wide.contains(&expected), "missing {expected}");
    }
    // Oldest first, so the backlog is consumed in chronological order.
    assert_eq!(wide.first().unwrap(), "ztf_20260805_programid1");
    assert_eq!(wide.last().unwrap(), "ztf_20260810_programid1");
}

// The other rollover test produces before starting the consumer, so it never
// exercises an empty subscription. This starts one with no data, then produces.
// Requires a local Kafka broker + redis.
#[tokio::test]
async fn test_consumer_started_with_no_data_still_consumes() {
    use boom::conf::{AppConfig, KafkaConsumerConfig};
    use boom::kafka::{consumer, delete_topic, initialize_topic};
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use std::time::Duration;

    let server = "localhost:9092";
    let now_ms = chrono::Utc::now().timestamp_millis();
    let prefix = format!("coldstarttest{now_ms}");
    let output_queue = format!("{prefix}_queue");
    let group_id = format!("{prefix}_group");
    let topic = format!("{prefix}_{}", chrono::Utc::now().format("%Y%m%d"));

    let app_config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let mut con = app_config.build_redis().await.unwrap();
    let _: () = con.del(&output_queue).await.unwrap_or(());

    // Topic exists but is completely empty — the between-nights state.
    initialize_topic(server, &topic, 1).await.unwrap();

    let cold_start_ts = now_ms / 1000 - 3600;
    let kafka_cfg = KafkaConsumerConfig {
        server: server.to_string(),
        group_id,
        schema_registry: None,
        schema_github_fallback_url: None,
        username: None,
        password: None,
        subscription_window_days: 1,
    };

    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let consumer_thread = {
        let config = app_config.clone();
        let oq = output_queue.clone();
        let subscription = vec![topic.clone()];
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.spawn(async move {
                let _ = consumer(
                    "0",
                    std::sync::Arc::new(move |_, _| subscription.clone()),
                    &oq,
                    0,
                    boom::kafka::StartDate::From(cold_start_ts).plan(1),
                    &config,
                    &kafka_cfg,
                    false,
                    "WINTER",
                )
                .await;
            });
            let _ = stop_rx.recv();
            rt.shutdown_timeout(std::time::Duration::from_secs(1));
        })
    };

    // Settle into the initial-assignment loop with nothing to read.
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(
        con.llen::<&str, usize>(&output_queue).await.unwrap_or(0) == 0,
        "nothing should have been consumed yet"
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", server)
        .create()
        .unwrap();
    let payloads: Vec<String> = (0..5).map(|i| format!("late-{i}")).collect();
    for p in &payloads {
        producer
            .send(
                FutureRecord::to(topic.as_str())
                    .payload(p.as_str())
                    .key("k")
                    .timestamp(chrono::Utc::now().timestamp_millis()),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }

    let seen = wait_for_payloads(&mut con, &output_queue, &payloads, Duration::from_secs(60)).await;
    assert!(
        payloads.iter().all(|p| seen.contains(p)),
        "a consumer that started with no data must still consume once data arrives; saw {seen:?}"
    );

    let _ = stop_tx.send(());
    let _ = consumer_thread.join();
    let _: () = con.del(&output_queue).await.unwrap_or(());
    let _ = delete_topic(server, &topic).await;
}
