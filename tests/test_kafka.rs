use boom::{
    kafka::{
        count_messages, delete_topic, AlertConsumer, AlertProducer, ZtfAlertConsumer,
        ZtfAlertProducer,
    },
    utils::{data::count_files_in_dir, enums::ProgramId, testing::TEST_CONFIG_FILE},
};

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
    let data_directory = Path::new("data/alerts/ztf/public").join(date_str);
    let expected_count = 271u32;

    let producer = ZtfAlertProducer::new(
        naive(date_str).date(),
        0,
        ProgramId::Public,
        "localhost:9092",
        false,
    );
    let result = producer.download_alerts_from_archive().await;

    // Verify the producer succeeded and reports the expected count:
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), expected_count as i64);

    // Verify the data directory exists and has the right number of avro files:
    assert_eq!(
        Path::new(&producer.data_directory())
            .canonicalize()
            .unwrap(),
        data_directory.canonicalize().unwrap()
    );
    assert!(data_directory.exists());
    let avro_count = count_files_in_dir(data_directory.to_str().unwrap(), Some(&["avro"])).unwrap();
    assert_eq!(avro_count, expected_count as usize);
}

#[tokio::test]
async fn test_produce_and_consume_from_archive() {
    let date_str = "20240617";
    let topic = uuid::Uuid::new_v4().to_string();
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

    let ztf_alert_consumer = ZtfAlertConsumer::new(None, Some(ProgramId::Public));

    ztf_alert_consumer
        .clear_output_queue(TEST_CONFIG_FILE)
        .await
        .unwrap();

    let timestamp = datetime.and_utc().timestamp();
    ztf_alert_consumer
        .consume(
            timestamp,
            TEST_CONFIG_FILE,
            true,
            None,
            None,
            None,
            Some(topic),
        )
        .await
        .unwrap();
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
