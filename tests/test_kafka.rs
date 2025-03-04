use boom::kafka::{download_alerts_from_archive, produce_from_archive};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};

use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::test]
async fn test_download_from_archive() {
    let date = "20231118";
    let result = download_alerts_from_archive(date);
    assert!(result.is_ok());
    assert!(std::path::Path::new("data/alerts/ztf/20231118").exists());
    assert_eq!(result.unwrap(), 271);
}

#[tokio::test]
async fn test_produce_from_archive() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let topic = uuid::Uuid::new_v4().to_string();

    let result = produce_from_archive("20240617", 0, Some(topic.clone())).await;
    assert!(result.is_ok());
    assert!(result.unwrap() == 710);
    assert!(std::path::Path::new("data/alerts/ztf/20240617").exists());

    let consumer: BaseConsumer = match ClientConfig::new()
        .set("group.id", "test")
        .set("bootstrap.servers", "localhost:9092")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            assert!(false, "Error creating consumer: {:?}", e);
            return;
        }
    };

    match consumer.subscribe(&[&topic]) {
        Ok(_) => {}
        Err(e) => {
            assert!(false, "Error subscribing to topic: {:?}", e);
            return;
        }
    }

    let metadata = match consumer.fetch_metadata(Some(&topic), std::time::Duration::from_secs(1)) {
        Ok(m) => m,
        Err(e) => {
            assert!(false, "Error fetching metadata: {:?}", e);
            return;
        }
    };

    let mut found_topic = false;

    for metadata_topic in metadata.topics().iter() {
        if metadata_topic.name() == topic {
            found_topic = true;
            assert_eq!(metadata_topic.partitions().len(), 1);
            match consumer.fetch_watermarks(
                metadata_topic.name(),
                metadata_topic.partitions()[0].id(),
                std::time::Duration::from_secs(1),
            ) {
                Ok((low, high)) => {
                    assert_eq!(high - low, 710);
                }
                Err(e) => {
                    assert!(false, "Error fetching watermarks: {:?}", e);
                    return;
                }
            }
        }
    }

    assert!(found_topic);
}
