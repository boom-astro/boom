use boom::kafka::{AlertProducer, ZtfAlertProducer};
use boom::utils::enums::ProgramId;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};

use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::test]
async fn test_download_from_archive() {
    let date_str = "20231118";
    let date = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d").unwrap();
    let ztf_alert_producer =
        ZtfAlertProducer::new(date, 0, ProgramId::Public, "localhost:9092", false);
    let result = ztf_alert_producer.download_alerts_from_archive().await;
    assert!(result.is_ok());
    assert!(std::path::Path::new(&format!("data/alerts/ztf/public/{}", &date_str)).exists());
    assert_eq!(result.unwrap(), 271);
}

#[tokio::test]
async fn test_produce_from_archive() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let date_str = "20240617".to_string();
    let date = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d").unwrap();
    let ztf_alert_producer =
        ZtfAlertProducer::new(date, 0, ProgramId::Public, "localhost:9092", false);

    let topic = uuid::Uuid::new_v4().to_string();

    let result = ztf_alert_producer.produce(Some(topic.clone())).await;
    assert!(result.is_ok());
    assert!(result.unwrap() == 710);
    assert!(std::path::Path::new(&format!("data/alerts/ztf/public/{}", &date_str)).exists());

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
            assert_eq!(metadata_topic.partitions().len(), 15);
            let mut total = 0;
            for partition in metadata_topic.partitions().iter() {
                match consumer.fetch_watermarks(
                    metadata_topic.name(),
                    partition.id(),
                    std::time::Duration::from_secs(1),
                ) {
                    Ok((low, high)) => {
                        assert!(low >= 0);
                        assert!(high >= 0);
                        total += high - low;
                    }
                    Err(e) => {
                        assert!(false, "Error fetching watermarks: {:?}", e);
                        return;
                    }
                }
            }

            assert_eq!(total, 710);
        }
    }

    assert!(found_topic);
}
