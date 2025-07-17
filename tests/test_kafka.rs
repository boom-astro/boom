use boom::{
    kafka::{AlertConsumer, AlertProducer, ZtfAlertConsumer, ZtfAlertProducer},
    utils::{enums::ProgramId, testing::TEST_CONFIG_FILE},
};

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
async fn test_produce_and_consume_from_archive() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let date_str = "20240617";
    let date =
        chrono::NaiveDateTime::parse_from_str(&format!("{}T00:00:00", date_str), "%Y%m%dT%H:%M:%S")
            .unwrap();
    let ztf_alert_producer =
        ZtfAlertProducer::new(date.date(), 0, ProgramId::Public, "localhost:9092", false);

    let topic = uuid::Uuid::new_v4().to_string();

    let result = ztf_alert_producer.produce(Some(topic.clone())).await;
    assert!(result.is_ok());
    assert!(result.unwrap() == 710);
    assert!(std::path::Path::new(&format!("data/alerts/ztf/public/{}", &date_str)).exists());

    let timestamp = date.and_utc().timestamp();

    let ztf_alert_consumer = ZtfAlertConsumer::new(None, Some(ProgramId::Public));

    ztf_alert_consumer
        .clear_output_queue(TEST_CONFIG_FILE)
        .await
        .unwrap();

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
