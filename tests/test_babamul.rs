use boom::{
    alert::{Candidate, DiaSource, LsstCandidate, ZtfCandidate},
    conf::AppConfig,
    enrichment::{EnrichedLsstAlert, EnrichedZtfAlert, LsstAlertProperties, ZtfAlertProperties},
    utils::{
        lightcurves::{Band, PerBandProperties},
        testing::TEST_CONFIG_FILE,
    },
};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    Message,
};
use std::time::Duration;

/// Create a mock enriched ZTF alert for testing
fn create_mock_enriched_ztf_alert(candid: i64, object_id: &str, is_rock: bool) -> EnrichedZtfAlert {
    // Create a minimal Candidate and ZtfCandidate using defaults
    let mut inner_candidate = Candidate::default();
    inner_candidate.candid = candid;
    inner_candidate.ra = 180.0;
    inner_candidate.dec = 0.0;
    inner_candidate.magpsf = 18.5;
    inner_candidate.sigmapsf = 0.1;
    inner_candidate.fid = 1; // g-band

    let candidate = ZtfCandidate {
        candidate: inner_candidate,
        psf_flux: 1000.0,
        psf_flux_err: 10.0,
        snr: 100.0,
        band: Band::G,
    };

    EnrichedZtfAlert {
        candid,
        object_id: object_id.to_string(),
        candidate,
        prv_candidates: vec![],
        fp_hists: vec![],
        properties: ZtfAlertProperties {
            rock: is_rock,
            star: false,
            near_brightstar: false,
            stationary: false,
            photstats: PerBandProperties::default(),
        },
        cutout_science: None,
        cutout_template: None,
        cutout_difference: None,
    }
}

/// Create a mock enriched LSST alert for testing
fn create_mock_enriched_lsst_alert(
    candid: i64,
    object_id: &str,
    reliability: f64,
    pixel_flags: bool,
    is_rock: bool,
) -> EnrichedLsstAlert {
    // Create a minimal DiaSource with default values
    let mut dia_source = DiaSource::default();
    dia_source.candid = candid;
    dia_source.visit = 123456789;
    dia_source.detector = 1;
    dia_source.dia_object_id = Some(987654321);
    dia_source.ss_object_id = None;
    dia_source.midpoint_mjd_tai = 60000.5;
    dia_source.ra = 180.0;
    dia_source.dec = 0.0;
    dia_source.psf_flux = Some(1000.0);
    dia_source.psf_flux_err = Some(10.0);
    dia_source.ap_flux = Some(1100.0);
    dia_source.ap_flux_err = Some(15.0);
    dia_source.pixel_flags = Some(pixel_flags);
    dia_source.reliability = Some(reliability as f32);

    EnrichedLsstAlert {
        candid,
        object_id: object_id.to_string(),
        candidate: LsstCandidate {
            dia_source,
            object_id: object_id.to_string(),
            jd: 2460000.5,
            magpsf: 18.5,
            sigmapsf: 0.1,
            diffmaglim: 20.5,
            isdiffpos: true,
            snr: 100.0,
            magap: 18.6,
            sigmagap: 0.12,
            is_sso: false,
        },
        prv_candidates: vec![],
        fp_hists: vec![],
        properties: LsstAlertProperties {
            rock: is_rock,
            stationary: false,
            photstats: PerBandProperties::default(),
        },
        cutout_science: None,
        cutout_template: None,
        cutout_difference: None,
    }
}

/// Consume messages from a Kafka topic for testing
async fn consume_kafka_messages(
    topic: &str,
    expected_count: usize,
    config: &AppConfig,
) -> Vec<Vec<u8>> {
    let group_id = uuid::Uuid::new_v4().to_string();
    let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &config.kafka.producer.server)
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create Kafka consumer");

    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe to topic");

    let mut messages = Vec::new();
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();

    let mut nb_errors = 0;
    let max_nb_errors = 5;
    while messages.len() < expected_count && start.elapsed() < timeout {
        match consumer.recv().await {
            Ok(message) => {
                if let Some(payload) = message.payload() {
                    messages.push(payload.to_vec());
                }
            }
            Err(e) => {
                eprintln!("Kafka receive error: {:?}", e);
                nb_errors += 1;
                if nb_errors >= max_nb_errors {
                    break;
                }
            }
        }
    }

    messages
}

// add a function to delete a kafka topic before each test to ensure a clean state
async fn delete_kafka_topic(topic: &str, config: &AppConfig) {
    use rdkafka::admin::{AdminClient, AdminOptions};

    let admin: AdminClient<_> = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &config.kafka.producer.server)
        .create()
        .expect("Failed to create Kafka admin client");

    let result = admin
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .unwrap();

    for res in result {
        match res {
            Ok(_) => println!("Successfully deleted topic {}", topic),
            Err((_, e)) => eprintln!("Failed to delete topic {}: {:?}", topic, e),
        }
    }
}

#[tokio::test]
async fn test_babamul_process_ztf_alerts() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Delete the topic before the test to ensure a clean state
    delete_kafka_topic("babamul.ztf.none", &config).await;

    // Create mock enriched ZTF alerts
    let alert1 = create_mock_enriched_ztf_alert(1234567890, "ZTF21aaaaaaa", false);
    let alert2 = create_mock_enriched_ztf_alert(1234567891, "ZTF21aaaaaab", false);

    // Process the alerts
    let result = babamul.process_ztf_alerts(vec![alert1, alert2]).await;
    assert!(
        result.is_ok(),
        "Failed to process ZTF alerts: {:?}",
        result.err()
    );

    // Consume messages from Kafka topic
    let topic = "babamul.ztf.none";
    let messages = consume_kafka_messages(topic, 2, &config).await;
    assert_eq!(messages.len(), 2, "Expected 2 messages in topic {}", topic);
}

#[tokio::test]
async fn test_babamul_process_lsst_alerts() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Delete the topic before the test to ensure a clean state
    delete_kafka_topic("babamul.lsst.none", &config).await;

    // Create mock enriched LSST alerts with good reliability and no flags
    let alert1 = create_mock_enriched_lsst_alert(9876543210, "LSST24aaaaaaa", 0.8, false, false);
    let alert2 = create_mock_enriched_lsst_alert(9876543211, "LSST24aaaaaab", 0.9, false, false);

    // Process the alerts
    let result = babamul.process_lsst_alerts(vec![alert1, alert2]).await;
    assert!(
        result.is_ok(),
        "Failed to process LSST alerts: {:?}",
        result.err()
    );

    // Consume messages from Kafka topic
    let topic = "babamul.lsst.none";
    let messages = consume_kafka_messages(topic, 2, &config).await;
    assert_eq!(messages.len(), 2, "Expected 2 messages in topic {}", topic);
}

#[tokio::test]
async fn test_babamul_filters_low_reliability() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Create alerts with low reliability (should be filtered out)
    let alert1 = create_mock_enriched_lsst_alert(9876543212, "LSST24aaaaaac", 0.3, false, false);
    let alert2 = create_mock_enriched_lsst_alert(9876543213, "LSST24aaaaaad", 0.4, false, false);

    // Process the alerts
    let result = babamul.process_lsst_alerts(vec![alert1, alert2]).await;
    assert!(
        result.is_ok(),
        "Failed to process LSST alerts: {:?}",
        result.err()
    );

    // No messages should be sent
    let topic = "babamul.lsst.none";
    let messages = consume_kafka_messages(topic, 0, &config).await;
    assert_eq!(
        messages.len(),
        0,
        "Expected 0 messages in topic {} for low reliability alerts",
        topic
    );
}

#[tokio::test]
async fn test_babamul_filters_rocks() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Create alerts marked as rocks (should be filtered out)
    let ztf_rock = create_mock_enriched_ztf_alert(1234567892, "ZTF21aaaaaac", true);
    let lsst_rock = create_mock_enriched_lsst_alert(9876543214, "LSST24aaaaaae", 0.9, false, true);

    // Process the alerts
    let ztf_result = babamul.process_ztf_alerts(vec![ztf_rock]).await;
    let lsst_result = babamul.process_lsst_alerts(vec![lsst_rock]).await;

    assert!(ztf_result.is_ok());
    assert!(lsst_result.is_ok());

    // No messages should be sent for rocks
    let ztf_messages = consume_kafka_messages("babamul.ztf.none", 0, &config).await;
    let lsst_messages = consume_kafka_messages("babamul.lsst.none", 0, &config).await;

    assert_eq!(
        ztf_messages.len(),
        0,
        "Expected 0 ZTF messages for rock alerts"
    );
    assert_eq!(
        lsst_messages.len(),
        0,
        "Expected 0 LSST messages for rock alerts"
    );
}

#[tokio::test]
async fn test_babamul_filters_pixel_flags() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Create LSST alert with pixel_flags set (should be filtered out)
    let alert = create_mock_enriched_lsst_alert(9876543215, "LSST24aaaaaaf", 0.9, true, false);

    let result = babamul.process_lsst_alerts(vec![alert]).await;
    assert!(result.is_ok());

    // No messages should be sent for alerts with pixel flags
    let messages = consume_kafka_messages("babamul.lsst.none", 0, &config).await;
    assert_eq!(
        messages.len(),
        0,
        "Expected 0 messages for alerts with pixel_flags"
    );
}
