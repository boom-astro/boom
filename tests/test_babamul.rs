use apache_avro::AvroSchema;
use boom::{
    alert::{Candidate, DiaSource, LsstCandidate, ZtfCandidate},
    conf::AppConfig,
    enrichment::{
        babamul::{EnrichedLsstAlert, EnrichedZtfAlert},
        LsstAlertProperties, ZtfAlertProperties,
    },
    utils::{
        cutouts::AlertCutout,
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
    inner_candidate.programid = 1; // public

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
        prv_nondetections: vec![],
        fp_hists: vec![],
        properties: ZtfAlertProperties {
            rock: is_rock,
            star: false,
            near_brightstar: false,
            stationary: false,
            photstats: PerBandProperties::default(),
            multisurvey_photstats: PerBandProperties::default(),
        },
        cutout_science: None,
        cutout_template: None,
        cutout_difference: None,
        survey_matches: None,
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
    dia_source.midpoint_mjd_tai = 60000.5;
    dia_source.ra = 180.0;
    dia_source.dec = 0.0;
    dia_source.psf_flux = Some(1000.0);
    dia_source.psf_flux_err = Some(10.0);
    dia_source.ap_flux = Some(1100.0);
    dia_source.ap_flux_err = Some(15.0);
    dia_source.pixel_flags = Some(pixel_flags);
    dia_source.reliability = Some(reliability as f32);

    let ss_object_id = if is_rock { Some(555555_i64) } else { None };
    dia_source.ss_object_id = ss_object_id;

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
        survey_matches: None,
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
    let timeout = Duration::from_secs(8);
    let start = std::time::Instant::now();

    let mut nb_errors = 0;
    let max_nb_errors = 5;
    while messages.len() < expected_count && start.elapsed() < timeout {
        match tokio::time::timeout(timeout - start.elapsed(), consumer.recv()).await {
            Ok(Ok(message)) => {
                if let Some(payload) = message.payload() {
                    messages.push(payload.to_vec());
                }
            }
            Ok(Err(e)) => {
                eprintln!("Kafka receive error: {:?}", e);
                nb_errors += 1;
                if nb_errors >= max_nb_errors {
                    break;
                }
            }
            Err(_) => {
                // timeout elapsed waiting for message
                break;
            }
        }
    }

    messages
}

// Delete a Kafka topic; tolerate "unknown topic" errors to avoid flakiness
async fn delete_kafka_topic(topic: &str, config: &AppConfig) {
    use rdkafka::admin::{AdminClient, AdminOptions};

    let admin: AdminClient<_> = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &config.kafka.producer.server)
        .create()
        .expect("Failed to create Kafka admin client");

    // Best effort delete; UnknownTopicOrPartition is fine
    if let Ok(results) = admin.delete_topics(&[topic], &AdminOptions::new()).await {
        for res in results {
            if let Err((_, e)) = res {
                // Ignore if topic does not exist; surface other errors
                if format!("{:?}", e).contains("UnknownTopicOrPartition") {
                    eprintln!("Topic {} did not exist before test (ignored)", topic);
                } else {
                    eprintln!("Failed to delete topic {}: {:?}", topic, e);
                }
            }
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

#[tokio::test]
async fn test_babamul_lsst_with_ztf_match() {
    use boom::alert::{
        DiaForcedSource, FpHist, LsstAlert, LsstAliases, LsstForcedPhot, LsstObject,
        PrvCandidate as ZtfPrvCandidateFields, ZtfAliases, ZtfForcedPhot, ZtfObject,
        ZtfPrvCandidate,
    };
    use boom::enrichment::EnrichmentWorker;
    use boom::utils::spatial::Coordinates;
    use flare::Time;
    use mongodb::bson::doc;
    use std::time::{SystemTime, UNIX_EPOCH};

    let db = boom::conf::get_test_db().await;
    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    delete_kafka_topic("babamul.lsst.none", &config).await;
    let now = Time::now().to_jd();

    // Use unique IDs based on current timestamp to avoid collisions
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let lsst_object_id = format!("LSST24enrichtest{}", timestamp);
    let ztf_match_id = format!("ZTF21enrichtest{}", timestamp);
    let lsst_alert_id = 9876543220i64 + (timestamp % 1000000) as i64;

    // Ensure a clean slate for these IDs
    let ztf_aux_collection = db.collection::<ZtfObject>("ZTF_alerts_aux");
    let lsst_alerts_collection = db.collection::<LsstAlert>("LSST_alerts");
    let lsst_aux_collection = db.collection::<LsstObject>("LSST_alerts_aux");
    let lsst_cutouts_collection = db.collection::<AlertCutout>("LSST_alerts_cutouts");

    ztf_aux_collection
        .delete_many(doc! {"_id": {"$in": [&ztf_match_id]}})
        .await
        .expect("Failed to cleanup ZTF aux fixture");
    lsst_alerts_collection
        .delete_many(doc! {"_id": {"$in": [lsst_alert_id]}})
        .await
        .expect("Failed to cleanup LSST alerts fixture");
    lsst_aux_collection
        .delete_many(doc! {"_id": {"$in": [&lsst_object_id]}})
        .await
        .expect("Failed to cleanup LSST aux fixture");
    lsst_cutouts_collection
        .delete_many(doc! {"_id": {"$in": [lsst_alert_id]}})
        .await
        .expect("Failed to cleanup LSST cutouts fixture");

    // Insert ZTF aux with alias data
    let ztf_prv_candidate = ZtfPrvCandidate {
        prv_candidate: ZtfPrvCandidateFields {
            jd: 2459999.5,
            fid: 1,
            pid: 1,
            programid: 1,
            ra: Some(180.0),
            dec: Some(0.0),
            magpsf: Some(19.0),
            sigmapsf: Some(0.07),
            diffmaglim: Some(21.0),
            ..Default::default()
        },
        psf_flux: Some(1300.0),
        psf_flux_err: Some(13.0),
        snr: Some(100.0),
        band: Band::G,
    };

    let ztf_forced_phot = ZtfForcedPhot {
        fp_hist: FpHist {
            fid: 1,
            pid: 1,
            rfid: 1,
            jd: 2459998.5,
            diffmaglim: Some(21.2),
            programid: 1,
            forcediffimflux: Some(1250.0),
            forcediffimfluxunc: Some(12.0),
            ..Default::default()
        },
        magpsf: Some(19.2),
        sigmapsf: Some(0.09),
        psf_flux: Some(1250.0),
        psf_flux_err: Some(12.0),
        isdiffpos: Some(true),
        snr: Some(100.0),
        band: Band::G,
    };

    let ztf_aux = ZtfObject {
        object_id: ztf_match_id.clone(),
        prv_candidates: vec![ztf_prv_candidate],
        prv_nondetections: Vec::new(),
        fp_hists: vec![ztf_forced_phot],
        cross_matches: None,
        aliases: Some(ZtfAliases {
            lsst: Vec::new(),
            decam: Vec::new(),
        }),
        coordinates: Coordinates::new(180.0, 0.0),
        created_at: now,
        updated_at: now,
    };

    ztf_aux_collection
        .insert_one(&ztf_aux)
        .await
        .expect("Failed to insert ZTF aux");

    // Insert LSST alert with good reliability and no flags
    let lsst_dia_source = {
        let mut dia_source = DiaSource::default();
        dia_source.candid = lsst_alert_id;
        dia_source.visit = 123456789;
        dia_source.detector = 1;
        dia_source.dia_object_id = Some(987654321);
        dia_source.midpoint_mjd_tai = 60000.5;
        dia_source.ra = 180.0;
        dia_source.dec = 0.0;
        dia_source.psf_flux = Some(1000.0);
        dia_source.psf_flux_err = Some(10.0);
        dia_source.ap_flux = Some(1100.0);
        dia_source.ap_flux_err = Some(15.0);
        dia_source.pixel_flags = Some(false);
        dia_source.reliability = Some(0.9);
        dia_source.band = Some(Band::G);
        dia_source
    };

    let lsst_candidate = LsstCandidate {
        dia_source: lsst_dia_source.clone(),
        object_id: lsst_object_id.clone(),
        jd: 2460000.5,
        magpsf: 18.5,
        sigmapsf: 0.1,
        diffmaglim: 20.5,
        isdiffpos: true,
        snr: 100.0,
        magap: 18.6,
        sigmagap: 0.12,
    };

    let lsst_alert = LsstAlert {
        candid: lsst_alert_id,
        object_id: lsst_object_id.clone(),
        ss_object_id: None,
        candidate: lsst_candidate.clone(),
        coordinates: Coordinates::new(180.0, 0.0),
        created_at: now,
        updated_at: now,
    };

    lsst_alerts_collection
        .insert_one(&lsst_alert)
        .await
        .expect("Failed to insert LSST alert");

    // Insert cutouts for the alert
    let cutout_doc = AlertCutout {
        candid: lsst_alert_id,
        object_id: lsst_object_id.clone(),
        science: vec![1, 2, 3, 4, 5],
        template: vec![6, 7, 8, 9, 10],
        difference: vec![11, 12, 13, 14, 15],
    };
    let cutout_storage = config
        .build_cutout_storage(&boom::utils::enums::Survey::Lsst)
        .await
        .expect("Failed to build cutout storage");

    cutout_storage
        .insert_cutouts(
            lsst_alert_id,
            &lsst_object_id,
            cutout_doc.science,
            cutout_doc.template,
            cutout_doc.difference,
        )
        .await
        .expect("Failed to insert LSST cutout");

    // Insert LSST aux with aliases pointing to ZTF
    let lsst_forced_phot = LsstForcedPhot {
        dia_forced_source: DiaForcedSource {
            dia_forced_source_id: 1,
            object_id: 987654321,
            ra: 180.0,
            dec: 0.0,
            visit: 123456789,
            detector: 1,
            psf_flux: Some(1150.0),
            psf_flux_err: Some(11.0),
            midpoint_mjd_tai: 60000.4,
            science_flux: Some(1150.0),
            science_flux_err: Some(11.0),
            band: Some(Band::G),
        },
        jd: 2459998.5,
        magpsf: Some(18.2),
        sigmapsf: Some(0.08),
        diffmaglim: 20.2,
        isdiffpos: Some(true),
        snr: Some(105.0),
    };

    let lsst_aux = LsstObject {
        object_id: lsst_object_id.clone(),
        prv_candidates: vec![lsst_candidate.clone()],
        fp_hists: vec![lsst_forced_phot],
        is_sso: false,
        cross_matches: None,
        aliases: Some(LsstAliases {
            ztf: vec![ztf_match_id.clone()],
            decam: Vec::new(),
        }),
        coordinates: Coordinates::new(180.0, 0.0),
        created_at: now,
        updated_at: now,
    };

    lsst_aux_collection
        .insert_one(&lsst_aux)
        .await
        .expect("Failed to insert LSST aux");

    // Create enrichment worker and process alert
    let mut enrichment_worker = boom::enrichment::LsstEnrichmentWorker::new(TEST_CONFIG_FILE)
        .await
        .expect("Failed to create enrichment worker");

    let processed = enrichment_worker
        .process_alerts(&[lsst_alert_id])
        .await
        .expect("Failed to process alerts");

    assert_eq!(
        processed.len(),
        1,
        "Expected 1 processed alert from enrichment worker"
    );

    // Verify that the Babamul message was published - since the alert passed enrichment
    // with good reliability and no pixel flags or rock flag, it should be sent to Babamul
    // Fetch a few messages to tolerate leftover topic data and search for our alert
    let messages = consume_kafka_messages("babamul.lsst.none", 3, &config).await;
    assert!(
        !messages.is_empty(),
        "Expected at least one Babamul message with enriched alert containing matches"
    );

    // Decode the Avro message to verify matches are present
    let schema = EnrichedLsstAlert::get_schema();
    // Read all records from all messages and check for matches on our alert
    let mut found_match = false;
    'outer: for msg in messages {
        let reader = apache_avro::Reader::with_schema(&schema, &msg[..])
            .expect("Failed to create Avro reader");

        for record_result in reader {
            let value = record_result.expect("Failed to read Avro record");

            if let apache_avro::types::Value::Record(fields) = value {
                // Ensure this record matches our object_id to avoid stale topic data
                let has_object_id = fields.iter().any(|(name, val)| {
                    name == "objectId"
                        && matches!(val, apache_avro::types::Value::String(s) if s == &lsst_object_id)
                });

                if !has_object_id {
                    continue;
                }

                if let Some((_, survey_matches_value)) =
                    fields.iter().find(|(name, _)| name == "survey_matches")
                {
                    if let apache_avro::types::Value::Union(_, boxed) = survey_matches_value {
                        if let apache_avro::types::Value::Record(fields) = &**boxed {
                            if let Some((_, ztf_value)) =
                                fields.iter().find(|(name, _)| name == "ztf")
                            {
                                if let apache_avro::types::Value::Union(_, ztf_boxed) = ztf_value {
                                    if let apache_avro::types::Value::Record(obj_fields) =
                                        &**ztf_boxed
                                    {
                                        found_match = obj_fields.iter().any(|(field_name, field_value)| {
                                            field_name == "object_id"
                                                && matches!(field_value, apache_avro::types::Value::String(s) if s == &ztf_match_id)
                                        });
                                        if found_match {
                                            break 'outer;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    assert!(
        found_match,
        "Expected to find ZTF match with object_id: {} in Babamul message",
        ztf_match_id
    );

    // Clean up inserted fixtures to avoid leaking state between tests
    ztf_aux_collection
        .delete_many(doc! {"_id": {"$in": [&ztf_match_id]}})
        .await
        .expect("Failed to cleanup ZTF aux fixture after test");
    lsst_alerts_collection
        .delete_many(doc! {"_id": {"$in": [lsst_alert_id]}})
        .await
        .expect("Failed to cleanup LSST alerts fixture after test");
    lsst_aux_collection
        .delete_many(doc! {"_id": {"$in": [&lsst_object_id]}})
        .await
        .expect("Failed to cleanup LSST aux fixture after test");
}

#[tokio::test]
async fn test_babamul_ztf_with_lsst_match() {
    use boom::alert::{
        AlertWorker, DiaForcedSource, DiaSource, LsstAliases, LsstCandidate, LsstForcedPhot,
        LsstObject, ZtfObject,
    };
    use boom::enrichment::EnrichmentWorker;
    use boom::utils::enums::Survey;
    use boom::utils::testing::AlertRandomizer;
    use flare::Time;
    use mongodb::bson::doc;
    use std::time::{SystemTime, UNIX_EPOCH};

    let db = boom::conf::get_test_db().await;
    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let mut ztf_alert_worker = boom::utils::testing::ztf_alert_worker().await;
    delete_kafka_topic("babamul.ztf.none", &config).await;
    let now = Time::now().to_jd();

    // Use unique ID based on current timestamp
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let lsst_match_id = format!("LSST21enrichtest{}", timestamp);

    // Use AlertRandomizer to create a realistic ZTF alert with all required fields
    let (ztf_candid, ztf_object_id, _, _, ztf_bytes) =
        AlertRandomizer::new_randomized(Survey::Ztf).get().await;

    // Get collection references and ensure a clean slate for these IDs
    let lsst_aux_collection = db.collection::<LsstObject>("LSST_alerts_aux");
    let ztf_aux_collection = db.collection::<ZtfObject>("ZTF_alerts_aux");
    let ztf_alerts_collection = db.collection::<boom::alert::ZtfAlert>("ZTF_alerts");

    // Clean up any existing fixtures with this ID
    lsst_aux_collection
        .delete_many(doc! {"_id": {"$in": [&lsst_match_id]}})
        .await
        .expect("Failed to cleanup LSST aux fixture");
    ztf_alerts_collection
        .delete_many(doc! {"_id": {"$in": [ztf_candid]}})
        .await
        .expect("Failed to cleanup ZTF alerts fixture");
    ztf_aux_collection
        .delete_many(doc! {"_id": {"$in": [&ztf_object_id]}})
        .await
        .expect("Failed to cleanup ZTF aux fixture");

    // Insert the alert and get cutouts
    ztf_alert_worker.process_alert(&ztf_bytes).await.unwrap();

    // Insert fake LSST aux for matching using typed structs

    let lsst_dia_source = {
        let mut dia = DiaSource::default();
        dia.candid = 1;
        dia.visit = 123456789;
        dia.detector = 1;
        dia.dia_object_id = Some(42);
        dia.midpoint_mjd_tai = 60000.5;
        dia.ra = 180.0;
        dia.dec = 0.0;
        dia.psf_flux = Some(1200.0);
        dia.psf_flux_err = Some(12.0);
        dia.ap_flux = Some(1250.0);
        dia.ap_flux_err = Some(13.0);
        dia.pixel_flags = Some(false);
        dia.reliability = Some(0.95);
        dia.band = Some(Band::G);
        dia
    };

    let lsst_candidate = LsstCandidate::try_from(lsst_dia_source.clone()).unwrap();

    let lsst_forced_phot = LsstForcedPhot::try_from(DiaForcedSource {
        dia_forced_source_id: 1,
        object_id: 42,
        ra: 180.0,
        dec: 0.0,
        visit: 123456789,
        detector: 1,
        psf_flux: Some(1150.0),
        psf_flux_err: Some(11.0),
        midpoint_mjd_tai: 60000.4,
        science_flux: Some(1150.0),
        science_flux_err: Some(11.0),
        band: Some(Band::G),
    })
    .unwrap();

    let lsst_aux = LsstObject {
        object_id: lsst_match_id.clone(),
        prv_candidates: vec![lsst_candidate],
        fp_hists: vec![lsst_forced_phot],
        is_sso: false,
        cross_matches: None,
        aliases: Some(LsstAliases {
            ztf: Vec::new(),
            decam: Vec::new(),
        }),
        coordinates: boom::utils::spatial::Coordinates::new(180.0, 0.0),
        created_at: now,
        updated_at: now,
    };

    lsst_aux_collection
        .insert_one(&lsst_aux)
        .await
        .expect("Failed to insert LSST aux");

    // Update the ZTF aux with aliases pointing to LSST
    let ztf_aux_collection = db.collection::<ZtfObject>("ZTF_alerts_aux");
    ztf_aux_collection
        .update_one(
            doc! {"_id": &ztf_object_id},
            doc! {"$set": {"aliases.LSST": [&lsst_match_id]}},
        )
        .await
        .expect("Failed to update ZTF aux with aliases");

    // Create enrichment worker and process alert
    let mut enrichment_worker = boom::enrichment::ZtfEnrichmentWorker::new(TEST_CONFIG_FILE)
        .await
        .expect("Failed to create enrichment worker");

    let processed = enrichment_worker
        .process_alerts(&[ztf_candid])
        .await
        .expect("Failed to process alerts");

    assert_eq!(
        processed.len(),
        1,
        "Expected 1 processed alert from enrichment worker"
    );

    // Verify that the Babamul message was published
    let messages = consume_kafka_messages("babamul.ztf.none", 3, &config).await;
    assert!(
        !messages.is_empty(),
        "Expected at least one Babamul message with enriched alert containing matches"
    );

    // Decode the Avro message to verify matches are present
    let schema = EnrichedZtfAlert::get_schema();
    // Read all records from all messages and check for matches on our alert
    let mut found_match = false;
    'outer: for msg in messages {
        let reader = apache_avro::Reader::with_schema(&schema, &msg[..])
            .expect("Failed to create Avro reader");

        for record_result in reader {
            let value = record_result.expect("Failed to read Avro record");

            if let apache_avro::types::Value::Record(fields) = value {
                // Ensure this record matches our object_id to avoid stale topic data
                let has_object_id = fields.iter().any(|(name, val)| {
                    name == "objectId"
                        && matches!(val, apache_avro::types::Value::String(s) if s == &ztf_object_id)
                });

                if !has_object_id {
                    continue;
                }

                if let Some((_, survey_matches_value)) =
                    fields.iter().find(|(name, _)| name == "survey_matches")
                {
                    if let apache_avro::types::Value::Union(_, boxed) = survey_matches_value {
                        if let apache_avro::types::Value::Record(fields) = &**boxed {
                            if let Some((_, lsst_value)) =
                                fields.iter().find(|(name, _)| name == "lsst")
                            {
                                if let apache_avro::types::Value::Union(_, lsst_boxed) = lsst_value
                                {
                                    if let apache_avro::types::Value::Record(obj_fields) =
                                        &**lsst_boxed
                                    {
                                        found_match = obj_fields.iter().any(|(field_name, field_value)| {
                                            field_name == "object_id"
                                                && matches!(field_value, apache_avro::types::Value::String(s) if s == &lsst_match_id)
                                        });
                                        if found_match {
                                            break 'outer;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    assert!(
        found_match,
        "Expected to find LSST match with object_id: {} in Babamul message",
        lsst_match_id
    );

    // Clean up inserted fixtures to avoid leaking state between tests
    lsst_aux_collection
        .delete_many(doc! {"_id": {"$in": [&lsst_match_id]}})
        .await
        .expect("Failed to cleanup LSST aux fixture after test");
    ztf_alerts_collection
        .delete_many(doc! {"_id": {"$in": [ztf_candid]}})
        .await
        .expect("Failed to cleanup ZTF alerts fixture after test");
    ztf_aux_collection
        .delete_many(doc! {"_id": {"$in": [&ztf_object_id]}})
        .await
        .expect("Failed to cleanup ZTF aux fixture after test");
}
