use apache_avro::AvroSchema;
use boom::{
    alert::{Candidate, DiaSource, LsstCandidate, ZtfCandidate},
    conf::AppConfig,
    enrichment::{
        babamul::{EnrichedLsstAlert, EnrichedZtfAlert},
        LsstAlertProperties, ZtfAlertProperties,
    },
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

/// Create realistic LSPSC cross-matches with configurable distance and score for testing
///
/// # Arguments
/// * `distance_arcsec` - Distance to nearest match (default: 0.5 for stellar)
/// * `score` - Score of nearest match (default: 0.95 for stellar)
/// * `single_match` - If true, only include the nearest match (useful for hostless testing)
///
/// # Thresholds for reference
/// * Stellar: distance ≤ 1.0 arcsec AND score > 0.5
/// * Hosted: any match has score < 0.5
/// * Hostless: matches exist but neither stellar nor hosted
fn create_lspsc_cross_matches(
    distance_arcsec: Option<f64>,
    score: Option<f64>,
    single_match: bool,
) -> std::collections::HashMap<String, Vec<serde_json::Value>> {
    use serde_json::json;

    let distance = distance_arcsec.unwrap_or(0.5); // Default: stellar distance
    let score = score.unwrap_or(0.95); // Default: stellar score

    let mut matches = std::collections::HashMap::new();
    let mut lspsc_matches = vec![json!({
        "_id": 1001,
        "ra": 150.001,
        "dec": 30.002,
        "distance_arcsec": distance,
        "score": score,
        "magwhite": 18.3
    })];

    // Add additional matches unless single_match is true
    if !single_match {
        lspsc_matches.extend(vec![
            json!({
                "_id": 1002,
                "ra": 150.02,
                "dec": 30.05,
                "distance_arcsec": 1.5,  // Beyond stellar threshold
                "score": 0.75,           // Above hosted threshold
                "magwhite": 19.1
            }),
            json!({
                "_id": 1003,
                "ra": 150.25,
                "dec": 30.10,
                "distance_arcsec": 5.0,  // Far match
                "score": 0.45,           // Below hosted threshold
                "magwhite": 20.2
            }),
        ]);
    }

    matches.insert("LSPSC".to_string(), lspsc_matches);
    matches
}

/// Create a mock enriched ZTF alert for testing
fn create_mock_enriched_ztf_alert(candid: i64, object_id: &str, is_rock: bool) -> EnrichedZtfAlert {
    // Create a minimal Candidate and ZtfCandidate using defaults
    let mut inner_candidate = Candidate::default();
    inner_candidate.candid = candid;
    inner_candidate.ra = 150.0;
    inner_candidate.dec = 30.0;
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
            multisurvey_photstats: Some(PerBandProperties::default()),
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
    ra_override: Option<f64>,
    dec_override: Option<f64>,
) -> EnrichedLsstAlert {
    create_mock_enriched_lsst_alert_with_matches(
        candid,
        object_id,
        reliability,
        pixel_flags,
        is_rock,
        None,
        None,
        ra_override,
        dec_override,
    )
}

fn create_mock_enriched_lsst_alert_with_matches(
    candid: i64,
    object_id: &str,
    reliability: f64,
    pixel_flags: bool,
    is_rock: bool,
    cross_matches: Option<std::collections::HashMap<String, Vec<serde_json::Value>>>,
    survey_matches: Option<boom::enrichment::LsstSurveyMatches>,
    ra_override: Option<f64>,
    dec_override: Option<f64>,
) -> EnrichedLsstAlert {
    // Create a minimal DiaSource with default values
    let mut dia_source = DiaSource::default();
    dia_source.candid = candid;
    dia_source.visit = 123456789;
    dia_source.detector = 1;
    dia_source.dia_object_id = Some(987654321);
    dia_source.midpoint_mjd_tai = 60000.5;
    dia_source.ra = ra_override.unwrap_or(150.0);
    dia_source.dec = dec_override.unwrap_or(30.0);
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
            star: Some(false),
            stationary: false,
            photstats: PerBandProperties::default(),
        },
        cutout_science: None,
        cutout_template: None,
        cutout_difference: None,
        survey_matches,
        cross_matches: boom::enrichment::babamul::CrossMatchesWrapper(cross_matches),
    }
}

/// Consume messages from a Kafka topic for testing
async fn consume_kafka_messages(topic: &str, config: &AppConfig) -> Vec<Vec<u8>> {
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
    while start.elapsed() < timeout {
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

#[test]
fn test_compute_babamul_category() {
    use boom::enrichment::{LsstSurveyMatches, ZtfMatch};

    // Test case 1: No ZTF match + stellar LSPSC → "no-ztf-match.stellar"
    // distance ≤ 1.0 AND score > 0.5
    let cross_matches = create_lspsc_cross_matches(Some(0.5), Some(0.95), false);
    let alert_stellar = create_mock_enriched_lsst_alert_with_matches(
        9876543210,
        "LSST24aaaaaaa",
        0.8,
        false,
        false,
        Some(cross_matches),
        None, // No ZTF survey match
        None,
        None,
    );
    let category = alert_stellar.compute_babamul_category();
    assert_eq!(
        category, "no-ztf-match.stellar",
        "Alert with close, high-score match should be stellar"
    );

    // Test case 2: No ZTF match + hosted LSPSC → "no-ztf-match.hosted"
    // nearest match has score < 0.5
    let cross_matches = create_lspsc_cross_matches(Some(2.0), Some(0.3), false);
    let alert_hosted = create_mock_enriched_lsst_alert_with_matches(
        9876543211,
        "LSST24aaaaaab",
        0.8,
        false,
        false,
        Some(cross_matches),
        None,
        None,
        None,
    );
    let category = alert_hosted.compute_babamul_category();
    assert_eq!(
        category, "no-ztf-match.hosted",
        "Alert with low-score match should be hosted"
    );

    // Test case 3: No ZTF match + hostless LSPSC → "no-ztf-match.hostless"
    // distance > 1.0 AND all scores > 0.5 (no hosted criteria met)
    let cross_matches = create_lspsc_cross_matches(Some(2.5), Some(0.8), true);
    let alert_hostless = create_mock_enriched_lsst_alert_with_matches(
        9876543212,
        "LSST24aaaaaac",
        0.8,
        false,
        false,
        Some(cross_matches),
        None,
        None,
        None,
    );
    let category = alert_hostless.compute_babamul_category();
    assert_eq!(
        category, "no-ztf-match.hostless",
        "Alert with distant, high-score match should be hostless"
    );

    // Test case 4: No ZTF match + no matches + in footprint → "no-ztf-match.hostless"
    let alert_no_matches = create_mock_enriched_lsst_alert_with_matches(
        9876543213,
        "LSST24aaaaaad",
        0.8,
        false,
        false,
        None,
        None, // No ZTF survey match
        None,
        None,
    );
    let category = alert_no_matches.compute_babamul_category();
    assert_eq!(
        category, "no-ztf-match.hostless",
        "Alert with no matches but in footprint should be hostless"
    );

    // Test case 5: ZTF match + stellar LSPSC → "ztf-match.stellar"
    let cross_matches = create_lspsc_cross_matches(Some(0.5), Some(0.95), false);
    let survey_matches = Some(LsstSurveyMatches {
        ztf: Some(ZtfMatch {
            object_id: "ZTF24aaaaaaa".to_string(),
            ra: 180.0,
            dec: 0.0,
            prv_candidates: vec![],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
    });
    let alert_ztf_stellar = create_mock_enriched_lsst_alert_with_matches(
        9876543214,
        "LSST24aaaaaae",
        0.8,
        false,
        false,
        Some(cross_matches),
        survey_matches,
        None,
        None,
    );
    let category = alert_ztf_stellar.compute_babamul_category();
    assert_eq!(
        category, "ztf-match.stellar",
        "Alert with ZTF match and stellar LSPSC should be ztf-match.stellar"
    );

    // Test case 6: ZTF match + hosted LSPSC → "ztf-match.hosted"
    let cross_matches = create_lspsc_cross_matches(Some(2.0), Some(0.3), false);
    let survey_matches = Some(LsstSurveyMatches {
        ztf: Some(ZtfMatch {
            object_id: "ZTF24aaaaaab".to_string(),
            ra: 180.1,
            dec: 0.1,
            prv_candidates: vec![],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
    });
    let alert_ztf_hosted = create_mock_enriched_lsst_alert_with_matches(
        9876543215,
        "LSST24aaaaaaf",
        0.8,
        false,
        false,
        Some(cross_matches),
        survey_matches,
        None,
        None,
    );
    let category = alert_ztf_hosted.compute_babamul_category();
    assert_eq!(
        category, "ztf-match.hosted",
        "Alert with ZTF match and hosted LSPSC should be ztf-match.hosted"
    );

    // Test case 7: ZTF match + hostless LSPSC → "ztf-match.hostless"
    let cross_matches = create_lspsc_cross_matches(Some(2.5), Some(0.8), true);
    let survey_matches = Some(LsstSurveyMatches {
        ztf: Some(ZtfMatch {
            object_id: "ZTF24aaaaaac".to_string(),
            ra: 180.2,
            dec: 0.2,
            prv_candidates: vec![],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
    });
    let alert_ztf_hostless = create_mock_enriched_lsst_alert_with_matches(
        9876543216,
        "LSST24aaaaaag",
        0.8,
        false,
        false,
        Some(cross_matches),
        survey_matches,
        None,
        None,
    );
    let category = alert_ztf_hostless.compute_babamul_category();
    assert_eq!(
        category, "ztf-match.hostless",
        "Alert with ZTF match and hostless LSPSC should be ztf-match.hostless"
    );

    // Test case 8: ZTF match + no LSPSC + in footprint → "ztf-match.hostless"
    let survey_matches = Some(LsstSurveyMatches {
        ztf: Some(ZtfMatch {
            object_id: "ZTF24aaaaaad".to_string(),
            ra: 180.5,
            dec: 0.5,
            prv_candidates: vec![],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
    });
    let alert_ztf_unknown = create_mock_enriched_lsst_alert_with_matches(
        9876543217,
        "LSST24aaaaaah",
        0.8,
        false,
        false,
        None, // No LSPSC cross-matches
        survey_matches,
        None,
        None,
    );
    let category = alert_ztf_unknown.compute_babamul_category();
    assert_eq!(
        category, "ztf-match.hostless",
        "Alert with ZTF match but no LSPSC and in footprint should be ztf-match.hostless"
    );

    // Test case 9: No LSPSC + no ZTF match + out of footprint → "unknown"
    let alert_unknown = create_mock_enriched_lsst_alert_with_matches(
        9876543218,
        "LSST24aaaaaai",
        0.8,
        false,
        false,
        Some(std::collections::HashMap::new()), // No matches
        None,                                   // No ZTF survey match
        Some(265.05),                           // RA out of footprint
        Some(-32.25),                           // Dec out of footprint
    );
    let category = alert_unknown.compute_babamul_category();
    assert_eq!(
        category, "no-ztf-match.unknown",
        "Alert with no matches and no ZTF match should be unknown"
    );

    // Test case 10: LSPSC exists but empty + no ZTF match + out of footprint → "unknown"
    let alert_empty_lspsc = create_mock_enriched_lsst_alert_with_matches(
        9876543219,
        "LSST24aaaaaaj",
        0.8,
        false,
        false,
        Some(std::collections::HashMap::new()), // Empty LSPSC matches
        None,                                   // No ZTF survey match
        Some(265.05),                           // RA out of footprint
        Some(-32.25),                           // Dec out of footprint
    );
    let category = alert_empty_lspsc.compute_babamul_category();
    assert_eq!(
        category, "no-ztf-match.unknown",
        "Alert with empty LSPSC matches and no ZTF match should be unknown"
    );
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

    assert_eq!(result.unwrap(), 2, "Expected 2 messages to be sent");
}

#[tokio::test]
async fn test_babamul_process_lsst_alerts() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);
    let topic = "babamul.lsst.no-ztf-match.hostless";

    // Delete the topic before the test to ensure a clean state
    delete_kafka_topic(topic, &config).await;

    // Create mock enriched LSST alerts with good reliability and no flags
    let alert1 =
        create_mock_enriched_lsst_alert(9876543210, "LSST24aaaaaaa", 0.8, false, false, None, None);
    let alert2 =
        create_mock_enriched_lsst_alert(9876543211, "LSST24aaaaaab", 0.9, false, false, None, None);

    // Process the alerts
    let result = babamul.process_lsst_alerts(vec![alert1, alert2]).await;

    assert_eq!(result.unwrap(), 2, "Expected 2 messages to be sent");
}

#[tokio::test]
async fn test_babamul_filters_low_reliability() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Create alerts with low reliability (should be filtered out)
    let alert1 =
        create_mock_enriched_lsst_alert(9876543212, "LSST24aaaaaac", 0.3, false, false, None, None);
    let alert2 =
        create_mock_enriched_lsst_alert(9876543213, "LSST24aaaaaad", 0.4, false, false, None, None);

    // Process the alerts
    let result = babamul.process_lsst_alerts(vec![alert1, alert2]).await;
    assert!(
        result.is_ok(),
        "Failed to process LSST alerts: {:?}",
        result.err()
    );

    // No messages should be sent
    assert_eq!(
        result.unwrap(),
        0,
        "Expected 0 messages for low-reliability alerts"
    );
}

#[tokio::test]
async fn test_babamul_filters_rocks() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Create alerts marked as rocks (should be filtered out)
    let ztf_rock = create_mock_enriched_ztf_alert(1234567892, "ZTF21aaaaaac", true);
    let lsst_rock =
        create_mock_enriched_lsst_alert(9876543214, "LSST24aaaaaae", 0.9, false, true, None, None);

    // Process the alerts
    let ztf_result = babamul.process_ztf_alerts(vec![ztf_rock]).await;
    let lsst_result = babamul.process_lsst_alerts(vec![lsst_rock]).await;

    assert!(ztf_result.is_ok());
    assert!(lsst_result.is_ok());

    // No messages should be sent for rocks
    assert_eq!(ztf_result.unwrap(), 0, "Expected 0 messages for ZTF rocks");
    assert_eq!(
        lsst_result.unwrap(),
        0,
        "Expected 0 messages for LSST rocks"
    );
}

#[tokio::test]
async fn test_babamul_filters_pixel_flags() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Create LSST alert with pixel_flags set (should be filtered out)
    let alert =
        create_mock_enriched_lsst_alert(9876543215, "LSST24aaaaaaf", 0.9, true, false, None, None);

    let result = babamul.process_lsst_alerts(vec![alert]).await;
    assert!(result.is_ok());

    // No messages should be sent for alerts with pixel flags
    assert_eq!(
        result.unwrap(),
        0,
        "Expected 0 messages for LSST alerts with pixel flags"
    );
}

#[tokio::test]
async fn test_babamul_lsst_with_ztf_match() {
    use boom::alert::{
        AlertCutout, DiaForcedSource, FpHist, LsstAlert, LsstAliases, LsstForcedPhot, LsstObject,
        PrvCandidate as ZtfPrvCandidateFields, ZtfAliases, ZtfForcedPhot, ZtfObject,
        ZtfPrvCandidate,
    };
    use boom::enrichment::EnrichmentWorker;
    use boom::utils::spatial::Coordinates;
    use flare::Time;
    use mongodb::bson::doc;
    use std::time::{SystemTime, UNIX_EPOCH};

    let topic = "babamul.lsst.ztf-match.hostless";

    let db = boom::conf::get_test_db().await;
    // Ensure the LSPSC catalog collection exists for Babamul validation
    db.collection::<mongodb::bson::Document>("LSPSC")
        .insert_one(doc! {"_init": true})
        .await
        .ok();
    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    delete_kafka_topic(topic, &config).await;
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
        dia_source.ra = 150.0;
        dia_source.dec = 30.0;
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
        cutout_science: vec![1, 2, 3, 4, 5],
        cutout_template: vec![6, 7, 8, 9, 10],
        cutout_difference: vec![11, 12, 13, 14, 15],
    };

    lsst_cutouts_collection
        .insert_one(&cutout_doc)
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
    let messages = consume_kafka_messages(topic, &config).await;
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
    lsst_cutouts_collection
        .delete_many(doc! {"_id": {"$in": [lsst_alert_id]}})
        .await
        .expect("Failed to cleanup LSST cutouts fixture after test");
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
    let ztf_cutouts_collection = db.collection::<boom::alert::AlertCutout>("ZTF_alerts_cutouts");

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
    ztf_cutouts_collection
        .delete_many(doc! {"_id": {"$in": [ztf_candid]}})
        .await
        .expect("Failed to cleanup ZTF cutouts fixture");

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
        dia.ra = 150.0;
        dia.dec = 30.0;
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
    let messages = consume_kafka_messages("babamul.ztf.none", &config).await;
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
    ztf_cutouts_collection
        .delete_many(doc! {"_id": {"$in": [ztf_candid]}})
        .await
        .expect("Failed to cleanup ZTF cutouts fixture after test");
}
