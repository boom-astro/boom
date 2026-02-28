use apache_avro::AvroSchema;
use boom::{
    alert::{Candidate, DiaSource, LsstCandidate, LsstPrvCandidate, ZtfCandidate},
    conf::AppConfig,
    enrichment::{
        babamul::{
            BabamulLsstAlert, BabamulSurveyMatch, BabamulSurveyMatches, BabamulZtfAlert,
            ForcedPhotometry,
        },
        EnrichmentWorker, LsstAlertForEnrichment, LsstAlertProperties, LsstEnrichmentWorker,
        LsstPhotometry, ZtfAlertProperties, ZtfForcedPhotometry, ZtfPhotometry,
    },
    utils::{
        lightcurves::{flux2mag, Band, PerBandProperties, ZTF_ZP},
        testing::TEST_CONFIG_FILE,
    },
};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    Message,
};
use std::collections::HashMap;
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
fn create_mock_enriched_ztf_alert(candid: i64, object_id: &str, is_rock: bool) -> BabamulZtfAlert {
    // Create a minimal Candidate and ZtfCandidate using defaults
    let mut inner_candidate = Candidate::default();
    inner_candidate.candid = candid;
    inner_candidate.ra = 150.0;
    inner_candidate.dec = 30.0;
    inner_candidate.magpsf = 18.5;
    inner_candidate.sigmapsf = 0.1;
    inner_candidate.fid = 1; // g-band
    inner_candidate.programid = 1; // public
    inner_candidate.drb = Some(1.0); // DRB value

    let candidate = ZtfCandidate::try_from(inner_candidate.clone()).unwrap();

    // let's make sure that the flux and flux_err generated when converting from Candidate to ZtfCandidate
    // can be converted back to the original magpsf and sigmapsf using the same ZP, to verify that the conversion is consistent
    let (new_magpsf, new_sigmapsf) = flux2mag(
        candidate.psf_flux.abs() / 1e9_f32, // convert back to Jy
        candidate.psf_flux_err / 1e9_f32,   // convert back to Jy
        ZTF_ZP,
    );
    assert!(
        (new_magpsf - candidate.candidate.magpsf).abs() < 1e-6,
        "Magnitude conversion mismatch: expected {}, got {}",
        candidate.candidate.magpsf,
        new_magpsf
    );
    assert!(
        (new_sigmapsf - candidate.candidate.sigmapsf).abs() < 1e-6,
        "Magnitude error conversion mismatch: expected {}, got {}",
        candidate.candidate.sigmapsf,
        new_sigmapsf
    );

    let magpsf = 15.949999;
    let sigmapsf = 0.002316;
    let flux = -11859.88;
    let flux_err = 25.300741;
    let magzpsci = 26.1352;
    let ztf_forced_photometry = ZtfForcedPhotometry {
        jd: 2460447.9202778,
        magpsf: Some(magpsf),
        sigmapsf: Some(sigmapsf),
        diffmaglim: 20.5,
        flux: Some(flux),
        flux_err: flux_err,
        snr_psf: Some(100.0),
        band: Band::G,
        ra: Some(150.0),
        dec: Some(30.0),
        magzpsci: Some(magzpsci),
        programid: 1,
        procstatus: Some("0".to_string()),
    };

    let fp_as_photometry = ZtfPhotometry::try_from(ztf_forced_photometry).unwrap();

    let new_flux = fp_as_photometry.flux.unwrap();
    let new_flux_err = fp_as_photometry.flux_err;

    // the new flux is on a fixed zeropoint (ZTF_ZP), verify
    // that converting it back to magnitude gives the same magpsf and sigmapsf as the original data
    let (new_magpsf, new_sigmapsf) = flux2mag(
        -new_flux as f32 * 1e-9,    // from nJy to Jy
        new_flux_err as f32 * 1e-9, // from nJy to Jy
        ZTF_ZP,
    );
    assert!(
        (new_magpsf - magpsf as f32).abs() < 1e-6,
        "Magnitude conversion mismatch: expected {}, got {}",
        magpsf,
        new_magpsf
    );
    assert!(
        (new_sigmapsf - sigmapsf as f32).abs() < 1e-6,
        "Magnitude error conversion mismatch: expected {}, got {}",
        sigmapsf,
        new_sigmapsf
    );

    BabamulZtfAlert {
        candid,
        object_id: object_id.to_string(),
        candidate,
        prv_candidates: vec![],
        prv_nondetections: vec![],
        fp_hists: vec![ForcedPhotometry {
            jd: fp_as_photometry.jd,
            flux: fp_as_photometry.flux,
            flux_err: fp_as_photometry.flux_err,
            band: fp_as_photometry.band,
        }],
        properties: ZtfAlertProperties {
            rock: is_rock,
            star: false,
            near_brightstar: false,
            stationary: false,
            photstats: PerBandProperties::default(),
            multisurvey_photstats: Some(PerBandProperties::default()),
        },
        survey_matches: BabamulSurveyMatches::default(),
    }
}

/// Create a mock enriched LSST alert for testing
async fn create_mock_enriched_lsst_alert(
    candid: i64,
    object_id: &str,
    reliability: f64,
    is_rock: bool,
    ra_override: Option<f64>,
    dec_override: Option<f64>,
) -> (BabamulLsstAlert, HashMap<String, Vec<serde_json::Value>>) {
    create_mock_enriched_lsst_alert_with_matches(
        candid,
        object_id,
        reliability,
        is_rock,
        None,
        None,
        ra_override,
        dec_override,
    )
    .await
}

async fn create_mock_enriched_lsst_alert_with_matches(
    candid: i64,
    object_id: &str,
    reliability: f64,
    is_rock: bool,
    cross_matches: Option<std::collections::HashMap<String, Vec<serde_json::Value>>>,
    survey_matches: Option<boom::enrichment::LsstSurveyMatches>,
    ra_override: Option<f64>,
    dec_override: Option<f64>,
) -> (BabamulLsstAlert, HashMap<String, Vec<serde_json::Value>>) {
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
    dia_source.reliability = Some(reliability as f32);
    dia_source.extendedness = Some(0.2); // Set extendedness to a non-zero value to avoid filtering out the alert

    let ss_object_id = if is_rock { Some(555555_i64) } else { None };
    dia_source.ss_object_id = ss_object_id;

    let enrichment_worker = LsstEnrichmentWorker::new(TEST_CONFIG_FILE).await.unwrap();

    let candidate = LsstCandidate {
        dia_source,
        object_id: object_id.to_string(),
        jd: 2460000.5,
        magpsf: 18.5,
        sigmapsf: 0.1,
        diffmaglim: 20.5,
        isdiffpos: true,
        snr_psf: Some(100.0),
        magap: 18.6,
        sigmagap: 0.12,
        snr_ap: Some(90.0),
        jdstarthist: Some(2459990.5),
        ndethist: Some(5),
        chipsf: Some(2.0),
    };
    let prv_candidate = LsstPhotometry {
        jd: 2460000.0,
        magpsf: Some(18.5),
        sigmapsf: Some(0.1),
        diffmaglim: 20.5,
        flux: Some(1000.0),
        flux_err: 10.0,
        snr_psf: Some(100.0),
        band: Band::R,
        ra: Some(ra_override.unwrap_or(150.0)),
        dec: Some(dec_override.unwrap_or(30.0)),
    };
    let lsst_alert_for_enrichment = LsstAlertForEnrichment {
        candid,
        object_id: object_id.to_string(),
        ss_object_id: ss_object_id.map(|id| id.to_string()),
        candidate,
        prv_candidates: vec![prv_candidate],
        fp_hists: vec![],
        cross_matches: cross_matches.clone(),
        survey_matches: survey_matches.clone(),
    };

    let properties = enrichment_worker
        .get_alert_properties(&lsst_alert_for_enrichment)
        .await
        .unwrap();

    BabamulLsstAlert::from_alert_and_properties(lsst_alert_for_enrichment, properties)
}

/// Consume messages from a Kafka topic and return them as a vector of byte arrays.
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

    let timeout = Duration::from_secs(8);
    let start = std::time::Instant::now();

    let mut messages = Vec::new();
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

#[tokio::test]
async fn test_compute_babamul_category_lsst() {
    use boom::enrichment::{LsstSurveyMatches, ZtfMatch};

    // Test case 1: No ZTF match + stellar LSPSC → "no-ztf-match.stellar"
    // distance ≤ 1.0 AND score > 0.5
    let cross_matches = create_lspsc_cross_matches(Some(0.5), Some(0.95), false);
    let (alert_stellar, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543210,
        "LSST24aaaaaaa",
        0.8,
        false,
        Some(cross_matches),
        None, // No ZTF survey match
        None,
        None,
    )
    .await;
    let category = alert_stellar.compute_babamul_category(&cross_matches);
    assert_eq!(
        category, "no-ztf-match.stellar",
        "Alert with close, high-score match should be stellar"
    );

    // Test case 2: No ZTF match + hosted LSPSC → "no-ztf-match.hosted"
    // nearest match has score < 0.5
    let cross_matches = create_lspsc_cross_matches(Some(2.0), Some(0.3), false);
    let (alert_hosted, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543211,
        "LSST24aaaaaab",
        0.8,
        false,
        Some(cross_matches),
        None,
        None,
        None,
    )
    .await;
    let category = alert_hosted.compute_babamul_category(&cross_matches);
    assert_eq!(
        category, "no-ztf-match.hosted",
        "Alert with low-score match should be hosted"
    );

    // Test case 3: No ZTF match + hostless LSPSC → "no-ztf-match.hostless"
    // distance > 1.0 AND all scores > 0.5 (no hosted criteria met)
    let cross_matches = create_lspsc_cross_matches(Some(2.5), Some(0.8), true);
    let (alert_hostless, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543212,
        "LSST24aaaaaac",
        0.8,
        false,
        Some(cross_matches),
        None,
        None,
        None,
    )
    .await;
    let category = alert_hostless.compute_babamul_category(&cross_matches);
    assert_eq!(
        category, "no-ztf-match.hostless",
        "Alert with distant, high-score match should be hostless"
    );

    // Test case 4: No ZTF match + no matches + in footprint → "no-ztf-match.hostless"
    let (alert_no_matches, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543213,
        "LSST24aaaaaad",
        0.8,
        false,
        None,
        None, // No ZTF survey match
        None,
        None,
    )
    .await;
    let category = alert_no_matches.compute_babamul_category(&cross_matches);
    assert_eq!(
        category, "no-ztf-match.hostless",
        "Alert with no matches but in footprint should be hostless"
    );

    let ztf_public_prv_candidate = ZtfPhotometry {
        jd: 2459999.5,
        magpsf: Some(19.0),
        sigmapsf: Some(0.07),
        diffmaglim: 21.0,
        flux: Some(1000.0),
        flux_err: 10.0,
        band: Band::G,
        ra: Some(180.0),
        dec: Some(0.0),
        snr_psf: Some(100.0),
        programid: 1,
    };

    // Test case 5: ZTF match + stellar LSPSC → "ztf-match.stellar"
    let cross_matches = create_lspsc_cross_matches(Some(0.5), Some(0.95), false);
    let survey_matches = Some(LsstSurveyMatches {
        ztf: Some(ZtfMatch {
            object_id: "ZTF24aaaaaaa".to_string(),
            ra: 180.0,
            dec: 0.0,
            prv_candidates: vec![ztf_public_prv_candidate.clone()],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
    });
    let (alert_ztf_stellar, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543214,
        "LSST24aaaaaae",
        0.8,
        false,
        Some(cross_matches),
        survey_matches,
        None,
        None,
    )
    .await;
    let category = alert_ztf_stellar.compute_babamul_category(&cross_matches);
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
            prv_candidates: vec![ztf_public_prv_candidate.clone()],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
    });
    let (alert_ztf_hosted, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543215,
        "LSST24aaaaaaf",
        0.8,
        false,
        Some(cross_matches),
        survey_matches,
        None,
        None,
    )
    .await;
    let category = alert_ztf_hosted.compute_babamul_category(&cross_matches);
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
            prv_candidates: vec![ztf_public_prv_candidate.clone()],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
    });
    let (alert_ztf_hostless, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543216,
        "LSST24aaaaaag",
        0.8,
        false,
        Some(cross_matches),
        survey_matches,
        None,
        None,
    )
    .await;
    let category = alert_ztf_hostless.compute_babamul_category(&cross_matches);
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
            prv_candidates: vec![ztf_public_prv_candidate.clone()],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
    });
    let (alert_ztf_unknown, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543217,
        "LSST24aaaaaah",
        0.8,
        false,
        None, // No LSPSC cross-matches
        survey_matches,
        None,
        None,
    )
    .await;
    let category = alert_ztf_unknown.compute_babamul_category(&cross_matches);
    assert_eq!(
        category, "ztf-match.hostless",
        "Alert with ZTF match but no LSPSC and in footprint should be ztf-match.hostless"
    );

    // Test case 9: No LSPSC + no ZTF match + out of footprint → "unknown"
    let (alert_unknown, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543218,
        "LSST24aaaaaai",
        0.8,
        false,
        Some(std::collections::HashMap::new()), // No matches
        None,                                   // No ZTF survey match
        Some(265.05),                           // RA out of footprint
        Some(-32.25),                           // Dec out of footprint
    )
    .await;
    let category = alert_unknown.compute_babamul_category(&cross_matches);
    assert_eq!(
        category, "no-ztf-match.unknown",
        "Alert with no matches and no ZTF match should be unknown"
    );

    // Test case 10: LSPSC exists but empty + no ZTF match + out of footprint → "unknown"
    let (alert_empty_lspsc, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543219,
        "LSST24aaaaaaj",
        0.8,
        false,
        Some(std::collections::HashMap::new()), // Empty LSPSC matches
        None,                                   // No ZTF survey match
        Some(265.05),                           // RA out of footprint
        Some(-32.25),                           // Dec out of footprint
    )
    .await;
    let category = alert_empty_lspsc.compute_babamul_category(&cross_matches);
    assert_eq!(
        category, "no-ztf-match.unknown",
        "Alert with empty LSPSC matches and no ZTF match should be unknown"
    );

    // Test case 11: ZTF match whose prv_candidates are all non-public (programid != 1) →
    // should be treated as if there is no ZTF match at all → "no-ztf-match.*"
    let cross_matches = create_lspsc_cross_matches(Some(2.5), Some(0.8), true);
    let survey_matches_non_public = Some(LsstSurveyMatches {
        ztf: Some(ZtfMatch {
            object_id: "ZTF24aaaaaae".to_string(),
            ra: 180.3,
            dec: 0.3,
            prv_candidates: vec![
                // programid = 2 → not public, will be filtered out by BabamulSurveyMatch::from
                ZtfPhotometry {
                    jd: 2459999.5,
                    magpsf: Some(19.0),
                    sigmapsf: Some(0.07),
                    diffmaglim: 21.0,
                    flux: Some(1000.0),
                    flux_err: 10.0,
                    band: Band::G,
                    ra: Some(180.3),
                    dec: Some(0.3),
                    snr_psf: Some(100.0),
                    programid: 2,
                },
            ],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
    });
    let (alert_no_public_data, cross_matches) = create_mock_enriched_lsst_alert_with_matches(
        9876543220,
        "LSST24aaaaaak",
        0.8,
        false,
        Some(cross_matches),
        survey_matches_non_public,
        None,
        None,
    )
    .await;
    // The ZTF match must have been dropped because its only prv_candidate is non-public
    assert!(
        alert_no_public_data.survey_matches.ztf.is_none(),
        "ZTF match with only non-public prv_candidates should be removed from survey_matches"
    );
    let category = alert_no_public_data.compute_babamul_category(&cross_matches);
    assert_eq!(
        category, "no-ztf-match.hostless",
        "Alert whose ZTF match was filtered out (no public prv_candidates) should be categorised as no-ztf-match"
    );
}

#[test]
fn test_compute_babamul_category_ztf() {
    // Test case 1: No LSST match + not stellar + sgscore1 > 0.5 → "no-lsst-match.hostless"
    let mut alert_no_lsst = create_mock_enriched_ztf_alert(1234567890, "ZTF21aaaaaaa", false);
    alert_no_lsst.survey_matches = BabamulSurveyMatches::default();
    alert_no_lsst.properties.star = false;
    alert_no_lsst.candidate.candidate.sgscore1 = Some(0.8); // Star-like
    let category = alert_no_lsst.compute_babamul_category();
    assert_eq!(
        category, "no-lsst-match.hostless",
        "ZTF alert with no LSST match, not stellar, and high sgscore should be no-lsst-match.hostless"
    );

    // Test case 2: No LSST match + stellar → "no-lsst-match.stellar"
    let mut alert_no_lsst_stellar =
        create_mock_enriched_ztf_alert(1234567891, "ZTF21aaaaaab", false);
    alert_no_lsst_stellar.survey_matches = BabamulSurveyMatches::default();
    alert_no_lsst_stellar.properties.star = true;
    let category = alert_no_lsst_stellar.compute_babamul_category();
    assert_eq!(
        category, "no-lsst-match.stellar",
        "ZTF alert with no LSST match and stellar should be no-lsst-match.stellar"
    );

    // Test case 3: LSST match + not stellar + sgscore1 > 0.5 → "lsst-match.hostless"
    let mut alert_lsst = create_mock_enriched_ztf_alert(1234567892, "ZTF21aaaaaac", false);
    alert_lsst.survey_matches = BabamulSurveyMatches {
        lsst: Some(BabamulSurveyMatch {
            object_id: "LSST24aaaaaaa".to_string(),
            ra: 150.0,
            dec: 30.0,
            prv_candidates: vec![],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
        ztf: None,
    };
    alert_lsst.properties.star = false;
    alert_lsst.candidate.candidate.sgscore1 = Some(0.8); // Star-like
    let category = alert_lsst.compute_babamul_category();
    assert_eq!(
        category, "lsst-match.hostless",
        "ZTF alert with LSST match, not stellar, and high sgscore should be lsst-match.hostless"
    );

    // Test case 4: LSST match + stellar → "lsst-match.stellar"
    let mut alert_lsst_stellar = create_mock_enriched_ztf_alert(1234567893, "ZTF21aaaaaad", false);
    alert_lsst_stellar.survey_matches = BabamulSurveyMatches {
        lsst: Some(BabamulSurveyMatch {
            object_id: "LSST24aaaaaab".to_string(),
            ra: 150.0,
            dec: 30.0,
            prv_candidates: vec![],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
        ztf: None,
    };
    alert_lsst_stellar.properties.star = true;
    let category = alert_lsst_stellar.compute_babamul_category();
    assert_eq!(
        category, "lsst-match.stellar",
        "ZTF alert with LSST match and stellar should be lsst-match.stellar"
    );

    // Test case 5: No LSST match + not stellar + sgscore1 <= 0.5 → "no-lsst-match.hosted"
    let mut alert_hosted = create_mock_enriched_ztf_alert(1234567894, "ZTF21aaaaaae", false);
    alert_hosted.survey_matches = BabamulSurveyMatches::default();
    alert_hosted.properties.star = false;
    alert_hosted.candidate.candidate.sgscore1 = Some(0.3); // Galaxy-like
    let category = alert_hosted.compute_babamul_category();
    assert_eq!(
        category, "no-lsst-match.hosted",
        "ZTF alert with no LSST match, not stellar, and low sgscore should be no-lsst-match.hosted"
    );

    // Test case 6: LSST match + not stellar + sgscore1 <= 0.5 → "lsst-match.hosted"
    let mut alert_lsst_hosted = create_mock_enriched_ztf_alert(1234567895, "ZTF21aaaaaaf", false);
    alert_lsst_hosted.survey_matches = BabamulSurveyMatches {
        lsst: Some(BabamulSurveyMatch {
            object_id: "LSST24aaaaaac".to_string(),
            ra: 150.0,
            dec: 30.0,
            prv_candidates: vec![],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
        ztf: None,
    };
    alert_lsst_hosted.properties.star = false;
    alert_lsst_hosted.candidate.candidate.sgscore1 = Some(0.4); // Galaxy-like
    let category = alert_lsst_hosted.compute_babamul_category();
    assert_eq!(
        category, "lsst-match.hosted",
        "ZTF alert with LSST match, not stellar, and low sgscore should be lsst-match.hosted"
    );

    // Test case 7: Negative sgscore (placeholder) should be ignored → "no-lsst-match.hostless"
    let mut alert_neg_sgscore = create_mock_enriched_ztf_alert(1234567896, "ZTF21aaaaaag", false);
    alert_neg_sgscore.survey_matches = BabamulSurveyMatches::default();
    alert_neg_sgscore.properties.star = false;
    alert_neg_sgscore.candidate.candidate.sgscore1 = Some(-99.0); // Placeholder value
    alert_neg_sgscore.candidate.candidate.sgscore2 = Some(-99.0);
    alert_neg_sgscore.candidate.candidate.sgscore3 = Some(-99.0);
    let category = alert_neg_sgscore.compute_babamul_category();
    assert_eq!(
        category, "no-lsst-match.hostless",
        "ZTF alert with negative sgscores (placeholders) should be hostless"
    );

    // Test case 8: sgscore2 or sgscore3 < 0.5 should mark as hosted
    let mut alert_sgscore2 = create_mock_enriched_ztf_alert(1234567897, "ZTF21aaaaaah", false);
    alert_sgscore2.survey_matches = BabamulSurveyMatches::default();
    alert_sgscore2.properties.star = false;
    alert_sgscore2.candidate.candidate.sgscore1 = Some(0.8); // High score (not hosted by sgscore1)
    alert_sgscore2.candidate.candidate.sgscore2 = Some(0.3); // Low score (hosted)
    let category = alert_sgscore2.compute_babamul_category();
    assert_eq!(
        category, "no-lsst-match.hosted",
        "ZTF alert with low sgscore2 should be hosted even if sgscore1 is high"
    );

    // Test case 9: No LSST match + near_brightstar=true + star=false → "no-lsst-match.stellar"
    let mut alert_near_brightstar =
        create_mock_enriched_ztf_alert(1234567898, "ZTF21aaaaaai", false);
    alert_near_brightstar.survey_matches = BabamulSurveyMatches::default();
    alert_near_brightstar.properties.star = false;
    alert_near_brightstar.properties.near_brightstar = true;
    let category = alert_near_brightstar.compute_babamul_category();
    assert_eq!(
        category, "no-lsst-match.stellar",
        "ZTF alert with near_brightstar=true and star=false should be no-lsst-match.stellar"
    );

    // Test case 10: LSST match + near_brightstar=true + star=false → "lsst-match.stellar"
    let mut alert_lsst_near_brightstar =
        create_mock_enriched_ztf_alert(1234567899, "ZTF21aaaaaaj", false);
    alert_lsst_near_brightstar.survey_matches = BabamulSurveyMatches {
        lsst: Some(BabamulSurveyMatch {
            object_id: "LSST24aaaaaad".to_string(),
            ra: 150.0,
            dec: 30.0,
            prv_candidates: vec![],
            prv_nondetections: vec![],
            fp_hists: vec![],
        }),
        ztf: None,
    };
    alert_lsst_near_brightstar.properties.star = false;
    alert_lsst_near_brightstar.properties.near_brightstar = true;
    let category = alert_lsst_near_brightstar.compute_babamul_category();
    assert_eq!(
        category, "lsst-match.stellar",
        "ZTF alert with LSST match, near_brightstar=true and star=false should be lsst-match.stellar"
    );
}

#[tokio::test]
async fn test_babamul_process_ztf_alerts() {
    use boom::enrichment::babamul::Babamul;
    use std::time::{SystemTime, UNIX_EPOCH};

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Expected topic for non-stellar ZTF alerts without LSST match
    let topic = "babamul.ztf.no-lsst-match.hostless";

    // Create unique objectIds to avoid matching stale messages
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let ztf_obj1 = format!("ZTF21aaaaaaa-{}", ts);
    let ztf_obj2 = format!("ZTF21aaaaaab-{}", ts + 1);

    // Create mock enriched ZTF alerts (not stellar, no LSST match)
    let alert1 = create_mock_enriched_ztf_alert(1234567890, &ztf_obj1, false);
    let alert2 = create_mock_enriched_ztf_alert(1234567891, &ztf_obj2, false);

    // Process the alerts
    let count = babamul
        .process_ztf_alerts(vec![alert1, alert2])
        .await
        .unwrap();
    assert_eq!(
        count, 2,
        "Expected to process 2 ZTF alerts, but processed {}",
        count
    );

    // Consume messages from Kafka topic and verify our specific alerts are present
    let messages = consume_kafka_messages(topic, &config).await;
    let expected: std::collections::HashSet<String> =
        [ztf_obj1.clone(), ztf_obj2.clone()].into_iter().collect();

    let schema = boom::enrichment::babamul::BabamulZtfAlert::get_schema();
    let mut found: std::collections::HashSet<String> = std::collections::HashSet::new();
    for msg in &messages {
        if let Ok(reader) = apache_avro::Reader::with_schema(&schema, &msg[..]) {
            for record in reader.flatten() {
                if let apache_avro::types::Value::Record(fields) = record {
                    if let Some((_, apache_avro::types::Value::String(obj_id))) =
                        fields.iter().find(|(n, _)| n == "objectId")
                    {
                        if expected.contains(obj_id) {
                            found.insert(obj_id.clone());
                        }
                    }
                }
            }
        }
    }

    assert!(
        found == expected,
        "Did not find all expected ZTF objectIds in topic {}. Found: {:?}",
        topic,
        found
    );
}

#[tokio::test]
async fn test_babamul_process_lsst_alerts() {
    use boom::enrichment::babamul::Babamul;
    use std::time::{SystemTime, UNIX_EPOCH};

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);
    let topic = "babamul.lsst.no-ztf-match.hostless";

    // Create unique objectIds to avoid matching stale messages
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let lsst_obj1 = format!("LSST24aaaaaaa-{}", ts);
    let lsst_obj2 = format!("LSST24aaaaaab-{}", ts + 1);

    // Create mock enriched LSST alerts with good reliability and no flags
    let alert1 =
        create_mock_enriched_lsst_alert(9876543210, &lsst_obj1, 0.8, false, None, None).await;
    let alert2 =
        create_mock_enriched_lsst_alert(9876543211, &lsst_obj2, 0.9, false, None, None).await;

    // Process the alerts
    let _result = babamul.process_lsst_alerts(vec![alert1, alert2]).await;

    // Consume messages and verify our specific alerts are present via objectId
    let messages = consume_kafka_messages(topic, &config).await;
    let expected: std::collections::HashSet<String> =
        [lsst_obj1.clone(), lsst_obj2.clone()].into_iter().collect();

    let schema = boom::enrichment::babamul::BabamulLsstAlert::get_schema();
    let mut found: std::collections::HashSet<String> = std::collections::HashSet::new();
    for msg in &messages {
        if let Ok(reader) = apache_avro::Reader::with_schema(&schema, &msg[..]) {
            for record in reader.flatten() {
                if let apache_avro::types::Value::Record(fields) = record {
                    if let Some((_, apache_avro::types::Value::String(obj_id))) =
                        fields.iter().find(|(n, _)| n == "objectId")
                    {
                        if expected.contains(obj_id) {
                            found.insert(obj_id.clone());
                        }
                    }
                }
            }
        }
    }

    assert!(
        found == expected,
        "Did not find all expected LSST objectIds in topic {}. Found: {:?}",
        topic,
        found
    );
}

#[tokio::test]
async fn test_babamul_filters_low_reliability() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Create alerts with low reliability (should be filtered out)
    let alert1 =
        create_mock_enriched_lsst_alert(9876543212, "LSST24aaaaaac", 0.3, false, None, None).await;
    let alert2 =
        create_mock_enriched_lsst_alert(9876543213, "LSST24aaaaaad", 0.4, false, None, None).await;

    // Process the alerts
    let result = babamul.process_lsst_alerts(vec![alert1, alert2]).await;
    assert!(
        result.is_ok(),
        "Failed to process LSST alerts: {:?}",
        result.err()
    );

    // Low reliability alerts should not be sent (filtered by Babamul)
    // Let's just verify the processing succeeded and no messages were sent to Kafka
    assert_eq!(
        result.unwrap(),
        0,
        "Expected 0 messages for LSST alerts with reliability below threshold"
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
        create_mock_enriched_lsst_alert(9876543214, "LSST24aaaaaae", 0.9, true, None, None).await;

    // Process the alerts
    let ztf_result = babamul.process_ztf_alerts(vec![ztf_rock]).await;
    let lsst_result = babamul.process_lsst_alerts(vec![lsst_rock]).await;

    assert!(ztf_result.is_ok());
    assert!(lsst_result.is_ok());

    // Rock alerts should not be sent to any topic
    // Since they're filtered out at the source, we just verify the processing succeeded
    assert_eq!(
        ztf_result.unwrap(),
        0,
        "Expected 0 messages for ZTF alerts marked as rocks"
    );
    assert_eq!(
        lsst_result.unwrap(),
        0,
        "Expected 0 messages for LSST alerts marked as rocks"
    );
}

#[tokio::test]
async fn test_babamul_filters_low_drb() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    // Create a ZTF alert with DRB below the ZTF_MIN_DRB threshold (0.2)
    let mut alert_low_drb = create_mock_enriched_ztf_alert(1234567900, "ZTF21aaaaaak", false);
    alert_low_drb.candidate.candidate.drb = Some(0.1); // Below ZTF_MIN_DRB threshold

    // Create a ZTF alert with no DRB value (None → defaults to 0.0, should be filtered)
    let mut alert_no_drb = create_mock_enriched_ztf_alert(1234567902, "ZTF21aaaaaam", false);
    alert_no_drb.candidate.candidate.drb = None;

    let result = babamul
        .process_ztf_alerts(vec![alert_low_drb, alert_no_drb])
        .await;
    assert!(
        result.is_ok(),
        "Failed to process ZTF alerts: {:?}",
        result.err()
    );

    // All low-DRB alerts should be filtered out
    assert_eq!(
        result.unwrap(),
        0,
        "Expected 0 messages for ZTF alerts with DRB below threshold"
    );
}

/// Create a `BabamulLsstAlert` that passes every Babamul LSST filter condition.
/// Used as the base for filter-condition tests: clone, mutate one field, assert it is dropped.
fn create_valid_babamul_lsst_alert() -> (BabamulLsstAlert, HashMap<String, Vec<serde_json::Value>>)
{
    let mut dia = DiaSource::default();
    dia.candid = 1_i64;
    dia.visit = 123456789;
    dia.detector = 1;
    dia.dia_object_id = Some(987654321);
    dia.midpoint_mjd_tai = 60000.5;
    dia.ra = 150.0;
    dia.dec = 30.0;
    dia.psf_flux = Some(1000.0);
    dia.psf_flux_err = Some(10.0);
    dia.ap_flux = Some(1100.0);
    dia.ap_flux_err = Some(15.0);
    dia.reliability = Some(0.9);
    dia.extendedness = Some(0.2); // valid: not None, not NaN, not 1.0
                                  // All quality-flag fields explicitly false
    dia.psf_flux_flag = Some(false);
    dia.psf_flux_flag_edge = Some(false);
    dia.ap_flux_flag = Some(false);
    dia.pixel_flags_streak = Some(false);
    dia.pixel_flags_nodata = Some(false);
    dia.pixel_flags_cr = Some(false);
    dia.pixel_flags_bad = Some(false);
    dia.pixel_flags_saturated = Some(false);
    dia.shape_flag = Some(false);
    dia.centroid_flag = Some(false);
    dia.glint_trail = Some(false);
    dia.is_dipole = Some(false);

    let candidate = LsstCandidate {
        dia_source: dia,
        object_id: "987654321".to_string(),
        jd: 2460000.5,
        magpsf: 18.5,
        sigmapsf: 0.1,
        diffmaglim: 20.5,
        isdiffpos: true,
        snr_psf: Some(100.0),
        chipsf: None,
        magap: 18.6,
        sigmagap: 0.12,
        snr_ap: Some(90.0),
        jdstarthist: None,
        ndethist: None,
    };

    let alert = BabamulLsstAlert {
        candid: 1,
        object_id: "987654321".to_string(),
        candidate,
        prv_candidates: vec![],
        fp_hists: vec![],
        properties: LsstAlertProperties {
            rock: false,
            stationary: false,
            star: Some(false),
            near_brightstar: Some(false),
            photstats: PerBandProperties::default(),
            multisurvey_photstats: PerBandProperties::default(),
        },
        survey_matches: BabamulSurveyMatches::default(),
    };

    (alert, HashMap::new())
}

/// Table-driven test covering every individual LSST filter condition in
/// `Babamul::process_lsst_alerts`. Starts from a known-good alert and mutates
/// exactly one field per case, asserting the alert is dropped (0 messages sent).
#[tokio::test]
async fn test_babamul_lsst_filter_conditions() {
    use boom::enrichment::babamul::Babamul;

    let config = AppConfig::from_path(TEST_CONFIG_FILE).unwrap();
    let babamul = Babamul::new(&config);

    #[rustfmt::skip]
    let filter_cases: &[(&str, fn(&mut BabamulLsstAlert))] = &[
        ("rock",               |a| a.properties.rock = true),
        ("low_snr_psf",        |a| a.candidate.snr_psf = Some(2.9)),
        ("snr_psf_none",       |a| a.candidate.snr_psf = None),
        ("low_reliability",    |a| a.candidate.dia_source.reliability = Some(0.4)),
        ("reliability_none",   |a| a.candidate.dia_source.reliability = None),
        ("psf_flux_flag",      |a| a.candidate.dia_source.psf_flux_flag = Some(true)),
        ("psf_flux_flag_edge", |a| a.candidate.dia_source.psf_flux_flag_edge = Some(true)),
        ("ap_flux_flag",       |a| a.candidate.dia_source.ap_flux_flag = Some(true)),
        ("pixel_flags_streak", |a| a.candidate.dia_source.pixel_flags_streak = Some(true)),
        ("pixel_flags_nodata", |a| a.candidate.dia_source.pixel_flags_nodata = Some(true)),
        ("pixel_flags_cr",     |a| a.candidate.dia_source.pixel_flags_cr = Some(true)),
        ("pixel_flags_bad",    |a| a.candidate.dia_source.pixel_flags_bad = Some(true)),
        ("pixel_flags_saturated", |a| a.candidate.dia_source.pixel_flags_saturated = Some(true)),
        ("shape_flag",         |a| a.candidate.dia_source.shape_flag = Some(true)),
        ("centroid_flag",      |a| a.candidate.dia_source.centroid_flag = Some(true)),
        ("extendedness_none",  |a| a.candidate.dia_source.extendedness = None),
        ("extendedness_nan",   |a| a.candidate.dia_source.extendedness = Some(f32::NAN)),
        ("extendedness_one",   |a| a.candidate.dia_source.extendedness = Some(1.0_f32)),
        ("glint_trail",        |a| a.candidate.dia_source.glint_trail = Some(true)),
        ("is_dipole",          |a| a.candidate.dia_source.is_dipole = Some(true)),
    ];

    for (label, mutate) in filter_cases {
        let (mut alert, cross_matches) = create_valid_babamul_lsst_alert();
        mutate(&mut alert);
        let result = babamul
            .process_lsst_alerts(vec![(alert, cross_matches)])
            .await;
        assert!(
            result.is_ok(),
            "process_lsst_alerts failed for filter case '{}': {:?}",
            label,
            result.err()
        );
        assert_eq!(
            result.unwrap(),
            0,
            "Expected 0 Kafka messages for filter condition '{}'",
            label
        );
    }

    // last but not least: test that the valid alert (with no mutations) is processed successfully
    let (alert, cross_matches) = create_valid_babamul_lsst_alert();
    let result = babamul
        .process_lsst_alerts(vec![(alert, cross_matches)])
        .await;
    assert!(
        result.is_ok(),
        "process_lsst_alerts failed for valid alert: {:?}",
        result.err()
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
            magap: Some(19.1),
            sigmagap: Some(0.1),
            diffmaglim: Some(21.0),
            ..Default::default()
        },
        psf_flux: Some(1300.0),
        psf_flux_err: Some(13.0),
        snr_psf: Some(100.0),
        ap_flux: Some(1350.0),
        ap_flux_err: Some(15.0),
        snr_ap: Some(90.0),
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
        snr_psf: Some(100.0),
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
        dia_source.extendedness = Some(0.2);
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
        snr_psf: Some(100.0),
        chipsf: Some(2.0),
        magap: 18.6,
        sigmagap: 0.12,
        snr_ap: Some(90.0),
        jdstarthist: Some(2459990.5),
        ndethist: Some(5),
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
            dia_object_id: 987654321,
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
        snr_psf: Some(105.0),
    };

    let lsst_aux = LsstObject {
        object_id: lsst_object_id.clone(),
        prv_candidates: vec![LsstPrvCandidate::try_from(lsst_candidate).unwrap()],
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
    let messages = consume_kafka_messages(topic, &config).await;

    assert!(
        !messages.is_empty(),
        "Expected to find Babamul message published to topic: {}",
        topic
    );

    // Try to decode and verify the ZTF match in the published messages
    // Skip messages that don't decode (e.g., due to schema mismatch with stale messages)
    let schema = BabamulLsstAlert::get_schema();
    let mut successful_decodes = 0;
    let mut found_match = false;
    for msg in &messages {
        let reader = match apache_avro::Reader::with_schema(&schema, &msg[..]) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Skipping message due to schema decode error: {:?}", e);
                continue;
            }
        };

        for record_result in reader {
            let value = match record_result {
                Ok(v) => {
                    successful_decodes += 1;
                    v
                }
                Err(e) => {
                    eprintln!("Skipping record due to decode error: {:?}", e);
                    continue;
                }
            };

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
                    if let apache_avro::types::Value::Record(match_fields) = survey_matches_value {
                        if let Some((_, ztf_value)) =
                            match_fields.iter().find(|(name, _)| name == "ztf")
                        {
                            if let apache_avro::types::Value::Union(_, boxed_value) = ztf_value {
                                if let apache_avro::types::Value::Record(obj_fields) =
                                    &**boxed_value
                                {
                                    if obj_fields.iter().any(|(field_name, field_value)| {
                                        field_name == "objectId"
                                            && matches!(field_value, apache_avro::types::Value::String(s) if s == &ztf_match_id)
                                    }) {
                                        found_match = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // If no messages decoded successfully, we still consider the test passing since messages were published
    // This handles the case where all messages have schema issues (stale messages in topic)
    if successful_decodes == 0 {
        eprintln!(
            "Warning: {} messages were published but none could be decoded (possible schema mismatch with stale data)",
            messages.len()
        );
    }

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

    assert!(
        found_match,
        "Did not find expected ZTF match in Babamul message for LSST alert with objectId: {}",
        lsst_object_id
    );
}

#[tokio::test]
async fn test_babamul_ztf_with_lsst_match() {
    use boom::alert::{
        AlertWorker, DiaForcedSource, DiaSource, LsstAliases, LsstForcedPhot, LsstObject, ZtfObject,
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
        dia.extendedness = Some(0.2);
        dia.band = Some(Band::G);
        dia
    };

    let lsst_forced_phot = LsstForcedPhot::try_from(DiaForcedSource {
        dia_forced_source_id: 1,
        dia_object_id: 42,
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
        prv_candidates: vec![LsstPrvCandidate::try_from(lsst_dia_source).unwrap()],
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

    // Verify that the Babamul message was published to one of the lsst-match topics
    // The exact topic depends on the alert's properties (stellar, sgscore)
    let topics = vec![
        "babamul.ztf.lsst-match.stellar",
        "babamul.ztf.lsst-match.hosted",
        "babamul.ztf.lsst-match.hostless",
    ];

    let mut messages = Vec::new();
    let mut found_topic = None;

    for topic in &topics {
        let topic_messages = consume_kafka_messages(topic, &config).await;
        if !topic_messages.is_empty() {
            messages = topic_messages;
            found_topic = Some(topic);
            break;
        }
    }

    assert!(
        found_topic.is_some(),
        "Expected to find Babamul message published to one of the LSST match topics for ZTF alert with objectId: {}",
        ztf_object_id
    );

    assert!(
        !messages.is_empty(),
        "Expected at least one Babamul message in one of the lsst-match topics"
    );

    // Try to decode and verify the LSST match in the published messages
    // Skip messages that don't decode (e.g., due to schema mismatch with stale messages)
    let schema = BabamulZtfAlert::get_schema();
    let mut successful_decodes = 0;
    let mut found_match = false;
    for msg in &messages {
        let reader = match apache_avro::Reader::with_schema(&schema, &msg[..]) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Skipping message due to schema decode error: {:?}", e);
                continue;
            }
        };

        for record_result in reader {
            let value = match record_result {
                Ok(v) => {
                    successful_decodes += 1;
                    v
                }
                Err(e) => {
                    eprintln!("Skipping record due to decode error: {:?}", e);
                    continue;
                }
            };

            if let apache_avro::types::Value::Record(fields) = value {
                // Ensure this record matches our object_id to avoid stale topic data
                let has_object_id = fields.iter().any(|(name, val)| {
                    name == "objectId"
                        && matches!(val, apache_avro::types::Value::String(s) if s == &ztf_object_id)
                });

                if !has_object_id {
                    continue;
                }

                // Check if survey_matches exists and log what we find
                if let Some((_, survey_matches_value)) =
                    fields.iter().find(|(name, _)| name == "survey_matches")
                {
                    if let apache_avro::types::Value::Record(match_fields) = survey_matches_value {
                        if let Some((_, lsst_value)) =
                            match_fields.iter().find(|(name, _)| name == "lsst")
                        {
                            if let apache_avro::types::Value::Union(_, boxed_value) = lsst_value {
                                if let apache_avro::types::Value::Record(obj_fields) =
                                    &**boxed_value
                                {
                                    if obj_fields.iter().any(|(field_name, field_value)| {
                                        field_name == "objectId"
                                            && matches!(field_value, apache_avro::types::Value::String(s) if s == &lsst_match_id)
                                    }) {
                                        found_match = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // If no messages decoded successfully, we still consider the test passing since messages were published
    // This handles the case where all messages have schema issues (stale messages in topic)
    if successful_decodes == 0 {
        eprintln!(
            "Warning: {} messages were published but none could be decoded (possible schema mismatch with stale data)",
            messages.len()
        );
    }
    // If messages decoded successfully, they should be published to the correct topic.
    // The exact payload details may vary depending on the enrichment pipeline state.

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

    assert!(
        found_match,
        "Did not find expected LSST match in Babamul message for ZTF alert with objectId: {}",
        ztf_object_id
    );
}
