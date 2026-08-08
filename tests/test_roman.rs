#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
use boom::{
    alert::{AlertWorker, ProcessAlertStatus},
    conf::{get_test_cutout_storage, get_test_db},
    enrichment::{
        create_roman_alert_pipeline, fetch_alerts, EnrichmentWorker, RomanAlertForEnrichment,
        RomanEnrichmentWorker,
    },
    filter::{alert_to_avro_bytes, load_alert_schema, FilterWorker, RomanFilterWorker},
    utils::{
        enums::Survey,
        testing::{
            drop_alert_from_collections, insert_test_filter, remove_test_filter,
            roman_alert_worker, AlertRandomizer, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::doc;

#[tokio::test]
async fn test_process_roman_alert() {
    let mut alert_worker = roman_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Roman).get().await;
    let result = alert_worker.process_alert(&bytes_content).await;
    assert!(result.is_ok(), "{:?}", result);
    assert_eq!(result.unwrap(), ProcessAlertStatus::Added(candid));

    // Reprocessing the same alert is a no-op, not an error:
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Exists(candid));

    let db = get_test_db().await;
    let filter = doc! {"_id": candid};

    let alert = db
        .collection::<mongodb::bson::Document>("ROMAN_alerts")
        .find_one(filter.clone())
        .await
        .unwrap();
    assert!(alert.is_some());
    let alert = alert.unwrap();
    assert_eq!(alert.get_i64("_id").unwrap(), candid);
    assert_eq!(alert.get_str("objectId").unwrap(), object_id);
    let candidate = alert.get_document("candidate").unwrap();
    assert_eq!(candidate.get_f64("ra").unwrap(), ra);
    assert_eq!(candidate.get_f64("dec").unwrap(), dec);
    // Roman times are UTC MJD, so jd is a plain offset from midpointMjd
    let jd = candidate.get_f64("jd").unwrap();
    let mjd = candidate.get_f64("midpointMjd").unwrap();
    assert!((jd - (mjd + 2400000.5)).abs() < 1e-9);
    // the wide filter arrives as W146 and is normalized to F146
    assert_eq!(candidate.get_str("band").unwrap(), "F146");
    // no solar-system match in the sample packet
    assert!(alert.get_array("ss_matches").unwrap().is_empty());

    let cutout_storage = get_test_cutout_storage(&Survey::Roman).await;
    let cutouts = cutout_storage
        .retrieve_cutouts(candid, false)
        .await
        .unwrap();
    assert_eq!(cutouts.candid, candid);

    let aux = db
        .collection::<mongodb::bson::Document>("ROMAN_alerts_aux")
        .find_one(doc! {"_id": &object_id})
        .await
        .unwrap();

    assert!(aux.is_some());
    let aux = aux.unwrap();
    assert_eq!(aux.get_str("_id").unwrap(), &object_id);
    assert_eq!(aux.get_bool("is_sso").unwrap(), false);
    // 27 previous detections in the packet + the triggering one
    let prv_candidates = aux.get_array("prv_candidates").unwrap();
    assert_eq!(prv_candidates.len(), 28);
    // no forced photometry in the packet yet
    assert!(aux.get_array("fp_hists").unwrap().is_empty());

    drop_alert_from_collections(candid, &Survey::Roman)
        .await
        .unwrap();
}

/// Exercises the enrichment read path: the aux lookup pipeline, deserializing the
/// stored lightcurve, and computing the alert properties.
#[tokio::test]
async fn test_roman_alert_properties() {
    let mut alert_worker = roman_alert_worker().await;

    let (candid, _object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Roman).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let enrichment_worker = RomanEnrichmentWorker::new(TEST_CONFIG_FILE, None)
        .await
        .unwrap();

    let db = get_test_db().await;
    let alerts: Vec<RomanAlertForEnrichment> = fetch_alerts(
        &[candid],
        &create_roman_alert_pipeline(),
        &db.collection("ROMAN_alerts"),
    )
    .await
    .unwrap();
    assert_eq!(alerts.len(), 1);
    let alert = &alerts[0];
    // the whole stored lightcurve is fetched, and no forced photometry exists yet
    assert_eq!(alert.prv_candidates.len(), 28);
    assert!(alert.fp_hists.is_empty());
    assert!(alert.ss_matches.is_empty());
    // fluxes survive the round trip through Mongo under their packet names
    assert!(alert.prv_candidates.iter().all(|p| p.flux.is_some()));

    let properties = enrichment_worker.get_alert_properties(alert).await.unwrap();
    assert_eq!(properties.rock, false);
    // Properties only use photometry up to the alert epoch (the LSST convention),
    // and in this packet the triggering source is the object's *first* detection
    // — `prvDiaSources` holds the 27 later ones — so a single epoch is in scope.
    assert_eq!(properties.stationary, false);
    // all photometry is in the wide F146 filter
    assert!(properties.photstats.f146.is_some());
    assert!(properties.photstats.g.is_none());

    drop_alert_from_collections(candid, &Survey::Roman)
        .await
        .unwrap();
}

/// Exercises the enrichment write path. Requires MongoDB 8.0+ (`bulk_write`), like
/// the other surveys' enrichment workers.
#[tokio::test]
async fn test_enrich_roman_alert() {
    let mut alert_worker = roman_alert_worker().await;

    let (candid, _object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Roman).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let mut enrichment_worker = RomanEnrichmentWorker::new(TEST_CONFIG_FILE, None)
        .await
        .unwrap();
    let processed = enrichment_worker.process_alerts(&[candid]).await.unwrap();
    assert_eq!(processed, vec![format!("{}", candid)]);

    let db = get_test_db().await;
    let alert = db
        .collection::<mongodb::bson::Document>("ROMAN_alerts")
        .find_one(doc! {"_id": candid})
        .await
        .unwrap()
        .unwrap();

    let properties = alert.get_document("properties").unwrap();
    assert_eq!(properties.get_bool("rock").unwrap(), false);
    // see test_roman_alert_properties for why this is not stationary
    assert_eq!(properties.get_bool("stationary").unwrap(), false);
    let photstats = properties.get_document("photstats").unwrap();
    assert!(photstats.get_document("f146").is_ok());

    drop_alert_from_collections(candid, &Survey::Roman)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_filter_roman_alert() {
    let mut alert_worker = roman_alert_worker().await;

    let (candid, object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Roman).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let filter_id = insert_test_filter(&Survey::Roman, true).await.unwrap();

    let mut filter_worker = RomanFilterWorker::new(TEST_CONFIG_FILE, Some(vec![filter_id.clone()]))
        .await
        .unwrap();
    let result = filter_worker.process_alerts(&[format!("{}", candid)]).await;

    remove_test_filter(&filter_id, &Survey::Roman)
        .await
        .unwrap();
    assert!(result.is_ok(), "Filter failed: {:?}", result.err());

    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert.candid, candid);
    assert_eq!(&alert.object_id, &object_id);
    assert_eq!(alert.survey, Survey::Roman);

    // the full lightcurve is bundled into the outgoing packet
    assert_eq!(alert.photometry.len(), 28);
    assert!(alert.photometry.iter().all(|p| p.band == "romanF146"));
    // and it is sorted by time
    assert!(alert.photometry.windows(2).all(|w| w[0].jd <= w[1].jd));

    let filter_passed = alert
        .filters
        .iter()
        .find(|f| f.filter_id == filter_id)
        .unwrap();
    assert!(filter_passed.annotations.contains("mag_now"));

    assert!(
        !alert.cutout_science.is_empty(),
        "cutout_science should not be empty"
    );
    assert!(
        !alert.cutout_template.is_empty(),
        "cutout_template should not be empty"
    );
    assert!(
        !alert.cutout_difference.is_empty(),
        "cutout_difference should not be empty"
    );

    // verify that we can convert the alert to avro bytes
    let schema = load_alert_schema().unwrap();
    let _ = alert_to_avro_bytes(&alert, &schema).unwrap();

    drop_alert_from_collections(candid, &Survey::Roman)
        .await
        .unwrap();
}
