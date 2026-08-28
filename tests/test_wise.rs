#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
use boom::{
    alert::{AlertWorker, ProcessAlertStatus},
    conf::{get_test_cutout_storage, get_test_db},
    filter::{alert_to_avro_bytes, load_alert_schema, FilterWorker, WiseFilterWorker},
    utils::{
        enums::Survey,
        testing::{
            drop_alert_from_collections, insert_test_filter, remove_test_filter, wise_alert_worker,
            AlertRandomizer, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::doc;

#[tokio::test]
async fn test_process_wise_alert() {
    let mut alert_worker = wise_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Wise).get().await;
    let result = alert_worker.process_alert(&bytes_content).await;
    assert!(result.is_ok(), "{:?}", result);
    assert_eq!(result.unwrap(), ProcessAlertStatus::Added(candid));

    // Re-processing the same alert is a no-op, not an error.
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Exists(candid));

    let db = get_test_db().await;
    let alert = db
        .collection::<mongodb::bson::Document>("WISE_alerts")
        .find_one(doc! {"_id": candid})
        .await
        .unwrap();
    assert!(alert.is_some());
    let alert = alert.unwrap();
    assert_eq!(alert.get_i64("_id").unwrap(), candid);
    assert_eq!(alert.get_str("objectId").unwrap(), object_id);
    let candidate = alert.get_document("candidate").unwrap();
    assert_eq!(candidate.get_f64("ra").unwrap(), ra);
    assert_eq!(candidate.get_f64("dec").unwrap(), dec);
    // infrared band + PSF photometry stored
    assert!(candidate.get_str("band").is_ok());
    assert!(candidate.get_f64("magpsf").is_ok() || candidate.get_i64("magpsf").is_ok());

    // full cutout triplet (WISE is imaging, unlike ASKAP)
    let cutout_storage = get_test_cutout_storage(&Survey::Wise).await;
    let cutouts = cutout_storage
        .retrieve_cutouts(candid, false)
        .await
        .unwrap();
    assert_eq!(cutouts.candid, candid);
    assert!(!cutouts.cutout_science.is_empty());
    assert!(!cutouts.cutout_template.is_empty());
    assert!(!cutouts.cutout_difference.is_empty());

    // aux: detection history (science candidate + prv_candidates) and forced phot
    let aux = db
        .collection::<mongodb::bson::Document>("WISE_alerts_aux")
        .find_one(doc! {"_id": &object_id})
        .await
        .unwrap();
    assert!(aux.is_some());
    let aux = aux.unwrap();
    assert!(!aux.get_array("prv_candidates").unwrap().is_empty());
    assert!(!aux.get_array("fp_hists").unwrap().is_empty());

    drop_alert_from_collections(candid, &Survey::Wise)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_filter_wise_alert() {
    let mut alert_worker = wise_alert_worker().await;

    let (candid, object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Wise).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let filter_id = insert_test_filter(&Survey::Wise, true).await.unwrap();

    let mut filter_worker = WiseFilterWorker::new(TEST_CONFIG_FILE, Some(vec![filter_id.clone()]))
        .await
        .unwrap();
    let result = filter_worker.process_alerts(&[format!("{}", candid)]).await;

    remove_test_filter(&filter_id, &Survey::Wise).await.unwrap();
    assert!(result.is_ok(), "Filter failed: {:?}", result.err());

    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert.candid, candid);
    assert_eq!(&alert.object_id, &object_id);
    assert_eq!(alert.survey, Survey::Wise);

    // photometry is the W1/W2 lightcurve (prv_candidates + forced phot)
    assert!(!alert.photometry.is_empty());
    assert!(alert.photometry.iter().all(|p| p.band.starts_with("wise")));

    // WISE_TEST_PIPELINE annotates mag_now = round(candidate.magpsf, 2)
    let filter_passed = alert
        .filters
        .iter()
        .find(|f| f.filter_id == filter_id)
        .unwrap();
    assert_eq!(filter_passed.annotations, "{\"mag_now\":16.32}");

    // deep-learning real-bogus score exposed as a classification
    assert_eq!(alert.classifications.len(), 1);
    assert_eq!(alert.classifications[0].classifier, "drb");

    // full cutout triplet
    assert!(!alert.cutout_science.is_empty());
    assert!(!alert.cutout_template.is_empty());
    assert!(!alert.cutout_difference.is_empty());

    let schema = load_alert_schema().unwrap();
    let _ = alert_to_avro_bytes(&alert, &schema).unwrap();

    drop_alert_from_collections(candid, &Survey::Wise)
        .await
        .unwrap();
}
