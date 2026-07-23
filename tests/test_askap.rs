#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
use boom::{
    alert::{AlertWorker, ProcessAlertStatus},
    conf::{get_test_cutout_storage, get_test_db},
    filter::{alert_to_avro_bytes, load_alert_schema, AskapFilterWorker, FilterWorker},
    utils::{
        enums::Survey,
        testing::{
            askap_alert_worker, drop_alert_from_collections, insert_test_filter,
            remove_test_filter, AlertRandomizer, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::doc;

#[tokio::test]
async fn test_process_askap_alert() {
    let mut alert_worker = askap_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Askap).get().await;
    let result = alert_worker.process_alert(&bytes_content).await;
    assert!(result.is_ok(), "{:?}", result);
    assert_eq!(result.unwrap(), ProcessAlertStatus::Added(candid));

    // Re-processing the same alert is a no-op, not an error.
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Exists(candid));

    let db = get_test_db().await;
    let alert = db
        .collection::<mongodb::bson::Document>("ASKAP_alerts")
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
    // radio fields present
    assert!(candidate.get_f64("flux_peak").is_ok());
    assert!(candidate.get_f64("snr").is_ok());

    // cutouts inserted (science only; template/difference absent for radio)
    let cutout_storage = get_test_cutout_storage(&Survey::Askap).await;
    let cutouts = cutout_storage
        .retrieve_cutouts(candid, false)
        .await
        .unwrap();
    assert_eq!(cutouts.candid, candid);
    assert!(!cutouts.cutout_science.is_empty());
    assert!(cutouts.cutout_template.is_none());
    assert!(cutouts.cutout_difference.is_none());

    // aux: 1 in-packet prv + the current detection, and 2 forced points
    let aux = db
        .collection::<mongodb::bson::Document>("ASKAP_alerts_aux")
        .find_one(doc! {"_id": &object_id})
        .await
        .unwrap();
    assert!(aux.is_some());
    let aux = aux.unwrap();
    assert_eq!(aux.get_str("_id").unwrap(), &object_id);
    assert_eq!(aux.get_array("prv_candidates").unwrap().len(), 2);
    assert_eq!(aux.get_array("fp_hists").unwrap().len(), 2);

    drop_alert_from_collections(candid, &Survey::Askap)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_filter_askap_alert() {
    let mut alert_worker = askap_alert_worker().await;

    let (candid, object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Askap).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let filter_id = insert_test_filter(&Survey::Askap, true).await.unwrap();

    let mut filter_worker = AskapFilterWorker::new(TEST_CONFIG_FILE, Some(vec![filter_id.clone()]))
        .await
        .unwrap();
    let result = filter_worker.process_alerts(&[format!("{}", candid)]).await;

    remove_test_filter(&filter_id, &Survey::Askap)
        .await
        .unwrap();
    assert!(result.is_ok(), "Filter failed: {:?}", result.err());

    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert.candid, candid);
    assert_eq!(&alert.object_id, &object_id);
    assert_eq!(alert.survey, Survey::Askap);

    // 2 prv points at or before the candidate jd; the 2 forced points are later
    // than the candidate and therefore excluded from the packet lightcurve.
    assert_eq!(alert.photometry.len(), 2);
    assert!(alert.photometry.iter().all(|p| p.band.starts_with("askap")));
    // 38.29... mJy -> nJy
    let flux = alert.photometry.last().unwrap().flux.unwrap();
    assert!((flux - 38.29584503173828e6).abs() < 1.0);

    let filter_passed = alert
        .filters
        .iter()
        .find(|f| f.filter_id == filter_id)
        .unwrap();
    assert_eq!(filter_passed.annotations, "{\"flux_now\":38.3}");

    // no real-bogus analog for ASKAP yet
    assert!(alert.classifications.is_empty());

    // science stamp only
    assert!(!alert.cutout_science.is_empty());
    assert!(alert.cutout_template.is_none());
    assert!(alert.cutout_difference.is_none());

    // the outgoing packet serializes with the optional-cutout Alert schema
    let schema = load_alert_schema().unwrap();
    let _ = alert_to_avro_bytes(&alert, &schema).unwrap();

    drop_alert_from_collections(candid, &Survey::Askap)
        .await
        .unwrap();
}
