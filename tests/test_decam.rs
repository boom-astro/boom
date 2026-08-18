#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
use boom::{
    alert::{AlertWorker, ProcessAlertStatus},
    conf::{get_test_cutout_storage, get_test_db},
    filter::{alert_to_avro_bytes, load_alert_schema, DecamFilterWorker, FilterWorker},
    utils::{
        enums::Survey,
        testing::{
            decam_alert_worker, drop_alert_from_collections, insert_test_filter,
            remove_test_filter, AlertRandomizer, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::doc;

#[tokio::test]
async fn test_process_decam_alert() {
    let mut alert_worker = decam_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Decam).get().await;
    let result = alert_worker.process_alert(&bytes_content).await;
    assert!(result.is_ok(), "{:?}", result);
    assert_eq!(result.unwrap(), ProcessAlertStatus::Added(candid));

    // Attempting to insert the error again is a no-op, not an error:
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Exists(candid));

    // let's query the database to check if the alert was inserted
    let db = get_test_db().await;
    let alert_collection_name = "DECAM_alerts";
    let filter = doc! {"_id": candid};

    let alert = db
        .collection::<mongodb::bson::Document>(alert_collection_name)
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

    // check that the cutouts were inserted
    let cutout_storage = get_test_cutout_storage(&Survey::Decam).await;
    let cutouts = cutout_storage
        .retrieve_cutouts(candid, false)
        .await
        .unwrap();
    assert_eq!(cutouts.candid, candid);

    // check that the aux collection was inserted
    let aux_collection_name = "DECAM_alerts_aux";
    let filter_aux = doc! {"_id": &object_id};
    let aux = db
        .collection::<mongodb::bson::Document>(aux_collection_name)
        .find_one(filter_aux.clone())
        .await
        .unwrap();

    assert!(aux.is_some());
    let aux = aux.unwrap();
    assert_eq!(aux.get_str("_id").unwrap(), &object_id);
    // check that we have the fp_hists array

    let fp_hists = aux.get_array("fp_hists").unwrap();
    assert_eq!(fp_hists.len(), 1);

    drop_alert_from_collections(candid, &Survey::Decam)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_filter_decam_alert() {
    let mut alert_worker = decam_alert_worker().await;

    let (candid, object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Decam).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let filter_id = insert_test_filter(&Survey::Decam, true).await.unwrap();

    let mut filter_worker = DecamFilterWorker::new(TEST_CONFIG_FILE, Some(vec![filter_id.clone()]))
        .await
        .unwrap();
    let result = filter_worker.process_alerts(&[format!("{}", candid)]).await;

    remove_test_filter(&filter_id, &Survey::Decam)
        .await
        .unwrap();
    assert!(result.is_ok(), "Filter failed: {:?}", result.err());

    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert.candid, candid);
    assert_eq!(&alert.object_id, &object_id);
    // one prv_candidate (the detection) + one fp_hist point
    assert_eq!(alert.photometry.len(), 2);

    let filter_passed = alert
        .filters
        .iter()
        .find(|f| f.filter_id == filter_id)
        .unwrap();
    assert_eq!(filter_passed.annotations, "{\"mag_now\":23.55}");

    // only the alert's real-bogus reliability score for now
    let classifications = &alert.classifications;
    assert_eq!(classifications.len(), 1);
    assert_eq!(classifications[0].classifier, "reliability");

    // verify the survey field is correct
    assert_eq!(alert.survey, Survey::Decam);

    // verify cutouts are non-empty
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

    drop_alert_from_collections(candid, &Survey::Decam)
        .await
        .unwrap();
}
