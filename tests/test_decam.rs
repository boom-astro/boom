use boom::{
    alert::{AlertWorker, ProcessAlertStatus},
    conf,
    filter::{alert_to_avro_bytes, load_alert_schema, DecamFilterWorker, FilterWorker},
    utils::enums::Survey,
    utils::testing::{
        decam_alert_worker, drop_alert_from_collections, insert_test_filter, remove_test_filter,
        AlertRandomizer, TEST_CONFIG_FILE,
    },
};
use mongodb::bson::doc;

#[tokio::test]
async fn test_decam_alert_from_avro_bytes() {
    let mut alert_worker = decam_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::default(Survey::Decam).get().await;
    let alert = alert_worker.alert_from_avro_bytes(&bytes_content).await;
    assert!(alert.is_ok());

    // validate the alert
    let alert = alert.unwrap();
    assert_eq!(alert.publisher, "DESIRT");
    assert_eq!(alert.object_id, object_id);
    assert_eq!(alert.candid, candid);

    // validate the candidate
    let candidate = alert.clone().candidate;
    assert_eq!(candidate.ra, ra);
    assert_eq!(candidate.dec, dec);

    // validate the cutouts
    assert_eq!(alert.cutout_science.clone().len(), 54561);
    assert_eq!(alert.cutout_template.clone().len(), 49810);
    assert_eq!(alert.cutout_difference.clone().len(), 54569);
}

#[tokio::test]
async fn test_process_decam_alert() {
    let mut alert_worker = decam_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::default(Survey::Decam).get().await;
    let result = alert_worker.process_alert(&bytes_content).await;
    assert!(result.is_ok(), "{:?}", result);
    assert_eq!(result.unwrap(), ProcessAlertStatus::Added(candid));

    // Attempting to insert the error again is a no-op, not an error:
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Exists(candid));

    // let's query the database to check if the alert was inserted
    let config = conf::load_config(TEST_CONFIG_FILE).unwrap();
    let db = conf::build_db(&config).await.unwrap();
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
    let cutout_collection_name = "DECAM_alerts_cutouts";
    let cutouts = db
        .collection::<mongodb::bson::Document>(cutout_collection_name)
        .find_one(filter.clone())
        .await
        .unwrap();
    assert!(cutouts.is_some());
    let cutouts = cutouts.unwrap();
    assert_eq!(cutouts.get_i64("_id").unwrap(), candid);
    assert!(cutouts.contains_key("cutoutScience"));
    assert!(cutouts.contains_key("cutoutTemplate"));
    assert!(cutouts.contains_key("cutoutDifference"));

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
    assert_eq!(fp_hists.len(), 61);

    drop_alert_from_collections(candid, "DECAM").await.unwrap();
}

#[tokio::test]
async fn test_filter_decam_alert() {
    let mut alert_worker = decam_alert_worker().await;

    let (candid, object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::default(Survey::Decam).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let filter_id = insert_test_filter(&Survey::Decam).await.unwrap();

    let mut filter_worker = DecamFilterWorker::new(TEST_CONFIG_FILE).await.unwrap();
    let result = filter_worker.process_alerts(&[format!("{}", candid)]).await;

    remove_test_filter(filter_id, &Survey::Decam).await.unwrap();
    assert!(result.is_ok());

    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert.candid, candid);
    assert_eq!(&alert.object_id, &object_id);
    assert_eq!(alert.photometry.len(), 61); // prv_candidates + prv_nondetections

    let filter_passed = alert
        .filters
        .iter()
        .find(|f| f.filter_id == filter_id)
        .unwrap();
    assert_eq!(filter_passed.annotations, "{\"mag_now\":23.23}");

    let classifications = &alert.classifications;
    assert_eq!(classifications.len(), 0);

    // verify that we can convert the alert to avro bytes
    let schema = load_alert_schema().unwrap();
    let encoded = alert_to_avro_bytes(&alert, &schema);
    assert!(encoded.is_ok())
}
