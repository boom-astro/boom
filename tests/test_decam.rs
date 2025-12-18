use boom::{
    alert::{AlertWorker, ProcessAlertStatus},
    conf::{get_test_cutout_storage, get_test_db},
    utils::{
        enums::Survey,
        testing::{decam_alert_worker, drop_alert_from_collections, AlertRandomizer},
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
    let cutouts = cutout_storage.retrieve_cutouts(candid).await.unwrap();
    assert_eq!(cutouts.candid, candid);
    assert_eq!(cutouts.object_id, object_id);

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
