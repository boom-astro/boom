#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
use boom::{
    alert::{sanitize_winter_avro, AlertWorker, ProcessAlertStatus, WinterRawAvroAlert},
    conf::{get_test_cutout_storage, get_test_db},
    utils::{
        enums::Survey,
        testing::{drop_alert_from_collections, winter_alert_worker, AlertRandomizer},
    },
};
use mongodb::bson::doc;

#[test]
fn test_sanitize_winter_avro_is_readable() {
    // The raw upstream WINTER avro has a duplicate field name and is rejected by
    // the strict avro Reader; the sanitized version must be parseable.
    let raw = std::fs::read("tests/data/alerts/winter/alert.avro").unwrap();
    assert!(
        apache_avro::Reader::new(&raw[..]).is_err(),
        "raw WINTER avro should be rejected by the strict reader"
    );
    let fixed = sanitize_winter_avro(&raw).unwrap();
    let reader = apache_avro::Reader::new(&fixed[..]).expect("sanitized avro should parse");
    let value = reader.into_iter().next().unwrap().unwrap();
    let alert: WinterRawAvroAlert = apache_avro::from_value(&value).unwrap();
    assert!(!alert.object_id.is_empty());
    // sanitization is idempotent
    let fixed2 = sanitize_winter_avro(&fixed).unwrap();
    assert!(apache_avro::Reader::new(&fixed2[..]).is_ok());
}

#[tokio::test]
async fn test_process_winter_alert() {
    let mut alert_worker = winter_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Wntr).get().await;
    let result = alert_worker.process_alert(&bytes_content).await;
    assert!(result.is_ok(), "{:?}", result);
    assert_eq!(result.unwrap(), ProcessAlertStatus::Added(candid));

    // Re-processing the same alert is a no-op, not an error.
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Exists(candid));

    let db = get_test_db().await;
    let filter = doc! {"_id": candid};
    let alert = db
        .collection::<mongodb::bson::Document>("WNTR_alerts")
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
    // the band must have been derived from fid and stored
    assert!(candidate.get_str("band").is_ok());

    // cutouts inserted
    let cutout_storage = get_test_cutout_storage(&Survey::Wntr).await;
    let cutouts = cutout_storage
        .retrieve_cutouts(candid, false)
        .await
        .unwrap();
    assert_eq!(cutouts.candid, candid);

    // aux collection inserted with prv_candidates (at least the current detection)
    let aux = db
        .collection::<mongodb::bson::Document>("WNTR_alerts_aux")
        .find_one(doc! {"_id": &object_id})
        .await
        .unwrap();
    assert!(aux.is_some());
    let aux = aux.unwrap();
    assert_eq!(aux.get_str("_id").unwrap(), &object_id);
    let prv_candidates = aux.get_array("prv_candidates").unwrap();
    assert!(!prv_candidates.is_empty());

    drop_alert_from_collections(candid, &Survey::Wntr)
        .await
        .unwrap();
}
