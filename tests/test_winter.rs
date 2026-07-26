#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
use boom::{
    alert::{sanitize_winter_avro, AlertWorker, ProcessAlertStatus, WinterRawAvroAlert},
    conf::{get_test_cutout_storage, get_test_db},
    filter::{alert_to_avro_bytes, load_alert_schema, FilterWorker, WinterFilterWorker},
    utils::{
        enums::Survey,
        testing::{
            drop_alert_from_collections, insert_custom_test_filter, remove_test_filter,
            winter_alert_worker, AlertRandomizer, TEST_CONFIG_FILE,
        },
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

#[test]
fn test_winter_candidate_missing_field_deserializes() {
    // Some upstream WINTER packets omit the candidate `field` entirely. That must
    // deserialize (with `field` defaulting) rather than failing the whole alert
    // with "missing field `field`". We simulate it by dropping `field` from the
    // decoded record, which is exactly the shape the strict serde path sees.
    use apache_avro::types::Value;
    let raw = std::fs::read("tests/data/alerts/winter/alert.avro").unwrap();
    let fixed = sanitize_winter_avro(&raw).unwrap();
    let reader = apache_avro::Reader::new(&fixed[..]).unwrap();
    let mut value = reader.into_iter().next().unwrap().unwrap();

    // Baseline: with `field` present it must still parse.
    apache_avro::from_value::<WinterRawAvroAlert>(&value).expect("baseline should parse");

    // Drop `field` from the nested candidate record.
    if let Value::Record(top) = &mut value {
        let candidate = top
            .iter_mut()
            .find(|(k, _)| k == "candidate")
            .map(|(_, v)| v)
            .expect("candidate field");
        if let Value::Record(fields) = candidate {
            let before = fields.len();
            fields.retain(|(k, _)| k != "field");
            assert_eq!(before - 1, fields.len(), "expected to drop `field`");
        } else {
            panic!("candidate is not a record");
        }
    } else {
        panic!("alert is not a record");
    }

    let alert: WinterRawAvroAlert =
        apache_avro::from_value(&value).expect("candidate missing `field` should still parse");
    assert_eq!(alert.candidate.field, 0, "absent `field` defaults to 0");
}

#[tokio::test]
async fn test_process_winter_alert() {
    let mut alert_worker = winter_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Winter).get().await;
    let result = alert_worker.process_alert(&bytes_content).await;
    assert!(result.is_ok(), "{:?}", result);
    assert_eq!(result.unwrap(), ProcessAlertStatus::Added(candid));

    // Re-processing the same alert is a no-op, not an error.
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Exists(candid));

    let db = get_test_db().await;
    let filter = doc! {"_id": candid};
    let alert = db
        .collection::<mongodb::bson::Document>("WINTER_alerts")
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
    let cutout_storage = get_test_cutout_storage(&Survey::Winter).await;
    let cutouts = cutout_storage
        .retrieve_cutouts(candid, false)
        .await
        .unwrap();
    assert_eq!(cutouts.candid, candid);

    // aux collection inserted with prv_candidates (at least the current detection)
    let aux = db
        .collection::<mongodb::bson::Document>("WINTER_alerts_aux")
        .find_one(doc! {"_id": &object_id})
        .await
        .unwrap();
    assert!(aux.is_some());
    let aux = aux.unwrap();
    assert_eq!(aux.get_str("_id").unwrap(), &object_id);
    let prv_candidates = aux.get_array("prv_candidates").unwrap();
    assert!(!prv_candidates.is_empty());

    drop_alert_from_collections(candid, &Survey::Winter)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_filter_winter_alert() {
    let mut alert_worker = winter_alert_worker().await;

    let (candid, object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Winter).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    // A permissive filter so the test is independent of the fixture's magnitude;
    // this still exercises the full WINTER filter path (build_loaded_filter ->
    // build_filter_pipeline -> build_winter_filter_pipeline).
    let pipeline = "[{\"$match\": {\"candidate.jd\": {\"$gt\": 0.0}}}, {\"$project\": {\"objectId\": 1, \"annotations.mag_now\": {\"$round\": [\"$candidate.magpsf\", 2]}}}]";
    let filter_id = insert_custom_test_filter(&Survey::Winter, pipeline)
        .await
        .unwrap();

    let mut filter_worker =
        WinterFilterWorker::new(TEST_CONFIG_FILE, Some(vec![filter_id.clone()]))
            .await
            .unwrap();
    let result = filter_worker.process_alerts(&[format!("{}", candid)]).await;

    remove_test_filter(&filter_id, &Survey::Winter)
        .await
        .unwrap();
    assert!(result.is_ok(), "Filter failed: {:?}", result.err());

    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert.candid, candid);
    assert_eq!(&alert.object_id, &object_id);
    assert_eq!(alert.survey, Survey::Winter);
    assert!(!alert.photometry.is_empty());

    let filter_passed = alert
        .filters
        .iter()
        .find(|f| f.filter_id == filter_id)
        .unwrap();
    assert!(filter_passed.annotations.contains("mag_now"));

    // verify cutouts are non-empty
    assert!(!alert.cutout_science.is_empty());
    assert!(!alert.cutout_template.is_empty());
    assert!(!alert.cutout_difference.is_empty());

    // verify that we can convert the alert to avro bytes
    let schema = load_alert_schema().unwrap();
    let _ = alert_to_avro_bytes(&alert, &schema).unwrap();

    drop_alert_from_collections(candid, &Survey::Winter)
        .await
        .unwrap();
}
