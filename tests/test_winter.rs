#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
use boom::{
    alert::{
        fid_to_band, sanitize_winter_avro, AlertError, AlertWorker, ProcessAlertStatus,
        WinterRawAvroAlert, DARK_FID,
    },
    conf::{get_test_cutout_storage, get_test_db},
    filter::{alert_to_avro_bytes, load_alert_schema, FilterWorker, WinterFilterWorker},
    utils::{
        enums::Survey,
        lightcurves::Band,
        testing::{
            drop_alert_from_collections, insert_custom_test_filter, remove_test_filter,
            winter_alert_worker, AlertRandomizer, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::doc;

#[test]
fn test_sanitize_winter_avro_is_readable() {
    // Both published schemas declare `sgmag1` twice, which the strict avro Reader rejects.
    for path in [
        "tests/data/alerts/winter/alert.avro",
        "tests/data/alerts/winter/alert_schemavsn_0.1.avro",
    ] {
        let raw = std::fs::read(path).unwrap();
        assert!(
            apache_avro::Reader::new(&raw[..]).is_err(),
            "{path}: raw WINTER avro should be rejected by the strict reader"
        );
        let fixed = sanitize_winter_avro(&raw).unwrap();
        let reader = apache_avro::Reader::new(&fixed[..]).unwrap_or_else(|e| panic!("{path}: {e}"));
        let value = reader.into_iter().next().unwrap().unwrap();
        let alert: WinterRawAvroAlert = apache_avro::from_value(&value).unwrap();
        assert!(!alert.object_id.is_empty(), "{path}: empty objectid");

        let fixed2 = sanitize_winter_avro(&fixed).unwrap();
        assert!(apache_avro::Reader::new(&fixed2[..]).is_ok(), "{path}");
    }
}

#[test]
fn test_winter_candidate_missing_field_deserializes() {
    // WINTER omits the candidate `field` entirely, so it has to fall back to the default.
    use apache_avro::types::Value;
    let raw = std::fs::read("tests/data/alerts/winter/alert.avro").unwrap();
    let fixed = sanitize_winter_avro(&raw).unwrap();
    let reader = apache_avro::Reader::new(&fixed[..]).unwrap();
    let value = reader.into_iter().next().unwrap().unwrap();

    let Value::Record(top) = &value else {
        panic!("alert is not a record");
    };
    let Some(Value::Record(candidate)) = top.iter().find(|(k, _)| k == "candidate").map(|(_, v)| v)
    else {
        panic!("candidate is not a record");
    };
    assert!(
        !candidate.iter().any(|(k, _)| k == "field"),
        "packet is expected to omit `field`"
    );

    let alert: WinterRawAvroAlert =
        apache_avro::from_value(&value).expect("candidate without `field` should parse");
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

#[test]
fn test_fid_maps_to_band() {
    // fid is 1-indexed. The upstream schema's doc string says 0=Y, 1=J, 2=H, 3=K,
    // which contradicts the alerts WINTER ships: its J-band data carries fid 2.
    assert_eq!(fid_to_band(1).unwrap(), Band::Y);
    assert_eq!(fid_to_band(2).unwrap(), Band::J);
    assert_eq!(fid_to_band(3).unwrap(), Band::H);
    // A dark frame and an unrecognised id are refused, never resolved to a
    // default: the band is what the photometry is later read as.
    assert!(matches!(fid_to_band(DARK_FID), Err(AlertError::DarkFrame)));
    assert!(matches!(fid_to_band(0), Err(AlertError::UnknownFid(0))));
    assert!(matches!(fid_to_band(9), Err(AlertError::UnknownFid(9))));
}

#[test]
fn test_real_alert_band_is_j() {
    // Kowalski reads these same fid=2 packets as 2massj, so the mapping is pinned to a real alert.
    let raw = std::fs::read("tests/data/alerts/winter/alert.avro").unwrap();
    let fixed = sanitize_winter_avro(&raw).unwrap();
    let reader = apache_avro::Reader::new(&fixed[..]).unwrap();
    let value = reader.into_iter().next().unwrap().unwrap();
    let alert: WinterRawAvroAlert = apache_avro::from_value(&value).unwrap();
    assert_eq!(alert.candidate.fid, 2);
    assert_eq!(fid_to_band(alert.candidate.fid).unwrap(), Band::J);
}
