use boom::{
    alert::{AlertWorker, ProcessAlertStatus},
    conf,
    filter::{alert_to_avro_bytes, load_alert_schema, FilterWorker, LsstFilterWorker},
    utils::{
        enums::Survey,
        testing::{
            drop_alert_from_collections, insert_test_filter, lsst_alert_worker, remove_test_filter,
            AlertRandomizer, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::doc;

#[tokio::test]
async fn test_lsst_alert_from_avro_bytes() {
    let mut alert_worker = lsst_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Lsst).get().await;
    let alert = alert_worker
        .alert_from_avro_bytes(&bytes_content)
        .await
        .unwrap();

    assert_eq!(alert.candid, candid);
    assert_eq!(alert.candidate.object_id, object_id);

    assert!((alert.candidate.dia_source.ra - ra).abs() < 1e-6);
    assert!((alert.candidate.dia_source.dec - dec).abs() < 1e-6);

    // add mag data to the candidate

    assert!((alert.candidate.dia_source.jd - 2457454.829282).abs() < 1e-6);
    assert!((alert.candidate.magpsf - 23.146893).abs() < 1e-6);
    assert!((alert.candidate.sigmapsf - 0.039097).abs() < 1e-6);
    assert!((alert.candidate.diffmaglim - 25.00841).abs() < 1e-5);
    assert!(alert.candidate.snr - 27.770037 < 1e-6);
    assert_eq!(alert.candidate.isdiffpos, true);
    assert_eq!(alert.candidate.dia_source.band.unwrap(), "g");

    // verify that the prv_candidates are present
    assert!(!alert.prv_candidates.is_none());
    let prv_candidates = alert.prv_candidates.unwrap();
    assert_eq!(prv_candidates.len(), 2);

    // validate the first prv_candidate
    let prv_candidate = prv_candidates.get(0).unwrap();

    assert!((prv_candidate.dia_source.jd - 2457454.7992).abs() < 1e-6);
    assert!((prv_candidate.magpsf - 24.763279).abs() < 1e-6);
    assert!((prv_candidate.sigmapsf - 0.329765).abs() < 1e-6);
    assert!((prv_candidate.diffmaglim - 24.309652).abs() < 1e-6);
    assert!(prv_candidate.snr - 3.292455 < 1e-6);
    assert_eq!(prv_candidate.isdiffpos, true);
    assert_eq!(prv_candidate.dia_source.band.clone().unwrap(), "g");

    // same for the fp_hists
    assert!(!alert.fp_hists.is_none());
    let fp_hists = alert.fp_hists.unwrap();
    assert_eq!(fp_hists.len(), 3);

    // validate the first fp_hist
    let fp_hist = fp_hists.get(0).unwrap();

    assert!((fp_hist.dia_forced_source.jd - 2457454.7992).abs() < 1e-6);
    assert!((fp_hist.magpsf.unwrap() - 24.735056).abs() < 1e-6);
    assert!((fp_hist.sigmapsf.unwrap() - 0.329754).abs() < 1e-6);
    assert!((fp_hist.diffmaglim - 24.281467).abs() < 1e-6);
    assert!((fp_hist.snr.unwrap() - 3.292566).abs() < 1e-6);
    assert_eq!(fp_hist.isdiffpos.unwrap(), true);
    assert_eq!(fp_hist.dia_forced_source.band.clone().unwrap(), "g");

    // validate the non detections
    assert!(!alert.prv_nondetections.is_none());
    // length should be 0
    assert_eq!(alert.prv_nondetections.unwrap().len(), 0);

    // TODO: find an LSST avro packet that has non detections so we can test it
}

#[tokio::test]
async fn test_process_lsst_alert() {
    let mut alert_worker = lsst_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Lsst).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    // Attempting to insert the error again is a no-op, not an error:
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Exists(candid));

    // let's query the database to check if the alert was inserted
    let config = conf::load_config(TEST_CONFIG_FILE).unwrap();
    let db = conf::build_db(&config).await.unwrap();
    let alert_collection_name = "LSST_alerts";
    let filter = doc! {"_id": candid};

    let alert = db
        .collection::<mongodb::bson::Document>(alert_collection_name)
        .find_one(filter.clone())
        .await
        .unwrap();

    assert!(alert.is_some());
    let alert = alert.unwrap();
    assert_eq!(alert.get_i64("_id").unwrap(), candid);
    assert_eq!(alert.get_str("objectId").unwrap(), &object_id);
    let candidate = alert.get_document("candidate").unwrap();
    assert_eq!(candidate.get_f64("ra").unwrap(), ra);
    assert_eq!(candidate.get_f64("dec").unwrap(), dec);

    // check that the cutouts were inserted
    let cutout_collection_name = "LSST_alerts_cutouts";
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
    let aux_collection_name = "LSST_alerts_aux";
    let filter_aux = doc! {"_id": &object_id};
    let aux = db
        .collection::<mongodb::bson::Document>(aux_collection_name)
        .find_one(filter_aux.clone())
        .await
        .unwrap();

    assert!(aux.is_some());
    let aux = aux.unwrap();
    assert_eq!(aux.get_str("_id").unwrap(), &object_id);
    // check that we have the arrays prv_candidates, prv_nondetections and fp_hists
    let prv_candidates = aux.get_array("prv_candidates").unwrap();
    assert_eq!(prv_candidates.len(), 3);

    let prv_nondetections = aux.get_array("prv_nondetections").unwrap();
    assert_eq!(prv_nondetections.len(), 0);

    let fp_hists = aux.get_array("fp_hists").unwrap();
    assert_eq!(fp_hists.len(), 3);

    drop_alert_from_collections(candid, "LSST").await.unwrap();
}

#[tokio::test]
async fn test_filter_lsst_alert() {
    let mut alert_worker = lsst_alert_worker().await;

    let (candid, object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Lsst).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let filter_id = insert_test_filter(&Survey::Lsst).await.unwrap();

    let mut filter_worker = LsstFilterWorker::new(TEST_CONFIG_FILE).await.unwrap();
    let result = filter_worker.process_alerts(&[format!("{}", candid)]).await;

    remove_test_filter(filter_id, &Survey::Lsst).await.unwrap();
    assert!(result.is_ok());

    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert.candid, candid);
    assert_eq!(&alert.object_id, &object_id);
    assert_eq!(alert.photometry.len(), 3); // prv_candidates + prv_nondetections

    let filter_passed = alert
        .filters
        .iter()
        .find(|f| f.filter_id == filter_id)
        .unwrap();
    assert_eq!(filter_passed.annotations, "{\"mag_now\":23.15}");

    let classifications = &alert.classifications;
    assert_eq!(classifications.len(), 0);

    // verify that we can convert the alert to avro bytes
    let schema = load_alert_schema().unwrap();
    let encoded = alert_to_avro_bytes(&alert, &schema);
    assert!(encoded.is_ok())
}
