use boom::{
    alert::{AlertWorker, ProcessAlertStatus, LSST_ZTF_XMATCH_RADIUS, ZTF_DEC_RANGE},
    conf,
    enrichment::{EnrichmentWorker, LsstEnrichmentWorker},
    filter::{alert_to_avro_bytes, load_alert_schema, FilterWorker, LsstFilterWorker},
    utils::{
        enums::Survey,
        testing::{
            drop_alert_from_collections, insert_test_filter, lsst_alert_worker, remove_test_filter,
            ztf_alert_worker, AlertRandomizer, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::doc;

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
    assert_eq!(prv_candidates.len(), 1);

    // let prv_nondetections = aux.get_array("prv_nondetections").unwrap();
    // assert_eq!(prv_nondetections.len(), 0);
    // TODO: check again once non detections are added back to the schema

    let fp_hists = aux.get_array("fp_hists").unwrap();
    assert_eq!(fp_hists.len(), 0);

    drop_alert_from_collections(candid, "LSST").await.unwrap();
}

#[tokio::test]
async fn test_process_lsst_alert_xmatch() {
    let config = conf::load_config(TEST_CONFIG_FILE).unwrap();
    let db = conf::build_db(&config).await.unwrap();

    let mut alert_worker = lsst_alert_worker().await;
    let lsst_alert_randomizer =
        AlertRandomizer::new_randomized(Survey::Lsst).dec(ZTF_DEC_RANGE.1 - 10.0);

    let (_, object_id, ra, dec, _) = lsst_alert_randomizer.clone().get().await;
    let aux_collection_name = "LSST_alerts_aux";
    let filter_aux = doc! {"_id": &object_id};

    // ZTF setup
    let mut ztf_alert_worker = ztf_alert_worker().await;

    // 1. nearby ZTF alert, LSST alert should have a ZTF alias
    let (_, ztf_object_id, _, _, ztf_bytes_content) = AlertRandomizer::new_randomized(Survey::Ztf)
        .ra(ra)
        .dec(dec + 0.9 * LSST_ZTF_XMATCH_RADIUS.to_degrees())
        .get()
        .await;
    ztf_alert_worker
        .process_alert(&ztf_bytes_content)
        .await
        .unwrap();

    let (_, _, _, _, bytes_content) = lsst_alert_randomizer.clone().rand_candid().get().await;
    alert_worker.process_alert(&bytes_content).await.unwrap();
    let aux = db
        .collection::<mongodb::bson::Document>(aux_collection_name)
        .find_one(filter_aux.clone())
        .await
        .unwrap()
        .unwrap();
    let ztf_matches = aux
        .get_document("aliases")
        .unwrap()
        .get_array("ZTF")
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ztf_matches, vec![ztf_object_id.clone()]);
}

#[tokio::test]
async fn test_enrich_lsst_alert() {
    let mut alert_worker = lsst_alert_worker().await;

    let config = conf::load_config(TEST_CONFIG_FILE).unwrap();
    let mut con = conf::build_redis(&config).await.unwrap();

    // we only randomize the candid and object_id here, since the ra/dec
    // are features of the models and would change the results
    let (candid, _, _, _, bytes_content) = AlertRandomizer::new(Survey::Lsst)
        .rand_candid()
        .rand_object_id()
        .get()
        .await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let mut enrichment_worker = LsstEnrichmentWorker::new(TEST_CONFIG_FILE).await.unwrap();
    let result = enrichment_worker
        .process_alerts(&[candid], Some(&mut con))
        .await;
    assert!(result.is_ok());

    // the result should be a vec of String, for ZTF with the format
    // "programid,candid" which is what the filter worker expects
    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert, &format!("{}", candid));

    // check that the alert was inserted in the DB, and ML scores added later
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

    // the enrichment worker also adds "properties" to the alert
    let properties = alert.get_document("properties").unwrap();
    assert_eq!(properties.get_bool("rock").unwrap(), false);
    assert_eq!(properties.get_bool("stationary").unwrap(), false);
    // the properties also include "photstats, a document with bands as keys and
    // as values the rate of evolution (mag/day) before and after peak
    let photstats = properties.get_document("photstats").unwrap();

    assert!(photstats.contains_key("r"));
    let r_stats = photstats.get_document("r").unwrap();
    let peak_mag = r_stats.get_f64("peak_mag").unwrap();
    let peak_jd = r_stats.get_f64("peak_jd").unwrap();
    assert!((peak_mag - 23.674994).abs() < 1e-6);
    assert!((peak_jd - 2460961.733092).abs() < 1e-6);
}

#[tokio::test]
async fn test_filter_lsst_alert() {
    let mut alert_worker = lsst_alert_worker().await;

    let (candid, object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Lsst).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let filter_id = insert_test_filter(&Survey::Lsst, true, false)
        .await
        .unwrap();

    let mut filter_worker = LsstFilterWorker::new(TEST_CONFIG_FILE, Some(vec![filter_id.clone()]))
        .await
        .unwrap();
    let result = filter_worker.process_alerts(&[format!("{}", candid)]).await;

    remove_test_filter(&filter_id, &Survey::Lsst).await.unwrap();
    assert!(result.is_ok());

    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert.candid, candid);
    assert_eq!(&alert.object_id, &object_id);
    assert_eq!(alert.photometry.len(), 1); // prv_candidates + prv_nondetections

    let filter_passed = alert
        .filters
        .iter()
        .find(|f| f.filter_id == filter_id)
        .unwrap();
    assert_eq!(filter_passed.annotations, "{\"mag_now\":23.67}");

    let classifications = &alert.classifications;
    assert_eq!(classifications.len(), 0);

    // verify that we can convert the alert to avro bytes
    let schema = load_alert_schema().unwrap();
    let encoded = alert_to_avro_bytes(&alert, &schema);
    assert!(encoded.is_ok())
}
