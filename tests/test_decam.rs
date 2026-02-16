use boom::{
    alert::{AlertWorker, ProcessAlertStatus},
    conf::get_test_db,
    enrichment::{DecamEnrichmentWorker, EnrichmentWorker},
    utils::{
        enums::Survey,
        testing::{
            decam_alert_worker, drop_alert_from_collections, AlertRandomizer, TEST_CONFIG_FILE,
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
async fn test_enrich_decam_alert() {
    let mut alert_worker = decam_alert_worker().await;

    // we only randomize the candid and object_id here, since the ra/dec
    // are features of the models and would change the results
    let (candid, _, _, _, bytes_content) = AlertRandomizer::new(Survey::Decam)
        .rand_candid()
        .rand_object_id()
        .get()
        .await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let mut enrichment_worker = DecamEnrichmentWorker::new(TEST_CONFIG_FILE).await.unwrap();
    let result = enrichment_worker.process_alerts(&[candid]).await;
    assert!(result.is_ok(), "Enrichment failed: {:?}", result.err());

    // the result should be a vec of String, for DECAM with the format
    // "candid" which is what the filter worker expects
    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert, &format!("{}", candid));

    // check that the alert was inserted in the DB, and ML scores added later
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

    // this object is a variable star, so all scores except acai_v should be ~0.0
    // (we've also verified that the scores we get here were close to Kowalski's)
    let classifications = alert.get_document("classifications").unwrap();
    assert!(classifications.get_f64("drb").unwrap() < 0.01);

    // the enrichment worker also adds "properties" to the alert
    let properties = alert.get_document("properties").unwrap();
    assert_eq!(properties.get_bool("stationary").unwrap(), true);

    // // the properties also include "photstats, a document with bands as keys and
    // // as values the rate of evolution (mag/day) before and after peak
    let photstats = properties.get_document("photstats").unwrap();

    // check the values for the g band
    assert!(photstats.contains_key("g"));
    let g_stats = photstats.get_document("g").unwrap();
    let peak_mag = g_stats.get_f64("peak_mag").unwrap();
    let peak_jd = g_stats.get_f64("peak_jd").unwrap();
    let dt = g_stats.get_f64("dt").unwrap();
    assert!((peak_mag - 22.595936).abs() < 1e-6);
    assert!((peak_jd - 2460709.838387).abs() < 1e-6);
    assert!((dt - 91.826569).abs() < 1e-6);

    // g fading stats
    let fading = g_stats.get_document("fading").unwrap();
    let fading_rate = fading.get_f64("rate").unwrap();
    let fading_rate_error = fading.get_f64("rate_error").unwrap();
    let fading_red_chi2 = fading.get_f64("red_chi2").unwrap();
    let fading_nb_data = fading.get_i32("nb_data").unwrap();
    let fading_dt = fading.get_f64("dt").unwrap();
    assert!((fading_rate - 0.005666).abs() < 1e-6);
    assert!((fading_rate_error - 0.001097).abs() < 1e-6);
    assert!((fading_red_chi2 - 0.906818).abs() < 1e-6);
    assert_eq!(fading_nb_data, 18);
    assert!((fading_dt - 91.826569).abs() < 1e-6);

    // check the values for the r band
    assert!(photstats.contains_key("r"));
    let r_stats = photstats.get_document("r").unwrap();
    let peak_mag = r_stats.get_f64("peak_mag").unwrap();
    let peak_jd = r_stats.get_f64("peak_jd").unwrap();
    let dt = r_stats.get_f64("dt").unwrap();
    assert!((peak_mag - 22.409403).abs() < 1e-6);
    assert!((peak_jd - 2460721.819624).abs() < 1e-6);
    assert!((dt - 91.853958).abs() < 1e-6);

    // r rising stats
    let rising = r_stats.get_document("rising").unwrap();
    let rising_rate = rising.get_f64("rate").unwrap();
    let rising_rate_error = rising.get_f64("rate_error").unwrap();
    let rising_red_chi2 = rising.get_f64("red_chi2").unwrap();
    let rising_nb_data = rising.get_i32("nb_data").unwrap();
    let rising_dt = rising.get_f64("dt").unwrap();
    assert!((rising_rate + 0.028852).abs() < 1e-6);
    assert!((rising_rate_error - 0.012712).abs() < 1e-6);
    assert!((rising_red_chi2 - 0.808667).abs() < 1e-6);
    assert_eq!(rising_nb_data, 3);
    assert!((rising_dt - 11.980281).abs() < 1e-6);

    // r fading stats
    let fading = r_stats.get_document("fading").unwrap();
    let fading_rate = fading.get_f64("rate").unwrap();
    let fading_rate_error = fading.get_f64("rate_error").unwrap();
    let fading_red_chi2 = fading.get_f64("red_chi2").unwrap();
    let fading_nb_data = fading.get_i32("nb_data").unwrap();
    let fading_dt = fading.get_f64("dt").unwrap();
    assert!((fading_rate - 0.007876).abs() < 1e-6);
    assert!((fading_rate_error - 0.001446).abs() < 1e-6);
    assert!((fading_red_chi2 - 1.060650).abs() < 1e-6);
    assert_eq!(fading_nb_data, 17);
    assert!((fading_dt - 79.873680).abs() < 1e-6);

    // check the values for the i band
    assert!(photstats.contains_key("i"));
    let i_stats = photstats.get_document("i").unwrap();
    let peak_mag = i_stats.get_f64("peak_mag").unwrap();
    let peak_jd = i_stats.get_f64("peak_jd").unwrap();
    let dt = i_stats.get_f64("dt").unwrap();
    assert!((peak_mag - 22.592588).abs() < 1e-6);
    assert!((peak_jd - 2460782.796463).abs() < 1e-6);
    assert!((dt - 43.843380).abs() < 1e-6);

    // i rising stats
    let rising = i_stats.get_document("rising").unwrap();
    let rising_rate = rising.get_f64("rate").unwrap();
    let rising_rate_error = rising.get_f64("rate_error").unwrap();
    let rising_red_chi2 = rising.get_f64("red_chi2").unwrap();
    let rising_nb_data = rising.get_i32("nb_data").unwrap();
    let rising_dt = rising.get_f64("dt").unwrap();
    assert!((rising_rate + 0.006103).abs() < 1e-6);
    assert!((rising_rate_error - 0.007241).abs() < 1e-6);
    assert!((rising_red_chi2 - 2.803902).abs() < 1e-6);
    assert_eq!(rising_nb_data, 7);
    assert!((rising_dt - 24.958101).abs() < 1e-6);

    // i fading stats
    let fading = i_stats.get_document("fading").unwrap();
    let fading_rate = fading.get_f64("rate").unwrap();
    let fading_rate_error = fading.get_f64("rate_error").unwrap();
    let fading_red_chi2 = fading.get_f64("red_chi2").unwrap();
    let fading_nb_data = fading.get_i32("nb_data").unwrap();
    let fading_dt = fading.get_f64("dt").unwrap();
    assert!((fading_rate - 0.028942).abs() < 1e-6);
    assert!((fading_rate_error - 0.011173).abs() < 1e-6);
    assert!((fading_red_chi2 - 1.173454).abs() < 1e-6);
    assert_eq!(fading_nb_data, 8);
    assert!((fading_dt - 18.885277).abs() < 1e-6);

    // check the values for the z band
    assert!(photstats.contains_key("z"));
    let z_stats = photstats.get_document("z").unwrap();
    let peak_mag = z_stats.get_f64("peak_mag").unwrap();
    let peak_jd = z_stats.get_f64("peak_jd").unwrap();
    let dt = z_stats.get_f64("dt").unwrap();
    assert!((peak_mag - 22.245645).abs() < 1e-6);
    assert!((peak_jd - 2460727.849294).abs() < 1e-6);
    assert!((dt - 6.028493).abs() < 1e-6);

    // z rising stats
    let rising = z_stats.get_document("rising").unwrap();
    let rising_rate = rising.get_f64("rate").unwrap();
    let rising_rate_error = rising.get_f64("rate_error").unwrap();
    let rising_red_chi2 = rising.get_f64("red_chi2").unwrap();
    let rising_nb_data = rising.get_i32("nb_data").unwrap();
    let rising_dt = rising.get_f64("dt").unwrap();
    assert!((rising_rate + 0.075088).abs() < 1e-6);
    assert!((rising_rate_error - 0.039928).abs() < 1e-6);
    assert!((rising_red_chi2 - 0.036359).abs() < 1e-6);
    assert_eq!(rising_nb_data, 3);
    assert!((rising_dt - 6.028493).abs() < 1e-6);
}
