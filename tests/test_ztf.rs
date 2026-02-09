use boom::{
    alert::{
        AlertWorker, ProcessAlertStatus, DECAM_DEC_RANGE, LSST_DEC_RANGE, ZTF_DECAM_XMATCH_RADIUS,
        ZTF_LSST_XMATCH_RADIUS,
    },
    conf::get_test_db,
    enrichment::{EnrichmentWorker, ZtfEnrichmentWorker},
    filter::{alert_to_avro_bytes, load_alert_schema, FilterWorker, Origin, ZtfFilterWorker},
    utils::{
        enums::Survey,
        lightcurves::{flux2mag, ZTF_ZP},
        testing::{
            decam_alert_worker, drop_alert_from_collections, insert_test_filter, lsst_alert_worker,
            remove_test_filter, ztf_alert_worker, AlertRandomizer, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::doc;

#[tokio::test]
async fn test_process_ztf_alert() {
    let mut alert_worker = ztf_alert_worker().await;

    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Ztf).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    // Attempting to insert the error again is a no-op, not an error:
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Exists(candid));

    // let's query the database to check if the alert was inserted
    let db = get_test_db().await;
    let alert_collection_name = "ZTF_alerts";
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
    let cutout_collection_name = "ZTF_alerts_cutouts";
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
    let aux_collection_name = "ZTF_alerts_aux";
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
    assert_eq!(prv_candidates.len(), 8);

    let prv_nondetections = aux.get_array("prv_nondetections").unwrap();
    assert_eq!(prv_nondetections.len(), 3);

    let fp_hists = aux.get_array("fp_hists").unwrap();
    assert_eq!(fp_hists.len(), 10);

    drop_alert_from_collections(candid, "ZTF").await.unwrap();
}

#[tokio::test]
async fn test_process_ztf_alert_xmatch() {
    let db = get_test_db().await;

    // ZTF setup: the dec should be *below* the LSST dec limit:
    let mut alert_worker = ztf_alert_worker().await;
    let ztf_alert_randomizer =
        AlertRandomizer::new_randomized(Survey::Ztf).dec(LSST_DEC_RANGE.1 - 10.0);

    let (_, object_id, ra, dec, bytes_content) = ztf_alert_randomizer.clone().get().await;
    let aux_collection_name = "ZTF_alerts_aux";
    let filter_aux = doc! {"_id": &object_id};

    // LSST setup
    let mut lsst_alert_worker = lsst_alert_worker().await;

    // 1. LSST alert further than max radius, ZTF alert should not have an LSST alias
    let (_, _, _, _, lsst_bytes_content) = AlertRandomizer::new_randomized(Survey::Lsst)
        .ra(ra)
        .dec(dec + 1.1 * ZTF_LSST_XMATCH_RADIUS.to_degrees())
        .get()
        .await;
    lsst_alert_worker
        .process_alert(&lsst_bytes_content)
        .await
        .unwrap();

    alert_worker.process_alert(&bytes_content).await.unwrap();
    let aux = db
        .collection::<mongodb::bson::Document>(aux_collection_name)
        .find_one(filter_aux.clone())
        .await
        .unwrap()
        .unwrap();
    let matches = aux
        .get_document("aliases")
        .unwrap()
        .get_array("LSST")
        .unwrap();
    assert_eq!(matches.len(), 0);

    // 2. nearby LSST alert, ZTF alert should have an LSST alias
    let (_, lsst_object_id, _, _, lsst_bytes_content) =
        AlertRandomizer::new_randomized(Survey::Lsst)
            .ra(ra)
            .dec(dec + 0.9 * ZTF_LSST_XMATCH_RADIUS.to_degrees())
            .get()
            .await;
    lsst_alert_worker
        .process_alert(&lsst_bytes_content)
        .await
        .unwrap();

    let (_, _, _, _, bytes_content) = ztf_alert_randomizer.clone().rand_candid().get().await;
    alert_worker.process_alert(&bytes_content).await.unwrap();
    let aux = db
        .collection::<mongodb::bson::Document>(aux_collection_name)
        .find_one(filter_aux.clone())
        .await
        .unwrap()
        .unwrap();
    let lsst_matches = aux
        .get_document("aliases")
        .unwrap()
        .get_array("LSST")
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lsst_matches, vec![lsst_object_id.clone()]);

    // 3. Closer LSST alert, ZTF alert should have a new LSST alias
    let (_, lsst_object_id, _, _, lsst_bytes_content) =
        AlertRandomizer::new_randomized(Survey::Lsst)
            .ra(ra)
            .dec(dec + 0.1 * ZTF_LSST_XMATCH_RADIUS.to_degrees())
            .get()
            .await;
    lsst_alert_worker
        .process_alert(&lsst_bytes_content)
        .await
        .unwrap();

    let (_, _, _, _, bytes_content) = ztf_alert_randomizer.clone().rand_candid().get().await;
    alert_worker.process_alert(&bytes_content).await.unwrap();
    let aux = db
        .collection::<mongodb::bson::Document>(aux_collection_name)
        .find_one(filter_aux.clone())
        .await
        .unwrap()
        .unwrap();
    let lsst_matches = aux
        .get_document("aliases")
        .unwrap()
        .get_array("LSST")
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lsst_matches, vec![lsst_object_id.clone()]);

    // 4. Further LSST alert, ZTF alert should NOT have a new LSST alias
    let (_, bad_lsst_object_id, _, _, lsst_bytes_content) =
        AlertRandomizer::new_randomized(Survey::Lsst)
            .ra(ra)
            .dec(dec + 0.5 * ZTF_LSST_XMATCH_RADIUS.to_degrees())
            .get()
            .await;
    lsst_alert_worker
        .process_alert(&lsst_bytes_content)
        .await
        .unwrap();

    let (_, _, _, _, bytes_content) = ztf_alert_randomizer.clone().rand_candid().get().await;
    alert_worker.process_alert(&bytes_content).await.unwrap();
    let aux = db
        .collection::<mongodb::bson::Document>(aux_collection_name)
        .find_one(filter_aux.clone())
        .await
        .unwrap()
        .unwrap();
    let lsst_matches = aux
        .get_document("aliases")
        .unwrap()
        .get_array("LSST")
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lsst_matches, vec![lsst_object_id.clone()]);
    assert_ne!(lsst_matches, vec![bad_lsst_object_id]);

    // 5. This ZTF alert is above the LSST dec cutoff and therefore should not
    //    even attempt to match. Test this by creating an LSST alert with an
    //    unrealistically high dec that ZTF would otherwise match without this
    //    constraint:
    let (_, bad_object_id, bad_ra, bad_dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Ztf)
            .dec(LSST_DEC_RANGE.1 + 10.0)
            .get()
            .await;

    let (_, _, _, _, lsst_bytes_content) = AlertRandomizer::new_randomized(Survey::Lsst)
        .ra(bad_ra)
        .dec(bad_dec + 0.9 * ZTF_LSST_XMATCH_RADIUS.to_degrees())
        .get()
        .await;
    lsst_alert_worker
        .process_alert(&lsst_bytes_content)
        .await
        .unwrap();

    alert_worker.process_alert(&bytes_content).await.unwrap();
    let bad_filter_aux = doc! {"_id": &bad_object_id};
    let aux = db
        .collection::<mongodb::bson::Document>(aux_collection_name)
        .find_one(bad_filter_aux)
        .await
        .unwrap()
        .unwrap();
    let lsst_matches = aux
        .get_document("aliases")
        .unwrap()
        .get_array("LSST")
        .unwrap()
        .iter()
        .map(|x| x.as_i64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lsst_matches.len(), 0);

    // DECAM setup (here we just verify that xmatching is done, and do not test all possible cases):
    let ztf_alert_randomizer =
        AlertRandomizer::new_randomized(Survey::Ztf).dec(DECAM_DEC_RANGE.1 - 10.0);

    let (_, object_id, ra, dec, bytes_content) = ztf_alert_randomizer.get().await;
    let filter_aux = doc! {"_id": &object_id};

    let mut decam_alert_worker = decam_alert_worker().await;
    let (_, decam_object_id, _, _, decam_bytes_content) =
        AlertRandomizer::new_randomized(Survey::Decam)
            .ra(ra)
            .dec(dec + 0.9 * ZTF_DECAM_XMATCH_RADIUS.to_degrees())
            .get()
            .await;

    decam_alert_worker
        .process_alert(&decam_bytes_content)
        .await
        .unwrap();

    alert_worker.process_alert(&bytes_content).await.unwrap();
    let aux = db
        .collection::<mongodb::bson::Document>(aux_collection_name)
        .find_one(filter_aux.clone())
        .await
        .unwrap()
        .unwrap();
    let matches = aux
        .get_document("aliases")
        .unwrap()
        .get_array("DECAM")
        .unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches.get(0).unwrap().as_str().unwrap(), &decam_object_id);
}

#[tokio::test]
async fn test_enrich_ztf_alert() {
    let mut alert_worker = ztf_alert_worker().await;

    // we only randomize the candid and object_id here, since the ra/dec
    // are features of the models and would change the results
    let (candid, _, _, _, bytes_content) = AlertRandomizer::new(Survey::Ztf)
        .rand_candid()
        .rand_object_id()
        .get()
        .await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let mut enrichment_worker = ZtfEnrichmentWorker::new(TEST_CONFIG_FILE).await.unwrap();
    let result = enrichment_worker.process_alerts(&[candid]).await;
    assert!(result.is_ok(), "Enrichment failed: {:?}", result.err());

    // the result should be a vec of String, for ZTF with the format
    // "programid,candid" which is what the filter worker expects
    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert, &format!("1,{}", candid));

    // check that the alert was inserted in the DB, and ML scores added later
    let db = get_test_db().await;
    let alert_collection_name = "ZTF_alerts";
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
    assert!(classifications.get_f64("acai_h").unwrap() < 0.01);
    assert!(classifications.get_f64("acai_n").unwrap() < 0.01);
    assert!(classifications.get_f64("acai_v").unwrap() > 0.99);
    assert!(classifications.get_f64("acai_o").unwrap() < 0.01);
    assert!(classifications.get_f64("acai_b").unwrap() < 0.01);
    assert!(classifications.get_f64("btsbot").unwrap() < 0.01);

    // the enrichment worker also adds "properties" to the alert
    let properties = alert.get_document("properties").unwrap();
    assert_eq!(properties.get_bool("rock").unwrap(), false);
    assert_eq!(properties.get_bool("star").unwrap(), true);
    assert_eq!(properties.get_bool("near_brightstar").unwrap(), true);
    assert_eq!(properties.get_bool("stationary").unwrap(), true);

    // the properties also include "photstats, a document with bands as keys and
    // as values the rate of evolution (mag/day) before and after peak
    let photstats = properties.get_document("photstats").unwrap();

    // check the values for the g band
    assert!(photstats.contains_key("g"));
    let g_stats = photstats.get_document("g").unwrap();

    // g basic stats
    let peak_mag = g_stats.get_f64("peak_mag").unwrap();
    let peak_jd = g_stats.get_f64("peak_jd").unwrap();
    let dt = g_stats.get_f64("dt").unwrap();
    assert!((peak_mag - 15.6940).abs() < 1e-6);
    assert!((peak_jd - 2460441.971956).abs() < 1e-6);
    assert!((dt - 26.956516).abs() < 1e-6);

    // g rising stats
    let rising = g_stats.get_document("rising").unwrap();
    let rising_rate = rising.get_f64("rate").unwrap();
    let rising_red_chi2 = rising.get_f64("red_chi2").unwrap();
    let rising_dt = rising.get_f64("dt").unwrap();
    assert!((rising_rate + 0.086736).abs() < 1e-6);
    assert!((rising_red_chi2 - 82.419716).abs() < 1e-6); // bad fit
    assert!((rising_dt - 21.008194).abs() < 1e-6);

    // g fading stats
    let fading = g_stats.get_document("fading").unwrap();
    let fading_rate = fading.get_f64("rate").unwrap();
    let fading_red_chi2 = fading.get_f64("red_chi2").unwrap();
    let fading_dt = fading.get_f64("dt").unwrap();
    assert!((fading_rate - 0.038436).abs() < 1e-6);
    assert!((fading_red_chi2 - 1.654100).abs() < 1e-6); // decent fit
    assert!((fading_dt - 5.948322).abs() < 1e-6);

    // check the values for the r band
    assert!(photstats.contains_key("r"));
    let r_stats = photstats.get_document("r").unwrap();

    // r basic stats
    let peak_mag = r_stats.get_f64("peak_mag").unwrap();
    let peak_jd = r_stats.get_f64("peak_jd").unwrap();
    let dt = r_stats.get_f64("dt").unwrap();
    assert!((peak_mag - 14.3987).abs() < 1e-6);
    assert!((peak_jd - 2460441.922303).abs() < 1e-6);
    assert!((dt - 25.922222).abs() < 1e-6);

    // r rising stats
    let rising = r_stats.get_document("rising").unwrap();
    let rising_rate = rising.get_f64("rate").unwrap();
    let rising_red_chi2 = rising.get_f64("red_chi2").unwrap();
    let rising_dt = rising.get_f64("dt").unwrap();
    assert!((rising_rate + 0.023725).abs() < 1e-6);
    assert!((rising_red_chi2 - 70.454285).abs() < 1e-6); // bad fit
    assert!((rising_dt - 17.966065).abs() < 1e-6);

    // r fading stats
    let fading = r_stats.get_document("fading").unwrap();
    let fading_rate = fading.get_f64("rate").unwrap();
    let fading_red_chi2 = fading.get_f64("red_chi2").unwrap();
    let fading_dt = fading.get_f64("dt").unwrap();
    assert!((fading_rate - 0.063829).abs() < 1e-6);
    assert!(fading_red_chi2.is_nan()); // only 2 points after peak
    assert!((fading_dt - 7.956157).abs() < 1e-6);
}

#[tokio::test]
async fn test_filter_ztf_alert() {
    let mut alert_worker = ztf_alert_worker().await;

    let (candid, object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Ztf).get().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    // then run the enrichment worker to get the classifications
    let mut enrichment_worker = ZtfEnrichmentWorker::new(TEST_CONFIG_FILE).await.unwrap();
    let result = enrichment_worker.process_alerts(&[candid]).await;
    assert!(result.is_ok(), "Enrichment failed: {:?}", result.err());
    // the result should be a vec of String, for ZTF with the format
    // "programid,candid" which is what the filter worker expects
    let enrichment_output = result.unwrap();
    assert_eq!(enrichment_output.len(), 1);
    let candid_programid_str = &enrichment_output[0];
    assert_eq!(candid_programid_str, &format!("1,{}", candid));

    let filter_id = insert_test_filter(&Survey::Ztf, true).await.unwrap();

    let mut filter_worker = ZtfFilterWorker::new(TEST_CONFIG_FILE, Some(vec![filter_id.clone()]))
        .await
        .unwrap();
    let result = filter_worker
        .process_alerts(&[candid_programid_str.clone()])
        .await;

    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();
    assert!(result.is_ok(), "Filter failed: {:?}", result.err());

    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let alert = &alerts_output[0];
    assert_eq!(alert.candid, candid);
    assert_eq!(alert.object_id, object_id);
    assert_eq!(alert.photometry.len(), 21); // prv_candidates + prv_nondetections + fp_hists

    // let's validate that the photometry points were correctly parsed, and that the flux and flux_err values are consistent with the original values in the alert
    let fp_point = alert
        .photometry
        .iter()
        .find(|p| p.jd == 2460447.9202778 && p.origin == Origin::ForcedPhot)
        .unwrap();
    assert!(fp_point.flux.is_some());
    let flux = fp_point.flux.unwrap();
    assert!(flux < 0.0); // the first point is a negative detection, so the flux should be negative
    let flux_err = fp_point.flux_err;
    let band = &fp_point.band;
    assert_eq!(band, "ztfg");

    // we compute magpsf and magpsf_err from the flux values from the alert packet
    let magzpsci = 26.1352;
    let forcediffimflux = -11859.88;
    let forcediffimfluxunc = 25.300741;
    let (magpsf_forcediffimflux, magpsf_err) =
        flux2mag(-forcediffimflux, forcediffimfluxunc, magzpsci);

    // then we compute magpsf and magpsf_err from the flux and flux_err values
    // we compute in the alert worker (at a fixed ZP)
    let (magpsf, magpsf_err_from_flux) =
        flux2mag(-flux as f32 * 1e-9, flux_err as f32 * 1e-9, ZTF_ZP);

    // they should be consistent within a small tolerance
    assert!((magpsf_forcediffimflux - magpsf).abs() < 1e-6);
    assert!((magpsf_err - magpsf_err_from_flux).abs() < 1e-6);

    let filter_passed = alert
        .filters
        .iter()
        .find(|f| f.filter_id == filter_id)
        .unwrap();
    assert_eq!(filter_passed.annotations, "{\"mag_now\":14.91}");

    let classifications = &alert.classifications;
    assert_eq!(classifications.len(), 6);

    // verify that we can convert the alert to avro bytes
    let schema = load_alert_schema().unwrap();
    let _ = alert_to_avro_bytes(&alert, &schema).unwrap();
}
