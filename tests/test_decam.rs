#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
use boom::{
    alert::{AlertWorker, ProcessAlertStatus, DECAM_LSST_XMATCH_RADIUS, DECAM_ZTF_XMATCH_RADIUS},
    conf::{get_test_cutout_storage, get_test_db},
    enrichment::{EnrichmentWorker, ZtfEnrichmentWorker},
    filter::{
        alert_to_avro_bytes, load_alert_schema, DecamFilterWorker, FilterWorker, LsstFilterWorker,
        ZtfFilterWorker,
    },
    utils::{
        enums::Survey,
        testing::{
            decam_alert_worker, drop_alert_from_collections, insert_custom_test_filter,
            insert_test_filter, lsst_alert_worker, remove_test_filter, ztf_alert_worker,
            AlertRandomizer, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::{doc, Bson, Document};

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
    // the 4 prvCandidates of the packet plus the detection itself
    let prv_candidates = aux.get_array("prv_candidates").unwrap();
    assert_eq!(prv_candidates.len(), 5);
    let fp_hists = aux.get_array("fp_hists").unwrap();
    assert!(fp_hists.is_empty());

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
    assert_eq!(alert.photometry.len(), 5);

    let filter_passed = alert
        .filters
        .iter()
        .find(|f| f.filter_id == filter_id)
        .unwrap();
    assert_eq!(filter_passed.annotations, "{\"mag_now\":20.71}");

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

#[tokio::test]
async fn test_filter_decam_alert_with_ztf_and_lsst_matches() {
    // Inside both the ZTF and the LSST dec ranges, so both matches are attempted.
    let (candid, object_id, ra, dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Decam)
            .dec(0.0)
            .get()
            .await;

    let mut ztf_worker = ztf_alert_worker().await;
    let (ztf_candid, ztf_object_id, _, _, ztf_bytes_content) =
        AlertRandomizer::new_randomized(Survey::Ztf)
            .ra(ra)
            .dec(dec + 0.9 * DECAM_ZTF_XMATCH_RADIUS.to_degrees())
            .get()
            .await;
    ztf_worker.process_alert(&ztf_bytes_content).await.unwrap();

    let mut lsst_worker = lsst_alert_worker().await;
    let (lsst_candid, lsst_object_id, _, _, lsst_bytes_content) =
        AlertRandomizer::new_randomized(Survey::Lsst)
            .ra(ra)
            .dec(dec - 0.9 * DECAM_LSST_XMATCH_RADIUS.to_degrees())
            .get()
            .await;
    lsst_worker
        .process_alert(&lsst_bytes_content)
        .await
        .unwrap();

    let mut alert_worker = decam_alert_worker().await;
    let status = alert_worker.process_alert(&bytes_content).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    let filter_id = insert_custom_test_filter(
        &Survey::Decam,
        r#"[{"$match": {"aliases.ZTF.0": {"$exists": true}}}, {"$project": {"objectId": 1, "annotations.n_ztf": {"$size": "$ZTF.prv_candidates"}, "annotations.n_lsst": {"$size": "$LSST.prv_candidates"}}}]"#,
    )
    .await
    .unwrap();
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
    assert_eq!(&alert.object_id, &object_id);
    // The ZTF test alerts are older than the 365-day window filters read ZTF history over.
    assert_eq!(alert.filters[0].annotations, "{\"n_ztf\":0,\"n_lsst\":1}");

    let ztf_match = alert
        .survey_matches
        .ztf
        .as_ref()
        .expect("survey_matches.ztf should be Some when a ZTF alias exists");
    assert_eq!(ztf_match.object_id, ztf_object_id);
    // ZTF test data has 8 prv_candidates + 3 prv_nondetections + 10 fp_hists = 21 photometry points.
    assert_eq!(ztf_match.photometry.len(), 21);
    assert!(ztf_match.photometry.iter().all(|p| p.survey == Survey::Ztf));

    let lsst_match = alert
        .survey_matches
        .lsst
        .as_ref()
        .expect("survey_matches.lsst should be Some when an LSST alias exists");
    assert_eq!(lsst_match.object_id, lsst_object_id);
    // LSST test data has 1 prv_candidate and 0 fp_hists.
    assert_eq!(lsst_match.photometry.len(), 1);

    assert!(alert.survey_matches.decam.is_none());

    let schema = load_alert_schema().unwrap();
    let _ = alert_to_avro_bytes(&alert, &schema).unwrap();

    drop_alert_from_collections(candid, &Survey::Decam)
        .await
        .unwrap();
    drop_alert_from_collections(ztf_candid, &Survey::Ztf)
        .await
        .unwrap();
    drop_alert_from_collections(lsst_candid, &Survey::Lsst)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_ztf_and_lsst_alerts_carry_their_decam_match() {
    let (decam_candid, decam_object_id, ra, dec, decam_bytes_content) =
        AlertRandomizer::new_randomized(Survey::Decam)
            .dec(0.0)
            .get()
            .await;

    // The ZTF and LSST alerts come first, so their DECam alias can only come
    // from the DECam alert.
    let mut ztf_worker = ztf_alert_worker().await;
    let (ztf_candid, ztf_object_id, _, _, ztf_bytes_content) =
        AlertRandomizer::new_randomized(Survey::Ztf)
            .ra(ra)
            .dec(dec + 0.9 * DECAM_ZTF_XMATCH_RADIUS.to_degrees())
            .get()
            .await;
    ztf_worker.process_alert(&ztf_bytes_content).await.unwrap();

    let mut lsst_worker = lsst_alert_worker().await;
    let (lsst_candid, lsst_object_id, _, _, lsst_bytes_content) =
        AlertRandomizer::new_randomized(Survey::Lsst)
            .ra(ra)
            .dec(dec - 0.9 * DECAM_LSST_XMATCH_RADIUS.to_degrees())
            .get()
            .await;
    lsst_worker
        .process_alert(&lsst_bytes_content)
        .await
        .unwrap();

    let mut decam_worker = decam_alert_worker().await;
    decam_worker
        .process_alert(&decam_bytes_content)
        .await
        .unwrap();

    let db = get_test_db().await;
    for (collection, object_id) in [
        ("ZTF_alerts_aux", &ztf_object_id),
        ("LSST_alerts_aux", &lsst_object_id),
    ] {
        let aux = db
            .collection::<Document>(collection)
            .find_one(doc! {"_id": object_id})
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            aux.get_document("aliases")
                .unwrap()
                .get_array("DECAM")
                .unwrap(),
            &vec![Bson::String(decam_object_id.clone())],
            "{collection} was not linked to the DECam object"
        );
    }

    let mut enrichment_worker = ZtfEnrichmentWorker::new(TEST_CONFIG_FILE, None)
        .await
        .unwrap();
    let enrichment_output = enrichment_worker
        .process_alerts(&[ztf_candid])
        .await
        .unwrap();
    let filter_id = insert_test_filter(&Survey::Ztf, true).await.unwrap();
    let mut filter_worker = ZtfFilterWorker::new(TEST_CONFIG_FILE, Some(vec![filter_id.clone()]))
        .await
        .unwrap();
    let result = filter_worker.process_alerts(&enrichment_output).await;
    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();
    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let decam_match = alerts_output[0]
        .survey_matches
        .decam
        .as_ref()
        .expect("ZTF survey_matches.decam should be Some when a DECam alias exists");
    assert_eq!(decam_match.object_id, decam_object_id);
    assert_eq!(decam_match.photometry.len(), 5);
    assert!(decam_match
        .photometry
        .iter()
        .all(|p| p.survey == Survey::Decam));

    let filter_id = insert_test_filter(&Survey::Lsst, true).await.unwrap();
    let mut filter_worker = LsstFilterWorker::new(TEST_CONFIG_FILE, Some(vec![filter_id.clone()]))
        .await
        .unwrap();
    let result = filter_worker
        .process_alerts(&[format!("{}", lsst_candid)])
        .await;
    remove_test_filter(&filter_id, &Survey::Lsst).await.unwrap();
    let alerts_output = result.unwrap();
    assert_eq!(alerts_output.len(), 1);
    let decam_match = alerts_output[0]
        .survey_matches
        .decam
        .as_ref()
        .expect("LSST survey_matches.decam should be Some when a DECam alias exists");
    assert_eq!(decam_match.object_id, decam_object_id);
    assert_eq!(decam_match.photometry.len(), 5);

    let schema = load_alert_schema().unwrap();
    let _ = alert_to_avro_bytes(&alerts_output[0], &schema).unwrap();

    drop_alert_from_collections(decam_candid, &Survey::Decam)
        .await
        .unwrap();
    drop_alert_from_collections(ztf_candid, &Survey::Ztf)
        .await
        .unwrap();
    drop_alert_from_collections(lsst_candid, &Survey::Lsst)
        .await
        .unwrap();
}
