use boom::{
    alert::{AlertWorker, ZtfAlertWorker},
    utils::db::mongify,
};

const CONFIG_FILE: &str = "tests/config.test.yaml";

#[tokio::test]
async fn test_alert_from_avro_bytes() {
    let mut alert_worker = ZtfAlertWorker::new(CONFIG_FILE).await.unwrap();

    let file_name = "tests/data/alerts/ztf/2695378462115010012.avro";
    let bytes_content = std::fs::read(file_name).unwrap();
    let alert = alert_worker.alert_from_avro_bytes(&bytes_content).await;
    assert!(alert.is_ok());

    // validate the alert
    let mut alert = alert.unwrap();
    assert_eq!(alert.schemavsn, "4.02");
    assert_eq!(alert.publisher, "ZTF (www.ztf.caltech.edu)");
    assert_eq!(alert.object_id, "ZTF18abudxnw");
    assert_eq!(alert.candid, 2695378462115010012);

    // validate the candidate
    let candidate = alert.clone().candidate;
    assert_eq!(candidate.ra, 295.3031995);
    assert_eq!(candidate.dec, -10.3958989);

    // validate the prv_candidates
    let prv_candidates = alert.clone().prv_candidates;
    assert!(!prv_candidates.is_none());

    let prv_candidates = prv_candidates.unwrap();
    assert_eq!(prv_candidates.len(), 10);

    let non_detection = prv_candidates.get(0).unwrap();
    assert_eq!(non_detection.magpsf.is_none(), true);
    assert_eq!(!non_detection.diffmaglim.is_none(), true);

    let detection = prv_candidates.get(1).unwrap();
    assert_eq!(detection.magpsf.is_some(), true);
    assert_eq!(detection.sigmapsf.is_some(), true);
    assert_eq!(detection.diffmaglim.is_some(), true);
    assert_eq!(detection.isdiffpos.is_some(), true);

    // validate the fp_hists
    let fp_hists = alert.clone().fp_hists;
    assert!(!fp_hists.is_none());

    let fp_hists = fp_hists.unwrap();
    assert_eq!(fp_hists.len(), 10);

    let fp_negative_flux = fp_hists.get(0).unwrap();
    assert_eq!(fp_negative_flux.forcediffimflux.is_some(), true);
    assert_eq!(fp_negative_flux.forcediffimflux.unwrap(), -11859.88);
    assert_eq!(fp_negative_flux.forcediffimfluxunc.is_some(), true);
    assert_eq!(fp_negative_flux.forcediffimfluxunc.unwrap(), 25.300741);
    assert_eq!(fp_negative_flux.procstatus.is_some(), true);
    assert_eq!(fp_negative_flux.procstatus.as_ref().unwrap(), "0");

    let fp_positive_flux = fp_hists.get(9).unwrap();
    assert_eq!(fp_positive_flux.forcediffimflux.is_some(), true);
    assert_eq!(fp_positive_flux.forcediffimflux.unwrap(), 138.203);
    assert_eq!(fp_positive_flux.forcediffimfluxunc.is_some(), true);
    assert_eq!(fp_positive_flux.forcediffimfluxunc.unwrap(), 46.038883);
    assert_eq!(fp_positive_flux.procstatus.is_some(), true);
    assert_eq!(fp_positive_flux.procstatus.as_ref().unwrap(), "0");

    // validate the cutouts
    assert_eq!(alert.cutout_science.clone().unwrap().len(), 13107);
    assert_eq!(alert.cutout_template.clone().unwrap().len(), 12410);
    assert_eq!(alert.cutout_difference.clone().unwrap().len(), 14878);

    let prv_candidates = alert.prv_candidates.take();
    let fp_hist = alert.fp_hists.take();

    // validate the prv_candidates
    assert!(!prv_candidates.is_none());
    assert_eq!(prv_candidates.clone().unwrap().len(), 10);

    // validate the fp_hist
    assert!(!fp_hist.is_none());
    assert_eq!(fp_hist.clone().unwrap().len(), 10);

    // validate the conversion to bson
    let alert_doc = mongify(&alert);
    assert_eq!(alert_doc.get_str("schemavsn").unwrap(), "4.02");
    assert_eq!(
        alert_doc.get_str("publisher").unwrap(),
        "ZTF (www.ztf.caltech.edu)"
    );
    assert_eq!(alert_doc.get_str("objectId").unwrap(), "ZTF18abudxnw");
    assert_eq!(alert_doc.get_i64("candid").unwrap(), 2695378462115010012);
    assert_eq!(
        alert_doc
            .get_document("candidate")
            .unwrap()
            .get_f64("ra")
            .unwrap(),
        295.3031995
    );
    assert_eq!(
        alert_doc
            .get_document("candidate")
            .unwrap()
            .get_f64("dec")
            .unwrap(),
        -10.3958989
    );

    // validate the conversion to bson for prv_candidates
    let prv_candidates_doc = prv_candidates
        .unwrap()
        .into_iter()
        .map(|x| mongify(&x))
        .collect::<Vec<_>>();
    assert_eq!(prv_candidates_doc.len(), 10);

    let non_detection = prv_candidates_doc.get(0).unwrap();
    assert!(!non_detection.get_f64("magpsf").is_ok());

    let detection = prv_candidates_doc.get(1).unwrap();
    assert_eq!(detection.get_f64("magpsf").unwrap(), 16.800199508666992);

    // validate the conversion to bson for fp_hist
    let fp_hist_doc = fp_hist
        .unwrap()
        .into_iter()
        .map(|x| mongify(&x))
        .collect::<Vec<_>>();
    assert_eq!(fp_hist_doc.len(), 10);

    let fp_negative_flux = fp_hist_doc.get(0).unwrap();
    assert_eq!(
        fp_negative_flux.get_f64("forcediffimflux").unwrap(),
        -11859.8798828125
    );

    let fp_positive_flux = fp_hist_doc.get(9).unwrap();
    assert_eq!(
        fp_positive_flux.get_f64("forcediffimflux").unwrap(),
        138.2030029296875
    );
}
