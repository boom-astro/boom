use boom::conf;
use boom::types;

#[test]
fn test_avro_to_alert() {
    let file_name = "tests/data/alerts/ztf/2695378462115010012.avro";
    let bytes_content = std::fs::read(file_name).unwrap();
    let alert = types::Alert::from_avro_bytes(bytes_content);
    assert!(alert.is_ok());
}

#[test]
fn test_avro_to_alert_unsafe() {
    let schema = types::ztf_alert_schema().unwrap();
    let file_name = "tests/data/alerts/ztf/2695378462115010012.avro";
    let bytes_content = std::fs::read(file_name).unwrap();
    let alert = types::Alert::from_avro_bytes_unsafe(bytes_content, &schema);
    assert!(alert.is_ok());
}

#[test]
fn test_alert() {
    let file_name = "tests/data/alerts/ztf/2695378462115010012.avro";
    let bytes_content = std::fs::read(file_name).unwrap();
    let alert = types::Alert::from_avro_bytes(bytes_content);
    assert!(alert.is_ok());

    // validate the alert
    let alert = alert.unwrap();
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
    assert!(detection.isdiffpos.is_some());
    assert_eq!(detection.isdiffpos.as_ref().unwrap(), "t");

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

    // split alert from its history
    let (alert_no_history, prv_candidates, fp_hist) = alert.pop_history();

    // validate the alert_no_history
    assert_eq!(alert_no_history.schemavsn, "4.02");
    assert_eq!(alert_no_history.publisher, "ZTF (www.ztf.caltech.edu)");
    assert_eq!(alert_no_history.object_id, "ZTF18abudxnw");
    assert_eq!(alert_no_history.candid, 2695378462115010012);
    assert_eq!(alert_no_history.candidate.ra, 295.3031995);
    assert_eq!(alert_no_history.candidate.dec, -10.3958989);

    // validate the prv_candidates
    assert!(!prv_candidates.is_none());
    assert_eq!(prv_candidates.clone().unwrap().len(), 10);

    // validate the fp_hist
    assert!(!fp_hist.is_none());
    assert_eq!(fp_hist.clone().unwrap().len(), 10);

    // validate the conversion to bson
    let alert_doc = alert_no_history.mongify();
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
        .map(|x| x.mongify())
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
        .map(|x| x.mongify())
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

#[test]
fn test_catalogxmatchconfig() {
    let ps1_projection = mongodb::bson::doc! {
        "_id": 1,
        "coordinates.radec_str": 1,
        "gMeanPSFMag": 1,
        "gMeanPSFMagErr": 1
    };
    let xmatch_config = types::CatalogXmatchConfig {
        catalog: "PS1_DR1".to_string(),
        radius: 2.0 * std::f64::consts::PI / 180.0 / 3600.0,
        use_distance: false,
        distance_key: None,
        distance_max: None,
        distance_max_near: None,
        projection: ps1_projection.clone(),
    };

    assert_eq!(xmatch_config.catalog, "PS1_DR1");
    assert_eq!(
        xmatch_config.radius,
        2.0 * std::f64::consts::PI / 180.0 / 3600.0
    );
    assert_eq!(xmatch_config.use_distance, false);
    assert_eq!(xmatch_config.distance_key, None);
    assert_eq!(xmatch_config.distance_max, None);
    assert_eq!(xmatch_config.distance_max_near, None);

    let projection = xmatch_config.projection;
    assert_eq!(projection, ps1_projection);

    // validate the from_config method
    let config = conf::load_config("tests/config.test.yaml").unwrap();
    let crossmatches = config.get_table("crossmatch").unwrap();
    let crossmatches_ztf = crossmatches.get("ZTF").cloned().unwrap();
    let crossmatches_ztf = crossmatches_ztf.into_array().unwrap();
    assert!(crossmatches_ztf.len() > 0);

    for crossmatch in crossmatches_ztf {
        let catalog_xmatch_config = types::CatalogXmatchConfig::from_config(crossmatch);
        assert!(catalog_xmatch_config.catalog.len() > 0);
        assert!(catalog_xmatch_config.radius > 0.0);
        assert!(catalog_xmatch_config.projection.len() > 0);
    }
}
