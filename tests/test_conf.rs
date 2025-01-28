use boom::conf;

#[test]
fn test_load_config() {
    let conf = conf::load_config("tests/config.test.yaml");
    assert!(conf.is_ok());

    let conf = conf.unwrap();

    let crossmatches = conf.get_table("crossmatch").unwrap();
    // check that ZTF is one of the keys
    assert!(crossmatches.get("ZTF").is_some());
    let crossmatches_ztf = crossmatches.get("ZTF").clone().cloned();
    assert!(crossmatches_ztf.is_some());
    let crossmatches_ztf = crossmatches_ztf.unwrap().clone().into_array().unwrap();
    // check that the crossmatch for ZTF is an array
    assert_eq!(crossmatches_ztf.len(), 9);
    

    let hello = conf.get_string("hello");
    assert!(hello.is_ok());

    let hello = hello.unwrap();
    assert_eq!(hello, "world");
}

#[test]
fn test_build_xmatch_configs() {
    let conf = conf::load_config("tests/config.test.yaml");

    let conf = conf.unwrap();

    let crossmatches = conf.get_table("crossmatch").unwrap();
    let crossmatches_ztf = crossmatches.get("ZTF").cloned().unwrap();
    let crossmatches_ztf = crossmatches_ztf.into_array().unwrap();
    assert!(crossmatches_ztf.len() > 0);

    let catalog_xmatch_configs = conf::build_xmatch_configs(&conf, "ZTF");

    assert_eq!(catalog_xmatch_configs.len(), 9);

    let first = &catalog_xmatch_configs[0];
    // verify that its a CatalogXmatchConfig
    assert_eq!(first.catalog, "PS1_DR1");
    assert_eq!(first.radius, 2.0 * std::f64::consts::PI / 180.0 / 3600.0);
    assert_eq!(first.use_distance, false);
    assert_eq!(first.distance_key, None);
    assert_eq!(first.distance_unit, None);
    assert_eq!(first.distance_max, None);
    assert_eq!(first.distance_max_near, None);

    let projection = &first.projection;
    // test reading a few of the expected fields
    assert_eq!(projection.get("_id").unwrap().as_i64().unwrap(), 1);
    assert_eq!(projection.get("coordinates.radec_str").unwrap().as_i64().unwrap(), 1);
    assert_eq!(projection.get("gMeanPSFMag").unwrap().as_i64().unwrap(), 1);
    assert_eq!(projection.get("gMeanPSFMagErr").unwrap().as_i64().unwrap(), 1);
}

#[tokio::test]
async fn test_build_db() {
    let conf = conf::load_config("tests/config.test.yaml");
    let conf = conf.unwrap();
    let db = conf::build_db(&conf).await;

    // try a simple query to just validate that the connection works
    let _collections = db.list_collection_names().await.unwrap();
}