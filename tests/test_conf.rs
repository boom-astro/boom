use boom::conf;
use boom::utils::testing::TEST_CONFIG_FILE;

#[test]
fn test_load_config() {
    let config = conf::load_config(TEST_CONFIG_FILE);
    assert!(config.is_ok());

    let config = config.unwrap();

    let crossmatches = config.get_table("crossmatch").unwrap();
    // check that ZTF is one of the keys
    assert!(crossmatches.get("ZTF").is_some());
    let crossmatches_ztf = crossmatches.get("ZTF").clone().cloned();
    assert!(crossmatches_ztf.is_some());
    let crossmatches_ztf = crossmatches_ztf.unwrap().clone().into_array().unwrap();
    // check that the crossmatch for ZTF is an array
    assert_eq!(crossmatches_ztf.len(), 4);

    let hello = config.get_string("hello");
    assert!(hello.is_ok());

    let hello = hello.unwrap();
    assert_eq!(hello, "world");
}

#[test]
fn test_build_xmatch_configs() {
    let config = conf::load_config(TEST_CONFIG_FILE).unwrap();

    let crossmatches = config.get_table("crossmatch").unwrap();
    let crossmatches_ztf = crossmatches.get("ZTF").cloned().unwrap();
    let crossmatches_ztf = crossmatches_ztf.into_array().unwrap();
    assert!(crossmatches_ztf.len() > 0);

    let catalog_xmatch_configs = conf::build_xmatch_configs(&config, "ZTF").unwrap();

    assert_eq!(catalog_xmatch_configs.len(), 4);

    let first = &catalog_xmatch_configs[0];
    // verify that its a CatalogXmatchConfig
    assert_eq!(first.catalog, "PS1_DR1");
    assert_eq!(first.radius, 2.0 * std::f64::consts::PI / 180.0 / 3600.0);
    assert_eq!(first.use_distance, false);
    assert_eq!(first.distance_key, None);
    assert_eq!(first.distance_max, None);
    assert_eq!(first.distance_max_near, None);

    let projection = &first.projection;
    // test reading a few of the expected fields
    assert_eq!(projection.get("_id").unwrap().as_i64().unwrap(), 1);
    assert_eq!(projection.get("gMeanPSFMag").unwrap().as_i64().unwrap(), 1);
    assert_eq!(
        projection.get("gMeanPSFMagErr").unwrap().as_i64().unwrap(),
        1
    );
}

#[tokio::test]
async fn test_build_db() {
    let config = conf::load_config(TEST_CONFIG_FILE).unwrap();
    let db = conf::build_db(&config).await.unwrap();

    // try a simple query to just validate that the connection works
    let _collections = db.list_collection_names().await.unwrap();
}

#[test]
fn test_catalogxmatchconfig() {
    let ps1_projection = mongodb::bson::doc! {
        "_id": 1,
        "gMeanPSFMag": 1,
        "gMeanPSFMagErr": 1
    };
    let xmatch_config = conf::CatalogXmatchConfig {
        catalog: "PS1_DR1".to_string(),
        radius: 2.0 * std::f64::consts::PI / 180.0 / 3600.0,
        use_distance: false,
        distance_key: None,
        distance_max: None,
        distance_max_near: None,
        projection: ps1_projection.clone(),
        n_max: None,
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
    assert_eq!(xmatch_config.n_max, None);

    let projection = xmatch_config.projection;
    assert_eq!(projection, ps1_projection);

    // validate the from_config method
    let config = conf::load_config(TEST_CONFIG_FILE).unwrap();
    let crossmatches = config.get_table("crossmatch").unwrap();
    let crossmatches_ztf = crossmatches.get("ZTF").cloned().unwrap();
    let crossmatches_ztf = crossmatches_ztf.into_array().unwrap();
    assert!(crossmatches_ztf.len() > 0);

    for crossmatch in crossmatches_ztf {
        let catalog_xmatch_config = conf::CatalogXmatchConfig::from_config(crossmatch).unwrap();
        assert!(catalog_xmatch_config.catalog.len() > 0);
        assert!(catalog_xmatch_config.radius > 0.0);
        assert!(catalog_xmatch_config.projection.len() > 0);
    }
}
