use boom::conf::{self, AppConfig};
use boom::utils::testing::TEST_CONFIG_FILE;

#[test]
fn test_load_raw_config() {
    let config = conf::load_raw_config(TEST_CONFIG_FILE);
    assert!(config.is_ok());

    let config = config.unwrap();

    let hello = config.get_string("hello");
    assert!(hello.is_ok());

    let hello = hello.unwrap();
    assert_eq!(hello, "world");
}

#[tokio::test]
async fn test_build_db() {
    let config = AppConfig::from_test_config().unwrap();
    let db = conf::build_db(&config).await.unwrap();
    // try a simple query to just validate that the connection works
    let _collections = db.list_collection_names().await.unwrap();
}

#[test]
fn test_load_xmatch_configs() {
    let config = AppConfig::from_test_config().unwrap();

    let crossmatch_config_ztf = config
        .crossmatch
        .get(&boom::utils::enums::Survey::Ztf)
        .cloned()
        .unwrap_or_default();
    assert!(crossmatch_config_ztf.len() > 0);
    for crossmatch in crossmatch_config_ztf.iter() {
        assert!(crossmatch.catalog.len() > 0);
        assert!(crossmatch.radius > 0.0);
        assert!(crossmatch.projection.len() > 0);
    }

    let first = &crossmatch_config_ztf[0];
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
