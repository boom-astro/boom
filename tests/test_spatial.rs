use boom::conf;
use boom::utils::spatial;
use boom::utils::testing::TEST_CONFIG_FILE;

#[tokio::test]
async fn test_xmatch() {
    let config = conf::load_config(TEST_CONFIG_FILE).unwrap();
    let db = conf::build_db(&config).await.unwrap();

    let catalog_xmatch_configs = conf::build_xmatch_configs(&config, "ZTF").unwrap();
    assert_eq!(catalog_xmatch_configs.len(), 4);

    let ra = 323.233462;
    let dec = 14.112528;

    let xmatches = spatial::xmatch(ra, dec, &catalog_xmatch_configs, &db).await;
    assert_eq!(xmatches.len(), 4);

    // xmatch is a Vec<Vec<bson::Document>>
    let _ps1_xmatch = &xmatches.get_array("PS1_DR1").unwrap();
}
