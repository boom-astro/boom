use boom::conf::AppConfig;
use boom::utils::spatial;

#[tokio::test]
async fn test_xmatch() {
    let config = AppConfig::from_test_config().unwrap();
    let db = config.build_db().await.unwrap();

    let catalog_xmatch_configs = config
        .crossmatch
        .get(&boom::utils::enums::Survey::Ztf)
        .cloned()
        .unwrap();
    assert_eq!(catalog_xmatch_configs.len(), 4);

    let ra = 323.233462;
    let dec = 14.112528;

    let xmatches = spatial::xmatch(ra, dec, &catalog_xmatch_configs, &db)
        .await
        .unwrap();
    assert_eq!(xmatches.len(), 4);

    let _ps1_xmatch = xmatches.get("PS1_DR1").unwrap();
}
