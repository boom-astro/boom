use boom::spatial;
use boom::conf;

#[test]
fn test_great_circle_distance() {
    let ra1 = 323.233462;
    let dec1 = 14.112528;
    let ra2 = 323.233462;
    let dec2 = 14.112528;

    let dist = spatial::great_circle_distance(ra1, dec1, ra2, dec2);
    assert_eq!(dist, 0.0);

    let ra2 = 300.0;
    let dec2 = 0.0;

    let dist = spatial::great_circle_distance(ra1, dec1, ra2, dec2);
    assert_eq!(dist, 26.979186555535495);
}

#[test]
fn test_in_ellipse() {
    let ra1 = 323.233462;
    let dec1 = 14.112528;
    let ra2 = 323.233462;
    let dec2 = 14.112528;

    let dist = spatial::in_ellipse(ra1, dec1, ra2, dec2, 0.0, 0.0, 0.0);
    assert_eq!(dist, true);

    let ra2 = 300.0;
    let dec2 = 0.0;

    let dist = spatial::in_ellipse(ra1, dec1, ra2, dec2, 1.0, 0.0, 0.0);
    assert_eq!(dist, false);
}

#[tokio::test]
async fn test_xmatch() {
    let conf = conf::load_config("tests/data/config.test.yaml").unwrap();
    let db = conf::build_db(&conf, false).await;

    let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);
    assert_eq!(catalog_xmatch_configs.len(), 9);

    let ra = 323.233462;
    let dec = 14.112528;

    let xmatches = spatial::xmatch(ra, dec, &catalog_xmatch_configs, &db).await;
    assert_eq!(xmatches.len(), 9);

    // xmatch is a Vec<Vec<bson::Document>>
    let _ps1_xmatch = &xmatches.get_array("PS1_DR1").unwrap();
}