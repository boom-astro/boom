use futures::stream::StreamExt;
use boom::conf;

#[test]
fn test_load_config() {
    let conf = conf::load_config("tests/data/config.test.yaml");
    assert!(conf.is_ok());

    let conf = conf.unwrap();

    let crossmatches = conf.get_array("crossmatch").unwrap();
    assert_eq!(crossmatches.len(), 9);

    let hello = conf.get_string("hello");
    assert!(hello.is_ok());

    let hello = hello.unwrap();
    assert_eq!(hello, "world");
}

#[test]
fn test_build_xmatch_configs() {
    let conf = conf::load_config("tests/data/config.test.yaml");

    let conf = conf.unwrap();

    let crossmatches = conf.get_array("crossmatch").unwrap();
    assert_eq!(crossmatches.len(), 9);

    let catalog_xmatch_configs = conf::build_xmatch_configs(&conf);

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
    let conf = conf::load_config("tests/data/config.test.yaml");
    let conf = conf.unwrap();
    let initialize = true;
    let db = conf::build_db(&conf, initialize).await;

    let collections = db.list_collection_names().await.unwrap();

    // we've set initialized to true, so the database should have:
    // - alerts collection
    // - a unique descending index on the "candid" field in the alerts collection

    assert!(collections.len() > 0);
    assert!(collections.contains(&"alerts".to_string()));

    let collection: mongodb::Collection<mongodb::bson::Document> = db.collection("alerts");
    let mut cursor = collection.list_indexes().await.unwrap();
    let mut found = false;
    while let Some(index) = cursor.next().await {
        println!("{:?}", index);
        let index_model = index.unwrap();
        let keys = index_model.keys;
        if keys.get("candid").is_some() {
            found = true;
            let options = index_model.options.unwrap();
            assert_eq!(options.unique, Some(true));
            assert_eq!(options.name, Some("candid_1".to_string()));
        }
    }
    assert!(found);
}