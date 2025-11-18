use boom::conf::{build_db, AppConfig};
use boom::utils::spatial::{self, CrossmatchCatalog, PanSTARRS};

#[tokio::test]
async fn test_xmatch() {
    let config = AppConfig::from_test_config().unwrap();
    let db = build_db(&config).await.unwrap();

    let mut catalog_xmatch_configs = config
        .crossmatch
        .get(&boom::utils::enums::Survey::Ztf)
        .cloned()
        .unwrap();
    assert_eq!(catalog_xmatch_configs.len(), 4);

    // generate a randomized catalog name for testing
    let ps1_catalog_name = format!(
        "test_ps1_{}",
        uuid::Uuid::new_v4().to_string().replace("-", "")
    );
    let mut found = false;
    for catalog_xmatch_config in catalog_xmatch_configs.iter_mut() {
        if catalog_xmatch_config.catalog == CrossmatchCatalog::PanSTARRS {
            catalog_xmatch_config.collection_name = ps1_catalog_name.clone();
            found = true;
        }
    }
    assert!(found, "PanSTARRS catalog config not found");

    let ra = 323.233462;
    let dec = 14.112528;

    // insert a document into the test PS1 catalog
    let collection: mongodb::Collection<PanSTARRS> = db.collection(&ps1_catalog_name);
    let ps1_doc = PanSTARRS {
        id: 1,
        ra: ra,
        dec: dec,
        g_mean_psf_mag: Some(18.5),
        g_mean_psf_mag_err: Some(0.03),
        r_mean_psf_mag: Some(18.0),
        r_mean_psf_mag_err: Some(0.02),
        i_mean_psf_mag: Some(17.8),
        i_mean_psf_mag_err: Some(0.02),
        z_mean_psf_mag: Some(17.5),
        z_mean_psf_mag_err: Some(0.02),
        y_mean_psf_mag: Some(17.3),
        y_mean_psf_mag_err: Some(0.02),
    };
    let insert_result = collection.insert_one(ps1_doc).await.unwrap();
    assert!(insert_result.inserted_id.as_i64().is_some());

    let xmatches = spatial::xmatch(ra, dec, &catalog_xmatch_configs, &db)
        .await
        .unwrap();

    // delete the test PS1 catalog
    db.collection::<mongodb::bson::Document>(&ps1_catalog_name)
        .drop()
        .await
        .unwrap();
    // print the xmatches for debugging
    println!("xmatches: {:?}", xmatches);

    let ps1_xmatches = xmatches.panstarrs.unwrap();
    assert_eq!(ps1_xmatches.len(), 1);
    let ps1_match = &ps1_xmatches[0];
    assert_eq!(ps1_match.panstarrs.id, 1);
    assert!((ps1_match.distance_arcsec - 0.0).abs() < 1e-6);
}
