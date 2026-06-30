use boom::conf::{AppConfig, CatalogXmatchConfig};
use boom::utils::enums::Survey;
use boom::utils::spatial;
use mongodb::bson::{doc, Document};

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

    let xmatches = spatial::xmatch(
        ra,
        dec,
        "ZTF18aaayemv",
        &boom::utils::enums::Survey::Ztf,
        &catalog_xmatch_configs,
        &db,
    )
    .await
    .unwrap();
    assert_eq!(xmatches.len(), 4);

    let _ps1_xmatch = xmatches.get("PS1_DR1").unwrap();
}

/// A watchlist catalog (name prefixed with `watchlist_`) must never surface
/// under the alert's `cross_matches`: positional matches are recorded on the
/// watchlist document itself, under `matching_<survey>_objects`. This is
/// verified for both ZTF and LSST.
#[tokio::test]
async fn test_xmatch_watchlist_excluded_from_cross_matches() {
    let config = AppConfig::from_test_config().unwrap();
    let db = config.build_db().await.unwrap();

    for survey in [Survey::Ztf, Survey::Lsst] {
        let watchlist_name = format!("watchlist_test_spatial_{}", survey).to_lowercase();
        let watchlist: mongodb::Collection<Document> = db.collection(&watchlist_name);
        // Start from a clean slate in case a previous run leaked state.
        watchlist.drop().await.unwrap();

        let ra = 323.233462;
        let dec = 14.112528;
        let object_id = "OBJ_watchlist_test";
        let entry_id = "wl_entry_1";

        // A single watchlist entry sitting right on the alert position. The
        // top-level `ra`/`dec` are required for the (non-distance) crossmatch
        // path to keep the match.
        watchlist
            .insert_one(doc! {
                "_id": entry_id,
                "coordinates": {
                    "radec_geojson": {
                        "type": "Point",
                        "coordinates": [ra - 180.0, dec],
                    }
                },
                "ra": ra,
                "dec": dec,
            })
            .await
            .unwrap();

        // 2 arcsec radius, well within the ~0 arcsec separation above.
        let watchlist_config = CatalogXmatchConfig::new(
            &watchlist_name,
            2.0,
            doc! { "_id": 1, "ra": 1, "dec": 1 },
            false,
            None,
            None,
            None,
            None,
        );

        let xmatches = spatial::xmatch(ra, dec, object_id, &survey, &[watchlist_config], &db)
            .await
            .unwrap();

        // The watchlist is NOT exposed in the returned cross_matches map.
        assert!(
            !xmatches.contains_key(&watchlist_name),
            "watchlist `{}` must not appear in cross_matches",
            watchlist_name
        );

        // Instead, the alert object_id was recorded on the matched watchlist
        // document, under the per-survey field.
        let field = spatial::watchlist_match_field(&survey);
        let entry = watchlist
            .find_one(doc! { "_id": entry_id })
            .await
            .unwrap()
            .unwrap();
        let matched: Vec<&str> = entry
            .get_array(&field)
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap())
            .collect();
        assert_eq!(
            matched,
            vec![object_id],
            "watchlist entry should record the alert object_id under `{}`",
            field
        );

        watchlist.drop().await.unwrap();
    }
}
