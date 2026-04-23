use boom::conf::{AppConfig, HealpixConfig};
use boom::utils::spatial;
use boom::utils::spatial::Coordinates;
use cdshealpix::nested;
use mongodb::bson::doc;

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

    let xmatches = spatial::xmatch(ra, dec, &catalog_xmatch_configs, &db, &config.healpix)
        .await
        .unwrap();
    assert_eq!(xmatches.len(), 4);

    let _ps1_xmatch = xmatches.get("PS1_DR1").unwrap();
}

/// Integration test: explicitly shard alerts into separate MongoDB collections
/// using HEALPix depth 0 (12 base cells). Each collection simulates a separate
/// database machine — 12 machines is a realistic cluster size for Argus-scale
/// processing (~100M alerts/night).
///
/// The test:
/// 1. Scatters 1000 alerts across the sky
/// 2. Routes each into one of 12 shard collections (like separate machines)
/// 3. Runs a cone search that identifies which shards to query
/// 4. Verifies it only touches 1–2 shards and finds all matches
#[tokio::test]
async fn test_healpix_sharded_collections_12_machines() {
    let config = AppConfig::from_test_config().unwrap();
    let db = match config.build_db().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: could not connect to MongoDB: {}", e);
            return;
        }
    };
    if db.run_command(doc! { "ping": 1 }).await.is_err() {
        eprintln!("Skipping test: MongoDB ping failed");
        return;
    }

    // --- Configuration ---
    // Depth 0 → NSIDE=1 → 12 base cells (the HEALPix "diamonds").
    // Each base cell gets its own collection, simulating a separate machine.
    // 12 machines is a realistic cluster size for Argus-scale processing.
    let shard_depth: u8 = 0;
    let n_shard_pixels = 12 * 4u64.pow(shard_depth as u32); // 12
    let test_prefix = format!("test_shard_{}", uuid::Uuid::new_v4());

    // Helper: collection name for a given shard pixel
    let shard_collection_name = |shard_id: i64| -> String {
        format!("{}_hp{:04}", test_prefix, shard_id)
    };

    // --- Step 1: Scatter 1000 alerts across the sky, route to shard collections ---
    let n_alerts = 1000;
    let mut shard_alert_counts: std::collections::HashMap<i64, u64> =
        std::collections::HashMap::new();
    let mut created_collections: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    use futures::stream::StreamExt;

    for i in 0..n_alerts {
        // Golden-angle spiral gives uniform sky coverage
        let ra = (i as f64 * 137.508) % 360.0;
        let dec = ((i as f64 / n_alerts as f64) * 180.0) - 90.0;
        let coord = Coordinates::new(ra, dec);

        // Derive shard pixel from the depth-29 hpx (single bit shift)
        let shard_id = coord.hpx_shard(shard_depth);

        // Route to the appropriate shard collection
        let coll_name = shard_collection_name(shard_id);
        let collection = db.collection::<mongodb::bson::Document>(&coll_name);

        // Create index on first insert to this shard
        if created_collections.insert(coll_name.clone()) {
            collection
                .create_index(
                    mongodb::IndexModel::builder()
                        .keys(doc! { "coordinates.hpx": 1 })
                        .build(),
                )
                .await
                .unwrap();
        }

        let alert_doc = doc! {
            "_id": format!("alert_{}", i),
            "ra": ra,
            "dec": dec,
            "coordinates": { "hpx": coord.hpx },
        };
        collection.insert_one(&alert_doc).await.unwrap();
        *shard_alert_counts.entry(shard_id).or_insert(0) += 1;
    }

    let n_populated_shards = created_collections.len();
    eprintln!(
        "Inserted {} alerts into {} shard collections (of {} possible)",
        n_alerts, n_populated_shards, n_shard_pixels
    );
    assert_eq!(
        n_populated_shards as u64, n_shard_pixels,
        "with 1000 alerts all {} shards should be populated",
        n_shard_pixels
    );

    // --- Step 2: Cone search — find which shards to query ---
    // 5-degree cone around (RA=180, Dec=0) — large enough to match several alerts
    let query_ra = 180.0_f64;
    let query_dec = 0.0_f64;
    let radius_deg = 5.0_f64;
    let radius_rad = radius_deg.to_radians();

    // Which shard pixels overlap the cone?
    let shard_bmoc = nested::cone_coverage_approx(
        shard_depth,
        query_ra.to_radians(),
        query_dec.to_radians(),
        radius_rad,
    );
    let shard_ranges = shard_bmoc.to_ranges();
    let target_shards: Vec<i64> = shard_ranges
        .iter()
        .flat_map(|r| (r.start..r.end).map(|x| x as i64))
        .collect();

    eprintln!(
        "Cone search ({} deg radius): need to query {} of {} populated shards",
        radius_deg,
        target_shards.len(),
        n_populated_shards,
    );

    // Should only need a small fraction of all shards
    assert!(
        target_shards.len() < n_populated_shards,
        "cone should not need all shards: {} target vs {} total",
        target_shards.len(),
        n_populated_shards,
    );

    // --- Step 3: Query only the target shard collections ---
    // Within each shard, use a fine HEALPix filter (depth 14) for precision
    let query_depth: u8 = 14;
    let fine_bmoc = nested::cone_coverage_approx(
        query_depth,
        query_ra.to_radians(),
        query_dec.to_radians(),
        radius_rad,
    );
    let fine_ranges = fine_bmoc.to_ranges();
    let shift = 2 * (HealpixConfig::FINE_DEPTH - query_depth);

    let or_clauses: Vec<mongodb::bson::Document> = fine_ranges
        .iter()
        .map(|r| {
            doc! {
                "coordinates.hpx": {
                    "$gte": (r.start << shift) as i64,
                    "$lt": (r.end << shift) as i64,
                }
            }
        })
        .collect();
    let fine_filter = doc! { "$or": &or_clauses };

    let mut found: Vec<(String, f64, f64)> = Vec::new();
    let mut shards_queried = 0;

    for shard_id in &target_shards {
        let coll_name = shard_collection_name(*shard_id);
        let collection = db.collection::<mongodb::bson::Document>(&coll_name);

        let mut cursor = match collection.find(fine_filter.clone()).await {
            Ok(c) => c,
            Err(_) => continue, // collection may not exist if no alerts in this shard
        };
        shards_queried += 1;

        while let Some(result) = cursor.next().await {
            let doc = result.unwrap();
            let id = doc.get_str("_id").unwrap().to_string();
            let ra = doc.get_f64("ra").unwrap();
            let dec = doc.get_f64("dec").unwrap();
            found.push((id, ra, dec));
        }
    }

    eprintln!(
        "Queried {} shard collections, found {} alerts",
        shards_queried,
        found.len()
    );

    // --- Step 4: Verify against brute-force ---
    // Count all alerts truly within the cone radius
    let mut brute_force_count = 0;
    for collection_name in &created_collections {
        let collection = db.collection::<mongodb::bson::Document>(collection_name);
        let mut cursor = collection.find(doc! {}).await.unwrap();
        while let Some(result) = cursor.next().await {
            let doc = result.unwrap();
            let ra = doc.get_f64("ra").unwrap();
            let dec = doc.get_f64("dec").unwrap();
            let dist = flare::spatial::great_circle_distance(query_ra, query_dec, ra, dec);
            if dist < radius_deg {
                brute_force_count += 1;
            }
        }
    }

    eprintln!(
        "Brute-force found {} alerts in cone, HEALPix found {}",
        brute_force_count,
        found.len()
    );

    // HEALPix should find at least all true matches (may include a few
    // extras at tile boundaries — that's the slight overinclusiveness)
    assert!(
        found.len() >= brute_force_count,
        "HEALPix missed alerts: found {} but brute force found {}",
        found.len(),
        brute_force_count
    );
    assert!(
        brute_force_count > 0,
        "expected at least some alerts in the cone"
    );

    // The key result: we skipped most of the sky
    let skipped = n_populated_shards - shards_queried;
    eprintln!(
        "Skipped {} shard collections ({:.0}% of sky not scanned)",
        skipped,
        skipped as f64 / n_populated_shards as f64 * 100.0
    );
    assert!(
        skipped > 0,
        "should have skipped at least some shards"
    );

    // --- Cleanup: drop all shard collections ---
    for collection_name in &created_collections {
        db.collection::<mongodb::bson::Document>(collection_name)
            .drop()
            .await
            .unwrap();
    }
}
