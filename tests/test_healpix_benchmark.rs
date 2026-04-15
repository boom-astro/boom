//! Benchmark: HEALPix range queries vs MongoDB 2dsphere for cone searches.
//!
//! Generates a synthetic catalog of N random sky points, indexes it both ways,
//! then runs M cone searches using each method and compares wall-clock time.
//!
//! Requires MongoDB on localhost:27017 (uses the test config credentials).
//! Run with:
//!   cargo test --test test_healpix_benchmark --release -- --nocapture

use boom::utils::spatial::{cone_to_hpx_filter, Coordinates};
use futures::stream::StreamExt;
use mongodb::bson::doc;
use rand::RngExt;
use std::time::Instant;

const CATALOG_SIZE: usize = 1_000_000;
const N_QUERIES: usize = 500;

#[tokio::test]
async fn bench_healpix_vs_2dsphere() {
    // Use a clean MongoDB instance (the sharding demo container on port 27081,
    // or fall back to the test config). No auth required.
    let db = match mongodb::Client::with_uri_str("mongodb://localhost:27081").await {
        Ok(client) => {
            let db = client.database("bench_healpix");
            if db.run_command(doc! { "ping": 1 }).await.is_ok() {
                db
            } else {
                eprintln!("Skipping benchmark: MongoDB on port 27081 not responding.\n\
                           Start with: docker compose -f tests/sharding-demo/docker-compose.yaml up -d");
                return;
            }
        }
        Err(e) => {
            eprintln!("Skipping benchmark: {}", e);
            return;
        }
    };

    let collection_name = format!("bench_healpix_{}", uuid::Uuid::new_v4());
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);

    // --- Generate synthetic catalog ---
    eprintln!("Generating {} random sky points...", CATALOG_SIZE);
    let mut rng = rand::rng();
    let mut docs = Vec::with_capacity(CATALOG_SIZE);

    for i in 0..CATALOG_SIZE {
        let ra: f64 = rng.random::<f64>() * 360.0;
        // uniform on sphere: dec = arcsin(2u - 1) where u ∈ [0,1]
        let u: f64 = rng.random();
        let dec: f64 = (2.0 * u - 1.0_f64).asin().to_degrees();

        let coord = Coordinates::new(ra, dec);
        docs.push(doc! {
            "_id": i as i64,
            "ra": ra,
            "dec": dec,
            "coordinates": {
                "radec_geojson": {
                    "type": "Point",
                    "coordinates": [ra - 180.0, dec],
                },
                "hpx": coord.hpx,
            },
        });
    }

    // Bulk insert
    eprintln!("Inserting into MongoDB...");
    let insert_start = Instant::now();
    // Insert in batches of 10K to avoid hitting BSON size limits
    for chunk in docs.chunks(10_000) {
        collection.insert_many(chunk.to_vec()).await.unwrap();
    }
    let insert_time = insert_start.elapsed();
    eprintln!("Insert: {:.2}s", insert_time.as_secs_f64());

    // --- Create both indexes ---
    eprintln!("Creating indexes...");
    let idx_start = Instant::now();
    collection
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "coordinates.radec_geojson": "2dsphere" })
                .build(),
        )
        .await
        .unwrap();
    let geojson_idx_time = idx_start.elapsed();

    let idx_start = Instant::now();
    collection
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "coordinates.hpx": 1 })
                .build(),
        )
        .await
        .unwrap();
    let hpx_idx_time = idx_start.elapsed();
    eprintln!(
        "Index creation: 2dsphere={:.2}s, hpx={:.2}s",
        geojson_idx_time.as_secs_f64(),
        hpx_idx_time.as_secs_f64()
    );

    // --- Generate random query positions ---
    let mut query_positions: Vec<(f64, f64)> = Vec::with_capacity(N_QUERIES);
    for _ in 0..N_QUERIES {
        let ra: f64 = rng.random::<f64>() * 360.0;
        let u: f64 = rng.random();
        let dec: f64 = (2.0 * u - 1.0_f64).asin().to_degrees();
        query_positions.push((ra, dec));
    }

    // Cross-match radii to test (in arcseconds) with adaptive query depth.
    // The goal: use the coarsest depth where the cone fits in ~1-4 pixels.
    // This minimizes $or branches while keeping the search selective.
    let test_cases: Vec<(f64, u8)> = vec![
        (2.0, 14),   // 2" cone, depth 14 (~13" pixels) → 1 range
        (10.0, 12),  // 10" cone, depth 12 (~52" pixels) → 1 range
        (30.0, 11),  // 30" cone, depth 11 (~1.7' pixels) → 1-2 ranges
        (60.0, 10),  // 60" cone, depth 10 (~3.4' pixels) → 1-2 ranges
    ];

    // Print index sizes
    let stats = collection.aggregate(vec![
        doc! { "$collStats": { "storageStats": {} } },
    ]).await;
    if let Ok(mut cursor) = stats {
        if let Some(Ok(stats_doc)) = cursor.next().await {
            if let Ok(storage) = stats_doc.get_document("storageStats") {
                if let Ok(idx_sizes) = storage.get_document("indexSizes") {
                    eprintln!("\nIndex sizes:");
                    for (key, val) in idx_sizes {
                        let bytes = val.as_i64().or(val.as_i32().map(|v| v as i64)).unwrap_or(0);
                        eprintln!("  {}: {:.1} MB", key, bytes as f64 / 1024.0 / 1024.0);
                    }
                }
            }
        }
    }

    eprintln!(
        "\n{:<12} {:<8} {:<8} {:<12} {:<12} {:<12} {:<8}",
        "Radius", "Depth", "Ranges", "2dsphere", "HEALPix", "Speedup", "Queries"
    );
    eprintln!("{}", "-".repeat(76));

    for &(radius_arcsec, query_depth) in &test_cases {
        let radius_rad = (radius_arcsec / 3600.0_f64).to_radians();

        // --- Benchmark 2dsphere ---
        let mut geojson_total_hits = 0usize;
        let geo_start = Instant::now();
        for &(ra, dec) in &query_positions {
            let filter = doc! {
                "coordinates.radec_geojson": {
                    "$geoWithin": {
                        "$centerSphere": [[ra - 180.0, dec], radius_rad]
                    }
                }
            };
            let mut cursor = collection.find(filter).await.unwrap();
            while let Some(result) = cursor.next().await {
                let _ = result.unwrap();
                geojson_total_hits += 1;
            }
        }
        let geo_elapsed = geo_start.elapsed();

        // Count ranges for the first query (representative)
        let sample_bmoc = cdshealpix::nested::cone_coverage_approx(
            query_depth,
            query_positions[0].0.to_radians(),
            query_positions[0].1.to_radians(),
            radius_rad,
        );
        let n_ranges = sample_bmoc.to_ranges().len();

        // --- Benchmark HEALPix ---
        let mut hpx_total_hits = 0usize;
        let hpx_start = Instant::now();
        for &(ra, dec) in &query_positions {
            let filter = cone_to_hpx_filter(ra, dec, radius_rad, query_depth);
            let mut cursor = collection.find(filter).await.unwrap();
            while let Some(result) = cursor.next().await {
                let _ = result.unwrap();
                hpx_total_hits += 1;
            }
        }
        let hpx_elapsed = hpx_start.elapsed();

        let speedup = geo_elapsed.as_secs_f64() / hpx_elapsed.as_secs_f64();
        eprintln!(
            "{:<12} {:<8} {:<8} {:<12.3}s {:<12.3}s {:<12.1}x {}",
            format!("{}\"", radius_arcsec),
            query_depth,
            n_ranges,
            geo_elapsed.as_secs_f64(),
            hpx_elapsed.as_secs_f64(),
            speedup,
            N_QUERIES,
        );

        // HEALPix is slightly overinclusive, so it should find >= what 2dsphere finds
        assert!(
            hpx_total_hits >= geojson_total_hits,
            "HEALPix missed results at {}\" radius: hpx={} geo={}",
            radius_arcsec,
            hpx_total_hits,
            geojson_total_hits,
        );
    }

    eprintln!("\nCatalog: {} points, {} queries per radius", CATALOG_SIZE, N_QUERIES);

    // Cleanup
    collection.drop().await.unwrap();
}
