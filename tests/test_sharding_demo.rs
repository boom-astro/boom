//! Demo: 2-machine HEALPix sharding on a laptop.
//!
//! Requires two MongoDB instances running (see tests/sharding-demo/docker-compose.yaml):
//!   docker compose -f tests/sharding-demo/docker-compose.yaml up -d
//!
//! Run with:
//!   cargo test --test test_sharding_demo -- --nocapture

use boom::conf::HealpixConfig;
use boom::utils::spatial::Coordinates;
use cdshealpix::nested;
use futures::stream::StreamExt;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;

/// Connect to a MongoDB instance, returning None if unavailable.
async fn connect(uri: &str) -> Option<mongodb::Database> {
    let opts = ClientOptions::parse(uri).await.ok()?;
    let client = mongodb::Client::with_options(opts).ok()?;
    let db = client.database("boom_sharding_demo");
    // Verify connectivity
    db.run_command(doc! { "ping": 1 }).await.ok()?;
    Some(db)
}

/// Two-machine HEALPix sharding demo.
///
/// Setup: 12 HEALPix base cells (depth 0) split across 2 machines:
///   - Machine 0 (port 27081): base cells 0–5  (one half of the sky)
///   - Machine 1 (port 27082): base cells 6–11 (the other half)
///
/// The test:
///   1. Scatters 1000 alerts across the full sky
///   2. Routes each alert to the correct machine based on its HEALPix base cell
///   3. Runs a cone search: determines which machine(s) to query, queries only those
///   4. Verifies results match brute-force and that the other machine was not touched
#[tokio::test]
async fn test_two_machine_sharding() {
    // --- Connect to both machines ---
    let machine_0 = match connect("mongodb://localhost:27081").await {
        Some(db) => db,
        None => {
            eprintln!(
                "Skipping test: could not connect to mongo-shard-0 on port 27081.\n\
                 Start the demo cluster with:\n  \
                 docker compose -f tests/sharding-demo/docker-compose.yaml up -d"
            );
            return;
        }
    };
    let machine_1 = match connect("mongodb://localhost:27082").await {
        Some(db) => db,
        None => {
            eprintln!(
                "Skipping test: could not connect to mongo-shard-1 on port 27082.\n\
                 Start the demo cluster with:\n  \
                 docker compose -f tests/sharding-demo/docker-compose.yaml up -d"
            );
            return;
        }
    };

    let machines = [&machine_0, &machine_1];
    let collection_name = "alerts";

    // Clean up from any previous run
    for machine in &machines {
        machine
            .collection::<mongodb::bson::Document>(collection_name)
            .drop()
            .await
            .unwrap();
    }

    // Create hpx index on both machines
    for machine in &machines {
        machine
            .collection::<mongodb::bson::Document>(collection_name)
            .create_index(
                mongodb::IndexModel::builder()
                    .keys(doc! { "coordinates.hpx": 1 })
                    .build(),
            )
            .await
            .unwrap();
    }

    // --- Routing function: base cell → machine ---
    // 12 base cells split evenly: cells 0–5 → machine 0, cells 6–11 → machine 1
    let route = |hpx: i64| -> usize {
        let base_cell = hpx >> (2 * HealpixConfig::FINE_DEPTH); // depth-0 pixel
        if base_cell < 6 { 0 } else { 1 }
    };

    // --- Step 1: Scatter 1000 alerts, route to correct machine ---
    let n_alerts: usize = 1000;
    let mut counts = [0u64; 2];

    for i in 0..n_alerts {
        let ra = (i as f64 * 137.508) % 360.0;
        let dec = ((i as f64 / n_alerts as f64) * 180.0) - 90.0;
        let coord = Coordinates::new(ra, dec);

        let machine_idx = route(coord.hpx);
        let collection = machines[machine_idx]
            .collection::<mongodb::bson::Document>(collection_name);

        let alert_doc = doc! {
            "_id": format!("alert_{}", i),
            "ra": ra,
            "dec": dec,
            "coordinates": { "hpx": coord.hpx },
        };
        collection.insert_one(&alert_doc).await.unwrap();
        counts[machine_idx] += 1;
    }

    eprintln!("Machine 0 (cells 0–5):  {} alerts", counts[0]);
    eprintln!("Machine 1 (cells 6–11): {} alerts", counts[1]);
    eprintln!("Total: {}", counts[0] + counts[1]);

    // Both machines should have alerts (sky is roughly evenly divided)
    assert!(counts[0] > 100, "machine 0 should have substantial alerts");
    assert!(counts[1] > 100, "machine 1 should have substantial alerts");

    // --- Step 2: Cone search — which machine(s) to query? ---
    let query_ra = 120.0_f64;
    let query_dec = 45.0_f64;
    let radius_deg = 5.0_f64;
    let radius_rad = radius_deg.to_radians();

    // Find which base cells the cone overlaps
    let shard_bmoc = nested::cone_coverage_approx(
        0, // depth 0 = 12 base cells
        query_ra.to_radians(),
        query_dec.to_radians(),
        radius_rad,
    );
    let shard_ranges = shard_bmoc.to_ranges();
    let target_cells: Vec<i64> = shard_ranges
        .iter()
        .flat_map(|r| (r.start..r.end).map(|x| x as i64))
        .collect();

    // Which machines do we need?
    let mut need_machine = [false; 2];
    for cell in &target_cells {
        if *cell < 6 {
            need_machine[0] = true;
        } else {
            need_machine[1] = true;
        }
    }

    let machines_needed: usize = need_machine.iter().filter(|&&x| x).count();
    eprintln!(
        "\nCone search ({} deg around RA={}, Dec={}):",
        radius_deg, query_ra, query_dec
    );
    eprintln!("  Overlaps base cells: {:?}", target_cells);
    eprintln!(
        "  Need machine 0: {}, machine 1: {}",
        need_machine[0], need_machine[1]
    );

    // --- Step 3: Query only the needed machines ---
    // Fine filter at depth 14 for selectivity within each machine
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

    let mut found: Vec<String> = Vec::new();
    for (idx, needed) in need_machine.iter().enumerate() {
        if !needed {
            eprintln!("  Machine {}: SKIPPED (not in cone)", idx);
            continue;
        }
        let collection = machines[idx]
            .collection::<mongodb::bson::Document>(collection_name);
        let mut cursor = collection.find(fine_filter.clone()).await.unwrap();
        let mut machine_hits = 0;
        while let Some(result) = cursor.next().await {
            let doc = result.unwrap();
            found.push(doc.get_str("_id").unwrap().to_string());
            machine_hits += 1;
        }
        eprintln!("  Machine {}: QUERIED → {} hits", idx, machine_hits);
    }

    // --- Step 4: Verify against brute-force ---
    let mut brute_force_count = 0;
    for machine in &machines {
        let collection = machine.collection::<mongodb::bson::Document>(collection_name);
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

    eprintln!("\nResults:");
    eprintln!("  HEALPix search: {} alerts", found.len());
    eprintln!("  Brute-force:    {} alerts", brute_force_count);
    eprintln!("  Machines used:  {} of 2", machines_needed);

    assert!(
        found.len() >= brute_force_count,
        "HEALPix missed alerts: found {} but brute force found {}",
        found.len(),
        brute_force_count
    );
    assert!(brute_force_count > 0, "expected some alerts in the cone");

    // Key assertion: we should NOT need both machines for a 5-degree cone
    // (5 degrees is tiny compared to a base cell which is ~3400 deg²)
    assert_eq!(
        machines_needed, 1,
        "a 5-degree cone should fit within a single base cell / machine"
    );

    // --- Cleanup ---
    for machine in &machines {
        machine
            .collection::<mongodb::bson::Document>(collection_name)
            .drop()
            .await
            .unwrap();
    }
}
