use anyhow::Result;
use boom::alert::{deserialize_candidate, deserialize_cutout_as_bytes, AlertCutout, ZtfCandidate};
use boom::{alert::ZtfAlert, utils::o11y::logging::build_subscriber, utils::spatial::Coordinates};
use clap::Parser;
use futures::stream::StreamExt;
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, Bson},
    Client, Collection,
};
use redis::AsyncCommands;
use serde::Deserialize;
use std::{
    fs::File,
    io::{BufRead, BufWriter, Write},
    time::Instant,
};
use tracing::{error, info};

#[derive(Parser)]
struct Cli {
    #[arg(
        long,
        help = "Kowalski MongoDB URI (should use read-only credentials).",
        env = "KOWALSKI_MONGODB_URI"
    )]
    kowalski_uri: String,
    #[arg(
        long,
        help = "Boom MongoDB URI (should use write-enabled credentials).",
        env = "BOOM_MONGODB_URI"
    )]
    boom_uri: String,
    #[arg(
        long,
        help = "Boom MongoDB database name (should use write-enabled credentials).",
        env = "BOOM_MONGODB_NAME",
        default_value = "boom_test_import"
    )]
    boom_db_name: Option<String>,
    #[arg(
        long,
        help = "Redis URI, to queue alerts for enrichment processing.",
        env = "REDIS_URI"
    )]
    redis_uri: String,
    #[arg(
        long,
        help = "Whether to retrieve object IDs from Kowalski (defaults to false)."
    )]
    retrieve_obj_ids: Option<bool>,
    #[arg(
        long,
        help = "Batch size for processing object IDs (defaults to 1000).",
        default_value = "1"
    )]
    batch_size: Option<usize>,
    #[arg(long, help = "Number of parallel processes to use (defaults to 1).")]
    n_processes: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct KowalskiZtfAlert {
    candid: i64,
    #[serde(rename = "objectId")]
    object_id: String,
    #[serde(deserialize_with = "deserialize_candidate")]
    pub candidate: ZtfCandidate,
    #[serde(rename = "cutoutScience")]
    #[serde(deserialize_with = "deserialize_cutout_as_bytes")]
    pub cutout_science: Vec<u8>,
    #[serde(deserialize_with = "deserialize_cutout_as_bytes")]
    #[serde(rename = "cutoutTemplate")]
    pub cutout_template: Vec<u8>,
    #[serde(deserialize_with = "deserialize_cutout_as_bytes")]
    #[serde(rename = "cutoutDifference")]
    pub cutout_difference: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct ZtfObjectIdOnly {
    #[serde(rename = "_id")]
    object_id: String,
}

async fn get_kowalski_collections(kowalski_uri: &str) -> Collection<KowalskiZtfAlert> {
    let kowalski_client = Client::with_uri_str(kowalski_uri).await.unwrap();
    let kowalski_alert_collection: Collection<KowalskiZtfAlert> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts");
    kowalski_alert_collection
}

async fn get_boom_collections(
    boom_uri: &str,
    boom_db_name: &str,
) -> (
    Collection<ZtfObjectIdOnly>,
    Collection<ZtfAlert>,
    Collection<AlertCutout>,
) {
    let boom_client = Client::with_uri_str(boom_uri).await.unwrap();
    let boom_collection_idonly: Collection<ZtfObjectIdOnly> = boom_client
        .database(boom_db_name)
        .collection("ZTF_alerts_aux");
    let boom_alert_collection: Collection<ZtfAlert> =
        boom_client.database(boom_db_name).collection("ZTF_alerts");
    let boom_cutout_collection: Collection<AlertCutout> = boom_client
        .database(boom_db_name)
        .collection("ZTF_alert_cutouts");
    (
        boom_collection_idonly,
        boom_alert_collection,
        boom_cutout_collection,
    )
}

// Optimized: Use aggregation pipeline to get latest alert for multiple object IDs in one query
async fn get_latest_kowalski_alerts_bulk(
    obj_ids: &[String],
    kowalski_alert_collection: &Collection<KowalskiZtfAlert>,
) -> Vec<KowalskiZtfAlert> {
    if obj_ids.is_empty() {
        return vec![];
    }

    // Aggregation pipeline:
    // 1. Match documents with objectId in our list
    // 2. Sort by jd descending within each objectId
    // 3. Group by objectId and take the first (latest) document
    // Aggregation pipeline:
    // 1. Match documents with objectId in our list
    // 2. Project out the large cutout fields before sorting (to reduce memory usage)
    // 3. Sort by objectId and jd descending
    // 4. Group by objectId and keep only the candid of the latest alert
    // 5. Lookup the full document (with cutouts) using the candid
    let pipeline = vec![
        doc! {
            "$match": {
                "objectId": { "$in": obj_ids }
            }
        },
        // Project only the fields we need for sorting and grouping (exclude large cutouts)
        doc! {
            "$project": {
                "objectId": 1,
                "candid": 1,
                "candidate.jd": 1
            }
        },
        doc! {
            "$sort": {
                "objectId": 1,
                "candidate.jd": -1
            }
        },
        // Group by objectId and keep only the candid of the first (latest) document
        doc! {
            "$group": {
                "_id": "$objectId",
                "candid": { "$first": "$candid" }
            }
        },
        // Now lookup the full document (with cutouts) using the candid
        doc! {
            "$lookup": {
                "from": "ZTF_alerts",
                "localField": "candid",
                "foreignField": "candid",
                "as": "fullDoc"
            }
        },
        // Unwind the array from lookup (should only have 1 element)
        doc! {
            "$unwind": "$fullDoc"
        },
        // Replace root with the full document
        doc! {
            "$replaceRoot": { "newRoot": "$fullDoc" }
        },
        doc! {
            "$project": {
                "_id": 0,
                "classifications": 0,
                "publisher": 0,
                "schemavsn": 0,
                "coordinates": 0,
            }
        },
    ];

    let mut cursor = kowalski_alert_collection.aggregate(pipeline).await.unwrap();

    let mut results = Vec::new();
    while let Some(result) = cursor.next().await {
        match result {
            Ok(doc) => match mongodb::bson::from_document::<KowalskiZtfAlert>(doc) {
                Ok(alert) => results.push(alert),
                Err(e) => error!("Error deserializing alert: {}", e),
            },
            Err(e) => error!("Error reading cursor: {}", e),
        }
    }

    results
}

// Convert Kowalski alerts to Boom alerts and cutouts
fn make_boom_alerts_from_kowalski(
    kowalski_alerts: Vec<KowalskiZtfAlert>,
) -> (Vec<ZtfAlert>, Vec<AlertCutout>) {
    let mut boom_alerts = Vec::with_capacity(kowalski_alerts.len());
    let mut boom_cutouts = Vec::with_capacity(kowalski_alerts.len());

    for kowalski_alert in kowalski_alerts {
        let boom_alert = ZtfAlert {
            candid: kowalski_alert.candid,
            object_id: kowalski_alert.object_id.clone(),
            candidate: kowalski_alert.candidate.clone(),
            coordinates: Coordinates::new(
                kowalski_alert.candidate.candidate.ra,
                kowalski_alert.candidate.candidate.dec,
            ),
            created_at: flare::Time::now().to_jd(),
            updated_at: flare::Time::now().to_jd(),
        };
        let boom_cutout = AlertCutout {
            candid: kowalski_alert.candid,
            cutout_science: kowalski_alert.cutout_science,
            cutout_template: kowalski_alert.cutout_template,
            cutout_difference: kowalski_alert.cutout_difference,
        };
        boom_alerts.push(boom_alert);
        boom_cutouts.push(boom_cutout);
    }

    (boom_alerts, boom_cutouts)
}

async fn get_all_boom_obj_ids(boom_collection_idonly: &Collection<ZtfObjectIdOnly>) -> usize {
    let file_name = "boom_obj_ids.txt";
    if std::path::Path::new(file_name).exists() {
        let line_count = std::fs::read_to_string(file_name).unwrap().lines().count();
        info!(
            "File {} already exists with {} lines",
            file_name, line_count
        );
        return line_count;
    }
    let file = File::create(file_name).unwrap();
    let mut writer = BufWriter::new(file);
    let total_count = boom_collection_idonly
        .estimated_document_count()
        .await
        .unwrap();

    let progress_bar = ProgressBar::new(total_count as u64)
        .with_message("Retrieving Boom Object IDs")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );
    let mut obj_ids = Vec::with_capacity(total_count as usize);
    let mut cursor = boom_collection_idonly
        .find(doc! {})
        .projection(doc! {"_id": 1})
        .sort(doc! {"_id": 1})
        .await
        .unwrap();
    let mut count = 0;
    while let Some(result) = cursor.next().await {
        let obj_id = result.unwrap();
        obj_ids.push(obj_id.object_id);
        count += 1;
        if count % 100_000 == 0 {
            for id in obj_ids.iter() {
                writeln!(writer, "{}", id).unwrap();
            }
            obj_ids.clear();
            writer.flush().unwrap();
            progress_bar.inc(100_000);
        }
    }
    for id in obj_ids.iter() {
        writeln!(writer, "{}", id).unwrap();
        progress_bar.inc(1);
    }
    writer.flush().unwrap();
    progress_bar.finish_with_message("Retrieving complete");
    count
}

async fn get_all_boom_existing_obj_ids(
    boom_alert_collection: &Collection<ZtfAlert>,
) -> Vec<String> {
    let obj_ids: Vec<Bson> = boom_alert_collection
        .distinct("objectId", doc! {})
        .await
        .unwrap();
    let mut obj_ids: Vec<String> = obj_ids
        .into_iter()
        .filter_map(|bson| match bson {
            Bson::String(s) => Some(s),
            _ => None,
        })
        .collect();
    obj_ids.sort_unstable();
    obj_ids
}

async fn filter_existing_boom_obj_ids(
    file_path: &str,
    boom_alert_collection: &Collection<ZtfAlert>,
) -> usize {
    let existing_obj_ids = get_all_boom_existing_obj_ids(boom_alert_collection).await;
    let existing_obj_id_set: std::collections::HashSet<String> =
        existing_obj_ids.into_iter().collect();

    let deduplicated_file = format!("deduplicated_{}", file_path);
    let input_file = File::open(file_path).unwrap();
    let reader = std::io::BufReader::new(input_file);
    let output_file = File::create(&deduplicated_file).unwrap();
    let mut writer = BufWriter::new(output_file);
    let mut count_added = 0;
    let mut count_not_added = 0;
    for line in reader.lines() {
        let obj_id = line.unwrap();
        if !existing_obj_id_set.contains(&obj_id) {
            writeln!(writer, "{}", obj_id).unwrap();
            count_added += 1;
        } else {
            count_not_added += 1;
        }
    }
    writer.flush().unwrap();
    info!(
        "Wrote deduplicated object ids to file {} ({}/{} added)",
        deduplicated_file,
        count_added,
        count_added + count_not_added
    );

    // now let's load it back and randomize the order
    let file = File::open(&deduplicated_file).unwrap();
    let reader = std::io::BufReader::new(file);
    let mut obj_ids: Vec<String> = reader.lines().map(|l| l.unwrap()).collect();
    use rand::seq::SliceRandom;
    let mut rng = rand::rng();
    obj_ids.shuffle(&mut rng);
    let output_file = File::create(&deduplicated_file).unwrap();
    let mut writer = BufWriter::new(output_file);
    for obj_id in obj_ids.iter() {
        writeln!(writer, "{}", obj_id).unwrap();
    }
    writer.flush().unwrap();
    info!(
        "Shuffled deduplicated object ids in file {}",
        deduplicated_file
    );
    count_added
}

async fn worker(
    receiver: async_channel::Receiver<String>,
    batch_size: usize,
    kowalski_uri: &str,
    redis_uri: &str,
    boom_uri: &str,
    boom_db_name: &str,
) -> Result<usize> {
    let client_redis = redis::Client::open(redis_uri).expect("Failed to create Redis client");

    let mut con = client_redis
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to get multiplexed async connection");
    let output_queue_name = "ZTF_alerts_enrichment_queue";

    let kowalski_alert_collection = get_kowalski_collections(kowalski_uri).await;
    let (_, boom_alert_collection, boom_cutout_collection) =
        get_boom_collections(boom_uri, boom_db_name).await;
    let mut obj_ids = Vec::with_capacity(batch_size);
    let mut total_processed = 0;

    let options = mongodb::options::InsertManyOptions::builder()
        .ordered(false)
        .build();

    while let Ok(obj_id) = receiver.recv().await {
        obj_ids.push(obj_id);

        if obj_ids.len() >= batch_size {
            // Process batch
            let kowalski_alerts =
                get_latest_kowalski_alerts_bulk(&obj_ids, &kowalski_alert_collection).await;

            let (boom_alerts, boom_cutouts) = make_boom_alerts_from_kowalski(kowalski_alerts);

            let candids: Vec<i64> = boom_alerts.iter().map(|a| a.candid).collect();

            // Insert cutouts
            if !boom_cutouts.is_empty() {
                match boom_cutout_collection
                    .insert_many(boom_cutouts)
                    .with_options(options.clone())
                    .await
                {
                    Ok(_result) => {}
                    Err(e) => match *e.kind {
                        mongodb::error::ErrorKind::InsertMany(insert_many_error) => {
                            if let Some(write_errors) = insert_many_error.write_errors {
                                for write_error in write_errors {
                                    if write_error.code != 11000 {
                                        error!("Error inserting cutout: {:?}", write_error);
                                    }
                                }
                            }
                        }
                        _ => {
                            error!("Error inserting cutouts: {}", e);
                        }
                    },
                }
            }

            // Insert alerts
            if !boom_alerts.is_empty() {
                match boom_alert_collection
                    .insert_many(boom_alerts)
                    .with_options(options.clone())
                    .await
                {
                    Ok(_result) => {}
                    Err(e) => match *e.kind {
                        mongodb::error::ErrorKind::InsertMany(insert_many_error) => {
                            if let Some(write_errors) = insert_many_error.write_errors {
                                for write_error in write_errors {
                                    if write_error.code != 11000 {
                                        error!("Error inserting alert: {:?}", write_error);
                                    }
                                }
                            }
                        }
                        _ => {
                            error!("Error inserting alerts: {}", e);
                        }
                    },
                }
            }

            total_processed += obj_ids.len();
            obj_ids.clear();

            // Queue for enrichment
            con.lpush::<&str, Vec<i64>, usize>(&output_queue_name, candids)
                .await
                .expect("Failed to push to Redis queue");
        }
    }

    if !obj_ids.is_empty() {
        let kowalski_alerts =
            get_latest_kowalski_alerts_bulk(&obj_ids, &kowalski_alert_collection).await;

        let (boom_alerts, boom_cutouts) = make_boom_alerts_from_kowalski(kowalski_alerts);

        if !boom_cutouts.is_empty() {
            match boom_cutout_collection
                .insert_many(boom_cutouts)
                .with_options(options.clone())
                .await
            {
                Ok(_result) => {}
                Err(e) => match *e.kind {
                    mongodb::error::ErrorKind::InsertMany(insert_many_error) => {
                        if let Some(write_errors) = insert_many_error.write_errors {
                            for write_error in write_errors {
                                if write_error.code != 11000 {
                                    error!("Error inserting cutout: {:?}", write_error);
                                }
                            }
                        }
                    }
                    _ => {
                        error!("Error inserting cutouts: {}", e);
                    }
                },
            }
        }

        if !boom_alerts.is_empty() {
            match boom_alert_collection
                .insert_many(boom_alerts)
                .with_options(options.clone())
                .await
            {
                Ok(_result) => {}
                Err(e) => match *e.kind {
                    mongodb::error::ErrorKind::InsertMany(insert_many_error) => {
                        if let Some(write_errors) = insert_many_error.write_errors {
                            for write_error in write_errors {
                                if write_error.code != 11000 {
                                    error!("Error inserting alert: {:?}", write_error);
                                }
                            }
                        }
                    }
                    _ => {
                        error!("Error inserting alerts: {}", e);
                    }
                },
            }
        }

        total_processed += obj_ids.len();
        obj_ids.clear();
    }

    Ok(total_processed)
}

#[tokio::main]
async fn main() {
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let args = Cli::parse();
    let kowalski_uri = args.kowalski_uri;
    let boom_uri = args.boom_uri;
    let boom_db_name = args.boom_db_name.unwrap_or("boom_test_import".to_string());
    let retrieve_obj_ids = args.retrieve_obj_ids.unwrap_or(false);
    let batch_size = args.batch_size.unwrap_or(1000);
    let n_processes = args.n_processes.unwrap_or(1);
    let redis_uri = args.redis_uri;

    let obj_ids_file = "boom_obj_ids.txt";
    let count = match retrieve_obj_ids {
        true => {
            info!("Retrieving object IDs from BOOM");
            let (boom_obj_collection_idonly, _, _) =
                get_boom_collections(&boom_uri, &boom_db_name).await;
            let start = Instant::now();
            let count = get_all_boom_obj_ids(&boom_obj_collection_idonly).await;
            let duration = start.elapsed();
            info!("Retrieved and saved {} object ids in {:?}", count, duration);
            count
        }
        false => {
            let file = File::open(&obj_ids_file).unwrap();
            let reader = std::io::BufReader::new(file);
            let line_count = reader.lines().count();
            info!(
                "Using existing object IDs file {} with {} lines",
                obj_ids_file, line_count
            );
            line_count
        }
    };

    println!("Retrieved {} object IDs from BOOM", count);

    let deduplicated_count = filter_existing_boom_obj_ids(
        &obj_ids_file,
        &get_boom_collections(&boom_uri, &boom_db_name).await.1,
    )
    .await;

    if deduplicated_count == 0 {
        info!("No new object IDs to process after deduplication. Exiting.");
        return;
    }

    let obj_ids_file = format!("deduplicated_{}", obj_ids_file);

    // // Process in batches
    // let (
    //     _boom_obj_collection_idonly,
    //     boom_alert_collection,
    //     boom_cutout_collection
    // ) = get_boom_collections(&boom_uri, &boom_db_name).await;
    // let kowalski_alert_collection = get_kowalski_collections(&kowalski_uri).await;

    let channel_capacity = n_processes * batch_size * 2;

    let (s, r) = async_channel::bounded(channel_capacity);
    let mut workers = Vec::new();
    for _ in 0..n_processes {
        let worker_receiver = r.clone();
        let batch_size = batch_size;
        let kowalski_uri = kowalski_uri.clone();
        let boom_uri = boom_uri.clone();
        let boom_db_name = boom_db_name.to_string();
        let redis_uri = redis_uri.clone();
        let worker_handle = tokio::spawn(async move {
            worker(
                worker_receiver,
                batch_size,
                &kowalski_uri,
                &redis_uri,
                &boom_uri,
                &boom_db_name,
            )
            .await
        });
        workers.push(worker_handle);
    }

    let file = File::open(&obj_ids_file).unwrap();
    let reader = std::io::BufReader::new(file);

    let progress_bar = ProgressBar::new(deduplicated_count as u64)
        .with_message("Processing Kowalski Object IDs")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );

    let mut total_processed = 0;
    for line in reader.lines() {
        let obj_id = line.unwrap();
        match s.send(obj_id).await {
            Ok(_) => {
                progress_bar.inc(1);
            }
            Err(e) => {
                eprintln!("Failed to send obj_id to workers: {}", e);
                break;
            }
        }
    }

    drop(s); // Close the sender to signal no more messages
    for worker in workers {
        match worker.await {
            Ok(Ok(processed)) => {
                total_processed += processed;
            }
            Ok(Err(e)) => {
                eprintln!("Worker error: {}", e);
            }
            Err(e) => {
                eprintln!("Worker task join error: {}", e);
            }
        }
    }
    progress_bar.finish_with_message("Processing complete");
    println!("Total processed objects: {}", total_processed);
}
