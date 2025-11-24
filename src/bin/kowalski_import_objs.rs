use anyhow::{Error, Result};
use boom::{
    alert::{
        deserialize_fp_hists, deserialize_prv_candidate, deserialize_prv_candidates, ZtfForcedPhot,
        ZtfObject, ZtfPrvCandidate,
    },
    utils::o11y::logging::build_subscriber,
    utils::spatial::Coordinates,
};
use clap::Parser;
use futures::stream::StreamExt;
use indicatif::ProgressBar;
use mongodb::{bson::doc, Client, Collection};
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufWriter, Write},
    time::Instant,
};
use tracing::{error, info, warn};

#[derive(Parser)]
struct Cli {
    #[arg(long, help = "Number of parallel processes to use (defaults to 1).")]
    n_processes: Option<usize>,
    #[arg(
        long,
        help = "Batch size of object IDs to process per worker (defaults to 100)."
    )]
    batch_size: Option<usize>,
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
        help = "Whether to retrieve object IDs from Kowalski (defaults to false)."
    )]
    retrieve_obj_ids: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct KowalskiZtfObject {
    #[serde(rename = "_id")]
    object_id: String,
    #[serde(deserialize_with = "deserialize_prv_candidates")]
    prv_candidates: Option<Vec<ZtfPrvCandidate>>,
    #[serde(deserialize_with = "deserialize_fp_hists")]
    fp_hists: Option<Vec<ZtfForcedPhot>>,
}

#[derive(Debug, Deserialize)]
struct KowalskiZtfAlert {
    #[serde(rename = "objectId")]
    object_id: String,
    #[serde(deserialize_with = "deserialize_prv_candidate")]
    candidate: ZtfPrvCandidate,
}

#[derive(Debug, Deserialize)]
struct KowalskiObjId {
    #[serde(rename = "_id")]
    object_id: String,
}

fn process_object(
    ztf_obj: KowalskiZtfObject,
    alerts: &HashMap<String, Vec<KowalskiZtfAlert>>,
) -> Result<ZtfObject, Error> {
    let mut prv_candidates_doc = vec![];
    let mut prv_nondetections_doc = vec![];

    let mut brightest_candidate_mag = std::f32::MAX;
    let mut brightest_candidate_ra = 0.0;
    let mut brightest_candidate_dec = 0.0;
    for prv_candidate in ztf_obj.prv_candidates.unwrap() {
        if prv_candidate.prv_candidate.magpsf.is_some() {
            if brightest_candidate_mag > prv_candidate.prv_candidate.magpsf.unwrap() {
                brightest_candidate_mag = prv_candidate.prv_candidate.magpsf.unwrap();
                brightest_candidate_ra = prv_candidate.prv_candidate.ra.unwrap();
                brightest_candidate_dec = prv_candidate.prv_candidate.dec.unwrap();
            }
            prv_candidates_doc.push(prv_candidate);
        } else {
            prv_nondetections_doc.push(prv_candidate);
        }
    }
    for alert in alerts.get(&ztf_obj.object_id).unwrap_or(&vec![]) {
        let candidate = &alert.candidate;
        if brightest_candidate_mag > candidate.prv_candidate.magpsf.unwrap() {
            brightest_candidate_mag = candidate.prv_candidate.magpsf.unwrap();
            brightest_candidate_ra = candidate.prv_candidate.ra.unwrap();
            brightest_candidate_dec = candidate.prv_candidate.dec.unwrap();
        }
        prv_candidates_doc.push(candidate.clone());
    }
    if brightest_candidate_mag == std::f32::MAX {
        // throw an error
        return Err(anyhow::anyhow!(
            "No valid candidates with magpsf found for object {}",
            ztf_obj.object_id
        ));
    }
    // deduplicate the prv_candidates by jd and band, and sort by jd ascending
    prv_candidates_doc.sort_by(|a, b| {
        a.prv_candidate
            .jd
            .partial_cmp(&b.prv_candidate.jd)
            .unwrap()
            .then(a.band.cmp(&b.band))
    });
    prv_candidates_doc
        .dedup_by(|a, b| a.prv_candidate.jd == b.prv_candidate.jd && a.band == b.band);
    // same with the prv_nondetections
    prv_nondetections_doc.sort_by(|a, b| {
        a.prv_candidate
            .jd
            .partial_cmp(&b.prv_candidate.jd)
            .unwrap()
            .then(a.band.cmp(&b.band))
    });
    prv_nondetections_doc
        .dedup_by(|a, b| a.prv_candidate.jd == b.prv_candidate.jd && a.band == b.band);
    // and finally, with the fp_hists inside the ztf_obj.fp_hists
    let mut obj_fp_hists = vec![];
    if let Some(fp_hists) = ztf_obj.fp_hists {
        for fp_hist in fp_hists {
            obj_fp_hists.push(fp_hist);
        }
        obj_fp_hists.sort_by(|a, b| {
            a.fp_hist
                .jd
                .partial_cmp(&b.fp_hist.jd)
                .unwrap()
                .then(a.band.cmp(&b.band))
        });
        obj_fp_hists.dedup_by(|a, b| a.fp_hist.jd == b.fp_hist.jd && a.band == b.band);
    }
    let now = flare::Time::now().to_jd();
    Ok(ZtfObject {
        object_id: ztf_obj.object_id,
        prv_candidates: prv_candidates_doc,
        prv_nondetections: prv_nondetections_doc,
        fp_hists: obj_fp_hists,
        cross_matches: None,
        aliases: None,
        coordinates: Coordinates::new(brightest_candidate_ra, brightest_candidate_dec),
        created_at: now,
        updated_at: now,
    })
}

async fn get_kowalski_alerts_by_object_id(
    obj_ids: &Vec<String>,
    kowalski_alert_collection: &Collection<KowalskiZtfAlert>,
) -> HashMap<String, Vec<KowalskiZtfAlert>> {
    let mut alerts: HashMap<String, Vec<KowalskiZtfAlert>> = HashMap::new();
    let filter = doc! { "objectId": { "$in": obj_ids } };
    let mut cursor = kowalski_alert_collection
        .find(filter)
        .projection(doc! {"_id": 0, "cutoutScience": 0, "cutoutTemplate": 0, "cutoutDifference": 0, "schemavsn": 0, "classifications": 0, "oordinates": 0})
        .await
        .unwrap();
    while let Some(result) = cursor.next().await {
        let alert = result.unwrap();
        alerts
            .entry(alert.object_id.clone())
            .or_insert(vec![])
            .push(alert);
    }
    alerts
}

async fn insert_boom_objects(boom_objs: &Vec<ZtfObject>, boom_collection: &Collection<ZtfObject>) {
    for chunk in boom_objs.chunks(1000.min(boom_objs.len())) {
        match boom_collection.insert_many(chunk).ordered(false).await {
            Ok(_result) => {}
            Err(e) => {
                match *e.kind {
                    // if it's just duplicate errors (11000), we can ignore them
                    mongodb::error::ErrorKind::InsertMany(insert_many_error) => {
                        if let Some(write_errors) = insert_many_error.write_errors {
                            for write_error in write_errors {
                                if write_error.code != 11000 {
                                    error!("Error inserting document: {:?}", write_error);
                                }
                            }
                        }
                    }
                    _ => {
                        error!("Error inserting documents: {}", e);
                    }
                }
            }
        }
    }
}

async fn get_kowalski_collections(
    kowalski_uri: &str,
) -> (
    Collection<KowalskiZtfAlert>,
    Collection<KowalskiZtfObject>,
    Collection<KowalskiObjId>,
) {
    let kowalski_client = Client::with_uri_str(kowalski_uri).await.unwrap();
    let kowalski_alert_collection: Collection<KowalskiZtfAlert> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts");
    let kowalski_obj_collection: Collection<KowalskiZtfObject> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts_aux");
    let kowalski_obj_collection_idonly: Collection<KowalskiObjId> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts_aux");
    (
        kowalski_alert_collection,
        kowalski_obj_collection,
        kowalski_obj_collection_idonly,
    )
}

async fn get_boom_obj_collection(boom_uri: &str) -> Collection<ZtfObject> {
    let boom_client = Client::with_uri_str(boom_uri).await.unwrap();
    let boom_collection: Collection<ZtfObject> = boom_client
        .database("boom_test_import")
        .collection("ZTF_alerts_aux");
    // create indexes if they do not exist
    boom_collection
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "coordinates.radec_geojson": "2dsphere", "_id": 1 })
                .options(None)
                .build(),
        )
        .await
        .unwrap();
    boom_collection
}

// in some instances, we may not have an entry in Kowalski for the object itself, but we may have alerts for it
// when that happens, we need a method to create a ZtfObject from just the Kowalski alerts
fn make_obj_from_alerts(candidates: &Vec<KowalskiZtfAlert>) -> Result<ZtfObject, Error> {
    if candidates.is_empty() {
        return Err(anyhow::anyhow!("No candidates provided"));
    }
    let mut prv_candidates_doc = vec![];

    let mut brightest_candidate_mag = std::f32::MAX;
    let mut brightest_candidate_ra = 0.0;
    let mut brightest_candidate_dec = 0.0;
    for alert in candidates {
        let candidate = &alert.candidate;
        if brightest_candidate_mag > candidate.prv_candidate.magpsf.unwrap() {
            brightest_candidate_mag = candidate.prv_candidate.magpsf.unwrap();
            brightest_candidate_ra = candidate.prv_candidate.ra.unwrap();
            brightest_candidate_dec = candidate.prv_candidate.dec.unwrap();
        }
        prv_candidates_doc.push(candidate.clone());
    }
    if brightest_candidate_mag == std::f32::MAX {
        return Err(anyhow::anyhow!(
            "No valid candidates with magpsf found for object {}",
            candidates[0].object_id
        ));
    }
    // deduplicate the prv_candidates by jd and band, and sort by jd ascending
    prv_candidates_doc.sort_by(|a, b| {
        a.prv_candidate
            .jd
            .partial_cmp(&b.prv_candidate.jd)
            .unwrap()
            .then(a.band.cmp(&b.band))
    });
    prv_candidates_doc
        .dedup_by(|a, b| a.prv_candidate.jd == b.prv_candidate.jd && a.band == b.band);

    let now = flare::Time::now().to_jd();
    Ok(ZtfObject {
        object_id: candidates[0].object_id.clone(),
        prv_candidates: prv_candidates_doc,
        prv_nondetections: vec![],
        fp_hists: vec![],
        cross_matches: None,
        aliases: None,
        coordinates: Coordinates::new(brightest_candidate_ra, brightest_candidate_dec),
        created_at: now,
        updated_at: now,
    })
}

async fn make_boom_objs_from_kowalski(
    obj_ids: &Vec<String>,
    kowalski_obj_collection: &Collection<KowalskiZtfObject>,
    kowalski_alert_collection: &Collection<KowalskiZtfAlert>,
) -> Vec<ZtfObject> {
    let mut alerts = get_kowalski_alerts_by_object_id(&obj_ids, &kowalski_alert_collection).await;

    let mut boom_objs = vec![];
    let mut cursor = kowalski_obj_collection
        .find(doc! { "_id": { "$in": obj_ids } })
        .projection(doc! { "prv_candidates": 1, "fp_hists": 1 })
        .await
        .unwrap();
    while let Some(result) = cursor.next().await {
        match result {
            Ok(ztf_obj) => {
                let obj_id = ztf_obj.object_id.clone();
                match process_object(ztf_obj, &alerts) {
                    Ok(boom_obj) => {
                        boom_objs.push(boom_obj);
                    }
                    Err(e) => {
                        error!("Error processing object {}: {}", obj_id, e);
                    }
                }
                // remove the alerts for this object id to save memory
                alerts.remove(&obj_id);
            }
            Err(e) => {
                error!("Error fetching document: {}", e);
            }
        }
    }

    // process remaining alerts that did not have a corresponding Kowalski object
    for (obj_id, alert_list) in alerts.iter() {
        match make_obj_from_alerts(alert_list) {
            Ok(boom_obj) => {
                boom_objs.push(boom_obj);
                warn!("Created Boom object from alerts only for object {}", obj_id);
            }
            Err(e) => {
                error!("Error processing alerts for object {}: {}", obj_id, e);
            }
        }
    }
    boom_objs
}

// try the same as above but no pagination, just one cursor we retrieve all object ids from
// every 100,000 object ids collected, we print the progress and flush to disk
async fn get_all_kowalski_obj_ids(
    kowalski_obj_collection_idonly: &Collection<KowalskiObjId>,
) -> usize {
    let file_name = "kowalski_obj_ids.txt";
    // if the file exists, count the number of lines and return that as the count
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
    let total_count = kowalski_obj_collection_idonly
        .estimated_document_count()
        .await
        .unwrap();

    let progress_bar = ProgressBar::new(total_count as u64)
        .with_message("Retrieving Kowalski Object IDs")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );
    let mut obj_ids = Vec::with_capacity(total_count as usize);
    let mut cursor = kowalski_obj_collection_idonly
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
            // info!("Collected {}/{} object ids", count, total_count);
            for id in obj_ids.iter() {
                writeln!(writer, "{}", id).unwrap();
            }
            obj_ids.clear();
            writer.flush().unwrap();
            progress_bar.inc(100_000);
        }
    }
    // write remaining ids to file
    for id in obj_ids.iter() {
        writeln!(writer, "{}", id).unwrap();
        progress_bar.inc(1);
    }
    writer.flush().unwrap();

    // last step, load the object ids from the file and shuffle them
    info!("Shuffling object ids");
    let file = File::open(file_name).unwrap();
    let reader = std::io::BufReader::new(file);
    let mut all_obj_ids: Vec<String> = reader.lines().map(|l| l.unwrap()).collect();
    use rand::seq::SliceRandom;
    let mut rng = rand::rng();
    all_obj_ids.shuffle(&mut rng);
    let shuffled_file_name = "kowalski_obj_ids_shuffled.txt";
    let shuffled_file = File::create(shuffled_file_name).unwrap();
    let mut shuffled_writer = BufWriter::new(shuffled_file);
    for id in all_obj_ids.iter() {
        writeln!(shuffled_writer, "{}", id).unwrap();
    }
    shuffled_writer.flush().unwrap();
    count
}

async fn worker(
    receiver: async_channel::Receiver<String>,
    batch_size: usize,
    kowalski_uri: &str,
    boom_uri: &str,
) -> Result<usize> {
    let (kowalski_alert_collection, kowalski_obj_collection, _) =
        get_kowalski_collections(kowalski_uri).await;
    let boom_obj_collection = get_boom_obj_collection(boom_uri).await;

    let mut obj_ids = Vec::with_capacity(batch_size);
    let mut total_processed = 0;

    while let Ok(obj_id) = receiver.recv().await {
        obj_ids.push(obj_id);

        if obj_ids.len() >= batch_size {
            // TODO: process obj_ids to boom objects
            let boom_objs = make_boom_objs_from_kowalski(
                &obj_ids,
                &kowalski_obj_collection,
                &kowalski_alert_collection,
            )
            .await;
            insert_boom_objects(&boom_objs, &boom_obj_collection).await;
            obj_ids.clear();
            total_processed += batch_size;
        }
    }

    if !obj_ids.is_empty() {
        // Process remaining documents
        let boom_objs = make_boom_objs_from_kowalski(
            &obj_ids,
            &kowalski_obj_collection,
            &kowalski_alert_collection,
        )
        .await;
        insert_boom_objects(&boom_objs, &boom_obj_collection).await;
    }
    Ok(total_processed)
}

#[tokio::main]
async fn main() {
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let args = Cli::parse();
    let n_processes = args.n_processes.unwrap_or(1);
    let batch_size = args.batch_size.unwrap_or(100);
    let kowalski_uri = args.kowalski_uri;
    let boom_uri = args.boom_uri;
    let retrieve_obj_ids = args.retrieve_obj_ids.unwrap_or(false);

    let obj_ids_file = "kowalski_obj_ids_shuffled.txt".to_string();
    let count = match retrieve_obj_ids {
        true => {
            info!("Retrieving object IDs from Kowalski");
            let (_, _, kowalski_obj_collection_idonly) =
                get_kowalski_collections(&kowalski_uri).await;
            let start = Instant::now();
            let count = get_all_kowalski_obj_ids(&kowalski_obj_collection_idonly).await;
            let duration = start.elapsed();
            info!("Retrieved and saved {} object ids in {:?}", count, duration);
            count
        }
        false => {
            // count the number of lines in the obj_ids_file
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

    let channel_capacity = n_processes * batch_size * 2;

    let (s, r) = async_channel::bounded(channel_capacity);
    let mut workers = Vec::new();
    for _ in 0..n_processes {
        let worker_receiver = r.clone();
        let batch_size = batch_size;
        let kowalski_uri = kowalski_uri.clone();
        let boom_uri = boom_uri.clone();
        let worker_handle = tokio::spawn(async move {
            worker(worker_receiver, batch_size, &kowalski_uri, &boom_uri).await
        });
        workers.push(worker_handle);
    }

    // let's process the kowalski obj ids file in chunks of 1000
    // instead of loading all into memory, we read line by line and collect into a buffer
    let file = File::open(&obj_ids_file).unwrap();
    let reader = std::io::BufReader::new(file);

    let progress_bar = ProgressBar::new(count as u64)
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
