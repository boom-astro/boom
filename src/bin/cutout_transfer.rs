// Parallelized Rust version: Transfer cutouts from ZTF_alert_cutouts to ZTF_alerts_cutouts
// Uses multiple tokio tasks to speed up the transfer

use anyhow::Result;
use boom::{alert::AlertCutout, utils::o11y::logging::build_subscriber};
use clap::Parser;
use futures::stream::StreamExt;
use indicatif::ProgressBar;
use mongodb::{bson::doc, Client, Collection};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

#[derive(Parser)]
struct Cli {
    #[arg(long, help = "Boom MongoDB URI", env = "BOOM_MONGODB_URI")]
    boom_uri: String,
    #[arg(
        long,
        help = "Boom MongoDB database name",
        env = "BOOM_MONGODB_NAME",
        default_value = "boom_test_import"
    )]
    boom_db_name: String,
    #[arg(
        long,
        help = "Batch size for processing (default: 1000)",
        default_value = "1000"
    )]
    batch_size: usize,
    #[arg(
        long,
        help = "Number of concurrent workers (default: 10)",
        default_value = "10"
    )]
    workers: usize,
}

async fn transfer_cutouts_parallel(
    boom_uri: &str,
    boom_db_name: &str,
    batch_size: usize,
    num_workers: usize,
) -> Result<()> {
    let client = Client::with_uri_str(boom_uri).await?;
    let db = client.database(boom_db_name);

    let source_collection: Collection<AlertCutout> = db.collection("ZTF_alert_cutouts");
    let target_collection: Collection<AlertCutout> = db.collection("ZTF_alerts_cutouts");

    // Get total count for progress bar
    let total_count = source_collection.estimated_document_count().await?;
    info!(
        "Total documents to transfer: {} (using {} workers)",
        total_count, num_workers
    );

    let progress_bar = Arc::new(
        ProgressBar::new(total_count)
            .with_message("Transferring cutouts")
            .with_style(
                indicatif::ProgressStyle::default_bar()
                    .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta}) [{per_sec}]")
                    .unwrap(),
            ),
    );

    // Semaphore to limit concurrent tasks
    let semaphore = Arc::new(Semaphore::new(num_workers));

    // Shared collections for workers
    let target_collection = Arc::new(target_collection);

    let options = mongodb::options::InsertManyOptions::builder()
        .ordered(false) // Continue on duplicate key errors
        .build();

    // Read from source collection
    let mut cursor = source_collection.find(doc! {}).await?;
    let mut batch = Vec::with_capacity(batch_size);
    let mut tasks = Vec::new();

    let start = std::time::Instant::now();
    let mut total_read = 0;

    while let Some(result) = cursor.next().await {
        match result {
            Ok(cutout) => {
                batch.push(cutout);
                total_read += 1;

                if batch.len() >= batch_size {
                    // Acquire semaphore permit
                    let permit = semaphore.clone().acquire_owned().await?;
                    let batch_to_process =
                        std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                    let target_collection_clone = target_collection.clone();
                    let progress_bar_clone = progress_bar.clone();
                    let options_clone = options.clone();

                    // Spawn task to insert batch
                    let task = tokio::spawn(async move {
                        let batch_len = batch_to_process.len();
                        let result =
                            insert_batch(target_collection_clone, batch_to_process, options_clone)
                                .await;

                        progress_bar_clone.inc(batch_len as u64);
                        drop(permit); // Release semaphore
                        result
                    });

                    tasks.push(task);
                }
            }
            Err(e) => {
                error!("Error reading cutout: {}", e);
            }
        }
    }

    // Process remaining batch
    if !batch.is_empty() {
        let permit = semaphore.clone().acquire_owned().await?;
        let batch_len = batch.len();
        let target_collection_clone = target_collection.clone();
        let progress_bar_clone = progress_bar.clone();
        let options_clone = options.clone();

        let task = tokio::spawn(async move {
            let result = insert_batch(target_collection_clone, batch, options_clone).await;

            progress_bar_clone.inc(batch_len as u64);
            drop(permit);
            result
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut total_transferred = 0;
    let mut total_errors = 0;

    for task in tasks {
        match task.await {
            Ok(Ok((transferred, errors))) => {
                total_transferred += transferred;
                total_errors += errors;
            }
            Ok(Err(e)) => {
                error!("Task error: {}", e);
            }
            Err(e) => {
                error!("Task join error: {}", e);
            }
        }
    }

    let duration = start.elapsed();
    progress_bar.finish_with_message("Transfer complete");

    // Verification
    let source_count = source_collection.count_documents(doc! {}).await?;
    let target_count = target_collection.count_documents(doc! {}).await?;

    info!("\n=== Transfer Complete ===");
    info!("Duration: {:?}", duration);
    info!("Documents read: {}", total_read);
    info!("Documents transferred: {}", total_transferred);
    info!("Errors (non-duplicate): {}", total_errors);
    info!(
        "Throughput: {:.2} docs/sec",
        total_read as f64 / duration.as_secs_f64()
    );
    info!("\n=== Verification ===");
    info!("Source collection count: {}", source_count);
    info!("Target collection count: {}", target_count);

    Ok(())
}

async fn insert_batch(
    collection: Arc<Collection<AlertCutout>>,
    batch: Vec<AlertCutout>,
    options: mongodb::options::InsertManyOptions,
) -> Result<(usize, usize)> {
    let batch_len = batch.len();

    match collection.insert_many(batch).with_options(options).await {
        Ok(result) => Ok((result.inserted_ids.len(), 0)),
        Err(e) => {
            match *e.kind {
                mongodb::error::ErrorKind::InsertMany(ref insert_error) => {
                    let mut errors = 0;
                    // Count non-duplicate errors
                    if let Some(ref write_errors) = insert_error.write_errors {
                        for write_error in write_errors {
                            if write_error.code != 11000 {
                                error!("Error inserting cutout: {:?}", write_error);
                                errors += 1;
                            }
                        }
                    }

                    let transferred = batch_len - errors;

                    Ok((transferred, errors))
                }
                _ => {
                    error!("Error inserting batch: {}", e);
                    Ok((0, batch_len))
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let args = Cli::parse();

    info!("Starting cutout transfer...");
    info!("Source collection: ZTF_alert_cutouts");
    info!("Target collection: ZTF_alerts_cutouts");
    info!("Database: {}", args.boom_db_name);
    info!("Batch size: {}", args.batch_size);
    info!("Workers: {}", args.workers);

    transfer_cutouts_parallel(
        &args.boom_uri,
        &args.boom_db_name,
        args.batch_size,
        args.workers,
    )
    .await?;

    println!("\nTransfer complete!");
    println!("To drop the source collection after verification, run:");
    println!(
        "  mongosh \"{}\" --eval 'db.ZTF_alert_cutouts.drop()'",
        args.boom_uri
    );

    Ok(())
}
