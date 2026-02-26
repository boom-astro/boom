use boom::conf::{load_dotenv, AppConfig};
use boom::kafka::ConsumerError;
use boom::utils::{
    enums::Survey,
    o11y::logging::{as_error, build_subscriber, log_error},
};
use clap::Parser;
use flate2::read::GzDecoder;
use indicatif::ProgressBar;
use redis::AsyncCommands;
use std::fs::File;
use std::io::Read;
use tar::Archive;
use tracing::info;

#[derive(Parser)]
struct Cli {
    /// Survey to consume alerts from
    #[arg(value_enum)]
    survey: Survey,

    /// Path to the tarball file containing alerts
    #[arg(help = "Path to the .tar.gz file containing alerts")]
    path: String,

    /// Path to the configuration file
    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Clear the in-memory (Valkey) queue of alerts already consumed from Kafka
    #[arg(long)]
    clear: bool,

    /// Set a maximum number of alerts to hold in memory (Valkey), default is
    /// 15000
    #[arg(long, value_name = "MAX", default_value_t = 15000)]
    max_in_queue: usize,

    /// Name of the environment where this instance is deployed
    #[arg(long, env = "BOOM_DEPLOYMENT_ENV", default_value = "dev")]
    deployment_env: String,

    /// Show loading bar (requires counting files in tarball, which can be slow)
    #[arg(long)]
    show_loading_bar: bool,
}

async fn check_max_in_queue(
    con: &mut redis::aio::MultiplexedConnection,
    output_queue: &str,
    max_in_queue: usize,
) -> Result<(), ConsumerError> {
    if max_in_queue > 0 {
        loop {
            let nb_in_queue = con
                .llen::<&str, usize>(output_queue)
                .await
                .inspect_err(as_error!("failed to get queue length"))?;
            if nb_in_queue >= max_in_queue {
                info!(
                    "{} (limit: {}) items in queue, sleeping...",
                    nb_in_queue, max_in_queue
                );
                tokio::time::sleep(core::time::Duration::from_millis(1000)).await;
                continue;
            }
            break;
        }
    }
    Ok(())
}

// write a function that counts the number of .avro files in a tar.gz file and returns that number, without extracting them to disk
// or reading the contents of the .avro files into memory
async fn count_files_in_tarball(path: &str, extension: &str) -> Result<usize, ConsumerError> {
    // Open the tar.gz file
    let file = File::open(path)?;

    // Create a gzip decoder
    let decoder = GzDecoder::new(file);

    // Create a tar archive reader
    let mut archive = Archive::new(decoder);

    let mut total = 0;
    // Iterate over entries in the tarball
    for entry in archive.entries()? {
        let entry = entry?;

        // Get the file path within the tarball
        let path = entry.path()?;

        // Check if it's an .avro file
        if path.extension().and_then(|s| s.to_str()) == Some(extension) {
            total += 1;
        }
    }

    Ok(total)
}

async fn process_tarball(
    path: &str,
    survey: &Survey,
    max_in_queue: usize,
    clear: bool,
    show_loading_bar: bool,
    config: &AppConfig,
) -> Result<usize, ConsumerError> {
    let output_queue = format!("{}_alerts_packets_queue", survey.to_string().to_uppercase());

    let mut con = config
        .build_redis()
        .await
        .inspect_err(as_error!("failed to connect to Redis"))?;

    if clear {
        let _: () = con
            .del::<&str, ()>(&output_queue)
            .await
            .inspect_err(as_error!("failed to clear output queue"))?;
        info!("Cleared Redis queue: {}", output_queue);
    }

    if max_in_queue > 0 {
        check_max_in_queue(&mut con, &output_queue, max_in_queue).await?;
    }

    let progress_bar = match show_loading_bar {
        true => {
            info!("Counting files in tarball... (this may take a while, runs only when loading bar is shown)");
            let total_files = count_files_in_tarball(path, "avro")
                .await
                .inspect_err(as_error!("failed to count files in tarball"))?;

            Some(ProgressBar::new(total_files as u64)
                .with_message(format!("Pushing alerts to Redis queue: {}", output_queue))
                .with_style(
                    indicatif::ProgressStyle::default_bar()
                    .template("{spinner:.green} {msg} {wide_bar} [{elapsed_precise}] {human_pos}/{human_len} ({eta}) {per_sec}").unwrap()
                )).unwrap()
        }
        false => ProgressBar::hidden(),
    };

    // Open the tar.gz file
    let file = File::open(path).inspect_err(as_error!("failed to open tarball file: {}", path))?;

    // Create a gzip decoder
    let decoder = GzDecoder::new(file);

    // Create a tar archive reader
    let mut archive = Archive::new(decoder);

    let mut count = 0;
    // Iterate over entries in the tarball
    for entry in archive.entries()? {
        let mut entry = entry?;

        // Get the file path within the tarball
        let path = entry.path()?;

        // Check if it's an .avro file and doesn't start with "._" (to skip hidden files created by macOS)
        if path.extension().and_then(|s| s.to_str()) == Some("avro")
            && !path
                .file_name()
                .and_then(|s| s.to_str())
                .map_or(false, |s| s.starts_with("._"))
        {
            // Read the file contents as bytes
            let mut buffer = Vec::new();
            entry.read_to_end(&mut buffer)?;

            // Push the bytes to the Redis queue
            let _ = con
                .rpush::<&str, Vec<u8>, usize>(&output_queue, buffer.to_vec())
                .await
                .inspect_err(|error| {
                    log_error!(error, "failed to push message to queue");
                })?;

            count += 1;
            progress_bar.inc(1);

            if count % 1000 == 0 {
                if !show_loading_bar {
                    info!("Pushed {} alerts to Redis queue: {}", count, output_queue);
                }
                if max_in_queue > 0 {
                    check_max_in_queue(&mut con, &output_queue, max_in_queue).await?;
                }
            }
        }
    }

    Ok(count)
}

#[tokio::main]
async fn main() -> Result<(), ConsumerError> {
    // Load environment variables from .env file before anything else
    load_dotenv();

    let args = Cli::parse();
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let survey = args.survey;
    let config = boom::conf::load_config(Some(&args.config)).expect("Failed to load config");

    let start = std::time::Instant::now();
    let total = process_tarball(
        &args.path,
        &survey,
        args.max_in_queue,
        args.clear,
        args.show_loading_bar,
        &config,
    )
    .await
    .inspect_err(as_error!("failed to process tarball"))?;
    let duration = start.elapsed();
    info!(
        "Total alerts pushed to Redis queue: {} in {:.2?}",
        total, duration
    );
    Ok(())
}
