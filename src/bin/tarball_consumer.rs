use boom::conf::{load_dotenv, AppConfig};
use boom::utils::{
    enums::Survey,
    o11y::logging::{as_error, build_subscriber, log_error},
};
use clap::Parser;
use flate2::read::GzDecoder;
use redis::AsyncCommands;
use std::fs::File;
use std::io::Read;
use tar::Archive;
use tracing::{debug, info};

#[derive(Parser)]
struct Cli {
    /// Survey to consume alerts from
    #[arg(value_enum)]
    survey: Survey,

    /// Path to the tarball file containing alerts
    #[arg(help = "Path to the tarball file containing alerts")]
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
}

async fn check_max_in_queue(
    con: &mut redis::aio::MultiplexedConnection,
    output_queue: &str,
    max_in_queue: usize,
) -> Result<(), anyhow::Error> {
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

async fn process_tarball(
    path: &str,
    survey: &Survey,
    max_in_queue: usize,
    config: &AppConfig,
) -> Result<usize, anyhow::Error> {
    let output_queue = format!("{}_alerts_packets_queue", survey.to_string().to_uppercase());

    let mut con = config
        .build_redis()
        .await
        .expect("Failed to connect to Redis");

    check_max_in_queue(&mut con, &output_queue, max_in_queue).await?;

    // Open the tar.gz file
    let file = File::open(path)?;

    // Create a gzip decoder
    let decoder = GzDecoder::new(file);

    // Create a tar archive reader
    let mut archive = Archive::new(decoder);

    let mut total = 0;
    // Iterate over entries in the tarball
    for entry in archive.entries()? {
        let mut entry = entry?;

        // Get the file path within the tarball
        let path = entry.path()?;

        // Check if it's an .avro file
        if path.extension().and_then(|s| s.to_str()) == Some("avro") {
            debug!("Processing: {:?}", path);

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

            total += 1;

            if total % 1000 == 0 {
                info!("Submitted {} alerts to Redis queue", total);
                check_max_in_queue(&mut con, &output_queue, max_in_queue).await?;
            }
        }
    }

    Ok(total)
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Load environment variables from .env file before anything else
    load_dotenv();

    let args = Cli::parse();
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let survey = args.survey;
    let config = boom::conf::load_config(Some(&args.config)).expect("Failed to load config");
    process_tarball(&args.path, &survey, args.max_in_queue, &config).await?;
    Ok(())
}
