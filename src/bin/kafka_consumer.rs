use boom::{
    kafka::{AlertConsumer, DecamAlertConsumer, LsstAlertConsumer, ZtfAlertConsumer},
    utils::{
        enums::ProgramId,
        o11y::{
            logging::{build_subscriber, log_error, WARN},
            metrics::init_metrics,
        },
    },
};

use chrono::{NaiveDate, NaiveDateTime};
use clap::{Args, Parser, Subcommand};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tracing::{error, info, instrument};
use uuid::Uuid;

#[derive(Parser)]
struct Cli {
    /// Path to the configuration file
    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Number of threads to use to read the Kafka stream in parallel
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Clear the in-memory (Valkey) queue of alerts already consumed from Kafka
    #[arg(long)]
    clear: bool,

    /// Set a maximum number of alerts to hold in memory (Valkey), default is
    /// 15000
    #[arg(long, value_name = "MAX", default_value_t = 15000)]
    max_in_queue: usize,

    /// UUID associated with this instance of the consumer, generated
    /// automatically if not provided
    #[arg(long, env = "BOOM_CONSUMER_INSTANCE_ID")]
    instance_id: Option<Uuid>,

    /// Name of the environment where this instance is deployed
    #[arg(long, env = "BOOM_DEPLOYMENT_ENV", default_value = "dev")]
    deployment_env: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Consume alerts from DECam
    Decam(DecamArgs),

    /// Consume alerts from LSST
    Lsst(LsstArgs),

    /// Consume alerts from ZTF
    Ztf(ZtfArgs),
}

#[derive(Args)]
struct DecamArgs {
    /// UTC date for which we want to consume alerts, with format YYYYMMDD or
    /// YYYY-MM-DD
    #[arg(long, value_parser = parse_date, default_value_t = default_date())]
    date: NaiveDate,
}

#[derive(Args)]
struct LsstArgs {
    /// UTC date for which we want to consume alerts, with format YYYYMMDD or
    /// YYYY-MM-DD
    #[arg(long, value_parser = parse_date, default_value_t = default_date())]
    date: NaiveDate,
}

#[derive(Args)]
struct ZtfArgs {
    /// UTC date for which we want to consume alerts, with format YYYYMMDD or
    /// YYYY-MM-DD
    #[arg(long, value_parser = parse_date, default_value_t = default_date())]
    date: NaiveDate,

    /// ID of the program to consume the alerts (ZTF-only)
    #[arg(long, default_value_t, value_enum)]
    program_id: ProgramId,
}

fn parse_date(s: &str) -> Result<NaiveDate, String> {
    let date = NaiveDate::parse_from_str(s, "%Y%m%d")
        .or_else(|_| NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .map_err(|_| "expected a date in YYYYMMDD or YYYY-MM-DD format")?;
    Ok(date)
}

fn default_date() -> NaiveDate {
    chrono::Utc::now()
        .date_naive()
        .pred_opt()
        .expect("previous date is not representable")
}

fn to_utc_timestamp(date: NaiveDate) -> i64 {
    NaiveDateTime::from(date).and_utc().timestamp()
}

#[instrument(skip_all)]
async fn run(args: Cli, meter_provider: SdkMeterProvider) {
    match args.command {
        Commands::Ztf(sub_args) => {
            let consumer = ZtfAlertConsumer::new(None, Some(sub_args.program_id));
            if args.clear {
                let _ = consumer.clear_output_queue(&args.config);
            }
            match consumer
                .consume(
                    to_utc_timestamp(sub_args.date),
                    &args.config,
                    false,
                    Some(args.threads),
                    Some(args.max_in_queue),
                    None,
                    None,
                )
                .await
            {
                Ok(_) => info!("Successfully consumed alerts"),
                Err(e) => error!("Failed to consume alerts: {}", e),
            };
        }
        Commands::Lsst(sub_args) => {
            // TODO: let the user specify if they want to consume real or simulated LSST data
            let simulated = true;
            let consumer = LsstAlertConsumer::new(None, simulated);
            if args.clear {
                let _ = consumer.clear_output_queue(&args.config);
            }
            match consumer
                .consume(
                    to_utc_timestamp(sub_args.date),
                    &args.config,
                    false,
                    Some(args.threads),
                    Some(args.max_in_queue),
                    None,
                    None,
                )
                .await
            {
                Ok(_) => info!("Successfully consumed alerts"),
                Err(e) => error!("Failed to consume alerts: {}", e),
            };
        }
        Commands::Decam(sub_args) => {
            let consumer = DecamAlertConsumer::new(None);
            if args.clear {
                let _ = consumer.clear_output_queue(&args.config);
            }
            match consumer
                .consume(
                    to_utc_timestamp(sub_args.date),
                    &args.config,
                    false,
                    Some(args.threads),
                    Some(args.max_in_queue),
                    None,
                    None,
                )
                .await
            {
                Ok(_) => info!("Successfully consumed alerts"),
                Err(e) => error!("Failed to consume alerts: {}", e),
            };
        }
    }

    if let Err(error) = meter_provider.shutdown() {
        log_error!(WARN, error, "failed to shut down the meter provider");
    }
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let instance_id = args.instance_id.unwrap_or_else(Uuid::new_v4);
    let meter_provider = init_metrics(
        String::from("consumer"),
        instance_id,
        args.deployment_env.clone(),
    )
    .expect("failed to initialize metrics");

    run(args, meter_provider).await;
}
