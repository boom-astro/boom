use boom::{
    kafka::{AlertConsumer, DecamAlertConsumer, LsstAlertConsumer, ZtfAlertConsumer},
    utils::{
        enums::{ProgramId, Survey},
        o11y::{
            logging::{build_subscriber, log_error, WARN},
            metrics::init_metrics,
        },
    },
};

use chrono::{NaiveDate, NaiveDateTime};
use clap::Parser;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tracing::{error, info, instrument};
use uuid::Uuid;

#[derive(Parser)]
struct Cli {
    /// Survey to consume alerts from
    #[arg(value_enum)]
    survey: Survey,

    /// UTC date for which we want to consume alerts, with format YYYYMMDD
    /// [default: yesterday's date]
    #[arg(value_parser = parse_date)]
    date: Option<NaiveDate>, // Easier to deal with the default value after clap

    /// ID of the program to consume the alerts (ZTF-only)
    #[arg(default_value_t, value_enum)]
    program_id: ProgramId,

    /// Path to the configuration file
    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Number of processes to use to read the Kafka stream in parallel
    #[arg(long, default_value_t = 1)]
    processes: usize,

    /// Clear the in-memory (Valkey) queue of alerts already consumed from Kafka
    #[arg(long)]
    clear: bool,

    /// Set a maximum number of alerts to hold in memory (Valkey), default is
    /// 15000
    #[arg(long, value_name = "MAX", default_value_t = 15000)]
    max_in_queue: usize,

    /// Simulated mode (for testing purposes, LSST only)
    #[arg(long, default_value_t = false)]
    simulated: bool,

    /// UUID associated with this instance of the consumer, generated
    /// automatically if not provided
    #[arg(long, env = "BOOM_CONSUMER_INSTANCE_ID")]
    instance_id: Option<Uuid>,

    /// Name of the environment where this instance is deployed
    #[arg(long, env = "BOOM_DEPLOYMENT_ENV", default_value = "dev")]
    deployment_env: String,
}

fn parse_date(s: &str) -> Result<NaiveDate, String> {
    let date =
        NaiveDate::parse_from_str(s, "%Y%m%d").map_err(|_| "expected a date in YYYYMMDD format")?;
    Ok(date)
}

#[instrument(skip_all, fields(survey = %args.survey))]
async fn run(args: Cli, meter_provider: SdkMeterProvider) {
    let timestamp = NaiveDateTime::from(args.date.unwrap_or_else(|| {
        chrono::Utc::now()
            .date_naive()
            .pred_opt()
            .expect("previous date is not representable")
    }))
    .and_utc()
    .timestamp();

    match args.survey {
        Survey::Ztf => {
            let consumer = ZtfAlertConsumer::new(None, Some(args.program_id));
            if args.clear {
                let _ = consumer.clear_output_queue(&args.config);
            }
            match consumer
                .consume(
                    timestamp,
                    &args.config,
                    false,
                    Some(args.processes),
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
        Survey::Lsst => {
            let consumer = LsstAlertConsumer::new(None, args.simulated);
            if args.clear {
                let _ = consumer.clear_output_queue(&args.config);
            }
            match consumer
                .consume(
                    timestamp,
                    &args.config,
                    false,
                    Some(args.processes),
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
        Survey::Decam => {
            let consumer = DecamAlertConsumer::new(None);
            if args.clear {
                let _ = consumer.clear_output_queue(&args.config);
            }
            match consumer
                .consume(
                    timestamp,
                    &args.config,
                    false,
                    Some(args.processes),
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
