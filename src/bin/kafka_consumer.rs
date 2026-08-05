use boom::{
    conf::load_dotenv,
    kafka::{
        AlertConsumer, DecamAlertConsumer, LsstAlertConsumer, WinterAlertConsumer, ZtfAlertConsumer,
    },
    utils::{
        enums::{ProgramId, Survey},
        o11y::{
            logging::{build_subscriber_with_otel, log_error, WARN},
            metrics::init_metrics,
            tracing::init_tracing,
        },
        parser::parse_positive_usize,
    },
};

use chrono::{NaiveDate, NaiveDateTime};
use clap::Parser;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::{error, info};
use uuid::Uuid;

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, PartialEq)]
enum DateMode {
    /// Consume `date` and every night after it, rolling over to each new
    /// topic as it appears, and never exit
    #[default]
    From,
    /// Consume only the topic(s) for `date`, then exit. Offsets are not
    /// committed, so a night can be replayed as often as needed
    Single,
}

#[derive(Parser)]
struct Cli {
    /// Survey to consume alerts from
    #[arg(value_enum)]
    survey: Survey,

    /// UTC date for which we want to consume alerts, with format YYYYMMDD
    /// [default: today's date at 00:00:00 UTC]
    #[arg(value_parser = parse_date)]
    date: Option<NaiveDateTime>, // Easier to deal with the default value after clap

    /// Whether `date` is a starting point ("from", the default) or the only
    /// date to consume ("single")
    #[arg(long, value_enum, default_value_t = DateMode::From)]
    date_mode: DateMode,

    /// ID(s) of the program(s) to consume the alerts (ZTF-only). Defaults to "public" program if not specified (e.g. --programids public,partnership,caltech).
    #[arg(long, value_enum, value_delimiter = ',', default_value = "public")]
    programids: Vec<ProgramId>,

    /// Path to the configuration file
    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Number of processes to use to read the Kafka stream in parallel
    #[arg(long, default_value_t = 1, value_parser = parse_positive_usize)]
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

    /// Exit on end of file (for testing purposes)
    /// Not used in production
    #[arg(long, default_value_t = false)]
    exit_on_eof: bool,

    /// Override the topic name(s) (useful if data has been produced to a non-default topic)
    #[arg(long, value_name = "TOPICS")]
    topics_override: Option<Vec<String>>,

    /// Name of the environment where this instance is deployed
    #[arg(long, env = "BOOM_DEPLOYMENT_ENV", default_value = "dev")]
    deployment_env: String,
}

fn parse_date(s: &str) -> Result<NaiveDateTime, String> {
    let date =
        NaiveDate::parse_from_str(s, "%Y%m%d").map_err(|_| "expected a date in YYYYMMDD format")?;
    Ok(date.and_hms_opt(0, 0, 0).unwrap())
}

// No `#[instrument]`: one span for the process lifetime would grow until Tempo
// rejects the trace. The survey is already in the `service.name` attribute.
async fn run(
    args: Cli,
    meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<SdkTracerProvider>,
) {
    let date = args.date.unwrap_or_else(|| {
        let today = chrono::Utc::now().naive_utc().date();
        today.and_hms_opt(0, 0, 0).unwrap()
    });
    let timestamp = date.and_utc().timestamp();

    // `single` reuses the one-shot drain path, ungated on the environment since
    // it commits nothing.
    let exit_on_eof = match args.date_mode {
        DateMode::Single => true,
        DateMode::From => args.deployment_env == "dev" && args.exit_on_eof,
    };
    info!(
        "Consuming {} alerts from {} ({:?} mode)",
        args.survey,
        date.format("%Y-%m-%d"),
        args.date_mode
    );

    let topics = args.topics_override;

    match args.survey {
        Survey::Ztf => {
            let consumer = ZtfAlertConsumer::new(None, Some(args.programids));
            if args.clear {
                let _ = consumer.clear_output_queue(&args.config).await;
            }
            match consumer
                .consume(
                    topics,
                    timestamp,
                    None,
                    Some(args.processes),
                    Some(args.max_in_queue),
                    exit_on_eof,
                    &args.config,
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
                let _ = consumer.clear_output_queue(&args.config).await;
            }
            match consumer
                .consume(
                    topics,
                    timestamp,
                    None,
                    Some(args.processes),
                    Some(args.max_in_queue),
                    exit_on_eof,
                    &args.config,
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
                let _ = consumer.clear_output_queue(&args.config).await;
            }
            match consumer
                .consume(
                    topics,
                    timestamp,
                    None,
                    Some(args.processes),
                    Some(args.max_in_queue),
                    exit_on_eof,
                    &args.config,
                )
                .await
            {
                Ok(_) => info!("Successfully consumed alerts"),
                Err(e) => error!("Failed to consume alerts: {}", e),
            };
        }
        Survey::Winter => {
            let consumer = WinterAlertConsumer::new(None);
            if args.clear {
                let _ = consumer.clear_output_queue(&args.config).await;
            }
            match consumer
                .consume(
                    topics,
                    timestamp,
                    None,
                    Some(args.processes),
                    Some(args.max_in_queue),
                    exit_on_eof,
                    &args.config,
                )
                .await
            {
                Ok(_) => info!("Successfully consumed alerts"),
                Err(e) => error!("Failed to consume alerts: {}", e),
            };
        }
    }

    if let Some(meter_provider) = meter_provider {
        if let Err(error) = meter_provider.shutdown() {
            log_error!(WARN, error, "failed to shut down the meter provider");
        }
    }
    if let Some(tracer_provider) = tracer_provider {
        if let Err(error) = tracer_provider.shutdown() {
            log_error!(WARN, error, "failed to shut down the tracer provider");
        }
    }
}

#[tokio::main]
async fn main() {
    load_dotenv();

    let args = Cli::parse();

    let instance_id = args.instance_id.unwrap_or_else(Uuid::new_v4);
    // Matches the Compose service name so Grafana can correlate on one label.
    let service_name = format!("consumer-{}", args.survey.to_string().to_lowercase());
    let tracer_provider = init_tracing(
        service_name.clone(),
        instance_id,
        args.deployment_env.clone(),
    )
    .expect("failed to initialize tracing");

    let (subscriber, _guard) = build_subscriber_with_otel(tracer_provider.as_ref(), &service_name)
        .expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let meter_provider = init_metrics(service_name, instance_id, args.deployment_env.clone())
        .expect("failed to initialize metrics");

    run(args, meter_provider, tracer_provider).await;
}
