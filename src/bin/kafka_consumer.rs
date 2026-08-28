use boom::{
    conf::load_dotenv,
    kafka::{
        AlertConsumer, DecamAlertConsumer, LsstAlertConsumer, StartDate, WinterAlertConsumer,
        WiseAlertConsumer, ZtfAlertConsumer,
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
use clap::{ArgGroup, Parser};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Parser)]
#[command(group(ArgGroup::new("start")))]
struct Cli {
    /// Survey to consume alerts from
    #[arg(value_enum)]
    survey: Survey,

    /// UTC date (YYYYMMDD) to catch up from, rolling onto each new night's
    /// topic and never exiting [default: today]
    #[arg(long, group = "start", value_name = "DATE", value_parser = parse_date)]
    from: Option<NaiveDateTime>,

    /// UTC date (YYYYMMDD) to replay on its own, without rolling onto new
    /// nights. Runs in a per-date consumer group and commits nothing, so
    /// production consumers are untouched and the night stays replayable
    #[arg(long, group = "start", value_name = "DATE", value_parser = parse_date)]
    on: Option<NaiveDateTime>,

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

    /// Exit once the replayed topic(s) are drained, instead of staying up
    #[arg(long, requires = "on", conflicts_with = "from")]
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

async fn consume_with<C: AlertConsumer>(
    consumer: C,
    args: &Cli,
    start: StartDate,
    date_label: &str,
) {
    info!(
        "Consuming {} alerts (date {}, replay: {}, exit on EOF: {})",
        args.survey,
        date_label,
        args.on.is_some(),
        args.exit_on_eof
    );

    if args.clear {
        let _ = consumer.clear_output_queue(&args.config).await;
    }
    match consumer
        .consume(
            args.topics_override.clone(),
            start,
            None,
            Some(args.processes),
            Some(args.max_in_queue),
            args.exit_on_eof,
            &args.config,
        )
        .await
    {
        Ok(_) => info!("Successfully consumed alerts"),
        Err(e) => error!("Failed to consume alerts: {}", e),
    };
}

// No `#[instrument]`: `run` lives as long as the process, so one wrapping span
// would grow a single trace until Tempo rejects it.
async fn run(
    args: Cli,
    meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<SdkTracerProvider>,
) {
    let start = match (args.from, args.on) {
        (_, Some(date)) => StartDate::Pinned(date.and_utc().timestamp()),
        (Some(date), _) => StartDate::From(date.and_utc().timestamp()),
        _ => StartDate::Current,
    };
    let date_label = chrono::DateTime::from_timestamp(start.timestamp(), 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_default();

    match args.survey {
        Survey::Ztf => {
            let consumer = ZtfAlertConsumer::new(None, Some(args.programids.clone()));
            consume_with(consumer, &args, start, &date_label).await
        }
        Survey::Lsst => {
            let consumer = LsstAlertConsumer::new(None, args.simulated);
            consume_with(consumer, &args, start, &date_label).await
        }
        Survey::Decam => {
            consume_with(DecamAlertConsumer::new(None), &args, start, &date_label).await
        }
        Survey::Winter => {
            consume_with(WinterAlertConsumer::new(None), &args, start, &date_label).await
        }
        Survey::Wise => consume_with(WiseAlertConsumer::new(None), &args, start, &date_label).await,
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
    // Load environment variables from .env file before anything else
    load_dotenv();

    let args = Cli::parse();

    let instance_id = args.instance_id.unwrap_or_else(Uuid::new_v4);
    // Match the Compose service name (consumer-ztf, consumer-lsst, ...) so
    // Grafana can correlate traces, logs, and metrics on a single label.
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
