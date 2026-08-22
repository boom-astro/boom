use boom::{
    conf::load_dotenv,
    kafka::{
        AlertConsumer, DecamAlertConsumer, LsstAlertConsumer, StartDate, WinterAlertConsumer,
        ZtfAlertConsumer, MAX_CATCH_UP_DAYS,
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

// `run` deliberately is NOT `#[instrument]`'d. It runs for the entire lifetime
// of the consumer; wrapping it in a single span would make every per-batch /
// per-alert child span a descendant of the same root span, producing a single
// trace that grows unboundedly until Tempo rejects it. The survey is already
// captured in the OTel `service.name` resource attribute, so it doesn't need
// to be a span field here.
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
    let replay = args.on.is_some();
    let exit_on_eof = args.exit_on_eof;

    let date_label = chrono::DateTime::from_timestamp(start.timestamp(), 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_default();
    if start.catch_up_days() > MAX_CATCH_UP_DAYS {
        error!(
            "Catching up from {} would subscribe to {} nights, over the {}-day limit; \
             use --on to read that night alone",
            date_label,
            start.catch_up_days(),
            MAX_CATCH_UP_DAYS
        );
        return;
    }

    info!(
        "Consuming {} alerts (date {}, replay: {}, exit on EOF: {})",
        args.survey, date_label, replay, exit_on_eof
    );

    // If topic override is provided, use it. Otherwise, the consumer
    // will determine the topic based on the survey, program ID, and date.
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
                    start,
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
                    start,
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
                    start,
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
                    start,
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
