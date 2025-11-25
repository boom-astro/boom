use boom::{
    conf::{load_dotenv, AppConfig},
    scheduler::ThreadPool,
    utils::{
        db::initialize_survey_indexes,
        enums::Survey,
        o11y::{
            logging::{build_subscriber, log_error, WARN},
            metrics::init_metrics,
        },
        worker::WorkerType,
    },
};

use std::time::Duration;

use clap::Parser;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tokio::sync::oneshot;
use tracing::{info, info_span, instrument, warn, Instrument};
use uuid::Uuid;

#[derive(Parser)]
struct Cli {
    /// Name of stream/survey to process alerts for.
    #[arg(value_enum)]
    survey: Survey,

    /// Path to the configuration file
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// UUID associated with this instance of the scheduler, generated
    /// automatically if not provided
    #[arg(long, env = "BOOM_SCHEDULER_INSTANCE_ID")]
    instance_id: Option<Uuid>,

    /// Name of the environment where this instance is deployed
    #[arg(long, env = "BOOM_DEPLOYMENT_ENV", default_value = "dev")]
    deployment_env: String,
}

#[instrument(skip_all, fields(survey = %args.survey))]
async fn run(args: Cli, meter_provider: SdkMeterProvider) {
    let default_config_path = "config.yaml".to_string();
    let config_path = args.config.unwrap_or_else(|| {
        warn!("no config file provided, using {}", default_config_path);
        default_config_path
    });
    let config = AppConfig::from_path(&config_path).unwrap();

    // get num workers from config file
    let worker_config = config
        .workers
        .get(&args.survey)
        .expect("could not retrieve worker config for survey");
    let n_alert = worker_config.alert.n_workers;
    let n_enrichment = worker_config.enrichment.n_workers;
    let n_filter = worker_config.filter.n_workers;

    // initialize the indexes for the survey
    let db: mongodb::Database = config
        .build_db()
        .await
        .expect("could not create mongodb client");
    initialize_survey_indexes(&args.survey, &db)
        .await
        .expect("could not initialize indexes");

    // Spawn sigint handler task
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(
        async {
            info!("waiting for ctrl-c");
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for ctrl-c event");
            info!("received ctrl-c, sending shutdown signal");
            shutdown_tx
                .send(())
                .expect("failed to send shutdown signal, receiver disconnected");
        }
        .instrument(info_span!("sigint handler")),
    );

    let alert_pool = ThreadPool::new(
        WorkerType::Alert,
        n_alert as usize,
        args.survey.clone(),
        config_path.clone(),
    );
    let enrichment_pool = ThreadPool::new(
        WorkerType::Enrichment,
        n_enrichment as usize,
        args.survey.clone(),
        config_path.clone(),
    );
    let filter_pool = ThreadPool::new(
        WorkerType::Filter,
        n_filter as usize,
        args.survey,
        config_path,
    );

    // All that's left is to wait for sigint:
    let heartbeat_handle = tokio::spawn(
        async {
            loop {
                info!("heartbeat");
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
        .instrument(info_span!("heartbeat task")),
    );
    let _ = shutdown_rx
        .await
        .expect("failed to await shutdown signal, sender disconnected");

    // Shut down:
    info!("shutting down");
    heartbeat_handle.abort();
    drop(alert_pool);
    drop(enrichment_pool);
    drop(filter_pool);
    if let Err(error) = meter_provider.shutdown() {
        log_error!(WARN, error, "failed to shut down the meter provider");
    }
}

#[tokio::main]
async fn main() {
    // Load environment variables from .env file before anything else
    load_dotenv();

    let args = Cli::parse();

    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let instance_id = args.instance_id.unwrap_or_else(Uuid::new_v4);
    let meter_provider = init_metrics(
        String::from("scheduler"),
        instance_id,
        args.deployment_env.clone(),
    )
    .expect("failed to initialize metrics");

    run(args, meter_provider).await;
}
