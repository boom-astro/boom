use boom::{
    conf,
    scheduler::{get_num_workers, ThreadPool},
    utils::{
        db::initialize_survey_indexes, enums::Survey, o11y::build_subscriber, worker::WorkerType,
    },
};

use std::time::Duration;

use clap::Parser;
use tokio::sync::oneshot;
use tracing::{info, info_span, instrument, warn, Instrument};

#[derive(Parser)]
struct Cli {
    #[arg(
        value_enum,
        required = true,
        help = "Name of stream/survey to process alerts for."
    )]
    survey: Survey,

    #[arg(long, value_name = "FILE", help = "Path to the configuration file")]
    config: Option<String>,
}

#[instrument(skip_all, fields(survey = %args.survey))]
async fn run(args: Cli) {
    let default_config_path = "config.yaml".to_string();
    let config_path = args.config.unwrap_or_else(|| {
        warn!("no config file provided, using {}", default_config_path);
        default_config_path
    });
    let config = conf::load_config(&config_path).expect("could not load config file");

    // get num workers from config file
    let n_alert = get_num_workers(&config, &args.survey, "alert")
        .expect("could not retrieve number of alert workers");
    let n_ml = get_num_workers(&config, &args.survey, "ml")
        .expect("could not retrieve number of ml workers");
    let n_filter = get_num_workers(&config, &args.survey, "filter")
        .expect("could not retrieve number of filter workers");

    // initialize the indexes for the survey
    let db: mongodb::Database = conf::build_db(&config)
        .await
        .expect("could not create mongodb client");
    initialize_survey_indexes(&args.survey, &db)
        .await
        .expect("could not initialize indexes");

    // Spawn sigint handler task
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(
        async {
            info!("wating for ctrl-c");
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
    let ml_pool = ThreadPool::new(
        WorkerType::ML,
        n_ml as usize,
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
    drop(ml_pool);
    drop(filter_pool);
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");
    run(args).await;
}
