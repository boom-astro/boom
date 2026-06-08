#[cfg(target_os = "linux")]
use boom::utils::gpu::validate_gpu_configuration_for_survey;
use boom::{
    conf::{load_dotenv, AppConfig},
    enrichment::models::SharedModelPool,
    scheduler::{record_worker_pool_state, ThreadPool},
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
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tokio::sync::oneshot;
use tracing::{info, info_span, instrument, warn, Instrument};
use uuid::Uuid;

/// Sample one aux record at random and warn if it's missing crossmatches for
/// any catalog declared under `crossmatch.<survey>` in the config. The live
/// pipeline only crossmatches at first insert, so newly added catalogs never
/// reach pre-existing records — the user has to run `reprocess_crossmatch`.
async fn warn_if_missing_crossmatches(survey: &Survey, db: &mongodb::Database, config: &AppConfig) {
    let configured = match config.crossmatch.get(survey) {
        Some(v) if !v.is_empty() => v,
        _ => return,
    };
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));

    let mut cursor = match aux_collection
        .aggregate(vec![
            doc! { "$sample": { "size": 1 } },
            doc! { "$project": { "_id": 1, "cross_matches": 1 } },
        ])
        .await
    {
        Ok(c) => c,
        Err(e) => {
            warn!(survey = %survey, error = %e, "crossmatch coverage check: failed to sample");
            return;
        }
    };
    let sample = match cursor.try_next().await {
        Ok(Some(d)) => d,
        Ok(None) => return,
        Err(e) => {
            warn!(survey = %survey, error = %e, "crossmatch coverage check: failed to fetch sample");
            return;
        }
    };

    let object_id = sample.get_str("_id").unwrap_or("<unknown>").to_string();
    let cross_matches = sample.get_document("cross_matches").ok();
    let missing: Vec<&str> = configured
        .iter()
        .filter(|c| match cross_matches {
            Some(cm) => !cm.contains_key(&c.catalog),
            None => true,
        })
        .map(|c| c.catalog.as_str())
        .collect();

    if !missing.is_empty() {
        warn!(
            survey = %survey,
            sampled_object_id = %object_id,
            missing_catalogs = ?missing,
            "The configured catalogs `{}` are missing from the cross_matches of a random alerts_aux sample `{}`. \
             This may indicate that newly added catalogs have not been reprocessed for existing records. \
             The scheduler only crossmatches new alerts_aux, so existing objects \
             will not be updated with new catalogs. To populate the detected missing crossmatches \
             for existing records, run `reprocess_crossmatch --survey {} --catalogs {}` \
             with the appropriate processes and batch_size.",
            missing.join(", "),
            object_id,
            survey.to_string().to_lowercase(),
            missing.join(",")
        );
    }
}

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

    warn_if_missing_crossmatches(&args.survey, &db, &config).await;

    #[cfg(target_os = "linux")]
    validate_gpu_configuration_for_survey(&args.survey, &config)
        .expect("GPU configuration is invalid for the survey");

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

    // Load ONNX models at startup. When GPUs are enabled, create a pool of
    // shared model sets (one per device) to conserve VRAM — workers round-robin
    // across devices. When GPUs are disabled, pass None so each worker loads
    // its own private models on CPU (zero mutex contention).
    let shared_model_pool = if matches!(args.survey, Survey::Ztf) && config.gpu.enabled {
        Some(
            SharedModelPool::load(&config.gpu.device_ids)
                .expect("failed to load ONNX models on GPU"),
        )
    } else {
        None
    };

    let alert_pool = ThreadPool::new(
        WorkerType::Alert,
        n_alert as usize,
        args.survey.clone(),
        config_path.clone(),
        None,
    );
    let enrichment_pool = ThreadPool::new(
        WorkerType::Enrichment,
        n_enrichment as usize,
        args.survey.clone(),
        config_path.clone(),
        shared_model_pool,
    );
    let filter_pool = ThreadPool::new(
        WorkerType::Filter,
        n_filter as usize,
        args.survey.clone(),
        config_path,
        None,
    );

    let record_pool_metrics = || {
        record_worker_pool_state(
            &args.survey,
            "alert",
            alert_pool.live_worker_count(),
            alert_pool.total_worker_count(),
        );
        record_worker_pool_state(
            &args.survey,
            "enrichment",
            enrichment_pool.live_worker_count(),
            enrichment_pool.total_worker_count(),
        );
        record_worker_pool_state(
            &args.survey,
            "filter",
            filter_pool.live_worker_count(),
            filter_pool.total_worker_count(),
        );
    };

    // Emit an initial sample so dashboards show running workers immediately.
    record_pool_metrics();

    // Wait for shutdown signal, logging heartbeat every 60 seconds with live worker counts
    let mut shutdown_rx = shutdown_rx;
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                record_pool_metrics();
                info!(
                    alert = %format!("{}/{}", alert_pool.live_worker_count(), alert_pool.total_worker_count()),
                    enrichment = %format!("{}/{}", enrichment_pool.live_worker_count(), enrichment_pool.total_worker_count()),
                    filter = %format!("{}/{}", filter_pool.live_worker_count(), filter_pool.total_worker_count()),
                    "heartbeat: workers running"
                );
            }
        }
    }

    // Shut down:
    info!("shutting down");
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
