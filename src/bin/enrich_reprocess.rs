//! Standalone enrichment-only reprocessing binary.
//!
//! Runs a pool of enrichment workers that read candids from a configurable
//! Redis input queue, compute ML scores and photometric properties, and write
//! results back to MongoDB — but do **not** forward processed alerts to the
//! filter queue, and have Babamul forcibly disabled regardless of the config.
//!
//! Designed for backfill / reprocessing scenarios (e.g. after importing
//! historical alerts from Kowalski via `stream_kowalski_alerts`).
//!
//! # Examples
//!
//!   enrich_reprocess ztf --config config.yaml \
//!     --input-queue ZTF_alerts_enrichment_queue_reprocess
//!
//!   # Override worker count:
//!   enrich_reprocess ztf --config config.yaml \
//!     --input-queue ZTF_alerts_enrichment_queue_reprocess \
//!     --n-workers 8

#[cfg(target_os = "linux")]
use boom::utils::gpu::validate_gpu_configuration_for_survey;
use boom::{
    conf::{load_dotenv, AppConfig},
    enrichment::{
        models::{SharedModelPool, SharedModels},
        EnrichmentWorker, EnrichmentWorkerError, LsstEnrichmentWorker, ZtfEnrichmentWorker,
    },
    utils::{
        enums::Survey,
        o11y::logging::{as_error, build_subscriber, INFO},
        worker::{should_terminate, WorkerCmd},
    },
};

use std::{num::NonZero, sync::Arc, thread, time::Duration};

use clap::Parser;
use redis::AsyncCommands;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, info_span, span, warn, Instrument};

#[derive(Parser)]
#[command(
    about = "Run enrichment workers for reprocessing without forwarding to the filter queue or Babamul"
)]
struct Cli {
    /// Name of stream/survey to process alerts for.
    #[arg(value_enum)]
    survey: Survey,

    /// Path to the configuration file.
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Number of enrichment workers to spawn.
    /// Overrides the value from the config file when set.
    #[arg(long)]
    n_workers: Option<usize>,

    /// Redis queue to read candids from.
    #[arg(long)]
    input_queue: String,
}

/// Like `run_enrichment_worker` but with two differences:
///   1. Babamul is forcibly disabled after worker creation.
///   2. Processed alerts are not forwarded to the filter queue.
#[tokio::main]
async fn run_enrich_only<T: EnrichmentWorker>(
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
    shared_models: Option<Arc<SharedModels>>,
    input_queue: String,
) -> Result<(), EnrichmentWorkerError> {
    debug!(?config_path);
    let mut worker = T::new(config_path, shared_models).await?;
    worker.disable_babamul();

    let config = AppConfig::from_path(config_path)?;
    let survey = T::survey();
    let worker_config = config
        .workers
        .get(&survey)
        .ok_or(EnrichmentWorkerError::WorkerConfigMissing(survey))?;

    let mut con = config.build_redis().await?;

    let command_interval = worker_config.command_interval;
    let mut command_check_countdown = command_interval;

    loop {
        if command_check_countdown == 0 {
            if should_terminate(&mut receiver) {
                break;
            }
            command_check_countdown = command_interval;
        }

        let candids: Vec<i64> = con
            .rpop::<&str, Vec<i64>>(&input_queue, NonZero::new(1000))
            .await?;

        if candids.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            command_check_countdown = 0;
            continue;
        }

        command_check_countdown = command_check_countdown.saturating_sub(candids.len());

        // Enrich alerts (writes ML scores + properties to MongoDB).
        // Return value is discarded: we intentionally do not push to any output queue.
        worker.process_alerts(&candids).await?;
    }

    Ok(())
}

async fn run(args: Cli) {
    let default_config_path = "config.yaml".to_string();
    let config_path = args.config.unwrap_or_else(|| {
        warn!("no config file provided, using {}", default_config_path);
        default_config_path
    });
    let config = AppConfig::from_path(&config_path).unwrap();

    let survey_worker_config = config
        .workers
        .get(&args.survey)
        .expect("could not retrieve worker config for survey");
    let n_enrichment = args
        .n_workers
        .unwrap_or(survey_worker_config.enrichment.n_workers);

    #[cfg(target_os = "linux")]
    validate_gpu_configuration_for_survey(&args.survey, &config)
        .expect("GPU configuration is invalid for the survey");

    let shared_model_pool: Option<Arc<SharedModelPool>> =
        if matches!(args.survey, Survey::Ztf) && config.gpu.enabled {
            Some(
                SharedModelPool::load(&config.gpu.device_ids)
                    .expect("failed to load ONNX models on GPU"),
            )
        } else {
            None
        };

    let input_queue = args.input_queue;

    info!(
        survey = %args.survey,
        n_workers = n_enrichment,
        input_queue = %input_queue,
        "starting enrichment-only reprocess workers (Babamul disabled, no filter forwarding)"
    );

    let mut worker_handles: Vec<(thread::JoinHandle<()>, mpsc::Sender<WorkerCmd>)> =
        Vec::with_capacity(n_enrichment);

    if !matches!(args.survey, Survey::Ztf | Survey::Lsst) {
        eprintln!(
            "error: enrichment-only reprocessing is not supported for survey {:?}",
            args.survey
        );
        std::process::exit(1);
    }

    for _ in 0..n_enrichment {
        let (sender, receiver) = mpsc::channel(1);
        let config_path_clone = config_path.clone();
        let input_queue_clone = input_queue.clone();
        let survey = args.survey.clone();
        let shared_models = shared_model_pool.as_ref().map(|pool| pool.next_model_set());

        let handle = thread::spawn(move || {
            let tid = std::thread::current().id();
            span!(INFO, "enrich-only worker", ?tid, ?survey).in_scope(|| {
                info!("starting enrichment-only worker");
                let result = match survey {
                    Survey::Ztf => run_enrich_only::<ZtfEnrichmentWorker>(
                        receiver,
                        &config_path_clone,
                        shared_models,
                        input_queue_clone,
                    ),
                    Survey::Lsst => run_enrich_only::<LsstEnrichmentWorker>(
                        receiver,
                        &config_path_clone,
                        shared_models,
                        input_queue_clone,
                    ),
                    _ => unreachable!("survey validated before spawn loop"),
                };
                result.unwrap_or_else(as_error!("enrichment-only worker failed"));
            })
        });

        worker_handles.push((handle, sender));
    }

    // Sigint handler
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

    let mut shutdown_rx = shutdown_rx;
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                let total = worker_handles.len();
                let live = worker_handles
                    .iter()
                    .filter(|(h, _)| !h.is_finished())
                    .count();
                let dead = total - live;
                if live == 0 {
                    // Every worker has exited; nothing is draining the queue.
                    // Log loudly and shut down instead of idling forever.
                    error!(
                        total,
                        "all enrichment-only workers have died; nothing is draining the queue, shutting down"
                    );
                    break;
                } else if dead > 0 {
                    warn!(
                        live,
                        dead,
                        total,
                        "heartbeat: some enrichment-only workers have died; queue is draining at reduced capacity"
                    );
                } else {
                    info!(live, total, "heartbeat: workers running");
                }
            }
        }
    }

    info!("shutting down");
    for (_, sender) in &worker_handles {
        sender.send(WorkerCmd::TERM).await.unwrap_or_else(|_| {
            warn!("failed to send termination signal (thread likely already terminated)");
        });
    }
    for (handle, _) in worker_handles {
        match handle.join() {
            Ok(_) => info!("worker shut down"),
            Err(_) => warn!("worker panicked"),
        }
    }
}

#[tokio::main]
async fn main() {
    load_dotenv();

    let args = Cli::parse();

    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    // Worker enrichment loops run on dedicated OS threads, so a panic there
    // otherwise only prints to stderr and surfaces as a bare "worker panicked"
    // when the thread is joined. Install a hook that records the panic message
    // and location through tracing so failures are visible in structured logs,
    // then delegate to the default hook to preserve the usual stderr backtrace.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "unknown".to_string());
        let message = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        error!(
            panic.location = %location,
            panic.message = %message,
            "worker thread panicked"
        );
        default_hook(info);
    }));

    run(args).await;
}
