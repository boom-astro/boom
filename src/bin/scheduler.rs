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

#[cfg(target_os = "linux")]
const ZTF_MIN_FREE_VRAM_MIB: u64 = 10 * 1024;

#[cfg(target_os = "linux")]
fn validate_linux_gpu_runtime_preconditions() -> Result<(), &'static str> {
    // fail fast if the runtime library path is not explicitly configured.
    if std::env::var("ORT_DYLIB_PATH").map_or(true, |v| v.trim().is_empty()) {
        return Err("GPU is enabled but ORT_DYLIB_PATH is not set. \
Set ORT_DYLIB_PATH to a valid libonnxruntime.so path before starting scheduler.");
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_gpu_inference(device_ids: &[i32]) -> Result<(), Box<dyn std::error::Error>> {
    info!("Validating GPU inference: running one inference per configured CUDA device");

    use boom::enrichment::models::{BtsBotModel, Model};
    for &device_id in device_ids {
        info!(device_id, "Running BTSBotModel inference on device");
        let mut model = BtsBotModel::new_on_device("data/models/btsbot-v1.0.1.onnx", device_id)?;
        let metadata = ndarray::Array::from_shape_vec((1, 25), vec![0.5; 25])?;
        let triplet = ndarray::Array::from_shape_vec((1, 63, 63, 3), vec![0.5; 63 * 63 * 3])?;
        let _ = model.predict(&metadata, &triplet)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn parse_nvidia_smi_memory_free_output(
    output: &str,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let mut values = Vec::new();
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value = trimmed.parse::<u64>().map_err(|e| {
            std::io::Error::other(format!(
                "failed to parse nvidia-smi free memory value '{trimmed}': {e}"
            ))
        })?;
        values.push(value);
    }

    if values.is_empty() {
        return Err(std::io::Error::other("nvidia-smi returned no GPU free-memory values").into());
    }

    Ok(values)
}

#[cfg(target_os = "linux")]
fn query_nvidia_smi_free_memory_mib() -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let output = std::process::Command::new("nvidia-smi")
        .args(["--query-gpu=memory.free", "--format=csv,noheader,nounits"])
        .output()
        .map_err(|e| {
            std::io::Error::other(format!(
                "failed to execute nvidia-smi for GPU memory validation: {e}"
            ))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(std::io::Error::other(format!(
            "nvidia-smi failed while validating free GPU memory: {}",
            stderr.trim()
        ))
        .into());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_nvidia_smi_memory_free_output(&stdout)
}

#[cfg(target_os = "linux")]
fn validate_gpu_free_vram(
    device_ids: &[i32],
    min_free_vram_mib: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let free_by_gpu = query_nvidia_smi_free_memory_mib()?;

    for &device_id in device_ids {
        if device_id < 0 {
            return Err(std::io::Error::other(format!(
                "invalid CUDA device id {device_id}; device ids must be >= 0"
            ))
            .into());
        }

        let index = device_id as usize;
        let Some(&free_mib) = free_by_gpu.get(index) else {
            return Err(std::io::Error::other(format!(
                "configured CUDA device id {device_id} is out of range; nvidia-smi reported {} device(s)",
                free_by_gpu.len()
            ))
            .into());
        };

        if free_mib < min_free_vram_mib {
            return Err(std::io::Error::other(format!(
                "configured CUDA device {device_id} has only {free_mib} MiB free VRAM; ZTF enrichment requires at least {min_free_vram_mib} MiB ({:.1} GiB) free per device",
                min_free_vram_mib as f64 / 1024.0
            ))
            .into());
        }

        info!(
            device_id,
            free_vram_mib = free_mib,
            min_required_mib = min_free_vram_mib,
            "validated free GPU VRAM for ZTF enrichment"
        );
    }

    Ok(())
}

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
             for existing records, run `cargo run --bin reprocess_crossmatch -- \
             --survey {} --catalogs {}`.",
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
    {
        if matches!(args.survey, Survey::Ztf) && config.gpu.enabled {
            validate_linux_gpu_runtime_preconditions().expect("GPU runtime preconditions not met");
            validate_gpu_free_vram(&config.gpu.device_ids, ZTF_MIN_FREE_VRAM_MIB)
                .expect("configured GPU(s) do not have enough free VRAM for ZTF enrichment");
            validate_gpu_inference(&config.gpu.device_ids)
                .expect("failed to validate GPU inference");
            info!("Confirmed GPU runtime preconditions, free VRAM guardrail, and GPU inference");
        }
    }

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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::parse_nvidia_smi_memory_free_output;

    #[test]
    /// Verifies that the `nvidia-smi` parsing helper accepts the exact
    /// newline-separated MiB output format we rely on at startup.
    fn parses_memory_free_output_lines() {
        let parsed = parse_nvidia_smi_memory_free_output("12288\n8192\n").unwrap();
        assert_eq!(parsed, vec![12288, 8192]);
    }

    #[test]
    /// Verifies that malformed `nvidia-smi` output fails fast with a parse
    /// error instead of silently accepting bad VRAM data.
    fn rejects_invalid_memory_free_output() {
        let err = parse_nvidia_smi_memory_free_output("12288\nabc\n").unwrap_err();
        assert!(err
            .to_string()
            .contains("failed to parse nvidia-smi free memory value"));
    }
}
