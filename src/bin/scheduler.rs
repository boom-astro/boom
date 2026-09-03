#[cfg(target_os = "linux")]
use boom::utils::gpu::validate_gpu_configuration_for_survey;
use boom::{
    alert::recover_temp_queue,
    api::catalogs::WATCHLIST_PREFIX,
    conf::{load_dotenv, AppConfig, CatalogXmatchConfig},
    enrichment::models::SharedModelPool,
    scheduler::{record_mpc_orbits_state, record_worker_pool_state, ThreadPool},
    utils::{
        db::initialize_survey_indexes,
        enums::Survey,
        mpcorb,
        o11y::{
            logging::{build_subscriber_with_otel, log_error, WARN},
            metrics::init_metrics,
            tracing::init_tracing,
        },
        worker::WorkerType,
    },
};

use std::time::Duration;

use clap::Parser;
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tokio::sync::oneshot;
use tracing::{info, info_span, warn, Instrument};
use uuid::Uuid;

/// How stale the MPC catalogue may get before it is refreshed. MPCORB is
/// published daily and the elements' own epochs move far more slowly, so this is
/// about not drifting rather than about needing today's file exactly.
const MPC_ORBITS_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
/// How often to re-check. Well inside the max age, so a single failed attempt
/// still leaves several before the catalogue is actually stale.
const MPC_ORBITS_CHECK_INTERVAL: Duration = Duration::from_secs(4 * 60 * 60);

fn pool_state(pool: &ThreadPool) -> String {
    format!("{}/{}", pool.live_worker_count(), pool.total_worker_count())
}

/// Whether the catalogue is due a refresh. An absent one always is.
fn mpc_orbits_needs_refresh(age_seconds: Option<f64>, max_age: Duration) -> bool {
    age_seconds.is_none_or(|age| age >= max_age.as_secs_f64())
}

/// Keep `MPC_orbits` fresh for as long as the scheduler runs.
///
/// A missing catalogue costs geometry silently -- the alert still enriches
/// without it -- so this runs unattended, and the startup check covers a fresh
/// deployment. A failed refresh leaves the previous catalogue in place.
async fn keep_mpc_orbits_fresh(db: mongodb::Database) {
    let mut tick = tokio::time::interval(MPC_ORBITS_CHECK_INTERVAL);
    loop {
        // Fires immediately on the first pass, so startup is covered.
        tick.tick().await;

        let now = chrono::Utc::now().timestamp() as f64;
        let age = match mpcorb::orbits_age_seconds(&db, now).await {
            Ok(age) => age,
            // An unknown age is not an absent one: do not re-download on a blip.
            Err(error) => {
                log_error!(WARN, error, "could not read the age of MPC_orbits");
                continue;
            }
        };
        let count = db
            .collection::<Document>(mpcorb::ORBITS_COLLECTION)
            .estimated_document_count()
            .await
            .ok();
        record_mpc_orbits_state(age, count);

        if !mpc_orbits_needs_refresh(age, MPC_ORBITS_MAX_AGE) {
            info!(
                age_hours = age.unwrap_or(0.0) / 3600.0,
                orbits = count.unwrap_or(0),
                "MPC_orbits is current"
            );
            continue;
        }
        match age {
            Some(age) => warn!(age_hours = age / 3600.0, "MPC_orbits is stale, refreshing"),
            None => warn!("MPC_orbits is missing, populating it"),
        }

        // No progress bar: this output is a log, not a terminal.
        match mpcorb::refresh_orbits(Some(&db), mpcorb::DEFAULT_MPCORB_URL, 10_000, now, false)
            .await
        {
            Ok(report) => {
                for sample in &report.rejected_samples {
                    warn!("rejected record-shaped line: {}", sample);
                }
                info!(
                    orbits = report.parsed,
                    skipped = report.skipped,
                    "MPC_orbits refreshed"
                );
                record_mpc_orbits_state(Some(0.0), Some(report.parsed));
            }
            // The previous catalogue survives a failure, so geometry keeps working.
            Err(error) => log_error!(WARN, error, "failed to refresh MPC_orbits"),
        }
    }
}

/// Sample one aux record at random and warn if it is missing crossmatches for
/// any catalog declared under `crossmatch.<survey>` in the config, excluding
/// watchlist catalogs (prefixed with `watchlist_`). The live pipeline only
/// crossmatches at first insert, so newly added catalogs never reach
/// pre-existing records — the user has to run `reprocess_crossmatch`.
async fn warn_if_missing_crossmatches(survey: &Survey, db: &mongodb::Database, config: &AppConfig) {
    let configured: Vec<&CatalogXmatchConfig> = match config.crossmatch.get(survey) {
        Some(v) if !v.is_empty() => v
            .iter()
            .filter(|c| !c.catalog.starts_with(WATCHLIST_PREFIX))
            .collect(),
        _ => return,
    };
    if configured.is_empty() {
        return;
    }
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

// Not `#[instrument]`'d: a process-lifetime root span grows until Tempo balks.
async fn run(
    args: Cli,
    meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<SdkTracerProvider>,
) {
    let default_config_path = "config.yaml".to_string();
    let config_path = args.config.unwrap_or_else(|| {
        warn!("no config file provided, using {}", default_config_path);
        default_config_path
    });
    let config = AppConfig::from_path(&config_path).unwrap();

    let worker_config = config
        .workers
        .get(&args.survey)
        .expect("could not retrieve worker config for survey");
    let n_alert = worker_config.alert.n_workers;
    let n_enrichment = worker_config.enrichment.n_workers;
    let n_filter = worker_config.filter.n_workers;

    let db: mongodb::Database = config
        .build_db()
        .await
        .expect("could not create mongodb client");
    initialize_survey_indexes(&args.survey, &db)
        .await
        .expect("could not initialize indexes");

    warn_if_missing_crossmatches(&args.survey, &db, &config).await;

    // Only ZTF needs these; LSST carries the vectors in its own packet.
    if args.survey == Survey::Ztf {
        tokio::spawn(
            keep_mpc_orbits_fresh(db.clone()).instrument(info_span!("mpc orbits refresh")),
        );
    }

    #[cfg(target_os = "linux")]
    validate_gpu_configuration_for_survey(&args.survey, &config)
        .expect("GPU configuration is invalid for the survey");

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
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

    // A pool conserves VRAM; None makes each CPU worker load its own models.
    let shared_model_pool = if matches!(args.survey, Survey::Ztf) && config.gpu.is_active() {
        Some(
            SharedModelPool::load(&config.gpu.device_ids)
                .expect("failed to load ONNX models on GPU"),
        )
    } else {
        None
    };

    match config.build_redis().await {
        Ok(mut con) => match recover_temp_queue(&mut con, &args.survey.alert_input_queue()).await {
            Ok(0) => {}
            Ok(recovered) => warn!(recovered, "requeued alerts left in the alert temp queue"),
            Err(error) => log_error!(WARN, error, "failed to recover the alert temp queue"),
        },
        Err(error) => log_error!(
            WARN,
            error,
            "failed to connect to redis for temp queue recovery"
        ),
    }

    let mut alert_pool = ThreadPool::new(
        WorkerType::Alert,
        n_alert,
        args.survey.clone(),
        config_path.clone(),
        None,
    );
    let mut enrichment_pool = ThreadPool::new(
        WorkerType::Enrichment,
        n_enrichment,
        args.survey.clone(),
        config_path.clone(),
        shared_model_pool,
    );
    let mut filter_pool = ThreadPool::new(
        WorkerType::Filter,
        n_filter,
        args.survey.clone(),
        config_path,
        None,
    );

    // By reference, so the supervision tick below can still borrow them mutably.
    let record_pool_metrics =
        |survey: &Survey, alert: &ThreadPool, enrichment: &ThreadPool, filter: &ThreadPool| {
            for (kind, pool) in [
                ("alert", alert),
                ("enrichment", enrichment),
                ("filter", filter),
            ] {
                record_worker_pool_state(
                    survey,
                    kind,
                    pool.live_worker_count(),
                    pool.total_worker_count(),
                );
            }
        };

    // Emit an initial sample so dashboards show running workers immediately.
    record_pool_metrics(&args.survey, &alert_pool, &enrichment_pool, &filter_pool);

    // Supervise often to respawn fast, but only log the heartbeat once a minute.
    let mut supervise_tick = tokio::time::interval(Duration::from_secs(5));
    let mut heartbeat_tick = tokio::time::interval(Duration::from_secs(60));
    // Drop the immediate first ticks; the sample above already covers t=0.
    supervise_tick.tick().await;
    heartbeat_tick.tick().await;
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                break;
            }
            _ = supervise_tick.tick() => {
                alert_pool.supervise();
                enrichment_pool.supervise();
                filter_pool.supervise();
            }
            _ = heartbeat_tick.tick() => {
                record_pool_metrics(&args.survey, &alert_pool, &enrichment_pool, &filter_pool);
                info!(
                    alert = %pool_state(&alert_pool),
                    enrichment = %pool_state(&enrichment_pool),
                    filter = %pool_state(&filter_pool),
                    "heartbeat: workers running"
                );
            }
        }
    }

    info!("shutting down");
    drop(alert_pool);
    drop(enrichment_pool);
    drop(filter_pool);
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
    // Before anything else, so every later config read sees the variables.
    load_dotenv();

    let args = Cli::parse();

    let instance_id = args.instance_id.unwrap_or_else(Uuid::new_v4);
    // Must match the Compose service name or Grafana loses the correlation label.
    let service_name = format!("scheduler-{}", args.survey.to_string().to_lowercase());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_absent_catalogue_is_always_due_a_refresh() {
        assert!(mpc_orbits_needs_refresh(None, MPC_ORBITS_MAX_AGE));
    }

    #[test]
    fn test_fresh_catalogue_is_left_alone() {
        assert!(!mpc_orbits_needs_refresh(Some(0.0), MPC_ORBITS_MAX_AGE));
        assert!(!mpc_orbits_needs_refresh(Some(3600.0), MPC_ORBITS_MAX_AGE));
    }

    #[test]
    fn test_catalogue_past_the_max_age_is_refreshed() {
        let max = MPC_ORBITS_MAX_AGE.as_secs_f64();
        assert!(!mpc_orbits_needs_refresh(
            Some(max - 1.0),
            MPC_ORBITS_MAX_AGE
        ));
        assert!(mpc_orbits_needs_refresh(Some(max), MPC_ORBITS_MAX_AGE));
        assert!(mpc_orbits_needs_refresh(
            Some(max * 10.0),
            MPC_ORBITS_MAX_AGE
        ));
    }

    // Several checks must fit in the staleness window, or one failure strands it.
    #[test]
    fn test_check_interval_leaves_room_for_retries() {
        assert!(MPC_ORBITS_CHECK_INTERVAL * 3 <= MPC_ORBITS_MAX_AGE);
    }
}
