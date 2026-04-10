use boom::{
    conf::{load_dotenv, AppConfig},
    scheduler::{ThreadPool, WorkerSnapshot},
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

use actix_web::{web, App, HttpResponse, HttpServer};
use std::time::Duration;
use std::{
    sync::Arc,
    time::{Instant, SystemTime},
};

use clap::Parser;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, info_span, instrument, warn, Instrument};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize)]
struct PoolMetrics {
    configured_workers: usize,
    live_workers: usize,
    total_workers: usize,
    status: String,
    input_queue_depth: usize,
}

#[derive(Clone, Debug, Serialize)]
struct HealthStatus {
    service: String,
    status: String,
    ready: bool,
    instance_id: String,
    survey: String,
    uptime_seconds: u64,
    timestamp: String,
}

#[derive(Clone, Debug, Serialize)]
struct FullSchedulerStatus {
    service: String,
    status: String,
    ready: bool,
    instance_id: String,
    survey: String,
    uptime_seconds: u64,
    timestamp: String,
    pools: std::collections::HashMap<String, PoolMetrics>,
}

#[derive(Clone, Debug, Serialize)]
struct WorkerPoolStatus {
    live_workers: usize,
    total_workers: usize,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ScaleWorkerRequest {
    count: usize,
}

#[derive(Debug, Serialize)]
struct ScaleWorkerResponse {
    success: bool,
    message: String,
    pool_type: String,
    survey: String,
    action: String,
    count: usize,
    new_total_workers: usize,
}

#[derive(Debug, Serialize)]
struct WorkersListResponse {
    pool_type: String,
    survey: String,
    total_workers: usize,
    live_workers: usize,
    workers: Vec<WorkerSnapshot>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

/// Shared wrapper around all three thread pools for mutable access from HTTP handlers
struct SharedThreadPools {
    alert: Arc<Mutex<ThreadPool>>,
    enrichment: Arc<Mutex<ThreadPool>>,
    filter: Arc<Mutex<ThreadPool>>,
}

type SharedSchedulerStatus = Arc<RwLock<FullSchedulerStatus>>;
type SharedPoolsState = Arc<SharedThreadPools>;

fn classify_pool_status(live_workers: usize, total_workers: usize) -> String {
    if total_workers == 0 {
        return "disabled".to_string();
    }

    if live_workers == total_workers {
        return "healthy".to_string();
    }

    if live_workers == 0 {
        return "down".to_string();
    }

    "degraded".to_string()
}

fn aggregate_status(
    alert: &WorkerPoolStatus,
    enrichment: &WorkerPoolStatus,
    filter: &WorkerPoolStatus,
) -> String {
    let pools = [alert, enrichment, filter];

    if pools
        .iter()
        .all(|pool| pool.status == "healthy" || pool.status == "disabled")
    {
        return "healthy".to_string();
    }

    if pools.iter().any(|pool| pool.status == "down") {
        return "degraded".to_string();
    }

    "degraded".to_string()
}

fn pool_status_from_pool(pool: &ThreadPool) -> WorkerPoolStatus {
    let live_workers = pool.live_worker_count();
    let total_workers = pool.total_worker_count();

    WorkerPoolStatus {
        live_workers,
        total_workers,
        status: classify_pool_status(live_workers, total_workers),
    }
}

fn get_timestamp() -> String {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            let secs = duration.as_secs();
            let nanos = duration.subsec_nanos();
            let datetime = chrono::DateTime::<chrono::Utc>::from(
                SystemTime::UNIX_EPOCH
                    + Duration::from_secs(secs)
                    + Duration::from_nanos(nanos as u64),
            );
            datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        }
        Err(_) => "unknown".to_string(),
    }
}

async fn get_queue_depth(
    redis_conn: &mut redis::aio::MultiplexedConnection,
    queue_name: &str,
) -> usize {
    match redis_conn.llen::<&str, usize>(queue_name).await {
        Ok(depth) => depth,
        Err(e) => {
            warn!("Failed to get queue depth for {}: {}", queue_name, e);
            0
        }
    }
}

async fn scheduler_status_snapshot(
    alert_pool: &ThreadPool,
    enrichment_pool: &ThreadPool,
    filter_pool: &ThreadPool,
    survey: &Survey,
    instance_id: Uuid,
    started_at: Instant,
    mut redis_conn: redis::aio::MultiplexedConnection,
    configured_alert: usize,
    configured_enrichment: usize,
    configured_filter: usize,
) -> FullSchedulerStatus {
    let alert = pool_status_from_pool(alert_pool);
    let enrichment = pool_status_from_pool(enrichment_pool);
    let filter = pool_status_from_pool(filter_pool);
    let status = aggregate_status(&alert, &enrichment, &filter);
    let survey_str = survey.to_string().to_lowercase();

    let alert_queue = format!("{}_alerts_packets_queue", survey_str);
    let enrichment_queue = format!("{}_alerts_enrichment_queue", survey_str);
    let filter_queue = format!("{}_alerts_filter_queue", survey_str);

    let alert_depth = get_queue_depth(&mut redis_conn, &alert_queue).await;
    let enrichment_depth = get_queue_depth(&mut redis_conn, &enrichment_queue).await;
    let filter_depth = get_queue_depth(&mut redis_conn, &filter_queue).await;

    let mut pools = std::collections::HashMap::new();

    pools.insert(
        "alert".to_string(),
        PoolMetrics {
            configured_workers: configured_alert,
            live_workers: alert.live_workers,
            total_workers: alert.total_workers,
            status: alert.status,
            input_queue_depth: alert_depth,
        },
    );

    pools.insert(
        "enrichment".to_string(),
        PoolMetrics {
            configured_workers: configured_enrichment,
            live_workers: enrichment.live_workers,
            total_workers: enrichment.total_workers,
            status: enrichment.status,
            input_queue_depth: enrichment_depth,
        },
    );

    pools.insert(
        "filter".to_string(),
        PoolMetrics {
            configured_workers: configured_filter,
            live_workers: filter.live_workers,
            total_workers: filter.total_workers,
            status: filter.status,
            input_queue_depth: filter_depth,
        },
    );

    FullSchedulerStatus {
        service: "scheduler".to_string(),
        survey: survey_str,
        instance_id: instance_id.to_string(),
        uptime_seconds: started_at.elapsed().as_secs(),
        status,
        ready: true,
        timestamp: get_timestamp(),
        pools,
    }
}

async fn get_scheduler_health(state: web::Data<SharedSchedulerStatus>) -> HttpResponse {
    let full_status = state.read().await.clone();
    let health = HealthStatus {
        service: full_status.service,
        status: full_status.status,
        ready: full_status.ready,
        instance_id: full_status.instance_id,
        survey: full_status.survey,
        uptime_seconds: full_status.uptime_seconds,
        timestamp: full_status.timestamp,
    };
    HttpResponse::Ok().json(health)
}

async fn get_scheduler_status(state: web::Data<SharedSchedulerStatus>) -> HttpResponse {
    let status = state.read().await.clone();
    HttpResponse::Ok().json(status)
}

async fn add_workers(
    path: web::Path<String>,
    req: web::Json<ScaleWorkerRequest>,
    pools: web::Data<SharedPoolsState>,
) -> HttpResponse {
    let pool_type = path.into_inner();
    let count = req.count;

    if count == 0 {
        return HttpResponse::BadRequest().json(ErrorResponse {
            error: "count must be greater than 0".to_string(),
        });
    }

    let pool = match pool_type.to_lowercase().as_str() {
        "alert" => &pools.alert,
        "enrichment" => &pools.enrichment,
        "filter" => &pools.filter,
        _ => {
            return HttpResponse::BadRequest().json(ErrorResponse {
                error: format!("invalid pool type: {}", pool_type),
            });
        }
    };

    let mut pool_guard = pool.lock().await;
    let survey = pool_guard.survey_name().to_string().to_lowercase();

    for _ in 0..count {
        pool_guard.add_worker();
    }
    let new_count = pool_guard.total_worker_count();

    HttpResponse::Ok().json(ScaleWorkerResponse {
        success: true,
        message: format!("added {} workers to {} pool", count, pool_type),
        pool_type: pool_type.to_lowercase(),
        survey,
        action: "add".to_string(),
        count,
        new_total_workers: new_count,
    })
}

async fn remove_workers(
    path: web::Path<String>,
    req: web::Json<ScaleWorkerRequest>,
    pools: web::Data<SharedPoolsState>,
) -> HttpResponse {
    let pool_type = path.into_inner();
    let count = req.count;

    if count == 0 {
        return HttpResponse::BadRequest().json(ErrorResponse {
            error: "count must be greater than 0".to_string(),
        });
    }

    let pool = match pool_type.to_lowercase().as_str() {
        "alert" => &pools.alert,
        "enrichment" => &pools.enrichment,
        "filter" => &pools.filter,
        _ => {
            return HttpResponse::BadRequest().json(ErrorResponse {
                error: format!("invalid pool type: {}", pool_type),
            });
        }
    };

    let mut pool_guard = pool.lock().await;
    let survey = pool_guard.survey_name().to_string().to_lowercase();

    let mut removed_count = 0;
    for _ in 0..count {
        if pool_guard.remove_worker().await {
            removed_count += 1;
        } else {
            break;
        }
    }

    let new_count = pool_guard.total_worker_count();

    HttpResponse::Ok().json(ScaleWorkerResponse {
        success: true,
        message: format!("removed {} workers from {} pool", removed_count, pool_type),
        pool_type: pool_type.to_lowercase(),
        survey,
        action: "remove".to_string(),
        count: removed_count,
        new_total_workers: new_count,
    })
}

async fn get_workers(path: web::Path<String>, pools: web::Data<SharedPoolsState>) -> HttpResponse {
    let pool_type = path.into_inner();

    let pool = match pool_type.to_lowercase().as_str() {
        "alert" => &pools.alert,
        "enrichment" => &pools.enrichment,
        "filter" => &pools.filter,
        _ => {
            return HttpResponse::BadRequest().json(ErrorResponse {
                error: format!("invalid pool type: {}", pool_type),
            });
        }
    };

    let pool_guard = pool.lock().await;
    let survey = pool_guard.survey_name().to_string().to_lowercase();

    HttpResponse::Ok().json(WorkersListResponse {
        pool_type: pool_type.to_lowercase(),
        survey,
        total_workers: pool_guard.total_worker_count(),
        live_workers: pool_guard.live_worker_count(),
        workers: pool_guard.worker_snapshots(),
    })
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

    /// Host/IP where the scheduler health API is exposed
    #[arg(long, env = "BOOM_SCHEDULER_API_HOST")]
    scheduler_api_host: Option<String>,

    /// Port where the scheduler health API is exposed
    #[arg(long, env = "BOOM_SCHEDULER_API_PORT")]
    scheduler_api_port: Option<u16>,
}

#[instrument(skip_all, fields(survey = %args.survey))]
async fn run(args: Cli, meter_provider: SdkMeterProvider, instance_id: Uuid) {
    let default_config_path = "config.yaml".to_string();
    let config_path = args.config.unwrap_or_else(|| {
        warn!("no config file provided, using {}", default_config_path);
        default_config_path
    });
    let config = AppConfig::from_path(&config_path).unwrap();
    let scheduler_api_host = args
        .scheduler_api_host
        .unwrap_or_else(|| config.api.domain.clone());
    let scheduler_api_port = args
        .scheduler_api_port
        .unwrap_or_else(|| config.api.port.saturating_add(1));
    let started_at = Instant::now();

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

    // Build Redis connection for queue depth queries
    let redis_conn = config
        .build_redis()
        .await
        .expect("could not create redis client");

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
    let survey = args.survey.clone();
    let enrichment_pool = ThreadPool::new(
        WorkerType::Enrichment,
        n_enrichment as usize,
        survey.clone(),
        config_path.clone(),
    );
    let filter_pool = ThreadPool::new(
        WorkerType::Filter,
        n_filter as usize,
        survey.clone(),
        config_path,
    );

    // Wrap pools in Arc<Mutex<>> for mutable access from HTTP handlers
    let shared_pools = Arc::new(SharedThreadPools {
        alert: Arc::new(Mutex::new(alert_pool)),
        enrichment: Arc::new(Mutex::new(enrichment_pool)),
        filter: Arc::new(Mutex::new(filter_pool)),
    });

    let alert_pool_guard = shared_pools.alert.lock().await;
    let enrichment_pool_guard = shared_pools.enrichment.lock().await;
    let filter_pool_guard = shared_pools.filter.lock().await;

    let initial_status = scheduler_status_snapshot(
        &*alert_pool_guard,
        &*enrichment_pool_guard,
        &*filter_pool_guard,
        &survey,
        instance_id,
        started_at,
        redis_conn.clone(),
        n_alert as usize,
        n_enrichment as usize,
        n_filter as usize,
    )
    .await;

    drop(alert_pool_guard);
    drop(enrichment_pool_guard);
    drop(filter_pool_guard);

    let shared_status: SharedSchedulerStatus = Arc::new(RwLock::new(initial_status));
    let api_state = shared_status.clone();
    let pools_state = shared_pools.clone();
    let scheduler_api_server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(api_state.clone()))
            .app_data(web::Data::new(pools_state.clone()))
            .route("/health", web::get().to(get_scheduler_health))
            .route("/status", web::get().to(get_scheduler_status))
            .route("/workers/{pool_type}", web::get().to(get_workers))
            .route("/workers/{pool_type}/add", web::post().to(add_workers))
            .route(
                "/workers/{pool_type}/remove",
                web::post().to(remove_workers),
            )
    })
    .bind((scheduler_api_host.as_str(), scheduler_api_port))
    .unwrap_or_else(|error| {
        panic!(
            "failed to bind scheduler API on {}:{} ({})",
            scheduler_api_host, scheduler_api_port, error
        )
    })
    .run();
    let scheduler_api_handle = scheduler_api_server.handle();
    tokio::spawn(async move {
        if let Err(error) = scheduler_api_server.await {
            warn!(?error, "scheduler API server terminated with error");
        }
    });
    info!(
        host = scheduler_api_host,
        port = scheduler_api_port,
        "scheduler API started"
    );

    // Wait for shutdown signal, logging heartbeat every 60 seconds with live worker counts
    let mut shutdown_rx = shutdown_rx;
    let mut status_refresh_interval = tokio::time::interval(Duration::from_secs(5));
    let mut heartbeat_log_interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                break;
            }
            _ = status_refresh_interval.tick() => {
                let alert_pool_guard = shared_pools.alert.lock().await;
                let enrichment_pool_guard = shared_pools.enrichment.lock().await;
                let filter_pool_guard = shared_pools.filter.lock().await;

                let latest = scheduler_status_snapshot(
                    &*alert_pool_guard,
                    &*enrichment_pool_guard,
                    &*filter_pool_guard,
                    &survey,
                    instance_id,
                    started_at,
                    redis_conn.clone(),
                    n_alert as usize,
                    n_enrichment as usize,
                    n_filter as usize,
                ).await;
                drop(alert_pool_guard);
                drop(enrichment_pool_guard);
                drop(filter_pool_guard);

                let mut status_guard = shared_status.write().await;
                *status_guard = latest;
            }
            _ = heartbeat_log_interval.tick() => {
                let alert_pool_guard = shared_pools.alert.lock().await;
                let enrichment_pool_guard = shared_pools.enrichment.lock().await;
                let filter_pool_guard = shared_pools.filter.lock().await;

                info!(
                    alert = %format!("{}/{}", alert_pool_guard.live_worker_count(), alert_pool_guard.total_worker_count()),
                    enrichment = %format!("{}/{}", enrichment_pool_guard.live_worker_count(), enrichment_pool_guard.total_worker_count()),
                    filter = %format!("{}/{}", filter_pool_guard.live_worker_count(), filter_pool_guard.total_worker_count()),
                    "heartbeat: workers running"
                );
            }
        }
    }

    // Shut down:
    info!("shutting down");
    scheduler_api_handle.stop(true).await;
    // Pools are automatically dropped via Arc when shared_pools is dropped
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

    run(args, meter_provider, instance_id).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, test, App};

    fn make_status() -> FullSchedulerStatus {
        let mut pools = std::collections::HashMap::new();
        pools.insert(
            "alert".to_string(),
            PoolMetrics {
                configured_workers: 2,
                live_workers: 2,
                total_workers: 2,
                status: "healthy".to_string(),
                input_queue_depth: 5,
            },
        );
        pools.insert(
            "enrichment".to_string(),
            PoolMetrics {
                configured_workers: 1,
                live_workers: 1,
                total_workers: 1,
                status: "healthy".to_string(),
                input_queue_depth: 3,
            },
        );
        pools.insert(
            "filter".to_string(),
            PoolMetrics {
                configured_workers: 0,
                live_workers: 0,
                total_workers: 0,
                status: "disabled".to_string(),
                input_queue_depth: 0,
            },
        );

        FullSchedulerStatus {
            service: "scheduler".to_string(),
            survey: "ztf".to_string(),
            instance_id: "test-instance".to_string(),
            uptime_seconds: 42,
            status: "healthy".to_string(),
            ready: true,
            timestamp: "2026-04-10T12:34:56Z".to_string(),
            pools,
        }
    }

    #[actix_web::test]
    async fn test_worker_pool_status_classification() {
        assert_eq!(classify_pool_status(3, 3), "healthy");
        assert_eq!(classify_pool_status(1, 3), "degraded");
        assert_eq!(classify_pool_status(0, 3), "down");
        assert_eq!(classify_pool_status(0, 0), "disabled");
    }

    #[actix_web::test]
    async fn test_scheduler_health_endpoint_reflects_shared_status() {
        let shared_status: SharedSchedulerStatus = Arc::new(RwLock::new(make_status()));

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(shared_status.clone()))
                .route("/health", web::get().to(get_scheduler_health)),
        )
        .await;

        let req = test::TestRequest::get().uri("/health").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["service"], "scheduler");
        assert_eq!(body["survey"], "ztf");
        assert_eq!(body["status"], "healthy");
        assert_eq!(body["ready"], true);
        assert!(body["timestamp"].is_string());

        {
            let mut status = shared_status.write().await;
            status.status = "degraded".to_string();
            if let Some(alert_pool) = status.pools.get_mut("alert") {
                alert_pool.live_workers = 1;
                alert_pool.status = "degraded".to_string();
            }
            status.uptime_seconds = 99;
        }

        let req = test::TestRequest::get().uri("/health").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["status"], "degraded");
        assert_eq!(body["uptime_seconds"], 99);
    }

    #[actix_web::test]
    async fn test_scheduler_status_endpoint_includes_pools() {
        let shared_status: SharedSchedulerStatus = Arc::new(RwLock::new(make_status()));

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(shared_status.clone()))
                .route("/status", web::get().to(get_scheduler_status)),
        )
        .await;

        let req = test::TestRequest::get().uri("/status").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["service"], "scheduler");
        assert_eq!(body["survey"], "ztf");
        assert_eq!(body["pools"]["alert"]["configured_workers"], 2);
        assert_eq!(body["pools"]["alert"]["live_workers"], 2);
        assert_eq!(body["pools"]["alert"]["input_queue_depth"], 5);
        assert_eq!(body["pools"]["enrichment"]["input_queue_depth"], 3);
        assert_eq!(body["pools"]["filter"]["status"], "disabled");
    }

    #[actix_web::test]
    async fn test_add_workers_endpoint() {
        let alert_pool =
            ThreadPool::new(WorkerType::Alert, 1, Survey::Ztf, "config.yaml".to_string());
        let enrichment_pool = ThreadPool::new(
            WorkerType::Enrichment,
            1,
            Survey::Ztf,
            "config.yaml".to_string(),
        );
        let filter_pool = ThreadPool::new(
            WorkerType::Filter,
            0,
            Survey::Ztf,
            "config.yaml".to_string(),
        );

        let pools = Arc::new(SharedThreadPools {
            alert: Arc::new(Mutex::new(alert_pool)),
            enrichment: Arc::new(Mutex::new(enrichment_pool)),
            filter: Arc::new(Mutex::new(filter_pool)),
        });

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pools.clone()))
                .route("/workers/{pool_type}/add", web::post().to(add_workers)),
        )
        .await;

        // Test adding 2 workers to alert pool
        let req = test::TestRequest::post()
            .uri("/workers/alert/add")
            .set_json(serde_json::json!({"count": 2}))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["success"], true);
        assert_eq!(body["action"], "add");
        assert_eq!(body["count"], 2);
        assert_eq!(body["new_total_workers"], 3); // was 1, now 1+2=3

        // Verify the pool now has 3 workers
        let alert_pool_guard = pools.alert.lock().await;
        assert_eq!(alert_pool_guard.total_worker_count(), 3);
    }

    #[actix_web::test]
    async fn test_remove_workers_endpoint() {
        let alert_pool =
            ThreadPool::new(WorkerType::Alert, 3, Survey::Ztf, "config.yaml".to_string());
        let enrichment_pool = ThreadPool::new(
            WorkerType::Enrichment,
            1,
            Survey::Ztf,
            "config.yaml".to_string(),
        );
        let filter_pool = ThreadPool::new(
            WorkerType::Filter,
            0,
            Survey::Ztf,
            "config.yaml".to_string(),
        );

        let pools = Arc::new(SharedThreadPools {
            alert: Arc::new(Mutex::new(alert_pool)),
            enrichment: Arc::new(Mutex::new(enrichment_pool)),
            filter: Arc::new(Mutex::new(filter_pool)),
        });

        let app = test::init_service(App::new().app_data(web::Data::new(pools.clone())).route(
            "/workers/{pool_type}/remove",
            web::post().to(remove_workers),
        ))
        .await;

        // Test removing 2 workers from alert pool
        let req = test::TestRequest::post()
            .uri("/workers/alert/remove")
            .set_json(serde_json::json!({"count": 2}))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["success"], true);
        assert_eq!(body["action"], "remove");
        assert_eq!(body["count"], 2);
        assert_eq!(body["new_total_workers"], 1); // was 3, now 3-2=1

        // Verify the pool now has 1 worker
        let alert_pool_guard = pools.alert.lock().await;
        assert_eq!(alert_pool_guard.total_worker_count(), 1);
    }

    #[actix_web::test]
    async fn test_add_workers_invalid_pool_type() {
        let alert_pool =
            ThreadPool::new(WorkerType::Alert, 1, Survey::Ztf, "config.yaml".to_string());
        let enrichment_pool = ThreadPool::new(
            WorkerType::Enrichment,
            1,
            Survey::Ztf,
            "config.yaml".to_string(),
        );
        let filter_pool = ThreadPool::new(
            WorkerType::Filter,
            0,
            Survey::Ztf,
            "config.yaml".to_string(),
        );

        let pools = Arc::new(SharedThreadPools {
            alert: Arc::new(Mutex::new(alert_pool)),
            enrichment: Arc::new(Mutex::new(enrichment_pool)),
            filter: Arc::new(Mutex::new(filter_pool)),
        });

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pools.clone()))
                .route("/workers/{pool_type}/add", web::post().to(add_workers)),
        )
        .await;

        // Test adding to an invalid pool type
        let req = test::TestRequest::post()
            .uri("/workers/invalid/add")
            .set_json(serde_json::json!({"count": 1}))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert!(body["error"].is_string());
    }

    #[actix_web::test]
    async fn test_add_workers_zero_count() {
        let alert_pool =
            ThreadPool::new(WorkerType::Alert, 1, Survey::Ztf, "config.yaml".to_string());
        let enrichment_pool = ThreadPool::new(
            WorkerType::Enrichment,
            1,
            Survey::Ztf,
            "config.yaml".to_string(),
        );
        let filter_pool = ThreadPool::new(
            WorkerType::Filter,
            0,
            Survey::Ztf,
            "config.yaml".to_string(),
        );

        let pools = Arc::new(SharedThreadPools {
            alert: Arc::new(Mutex::new(alert_pool)),
            enrichment: Arc::new(Mutex::new(enrichment_pool)),
            filter: Arc::new(Mutex::new(filter_pool)),
        });

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pools.clone()))
                .route("/workers/{pool_type}/add", web::post().to(add_workers)),
        )
        .await;

        // Test adding 0 workers should fail
        let req = test::TestRequest::post()
            .uri("/workers/alert/add")
            .set_json(serde_json::json!({"count": 0}))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn test_get_workers_endpoint() {
        let alert_pool =
            ThreadPool::new(WorkerType::Alert, 2, Survey::Ztf, "config.yaml".to_string());
        let enrichment_pool = ThreadPool::new(
            WorkerType::Enrichment,
            1,
            Survey::Ztf,
            "config.yaml".to_string(),
        );
        let filter_pool = ThreadPool::new(
            WorkerType::Filter,
            0,
            Survey::Ztf,
            "config.yaml".to_string(),
        );

        let pools = Arc::new(SharedThreadPools {
            alert: Arc::new(Mutex::new(alert_pool)),
            enrichment: Arc::new(Mutex::new(enrichment_pool)),
            filter: Arc::new(Mutex::new(filter_pool)),
        });

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pools.clone()))
                .route("/workers/{pool_type}", web::get().to(get_workers)),
        )
        .await;

        let req = test::TestRequest::get().uri("/workers/alert").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(body["pool_type"], "alert");
        assert_eq!(body["survey"], "ztf");
        assert_eq!(body["total_workers"], 2);
        assert_eq!(body["live_workers"], 2);
        assert!(body["workers"].is_array());
        assert_eq!(body["workers"].as_array().unwrap().len(), 2);
        assert!(body["workers"][0]["id"].is_string());
        assert!(body["workers"][0]["thread_id"].is_string());
        assert!(body["workers"][0]["is_finished"].is_boolean());
        assert!(body["workers"][0]["has_join_handle"].is_boolean());
    }

    #[actix_web::test]
    async fn test_get_workers_invalid_pool_type() {
        let alert_pool =
            ThreadPool::new(WorkerType::Alert, 1, Survey::Ztf, "config.yaml".to_string());
        let enrichment_pool = ThreadPool::new(
            WorkerType::Enrichment,
            1,
            Survey::Ztf,
            "config.yaml".to_string(),
        );
        let filter_pool = ThreadPool::new(
            WorkerType::Filter,
            0,
            Survey::Ztf,
            "config.yaml".to_string(),
        );

        let pools = Arc::new(SharedThreadPools {
            alert: Arc::new(Mutex::new(alert_pool)),
            enrichment: Arc::new(Mutex::new(enrichment_pool)),
            filter: Arc::new(Mutex::new(filter_pool)),
        });

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pools.clone()))
                .route("/workers/{pool_type}", web::get().to(get_workers)),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/workers/invalid")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
