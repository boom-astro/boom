#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
use actix_web::middleware::from_fn;
use actix_web::{middleware::Logger, web, App, HttpServer};
use boom::api::auth::{auth_middleware, babamul_auth_middleware, get_auth};
use boom::api::db::build_db_api;
use boom::api::docs::{ApiDoc, BabamulApiDoc};
use boom::api::email::EmailService;
use boom::api::observability::request_metrics_middleware;
use boom::api::routes;
use boom::conf::{load_dotenv, AppConfig};
use boom::utils::cutouts::CutoutStorage;
use boom::utils::enums::Survey;
use boom::utils::o11y::{
    logging::{build_subscriber_with_otel, log_error, WARN},
    metrics::init_metrics,
    tracing::init_tracing,
};
use std::collections::HashMap;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};
use uuid::Uuid;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load environment variables from .env file before anything else
    load_dotenv();
    let config = AppConfig::from_default_path().unwrap();
    let database = build_db_api(&config).await.unwrap();
    let auth = get_auth(&config, &database).await.unwrap();
    let port = config.api.port;
    let deployment_env = std::env::var("BOOM_DEPLOYMENT_ENV").unwrap_or_else(|_| "dev".to_string());
    let instance_id = Uuid::new_v4();
    let tracer_provider = init_tracing(String::from("api"), instance_id, deployment_env.clone())
        .expect("failed to initialize tracing");
    let meter_provider = init_metrics(String::from("api"), instance_id, deployment_env)
        .expect("failed to initialize metrics");

    // Install a tracing subscriber that fans out to stdout and the OTLP
    // pipeline (Tempo). actix's `Logger` middleware emits access logs via the
    // `log` crate, so install `LogTracer` to forward them into tracing —
    // tracing-subscriber does NOT do this automatically.
    let (subscriber, _guard) = build_subscriber_with_otel(tracer_provider.as_ref(), "api")
        .expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");
    tracing_log::LogTracer::init().expect("failed to install LogTracer");

    // Initialize email service
    let email_service = EmailService::new();

    // Build cutout storage for each survey once at startup
    let mut cutout_storage_map: HashMap<Survey, CutoutStorage> = HashMap::new();
    for survey in [Survey::Ztf, Survey::Lsst, Survey::Decam] {
        let storage = config
            .build_cutout_storage(&survey)
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to initialize cutout storage for {}: {}", survey, e)
            });
        cutout_storage_map.insert(survey, storage);
    }
    let cutout_storages = web::Data::new(cutout_storage_map);

    let babamul_is_enabled = config.babamul.enabled;
    if babamul_is_enabled {
        tracing::info!("Babamul API endpoints are ENABLED");
    } else {
        tracing::info!("Babamul API endpoints are DISABLED");
    }

    // Create API docs from OpenAPI spec
    let api_doc = ApiDoc::openapi();
    let babamul_doc = BabamulApiDoc::openapi();

    let server_result = HttpServer::new(move || {
        let mut app = App::new()
            .app_data(web::Data::new(config.clone()))
            .app_data(web::Data::new(database.clone()))
            .app_data(web::Data::new(auth.clone()))
            .app_data(web::Data::new(email_service.clone()))
            .app_data(cutout_storages.clone())
            .wrap(from_fn(request_metrics_middleware));

        // Conditionally register Babamul endpoints if enabled
        if babamul_is_enabled {
            let babamul_avro_schemas = routes::babamul::surveys::BabamulAvroSchemas::new();
            app = app.service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(babamul_avro_schemas))
                    // Larger JSON payload limit for MOC/skymap uploads (100 MB)
                    .app_data(web::JsonConfig::default().limit(104_857_600))
                    .wrap(from_fn(babamul_auth_middleware))
                    // Public routes
                    .service(Scalar::with_url("/docs", babamul_doc.clone()))
                    .service(routes::babamul::surveys::get_babamul_schema)
                    .service(routes::babamul::post_babamul_signup)
                    .service(routes::babamul::post_babamul_activate)
                    .service(routes::babamul::post_babamul_auth)
                    .service(routes::babamul::post_babamul_forgot_password)
                    .service(routes::babamul::post_babamul_reset_password)
                    // Protected routes
                    .service(routes::babamul::get_babamul_profile)
                    .service(routes::babamul::post_kafka_credentials)
                    .service(routes::babamul::get_kafka_credentials)
                    .service(routes::babamul::delete_kafka_credential)
                    .service(routes::babamul::surveys::get_object)
                    .service(routes::babamul::surveys::get_object_xmatches)
                    .service(routes::babamul::surveys::get_objects_xmatches)
                    .service(routes::babamul::surveys::get_objects)
                    .service(routes::babamul::surveys::cone_search_objects)
                    .service(routes::babamul::surveys::get_cutouts)
                    .service(routes::babamul::surveys::get_alerts)
                    .service(routes::babamul::surveys::cone_search_alerts)
                    .service(routes::babamul::stats::get_nightly_stats)
                    .service(routes::babamul::stats::get_collection_stats)
                    .service(routes::babamul::stats::get_kafka_stats)
                    .service(routes::babamul::surveys::moc_search_alerts)
                    .service(routes::babamul::tokens::get_tokens)
                    .service(routes::babamul::tokens::post_token)
                    .service(routes::babamul::tokens::delete_token),
            )
        }

        app.service(
            actix_web::web::scope("")
                .wrap(from_fn(auth_middleware))
                // Public routes
                .service(Scalar::with_url("/docs", api_doc.clone()))
                .service(routes::info::get_health)
                .service(routes::auth::post_auth)
                // Protected routes
                .service(routes::info::get_db_info)
                .service(routes::kafka::get_kafka_acls)
                .service(routes::kafka::delete_kafka_credentials)
                .service(routes::filters::post_filter)
                .service(routes::filters::patch_filter)
                .service(routes::filters::get_filters)
                .service(routes::filters::get_filter)
                .service(routes::filters::post_filter_version)
                .service(routes::filters::post_filter_test)
                .service(routes::filters::post_filter_test_count)
                .service(routes::filters::get_filter_schema)
                .service(routes::users::post_user)
                .service(routes::users::get_users)
                .service(routes::users::delete_user)
                .service(routes::catalogs::get_catalogs)
                .service(routes::catalogs::get_catalog_indexes)
                .service(routes::catalogs::get_catalog_sample)
                .service(routes::queries::post_find_query)
                .service(routes::queries::post_cone_search_query)
                .service(routes::surveys::get_cutouts)
                .service(routes::queries::post_count_query)
                .service(routes::queries::post_estimated_count_query)
                .service(routes::queries::post_pipeline_query)
                .wrap(Logger::default()),
        )
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await;

    // Flush any buffered metrics/spans before exiting. Without these, recent
    // telemetry can be lost on shutdown (especially for short-lived dev
    // restarts). Providers are `None` when `OTEL_SDK_DISABLED=true`.
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

    server_result
}
