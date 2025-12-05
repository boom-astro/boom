use actix_web::middleware::from_fn;
use actix_web::{middleware::Logger, web, App, HttpServer};
use boom::api::auth::{auth_middleware, babamul_auth_middleware, get_auth};
use boom::api::db::build_db_api;
use boom::api::docs::{ApiDoc, BabamulApiDoc};
use boom::api::email::EmailService;
use boom::api::routes;
use boom::conf::{load_dotenv, AppConfig};
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load environment variables from .env file before anything else
    load_dotenv();
    let config = AppConfig::from_default_path().unwrap();
    let database = build_db_api(&config).await.unwrap();
    let auth = get_auth(&config, &database).await.unwrap();
    let port = config.api.port;

    // Initialize email service
    let email_service = EmailService::new();

    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let babamul_is_enabled = config.babamul.enabled;
    if babamul_is_enabled {
        println!("Babamul API endpoints are ENABLED");
    } else {
        println!("Babamul API endpoints are DISABLED");
    }

    // Create API docs from OpenAPI spec
    let api_doc = ApiDoc::openapi();
    let babamul_doc = BabamulApiDoc::openapi();

    HttpServer::new(move || {
        let mut app = App::new()
            .app_data(web::Data::new(config.clone()))
            .app_data(web::Data::new(database.clone()))
            .app_data(web::Data::new(auth.clone()))
            .app_data(web::Data::new(email_service.clone()))
            .service(Scalar::with_url("/docs", api_doc.clone()))
            .service(routes::info::get_health)
            .service(routes::auth::post_auth);

        // Conditionally register Babamul endpoints if enabled
        if babamul_is_enabled {
            let babamul_avro_schemas = routes::babamul::schemas::BabamulAvroSchemas::new();
            app = app
                .app_data(web::Data::new(babamul_avro_schemas))
                .service(Scalar::with_url("/babamul/docs", babamul_doc.clone()))
                .service(routes::babamul::schemas::get_babamul_schema)
                .service(routes::babamul::users::post_babamul_signup)
                .service(routes::babamul::users::post_babamul_activate)
                .service(routes::babamul::users::post_babamul_auth)
                .service(
                    actix_web::web::scope("")
                        .wrap(from_fn(babamul_auth_middleware))
                        .service(routes::babamul::users::get_babamul_profile)
                        .service(routes::babamul::surveys::get_object),
                )
        }

        app.service(
            actix_web::web::scope("")
                .wrap(from_fn(auth_middleware))
                .service(routes::info::get_db_info)
                .service(routes::kafka::get_kafka_acls)
                .service(routes::kafka::delete_kafka_acls_for_user)
                .service(routes::filters::post_filter)
                .service(routes::filters::patch_filter)
                .service(routes::filters::get_filters)
                .service(routes::filters::get_filter)
                .service(routes::filters::post_filter_version)
                .service(routes::filters::post_filter_test)
                .service(routes::users::post_user)
                .service(routes::users::get_users)
                .service(routes::users::delete_user)
                .service(routes::catalogs::get_catalogs)
                .service(routes::catalogs::get_catalog_indexes)
                .service(routes::catalogs::get_catalog_sample)
                .service(routes::queries::post_find_query)
                .service(routes::queries::post_cone_search_query)
                .service(routes::queries::post_count_query)
                .service(routes::queries::post_estimated_count_query)
                .service(routes::queries::post_pipeline_query),
        )
        .wrap(Logger::default())
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
