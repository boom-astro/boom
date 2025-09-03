use actix_web::middleware::from_fn;
use actix_web::{App, HttpServer, middleware::Logger, web};
use boom_api::auth::{auth_middleware, get_auth};
use boom_api::db::get_db;
use boom_api::docs::ApiDoc;
use boom_api::routes;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let database = get_db().await;
    let auth = get_auth(&database).await.unwrap();

    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    // Create API docs from OpenAPI spec
    let api_doc = ApiDoc::openapi();

    // get the port from an env variable
    let port = std::env::var("PORT").unwrap_or_else(|_| "4000".into());

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(database.clone()))
            .app_data(web::Data::new(auth.clone()))
            .service(Scalar::with_url("/docs", api_doc.clone()))
            .service(routes::info::get_health)
            .service(routes::info::get_db_info)
            .service(routes::auth::post_auth)
            .service(
                actix_web::web::scope("")
                    .wrap(from_fn(auth_middleware))
                    .service(routes::surveys::get_object)
                    .service(routes::filters::post_filter)
                    .service(routes::filters::patch_filter)
                    .service(routes::filters::get_filters)
                    .service(routes::filters::get_filter)
                    .service(routes::filters::post_filter_version)
                    .service(routes::users::post_user)
                    .service(routes::users::get_users)
                    .service(routes::users::delete_user)
                    .service(routes::catalogs::get_catalogs)
                    .service(routes::catalogs::get_catalog_indexes)
                    .service(routes::catalogs::get_catalog_sample)
                    .service(routes::queries::post_find_query)
                    .service(routes::queries::post_cone_search_query)
                    .service(routes::queries::post_count_query)
                    .service(routes::queries::post_estimated_count_query),
            )
            .wrap(Logger::default())
    })
    .bind(("0.0.0.0", port.parse().unwrap()))?
    .run()
    .await
}
