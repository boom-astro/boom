use actix_web::middleware::from_fn;
use actix_web::{App, HttpResponse, HttpServer, get, middleware::Logger, web};
use boom_api::auth::{auth_middleware, get_auth};
use boom_api::db::get_db;
use boom_api::models::response;
use boom_api::routes;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

/// Check the health of the API server
#[utoipa::path(
    get,
    path = "/",
    responses(
        (status = 200, description = "Health check successful")
    )
)]
#[get("/")]
pub async fn get_health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "message": "Greetings from BOOM!"
    }))
}

/// Get information about the database
#[utoipa::path(
    get,
    path = "/db-info",
    responses(
        (status = 200, description = "Database information retrieved successfully"),
    )
)]
#[get("/db-info")]
pub async fn get_db_info(db: web::Data<mongodb::Database>) -> HttpResponse {
    match db.run_command(mongodb::bson::doc! { "dbstats": 1 }).await {
        Ok(stats) => response::ok("success", serde_json::to_value(stats).unwrap()),
        Err(e) => response::internal_error(&format!("Error getting database info: {:?}", e)),
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "BOOM API",
        version = "0.1.0",
        description = "An HTTP REST interface to BOOM."
    ),
    paths(
        get_health,
        get_db_info,
        routes::users::post_user,
        routes::users::get_users,
        routes::users::delete_user,
        routes::surveys::get_object,
        routes::catalogs::get_catalogs,
        routes::catalogs::get_catalog_indexes,
        routes::catalogs::get_catalog_sample,
        routes::filters::post_filter,
        routes::filters::add_filter_version,
        routes::queries::post_count_query,
        routes::queries::post_estimated_count_query,
        routes::queries::post_find_query,
        routes::queries::post_cone_search_query,
    )
)]
struct ApiDoc;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let database = get_db().await;
    let auth = get_auth(&database).await.unwrap();

    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    // Create API docs from OpenAPI spec
    let api_doc = ApiDoc::openapi();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(database.clone()))
            .app_data(web::Data::new(auth.clone()))
            .service(Scalar::with_url("/docs", api_doc.clone()))
            .service(get_health)
            .service(get_db_info)
            .service(routes::auth::post_auth)
            .service(
                actix_web::web::scope("")
                    .wrap(from_fn(auth_middleware))
                    .service(routes::surveys::get_object)
                    .service(routes::filters::post_filter)
                    .service(routes::filters::add_filter_version)
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
    .bind(("0.0.0.0", 4000))?
    .run()
    .await
}
