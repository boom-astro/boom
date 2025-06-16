use actix_web::middleware::from_fn;
use actix_web::{App, HttpResponse, HttpServer, get, middleware::Logger, web};
use boom_api::api;
use boom_api::auth::{auth_middleware, get_auth};
use boom_api::db::get_db;

#[get("/")]
pub async fn get_health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "message": "Greetings from BOOM!"
    }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let database = get_db().await;
    let auth = get_auth(&database).await;

    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(database.clone()))
            .app_data(web::Data::new(auth.clone()))
            .service(api::auth::post_auth)
            .service(
                actix_web::web::scope("")
                    .wrap(from_fn(auth_middleware))
                    .service(api::query::get_info)
                    .service(api::query::sample)
                    .service(api::query::cone_search)
                    .service(api::query::count_documents)
                    .service(api::query::find)
                    .service(api::alerts::get_object)
                    .service(api::filters::post_filter)
                    .service(api::filters::add_filter_version)
                    .service(api::users::post_user)
                    .service(api::users::get_users)
                    .service(api::users::delete_user),
            )
            .wrap(Logger::default())
    })
    .bind(("0.0.0.0", 4000))?
    .run()
    .await
}
