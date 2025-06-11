use boom_api::api;
use boom_api::db::get_db;
use boom_api::models::response;

use actix_web::{App, HttpResponse, HttpServer, get, middleware::Logger, web};

#[get("/")]
pub async fn get_health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "message": "Greetings from BOOM!"
    }))
}

#[get("/db-info")]
pub async fn get_db_info(db: web::Data<mongodb::Database>) -> HttpResponse {
    match db.run_command(mongodb::bson::doc! { "dbstats": 1 }).await {
        Ok(stats) => response::ok("success", serde_json::to_value(stats).unwrap()),
        Err(e) => response::internal_error(&format!("Error getting database info: {:?}", e)),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let database = get_db().await;

    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(database.clone()))
            .service(get_health)
            .service(get_db_info)
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
            .service(api::users::delete_user)
            .wrap(Logger::default())
    })
    .bind(("0.0.0.0", 4000))?
    .run()
    .await
}
