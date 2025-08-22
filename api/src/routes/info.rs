use crate::models::response;
use actix_web::{HttpResponse, get, web};

/// Check the health of the API server
#[utoipa::path(
    get,
    path = "/",
    responses(
        (status = 200, description = "Health check successful")
    ),
    tags=["Info"]
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
    ),
    tags=["Info"]
)]
#[get("/db-info")]
pub async fn get_db_info(db: web::Data<mongodb::Database>) -> HttpResponse {
    match db.run_command(mongodb::bson::doc! { "dbstats": 1 }).await {
        Ok(stats) => response::ok("success", serde_json::to_value(stats).unwrap()),
        Err(e) => response::internal_error(&format!("Error getting database info: {:?}", e)),
    }
}
