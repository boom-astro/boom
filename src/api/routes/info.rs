use crate::api::models::response;
use crate::api::routes::users::User;
use actix_web::{get, web, HttpResponse};
use std::str;
use tracing::{debug, error};

/// Check the health of the API server
#[utoipa::path(
    get,
    path = "/",
    responses(
        (status = 200, description = "Health check successful")
    ),
    tags=["Info"]
)]
#[tracing::instrument(name = "info::get_health", skip(), fields(http.method = "GET", http.route = "/"))]
#[get("/")]
pub async fn get_health() -> HttpResponse {
    debug!("health check endpoint hit");
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
#[tracing::instrument(name = "info::get_db_info", skip(db, current_user),
fields(user_id = %current_user.id, is_admin = current_user.is_admin, http.method = "GET", http.route = "/api/db-info"))]
#[get("/db-info")]
pub async fn get_db_info(
    db: web::Data<mongodb::Database>,
    current_user: web::ReqData<User>,
) -> HttpResponse {
    debug!(user_id = %current_user.id, is_admin = current_user.is_admin, "db-info endpoint hit");
    // Only admins can access this endpoint
    if !current_user.is_admin {
        return response::forbidden("Access denied: Admins only");
    }
    match db.run_command(mongodb::bson::doc! { "dbstats": 1 }).await {
        Ok(stats) => response::ok("success", serde_json::to_value(stats).unwrap()),
        Err(e) => {
            error!(error = ?e, "Error getting database info");
            response::internal_error(&format!("Error getting database info: {:?}", e))
        }
    }
}
