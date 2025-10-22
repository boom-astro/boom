use crate::models::response;
use crate::routes::users::User;
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
pub async fn get_db_info(
    db: web::Data<mongodb::Database>,
    current_user: web::ReqData<User>,
) -> HttpResponse {
    // Only admins can access this endpoint
    if !current_user.is_admin {
        return response::forbidden("Access denied: Admins only");
    }
    match db.run_command(mongodb::bson::doc! { "dbstats": 1 }).await {
        Ok(stats) => response::ok("success", serde_json::to_value(stats).unwrap()),
        Err(e) => response::internal_error(&format!("Error getting database info: {:?}", e)),
    }
}

/// Get Kafka ACLs
#[utoipa::path(
    get,
    path = "/kafka-acls",
    responses(
        (status = 200, description = "Kafka ACLs retrieved successfully"),
    ),
    tags=["Info"]
)]
#[get("/kafka-acls")]
pub async fn get_kafka_acls(current_user: web::ReqData<User>) -> HttpResponse {
    // Only admins can access this endpoint
    if !current_user.is_admin {
        return response::forbidden("Access denied: Admins only");
    }
    // Placeholder implementation
    // TODO: Integrate with Kafka to fetch actual users
    let kafka_users = vec!["kafka_user_1", "kafka_user_2"];
    response::ok(
        "Kafka users retrieved successfully",
        serde_json::to_value(kafka_users).unwrap(),
    )
}
