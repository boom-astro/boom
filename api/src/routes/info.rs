use crate::models::response;
use crate::routes::users::User;
use actix_web::{HttpResponse, get, web};
use std::env;
use std::process::Command;
use std::str;

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
    // Determine broker and CLI path
    // Prefer env override for internal admin listener; default matches docker compose network
    let broker = env::var("KAFKA_INTERNAL_BROKER").unwrap_or_else(|_| "broker:29092".to_string());
    let cli_path = "/opt/kafka/bin/kafka-acls.sh";

    // Execute the Kafka CLI to list ACLs
    match Command::new(cli_path)
        .arg("--bootstrap-server")
        .arg(&broker)
        .arg("--list")
        .output()
    {
        Ok(output) => {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                // Return both raw output and parsed lines for convenience
                let entries: Vec<String> = stdout
                    .lines()
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect();
                let data = serde_json::json!({
                    "broker": broker,
                    "cli": cli_path,
                    "entries": entries,
                    "raw": stdout,
                });
                response::ok("Kafka ACLs retrieved successfully", data)
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                response::internal_error(&format!(
                    "Failed to retrieve Kafka ACLs (exit={:?}) via {} on {}: {}",
                    output.status.code(),
                    cli_path,
                    broker,
                    stderr.trim()
                ))
            }
        }
        Err(e) => response::internal_error(&format!("Error executing {}: {}", cli_path, e)),
    }
}
