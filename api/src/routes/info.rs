use crate::models::response;
use crate::routes::users::User;
use actix_web::{HttpResponse, get, web};
use std::collections::HashMap;
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

    #[derive(serde::Serialize, Debug, Clone)]
    struct KafkaAclEntry {
        principal: String,
        host: String,
        resource_type: String,
        resource_name: String,
        operation: String,
        permission_type: String,
        pattern_type: String,
    }

    fn parse_keyvals(segment: &str) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for part in segment.split(',') {
            let mut it = part.trim().splitn(2, '=');
            let key = it.next().unwrap_or("").trim();
            let val = it
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('"')
                .trim_matches('`')
                .to_string();
            if !key.is_empty() {
                map.insert(key.to_string(), val);
            }
        }
        map
    }

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

                // Parse stdout into structured ACL entries
                let mut entries: Vec<KafkaAclEntry> = Vec::new();
                let mut current_resource_type = String::new();
                let mut current_resource_name = String::new();
                let mut current_pattern_type = String::new();

                for raw_line in stdout.lines() {
                    let line = raw_line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    // Resource header line
                    if line.starts_with("Current ACLs for resource") {
                        // Try to extract the (...) payload either within backticks or directly
                        let content = if let Some(start_tick) = line.find('`') {
                            if let Some(end_tick) = line.rfind('`') {
                                line[start_tick + 1..end_tick].to_string()
                            } else {
                                line.to_string()
                            }
                        } else {
                            line.to_string()
                        };

                        if let Some(lparen) = content.find('(') {
                            if let Some(rparen) = content.rfind(')') {
                                let inner = &content[lparen + 1..rparen];
                                let kv = parse_keyvals(inner);
                                current_resource_type =
                                    kv.get("resourceType").cloned().unwrap_or_default();
                                current_resource_name = kv.get("name").cloned().unwrap_or_default();
                                current_pattern_type =
                                    kv.get("patternType").cloned().unwrap_or_default();
                            }
                        }
                        continue;
                    }

                    // ACL entry detail line
                    if let Some(lparen) = line.find('(') {
                        if let Some(rparen) = line.rfind(')') {
                            let inner = &line[lparen + 1..rparen];
                            let kv = parse_keyvals(inner);
                            let entry = KafkaAclEntry {
                                principal: kv.get("principal").cloned().unwrap_or_default(),
                                host: kv.get("host").cloned().unwrap_or_default(),
                                resource_type: current_resource_type.clone(),
                                resource_name: current_resource_name.clone(),
                                operation: kv.get("operation").cloned().unwrap_or_default(),
                                permission_type: kv
                                    .get("permissionType")
                                    .cloned()
                                    .unwrap_or_default(),
                                pattern_type: current_pattern_type.clone(),
                            };
                            // Only push if we have at least principal and operation
                            if !entry.principal.is_empty() && !entry.operation.is_empty() {
                                entries.push(entry);
                            }
                        }
                    }
                }

                response::ok(
                    "Kafka ACLs retrieved successfully",
                    serde_json::to_value(entries).unwrap(),
                )
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
