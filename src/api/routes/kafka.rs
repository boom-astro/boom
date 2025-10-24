//! Routes for managing BOOM's Kafka cluster.

use crate::api::models::response;
use crate::api::routes::users::User;
use actix_web::{get, post, web, HttpResponse};
use std::collections::HashMap;
use std::env;
use std::process::Command;
use std::str;
use utoipa::ToSchema;

/// Get Kafka ACLs
#[utoipa::path(
    get,
    path = "/kafka/acls",
    responses(
        (status = 200, description = "Kafka ACLs retrieved successfully"),
    ),
    tags=["Kafka"]
)]
#[get("/kafka/acls")]
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

#[derive(serde::Deserialize, Clone, ToSchema)]
pub struct AclPost {
    pub username: String,
    pub topic: String,
}

/// Add a user ACL entry
#[utoipa::path(
    post,
    path = "/kafka/acls",
    request_body = AclPost,
    responses(
        (status = 200, description = "Kafka ACL entry (read-only) added successfully"),
    ),
    tags=["Kafka"]
)]
#[post("/kafka/acls")]
pub async fn post_kafka_acl(
    body: web::Json<AclPost>,
    current_user: web::ReqData<User>,
) -> HttpResponse {
    // Only admins can access this endpoint
    if !current_user.is_admin {
        return response::forbidden("Access denied: Admins only");
    }
    // Determine broker and CLI path
    // Prefer env override for internal admin listener; default matches docker compose network
    let broker = env::var("KAFKA_INTERNAL_BROKER").unwrap_or_else(|_| "broker:29092".to_string());
    let cli_path = "/opt/kafka/bin/kafka-acls.sh";

    // Topic name must not be "*" -- it must be more specific
    if body.topic == "*" {
        return response::bad_request("Invalid topic name: cannot be '*'");
    }

    // TODO: Check that this user exists

    // Create READ operation entry
    match Command::new(cli_path)
        .arg("--bootstrap-server")
        .arg(&broker)
        .arg("--allow-principal")
        .arg(format!("User:{}", body.username))
        .arg("--add")
        .arg("--operation")
        .arg("READ")
        .arg("--topic")
        .arg(&body.topic)
        .output()
    {
        Ok(output) => {
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                return response::internal_error(&format!(
                    "Failed to add Kafka ACL via {} on {}: {}",
                    cli_path,
                    broker,
                    stderr.trim()
                ));
            }
        }
        Err(e) => {
            return response::internal_error(&format!("Error executing {}: {}", cli_path, e));
        }
    }
    // Now add DESCRIBE operation as well
    match Command::new(cli_path)
        .arg("--bootstrap-server")
        .arg(&broker)
        .arg("--allow-principal")
        .arg(format!("User:{}", body.username))
        .arg("--add")
        .arg("--operation")
        .arg("DESCRIBE")
        .arg("--topic")
        .arg(&body.topic)
        .output()
    {
        Ok(output) => {
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                return response::internal_error(&format!(
                    "Failed to add Kafka ACL via {} on {}: {}",
                    cli_path,
                    broker,
                    stderr.trim()
                ));
            }
        }
        Err(e) => {
            return response::internal_error(&format!("Error executing {}: {}", cli_path, e));
        }
    }

    response::ok("Kafka ACL added successfully", serde_json::json!({}))
}
