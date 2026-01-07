//! Functionality for working with Kafka in the API.
use std::collections::HashMap;
use std::process::Command;

#[derive(serde::Serialize, Debug, Clone)]
pub struct KafkaAclEntry {
    principal: String,
    host: String,
    resource_type: String,
    resource_name: String,
    operation: String,
    permission_type: String,
    pattern_type: String,
}

pub fn get_acls(broker: &str) -> Result<Vec<KafkaAclEntry>, Box<dyn std::error::Error>> {
    get_acls_internal(broker, None)
}

pub fn get_acls_for_client_id(
    client_id: &str,
    broker: &str,
) -> Result<Vec<KafkaAclEntry>, Box<dyn std::error::Error>> {
    get_acls_internal(broker, Some(client_id))
}

fn get_acls_internal(
    broker: &str,
    client_id: Option<&str>,
) -> Result<Vec<KafkaAclEntry>, Box<dyn std::error::Error>> {
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

    // Try to find the right command name
    let acls_cli = if which::which("kafka-acls").is_ok() {
        "kafka-acls"
    } else {
        "kafka-acls.sh"
    };

    // Build the command to list ACLs, optionally filtered by principal
    let mut cmd = Command::new(acls_cli);
    cmd.arg("--bootstrap-server").arg(&broker).arg("--list");

    if let Some(id) = client_id {
        cmd.arg("--principal").arg(format!("User:{}", id));
    }

    // Execute the Kafka CLI to list ACLs
    match cmd.output() {
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
                        // Extract the content inside backticks (preferred) or the whole line
                        let content = if let Some(start_tick) = line.find('`') {
                            if let Some(end_tick) = line.rfind('`') {
                                line[start_tick + 1..end_tick].to_string()
                            } else {
                                line.to_string()
                            }
                        } else {
                            line.to_string()
                        };

                        // First try key=val pattern inside parentheses, e.g. ResourcePattern(resourceType=TOPIC, name=foo, patternType=LITERAL)
                        if let (Some(lparen), Some(rparen)) =
                            (content.find('('), content.rfind(')'))
                        {
                            let inner = &content[lparen + 1..rparen];
                            let kv = parse_keyvals(inner);
                            current_resource_type =
                                kv.get("resourceType").cloned().unwrap_or_else(|| {
                                    kv.get("resourceTypeName").cloned().unwrap_or_default()
                                });
                            current_resource_name = kv.get("name").cloned().unwrap_or_default();
                            current_pattern_type =
                                kv.get("patternType").cloned().unwrap_or_else(|| {
                                    kv.get("patternTypeLiteral").cloned().unwrap_or_default()
                                });
                        } else {
                            // Fallback: sometimes the header is like `Topic:LITERAL:foo` or `Group:PREFIXED:bar`
                            let header = content
                                .trim_start_matches("Current ACLs for resource")
                                .trim()
                                .trim_matches('`')
                                .trim_matches(':')
                                .trim();
                            let parts: Vec<&str> = header.split(':').collect();
                            if parts.len() >= 3 {
                                current_resource_type = parts[0].trim().to_string();
                                current_pattern_type = parts[1].trim().to_string();
                                current_resource_name = parts[2..].join(":");
                            }
                        }
                        continue;
                    }

                    // ACL entry detail line
                    if let Some(lparen) = line.find('(') {
                        if let Some(rparen) = line.rfind(')') {
                            let inner = &line[lparen + 1..rparen];
                            let kv = parse_keyvals(inner);

                            // Derive resource info: prefer inline resource=Type:Pattern:Name, else use current header context
                            let mut resource_type = current_resource_type.clone();
                            let mut resource_name = current_resource_name.clone();
                            let mut pattern_type = current_pattern_type.clone();
                            if let Some(res) = kv.get("resource") {
                                let parts: Vec<&str> = res.split(':').collect();
                                if parts.len() >= 3 {
                                    resource_type = parts[0].trim().to_string();
                                    pattern_type = parts[1].trim().to_string();
                                    resource_name = parts[2..].join(":");
                                }
                            }

                            // Support both permissionType=ALLOW and permission=ALLOW
                            let permission = kv
                                .get("permissionType")
                                .cloned()
                                .or_else(|| kv.get("permission").cloned())
                                .unwrap_or_default();

                            let entry = KafkaAclEntry {
                                principal: kv.get("principal").cloned().unwrap_or_default(),
                                host: kv.get("host").cloned().unwrap_or_default(),
                                resource_type,
                                resource_name,
                                operation: kv.get("operation").cloned().unwrap_or_default(),
                                permission_type: permission,
                                pattern_type,
                            };
                            // Only push if we have at least principal and operation
                            if !entry.principal.is_empty() && !entry.operation.is_empty() {
                                entries.push(entry);
                            }
                        }
                    }
                }

                Ok(entries)
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                let error_msg = if let Some(id) = client_id {
                    format!(
                        "Failed to retrieve Kafka ACLs for client {} (exit={:?}) via {} on {}: {}",
                        id,
                        output.status.code(),
                        acls_cli,
                        broker,
                        stderr.trim()
                    )
                } else {
                    format!(
                        "Failed to retrieve Kafka ACLs (exit={:?}) via {} on {}: {}",
                        output.status.code(),
                        acls_cli,
                        broker,
                        stderr.trim()
                    )
                };
                Err(error_msg.into())
            }
        }
        Err(e) => Err(format!("Error executing {}: {}", acls_cli, e).into()),
    }
}

pub fn delete_acls_for_client_id(
    client_id: &str,
    broker: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Try to find the right command name
    let acls_cli = if which::which("kafka-acls").is_ok() {
        "kafka-acls"
    } else {
        "kafka-acls.sh"
    };

    // Fetch ACLs for this specific client using the new method
    let user_acls = get_acls_for_client_id(client_id, broker)?;

    let mut errors: Vec<String> = Vec::new();
    for acl in user_acls {
        let resource_flag = match acl.resource_type.as_str() {
            "TOPIC" => "--topic",
            "GROUP" => "--group",
            "CLUSTER" => "--cluster",
            "TRANSACTIONAL_ID" => "--transactional-id",
            "DELEGATION_TOKEN" => "--delegation-token",
            _ => {
                errors.push(format!(
                    "Unknown resource type '{}' for ACL entry: {:?}",
                    acl.resource_type, acl
                ));
                continue;
            }
        };
        let pattern_type = match acl.pattern_type.as_str() {
            "LITERAL" => "LITERAL",
            "PREFIXED" => "PREFIXED",
            "ANY" => "ANY",
            _ => {
                errors.push(format!(
                    "Unknown pattern type '{}' for ACL entry: {:?}",
                    acl.pattern_type, acl
                ));
                continue;
            }
        };
        let permission_flag = match acl.permission_type.as_str() {
            "ALLOW" => "--allow-principal",
            "DENY" => "--deny-principal",
            _ => {
                errors.push(format!(
                    "Unknown permission type '{}' for ACL entry: {:?}",
                    acl.permission_type, acl
                ));
                continue;
            }
        };
        let mut cmd = Command::new(acls_cli);
        cmd.arg("--bootstrap-server")
            .arg(&broker)
            .arg("--remove")
            .arg(permission_flag)
            .arg(format!("User:{}", client_id))
            .arg("--operation")
            .arg(acl.operation)
            .arg(resource_flag)
            .arg(acl.resource_name)
            .arg("--resource-pattern-type")
            .arg(pattern_type)
            .arg("--force");

        let output = cmd.output();

        let output = match output {
            Ok(o) => o,
            Err(e) => {
                errors.push(format!("Failed to execute {}: {}", acls_cli, e));
                continue;
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Tolerate idempotent removals where nothing matched
            let msg = stderr.to_string();
            if !(msg.contains("No ACLs found") || msg.contains("No matching ACLs found")) {
                errors.push(msg);
            }
        }
    }

    if !errors.is_empty() {
        let combined = errors.join("; ");
        eprintln!("Error deleting ACLs for user {}: {}", client_id, combined);
        return Err(format!("Failed to delete ACLs for user {}: {}", client_id, combined).into());
    }

    Ok(())
}

// let's add a delete client id function that deletes both SCRAM credentials and ACLs
pub fn delete_kafka_credentials_and_acls(
    client_id: &str,
    broker: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // First delete SCRAM credentials
    {
        // Try to find the right command name
        let configs_cli = if which::which("kafka-configs").is_ok() {
            "kafka-configs"
        } else {
            "kafka-configs.sh"
        };

        let output = Command::new(configs_cli)
            .arg("--bootstrap-server")
            .arg(&broker)
            .arg("--alter")
            .arg("--entity-type")
            .arg("users")
            .arg("--entity-name")
            .arg(client_id)
            .arg("--delete-config")
            .arg("SCRAM-SHA-512")
            .output()
            .map_err(|e| format!("Failed to execute {}: {}", configs_cli, e))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Log the error but don't fail if the user doesn't exist
            eprintln!(
                "Note: SCRAM user deletion may have failed (user may not exist): {}",
                stderr
            );
        }
    }
    // Then delete ACLs
    delete_acls_for_client_id(client_id, broker)?;
    Ok(())
}
