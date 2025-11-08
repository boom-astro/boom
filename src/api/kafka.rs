use std::env;
/// Functionality for working with Kafka in the API.
use std::process::Command;

pub fn delete_acls_for_user(user: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Determine broker
    let broker = env::var("KAFKA_INTERNAL_BROKER").unwrap_or_else(|_| "broker:29092".to_string());

    // Try to find the right command name
    let acls_cli = if which::which("kafka-acls").is_ok() {
        "kafka-acls"
    } else {
        "kafka-acls.sh"
    };

    // Remove the same ACLs we add during activation
    // These should be idempotent: if an ACL isn't present, we ignore the "No matching ACLs found" message.
    let removals = vec![
        // Topic READ on * (match all)
        ("--topic", "*", "READ", "match"),
        // Topic DESCRIBE on * (match all)
        ("--topic", "*", "DESCRIBE", "match"),
        // Group READ on * (match all)
        ("--group", "*", "READ", "match"),
    ];

    let mut errors: Vec<String> = Vec::new();
    for (resource_flag, resource, operation, pattern_type) in removals {
        // Build command with args so we can log the exact invocation for debugging
        let mut cmd = Command::new(acls_cli);
        cmd.arg("--bootstrap-server")
            .arg(&broker)
            .arg("--remove")
            .arg("--allow-principal")
            .arg(format!("User:{}", user))
            .arg("--operation")
            .arg(operation)
            .arg(resource_flag)
            .arg(resource)
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
        eprintln!("Error deleting ACLs for user {}: {}", user, combined);
        return Err(format!("Failed to delete ACLs for user {}: {}", user, combined).into());
    }

    Ok(())
}
