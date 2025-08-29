use config::{Config, File};
use dotenvy;
use regex::Regex;
use std::collections::HashMap;
use std::env;
use tracing::{debug, info, warn};

#[derive(thiserror::Error, Debug)]
pub enum ExpandError {
    #[error("Missing environment variable '{var_name}' for placeholder '{placeholder}'")]
    MissingVariable {
        var_name: String,
        placeholder: String,
    },
}

/// Loads environment variables from a .env file if it exists.
/// This function should be called early in the application startup,
/// typically before any configuration loading.
///
/// The function looks for .env files in this order:
/// 1. .env in the current working directory
/// 2. .env in the parent directory (useful when running from subdirs)
/// 3. If none found, continues without error (env vars may be set by system)
pub fn load_dotenv() {
    // Try current directory first
    if std::path::Path::new(".env").exists() {
        match dotenvy::dotenv() {
            Ok(_) => info!("Loaded environment variables from .env file"),
            Err(e) => warn!("Found .env file but failed to load it: {}", e),
        }
        return;
    }

    // Try parent directory (useful when running from subdirectories like api/)
    if std::path::Path::new("../.env").exists() {
        match dotenvy::from_path("../.env") {
            Ok(_) => info!("Loaded environment variables from ../.env file"),
            Err(e) => warn!("Found ../.env file but failed to load it: {}", e),
        }
        return;
    }

    // No .env file found - this is fine, environment variables may be set by the system
    debug!("No .env file found, using system environment variables only");
}

/// Expands environment variable placeholders in a string.
/// Supports both ${VAR_NAME} and ${VAR_NAME:-default_value} syntax.
///
/// Examples:
/// - "${BOOM_DB_PASSWORD}" -> reads from BOOM_DB_PASSWORD env var
/// - "${BOOM_DB_PASSWORD:-defaultpass}" -> reads from BOOM_DB_PASSWORD, falls back to "defaultpass"
fn expand_env_vars(input: &str) -> Result<String, ExpandError> {
    let re = Regex::new(r"\$\{([^}:]+)(?::-(.*?))?\}").unwrap();
    let mut result = input.to_string();
    let mut replacements: HashMap<String, String> = HashMap::new();

    for capture in re.captures_iter(input) {
        let full_match = capture.get(0).unwrap().as_str();
        let var_name = capture.get(1).unwrap().as_str();
        let default_value = capture.get(2).map(|m| m.as_str());

        // Check if we already processed this variable
        if let Some(replacement) = replacements.get(full_match) {
            result = result.replace(full_match, replacement);
            continue;
        }

        // Get the environment variable value
        let env_value = match env::var(var_name) {
            Ok(value) => {
                debug!("Expanded environment variable: {} = [REDACTED]", var_name);
                value
            }
            Err(_) => {
                if let Some(default) = default_value {
                    warn!(
                        "Environment variable {} not found, using default value",
                        var_name
                    );
                    default.to_string()
                } else {
                    return Err(ExpandError::MissingVariable {
                        var_name: var_name.to_string(),
                        placeholder: full_match.to_string(),
                    });
                }
            }
        };

        replacements.insert(full_match.to_string(), env_value.clone());
        result = result.replace(full_match, &env_value);
    }

    Ok(result)
}

pub struct AuthConfig {
    pub secret_key: String,
    pub token_expiration: usize, // in seconds
    pub admin_username: String,
    pub admin_password: String,
    pub admin_email: String,
}

pub struct DatabaseConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
}

pub struct AppConfig {
    pub auth: AuthConfig,
    pub database: DatabaseConfig,
}

impl AppConfig {
    pub fn from_default_path() -> Self {
        load_config(None)
    }

    pub fn from_path(config_path: &str) -> Self {
        load_config(Some(config_path))
    }
}

pub fn load_config(config_path: Option<&str>) -> AppConfig {
    let config_fpath = config_path.unwrap_or("config.yaml");

    // Read and expand environment variables in the config file
    let file_content = std::fs::read_to_string(config_fpath)
        .unwrap_or_else(|_| panic!("config file {} should exist", config_fpath));

    let expanded_content = expand_env_vars(&file_content)
        .unwrap_or_else(|e| panic!("Failed to expand environment variables: {}", e));

    // Write to a temporary file and load
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temporary file");

    std::fs::write(temp_file.path(), expanded_content).expect("Failed to write to temporary file");

    let config = Config::builder()
        .add_source(File::from(temp_file.path()))
        .build()
        .expect("a config.yaml file should exist");

    // Load API configuration
    let api_conf = config
        .get_table("api")
        .expect("an api table should exist in the config file");

    // Load Auth configuration - all values required
    let auth_conf = api_conf
        .get("auth")
        .and_then(|auth| auth.clone().into_table().ok())
        .expect("an auth table should exist in the config file");

    let secret_key = auth_conf
        .get("secret_key")
        .expect("secret_key must be set in config")
        .clone()
        .into_string()
        .unwrap_or_else(|e| panic!("Invalid secret_key: {}", e));

    let token_expiration = auth_conf
        .get("token_expiration")
        .map(|te| {
            te.clone()
                .into_int()
                .unwrap_or_else(|e| panic!("Invalid token_expiration: {}", e)) as usize
        })
        .unwrap_or(0); // Token expiration can default to 0 (disabled)

    let admin_username = auth_conf
        .get("admin_username")
        .map(|au| {
            au.clone()
                .into_string()
                .unwrap_or_else(|e| panic!("Invalid admin_username: {}", e))
        })
        .unwrap_or_else(|| "admin".to_string()); // Username can have a reasonable default

    let admin_password = auth_conf
        .get("admin_password")
        .expect("admin_password must be set in config")
        .clone()
        .into_string()
        .unwrap_or_else(|e| panic!("Invalid admin_password: {}", e));

    let admin_email = auth_conf
        .get("admin_email")
        .map(|ae| {
            ae.clone()
                .into_string()
                .unwrap_or_else(|e| panic!("Invalid admin_email: {}", e))
        })
        .unwrap_or_else(|| "admin@example.com".to_string()); // Email can have a reasonable default
    let auth = AuthConfig {
        secret_key,
        token_expiration,
        admin_username,
        admin_password,
        admin_email,
    };

    // Validate critical auth settings
    if auth.secret_key.is_empty() {
        panic!("SECRET_KEY must be set - cannot run with empty JWT secret");
    }
    if auth.admin_password.is_empty() {
        panic!("ADMIN_PASSWORD must be set - cannot run with empty admin password");
    }

    // Load DB configuration
    let db_conf = config
        .get_table("database")
        .expect("a database table should exist in the config file");

    let host = db_conf
        .get("host")
        .map(|h| {
            h.clone()
                .into_string()
                .unwrap_or_else(|e| panic!("Invalid host: {}", e))
        })
        .unwrap_or_else(|| "localhost".to_string()); // Host can have a reasonable default

    let port = db_conf
        .get("port")
        .map(|p| {
            p.clone()
                .into_int()
                .unwrap_or_else(|e| panic!("Invalid port: {}", e)) as u16
        })
        .unwrap_or(27017); // Port can have a reasonable default

    let name = db_conf
        .get("name")
        .map(|n| {
            n.clone()
                .into_string()
                .unwrap_or_else(|e| panic!("Invalid name: {}", e))
        })
        .unwrap_or_else(|| "boom".to_string()); // Database name can have a reasonable default

    let username = db_conf
        .get("username")
        .and_then(|username| username.clone().into_string().ok())
        .unwrap_or_else(|| "mongoadmin".to_string()); // Username can have a reasonable default

    let password = db_conf
        .get("password")
        .expect("database password must be set in config")
        .clone()
        .into_string()
        .unwrap_or_else(|e| panic!("Invalid database password: {}", e));

    let database = DatabaseConfig {
        name,
        host,
        port,
        username,
        password,
    };

    // Validate critical database settings
    if database.password.is_empty() {
        panic!("DATABASE_PASSWORD must be set - cannot run with empty database password");
    }

    AppConfig { auth, database }
}
