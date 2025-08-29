use config::{Config, File};
use regex::Regex;
use std::collections::HashMap;
use std::env;
use tracing::{debug, warn};

#[derive(thiserror::Error, Debug)]
pub enum ExpandError {
    #[error("Missing environment variable '{var_name}' for placeholder '{placeholder}'")]
    MissingVariable {
        var_name: String,
        placeholder: String,
    },
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

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig {
            secret_key: "1234".to_string(),
            token_expiration: 0,
            admin_username: "admin".to_string(),
            admin_password: "adminsecret".to_string(),
            admin_email: "admin@example.com".to_string(),
        }
    }
}

pub struct DatabaseConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        DatabaseConfig {
            name: "boom".to_string(),
            host: "localhost".to_string(),
            port: 27017,
            username: "mongoadmin".to_string(),
            password: "mongoadminsecret".to_string(),
        }
    }
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

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            auth: AuthConfig::default(),
            database: DatabaseConfig::default(),
        }
    }
}

pub fn load_config(config_path: Option<&str>) -> AppConfig {
    let config_fpath = config_path.unwrap_or("config.yaml");
    let default_config = AppConfig::default();

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

    // Load Auth configuration
    let auth_conf = api_conf
        .get("auth")
        .and_then(|auth| auth.clone().into_table().ok())
        .expect("an auth table should exist in the config file");
    let secret_key = match auth_conf.get("secret_key") {
        Some(secret_key) => secret_key
            .clone()
            .into_string()
            .unwrap_or_else(|e| panic!("Invalid secret_key: {}", e)),
        None => default_config.auth.secret_key.clone(),
    };
    let token_expiration = match auth_conf.get("token_expiration") {
        Some(token_expiration) => token_expiration
            .clone()
            .into_int()
            .unwrap_or_else(|e| panic!("Invalid token_expiration: {}", e))
            as usize,
        None => default_config.auth.token_expiration,
    };
    let admin_username = match auth_conf.get("admin_username") {
        Some(admin_username) => admin_username
            .clone()
            .into_string()
            .unwrap_or_else(|e| panic!("Invalid admin_username: {}", e)),
        None => default_config.auth.admin_username.clone(),
    };
    let admin_password = match auth_conf.get("admin_password") {
        Some(admin_password) => admin_password
            .clone()
            .into_string()
            .unwrap_or_else(|e| panic!("Invalid admin_password: {}", e)),
        None => default_config.auth.admin_password.clone(),
    };
    let admin_email = match auth_conf.get("admin_email") {
        Some(admin_email) => admin_email
            .clone()
            .into_string()
            .unwrap_or_else(|e| panic!("Invalid admin_email: {}", e)),
        None => default_config.auth.admin_email.clone(),
    };
    let auth = AuthConfig {
        secret_key,
        token_expiration,
        admin_username,
        admin_password,
        admin_email,
    };

    // Load DB configuration
    let db_conf = config
        .get_table("database")
        .expect("a database table should exist in the config file");
    let host = match db_conf.get("host") {
        Some(host) => host
            .clone()
            .into_string()
            .unwrap_or_else(|e| panic!("Invalid host: {}", e)),
        None => default_config.database.host.clone(),
    };
    let port = match db_conf.get("port") {
        Some(port) => port
            .clone()
            .into_int()
            .unwrap_or_else(|e| panic!("Invalid port: {}", e)) as u16,
        None => default_config.database.port,
    };
    let name = match db_conf.get("name") {
        Some(name) => name
            .clone()
            .into_string()
            .unwrap_or_else(|e| panic!("Invalid name: {}", e)),
        None => default_config.database.name.clone(),
    };
    let username = db_conf
        .get("username")
        .and_then(|username| username.clone().into_string().ok())
        .unwrap_or(default_config.database.username);
    let password = db_conf
        .get("password")
        .and_then(|password| password.clone().into_string().ok())
        .unwrap_or(default_config.database.password);

    let database = DatabaseConfig {
        name,
        host,
        port,
        username,
        password,
    };

    AppConfig { auth, database }
}
