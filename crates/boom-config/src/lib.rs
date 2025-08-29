use config::{Config, File};
use dotenvy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::Path;
use tracing::{debug, info, warn};

#[derive(thiserror::Error, Debug)]
pub enum BoomConfigError {
    #[error("failed to load config")]
    InvalidConfigError(#[from] config::ConfigError),
    #[error("could not find config file")]
    ConfigFileNotFound,
    #[error("missing key in config")]
    MissingKeyError,
    #[error("environment variable expansion error")]
    EnvExpansionError(#[from] ExpandError),
}

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

/// Core function to load configuration with environment variable expansion
pub fn load_config(filepath: &str) -> Result<Config, BoomConfigError> {
    let path = Path::new(filepath);

    if !path.exists() {
        return Err(BoomConfigError::ConfigFileNotFound);
    }

    // Expand environment variables by reading the raw config as a string
    // and doing text replacement before parsing
    let file_content =
        std::fs::read_to_string(filepath).map_err(|e| config::ConfigError::Foreign(Box::new(e)))?;

    let expanded_content = expand_env_vars(&file_content)?;

    // Write to a temporary file and reload
    let temp_file =
        tempfile::NamedTempFile::new().map_err(|e| config::ConfigError::Foreign(Box::new(e)))?;

    std::fs::write(temp_file.path(), expanded_content)
        .map_err(|e| config::ConfigError::Foreign(Box::new(e)))?;

    let conf = Config::builder()
        .add_source(File::from(temp_file.path()))
        .build()?;

    Ok(conf)
}

/// Configuration for database connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub max_pool_size: Option<u32>,
    pub replica_set: Option<String>,
    pub srv: Option<bool>,
}

impl DatabaseConfig {
    pub fn from_config(config: &Config) -> Result<Self, BoomConfigError> {
        let db_conf = config.get_table("database")?;

        let host = db_conf
            .get("host")
            .map(|h| h.clone().into_string())
            .transpose()?
            .unwrap_or_else(|| "localhost".to_string());

        let port = db_conf
            .get("port")
            .map(|p| p.clone().into_int())
            .transpose()?
            .map(|p| p as u16)
            .unwrap_or(27017);

        let name = db_conf
            .get("name")
            .map(|n| n.clone().into_string())
            .transpose()?
            .unwrap_or_else(|| "boom".to_string());

        let max_pool_size = db_conf
            .get("max_pool_size")
            .map(|mps| mps.clone().into_int())
            .transpose()?
            .map(|mps| mps as u32);

        let replica_set = db_conf
            .get("replica_set")
            .map(|rs| rs.clone().into_string())
            .transpose()?;

        let username = db_conf
            .get("username")
            .map(|u| u.clone().into_string())
            .transpose()?;

        let password = db_conf
            .get("password")
            .map(|p| p.clone().into_string())
            .transpose()?;

        let srv = db_conf
            .get("srv")
            .map(|s| s.clone().into_bool())
            .transpose()?;

        // Validate authentication
        if username.is_some() && password.is_none() {
            panic!("username is set but password is not set");
        }

        if password.is_some() && username.is_none() {
            panic!("password is set but username is not set");
        }

        // Validate required secrets
        if let Some(ref pass) = password {
            if pass.is_empty() {
                panic!("DATABASE_PASSWORD must be set - cannot run with empty database password");
            }
        }

        Ok(DatabaseConfig {
            name,
            host,
            port,
            username,
            password,
            max_pool_size,
            replica_set,
            srv,
        })
    }
}

/// Configuration for Redis connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
}

impl RedisConfig {
    pub fn from_config(config: &Config) -> Result<Self, BoomConfigError> {
        let redis_conf = config.get_table("redis")?;

        let host = redis_conf
            .get("host")
            .map(|h| h.clone().into_string())
            .transpose()?
            .unwrap_or_else(|| "localhost".to_string());

        let port = redis_conf
            .get("port")
            .map(|p| p.clone().into_int())
            .transpose()?
            .map(|p| p as u16)
            .unwrap_or(6379);

        Ok(RedisConfig { host, port })
    }
}

/// Configuration for API authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub secret_key: String,
    pub token_expiration: usize, // in seconds
    pub admin_username: String,
    pub admin_password: String,
    pub admin_email: String,
}

impl AuthConfig {
    pub fn from_config(config: &Config) -> Result<Self, BoomConfigError> {
        let api_conf = config.get_table("api")?;

        let auth_conf = api_conf
            .get("auth")
            .and_then(|auth| auth.clone().into_table().ok())
            .ok_or(BoomConfigError::MissingKeyError)?;

        let secret_key = auth_conf
            .get("secret_key")
            .ok_or(BoomConfigError::MissingKeyError)?
            .clone()
            .into_string()
            .map_err(|_| BoomConfigError::MissingKeyError)?;

        let token_expiration = auth_conf
            .get("token_expiration")
            .map(|te| te.clone().into_int())
            .transpose()
            .map_err(|_| BoomConfigError::MissingKeyError)?
            .map(|te| te as usize)
            .unwrap_or(0);

        let admin_username = auth_conf
            .get("admin_username")
            .map(|au| au.clone().into_string())
            .transpose()
            .map_err(|_| BoomConfigError::MissingKeyError)?
            .unwrap_or_else(|| "admin".to_string());

        let admin_password = auth_conf
            .get("admin_password")
            .ok_or(BoomConfigError::MissingKeyError)?
            .clone()
            .into_string()
            .map_err(|_| BoomConfigError::MissingKeyError)?;

        let admin_email = auth_conf
            .get("admin_email")
            .map(|ae| ae.clone().into_string())
            .transpose()
            .map_err(|_| BoomConfigError::MissingKeyError)?
            .unwrap_or_else(|| "admin@example.com".to_string());

        // Validate critical auth settings
        if secret_key.is_empty() {
            panic!("SECRET_KEY must be set - cannot run with empty JWT secret");
        }
        if admin_password.is_empty() {
            panic!("ADMIN_PASSWORD must be set - cannot run with empty admin password");
        }

        Ok(AuthConfig {
            secret_key,
            token_expiration,
            admin_username,
            admin_password,
            admin_email,
        })
    }
}

/// Main application configuration that combines all sub-configurations
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub redis: Option<RedisConfig>,
    pub auth: Option<AuthConfig>,
}

impl AppConfig {
    pub fn from_default_path() -> Result<Self, BoomConfigError> {
        Self::from_path("config.yaml")
    }

    pub fn from_path(config_path: &str) -> Result<Self, BoomConfigError> {
        let config = load_config(config_path)?;

        let database = DatabaseConfig::from_config(&config)?;

        // Redis config is optional
        let redis = match RedisConfig::from_config(&config) {
            Ok(redis_config) => Some(redis_config),
            Err(_) => None,
        };

        // Auth config is optional (for API components)
        let auth = match AuthConfig::from_config(&config) {
            Ok(auth_config) => Some(auth_config),
            Err(_) => None,
        };

        Ok(AppConfig {
            database,
            redis,
            auth,
        })
    }
}
