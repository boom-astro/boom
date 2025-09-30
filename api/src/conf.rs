use config::{Config, File};
use dotenvy;
use serde::Deserialize;
use tracing::{debug, info, warn};

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

#[derive(Deserialize, Debug)]
pub struct AuthConfig {
    pub secret_key: String,
    pub token_expiration: usize, // in seconds
    pub admin_username: String,
    pub admin_password: String,
    pub admin_email: String,
}

#[derive(Deserialize, Debug)]
pub struct ApiConfig {
    pub auth: AuthConfig,
}

#[derive(Deserialize, Debug)]
pub struct DatabaseConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub max_pool_size: u32,
    pub replica_set: Option<String>,
    pub srv: bool,
}

#[derive(Deserialize, Debug)]
pub struct AppConfig {
    pub api: ApiConfig,
    pub database: DatabaseConfig,
}

impl AppConfig {
    pub fn from_default_path() -> Self {
        load_config(None)
    }

    pub fn from_path(config_path: &str) -> Self {
        load_config(Some(config_path))
    }

    pub fn from_test_config() -> Self {
        // Find the workspace root by looking for Cargo.toml with tests/ directory
        let mut current_dir = std::env::current_dir().expect("Failed to get current directory");
        let test_config_path = loop {
            let tests_dir = current_dir.join("tests");
            let test_config = tests_dir.join("config.test.yaml");

            // Check if we found the workspace root (has tests dir with config file)
            if test_config.exists() {
                break test_config;
            }

            // Move up to parent directory
            if let Some(parent) = current_dir.parent() {
                current_dir = parent.to_path_buf();
            } else {
                panic!("Could not find workspace root with tests/config.test.yaml");
            }
        };

        load_config(Some(test_config_path.to_str().expect("Invalid path")))
    }

    /// Validate that all required secrets are present
    fn validate_secrets(&self) -> Result<(), String> {
        if self.database.password.is_empty() {
            return Err(
                "Database password must be set via BOOM_DATABASE__PASSWORD environment variable"
                    .to_string(),
            );
        }

        if self.api.auth.secret_key.is_empty() {
            return Err(
                "API secret key must be set via BOOM_API__AUTH__SECRET_KEY environment variable"
                    .to_string(),
            );
        }

        if self.api.auth.admin_password.is_empty() {
            return Err("Admin password must be set via BOOM_API__AUTH__ADMIN_PASSWORD environment variable".to_string());
        }

        // Validate token expiration
        if self.api.auth.token_expiration <= 0 {
            return Err("Token expiration must be greater than 0 for security reasons".to_string());
        }

        Ok(())
    }
}

pub fn load_config(config_path: Option<&str>) -> AppConfig {
    load_dotenv();

    let config_file = config_path.unwrap_or("config.yaml");

    let config = Config::builder()
        .add_source(File::with_name(config_file))
        // Add environment variable overrides with BOOM prefix and __ separator for nesting
        .add_source(config::Environment::with_prefix("BOOM").separator("__"))
        .build()
        .expect("Failed to build configuration");

    let app_config: AppConfig = config
        .try_deserialize()
        .expect("Failed to deserialize configuration");

    // Validate that required secrets are present
    if let Err(e) = app_config.validate_secrets() {
        panic!("{}", e);
    }

    info!("Configuration loaded successfully");
    debug!("Database host: {}", app_config.database.host);
    debug!("Database name: {}", app_config.database.name);
    debug!("Admin username: {}", app_config.api.auth.admin_username);
    debug!(
        "Token expiration: {} seconds",
        app_config.api.auth.token_expiration
    );

    app_config
}
