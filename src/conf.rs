use crate::utils::{enums::Survey, o11y::logging::as_error};
use config::{Config, File, Value};
use dotenvy;
use mongodb::bson::doc;
use mongodb::Database;
use serde::Deserialize;
use std::{collections::HashMap, path::Path};
use tracing::{debug, error, info, instrument, warn};

const DEFAULT_CONFIG_PATH: &str = "config.yaml";

#[derive(thiserror::Error, Debug)]
pub enum BoomConfigError {
    #[error("failed to load config")]
    InvalidConfigError(#[from] config::ConfigError),
    #[error("failed to connect to database using config")]
    ConnectMongoError(#[from] mongodb::error::Error),
    #[error("failed to connect to redis using config")]
    ConnectRedisError(#[from] redis::RedisError),
    #[error("could not find config file")]
    ConfigFileNotFound,
    #[error("missing key in config: {0}")]
    MissingKeyError(String),
    #[error("failed to deserialize config: {0}")]
    InvalidSecretError(String),
}

/// Load environment variables from a .env file if it exists.
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

#[instrument(err)]
pub fn load_raw_config(filepath: &str) -> Result<Config, BoomConfigError> {
    let path = Path::new(filepath);

    if !path.exists() {
        return Err(BoomConfigError::ConfigFileNotFound);
    }

    load_dotenv();

    let conf = Config::builder()
        .add_source(File::from(path))
        .add_source(
            config::Environment::with_prefix("boom")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    Ok(conf)
}

#[instrument(skip_all, err)]
pub async fn build_db(conf: &AppConfig) -> Result<mongodb::Database, BoomConfigError> {
    let db_conf = &conf.database;

    let prefix = match db_conf.srv {
        true => "mongodb+srv://",
        false => "mongodb://",
    };

    let mut uri = prefix.to_string();

    let using_auth = !db_conf.username.is_empty() && !db_conf.password.is_empty();

    if using_auth {
        uri.push_str(&db_conf.username);
        uri.push_str(":");
        uri.push_str(&db_conf.password);
        uri.push_str("@");
    }

    uri.push_str(&db_conf.host);
    uri.push_str(":");
    uri.push_str(&db_conf.port.to_string());

    uri.push_str("/");
    uri.push_str(&db_conf.name);

    uri.push_str("?directConnection=true");

    if using_auth {
        uri.push_str(&format!("&authSource=admin"));
    }

    if let Some(replica_set) = &db_conf.replica_set {
        uri.push_str(&format!("&replicaSet={}", replica_set));
    }

    uri.push_str(&format!("&maxPoolSize={}", db_conf.max_pool_size));

    let client_mongo = mongodb::Client::with_uri_str(&uri).await?;
    let db = client_mongo.database(&db_conf.name);

    Ok(db)
}

#[instrument(skip_all, err)]
pub async fn build_redis(
    conf: &AppConfig,
) -> Result<redis::aio::MultiplexedConnection, BoomConfigError> {
    let host = &conf.redis.host;
    let port = conf.redis.port;

    let uri = format!("redis://{}:{}/", host, port);

    let client_redis =
        redis::Client::open(uri).inspect_err(as_error!("failed to connect to redis"))?;

    let con = client_redis
        .get_multiplexed_async_connection()
        .await
        .inspect_err(as_error!("failed to get multiplexed connection"))?;

    Ok(con)
}

#[derive(Debug, Clone)]
pub struct CatalogXmatchConfig {
    pub catalog: String,                     // name of the collection in the database
    pub radius: f64,                         // radius in radians
    pub projection: mongodb::bson::Document, // projection to apply to the catalog
    pub use_distance: bool,                  // whether to use the distance field in the crossmatch
    pub distance_key: Option<String>,        // name of the field to use for distance
    pub distance_max: Option<f64>,           // maximum distance in kpc
    pub distance_max_near: Option<f64>,      // maximum distance in arcsec for nearby objects
}

impl CatalogXmatchConfig {
    pub fn new(
        catalog: &str,
        radius: f64,
        projection: mongodb::bson::Document,
        use_distance: bool,
        distance_key: Option<String>,
        distance_max: Option<f64>,
        distance_max_near: Option<f64>,
    ) -> CatalogXmatchConfig {
        CatalogXmatchConfig {
            catalog: catalog.to_string(),
            radius: radius * std::f64::consts::PI / 180.0 / 3600.0, // convert arcsec to radians
            projection,
            use_distance,
            distance_key,
            distance_max,
            distance_max_near,
        }
    }

    // based on the code in the main function, create a from_config function
    #[instrument(skip_all, err)]
    pub fn from_config(config_value: Value) -> Result<CatalogXmatchConfig, BoomConfigError> {
        let hashmap_xmatch = config_value.into_table()?;

        let catalog = hashmap_xmatch
            .get("catalog")
            .ok_or(BoomConfigError::MissingKeyError("catalog".to_string()))?
            .clone()
            .into_string()?;

        let radius = hashmap_xmatch
            .get("radius")
            .ok_or(BoomConfigError::MissingKeyError("radius".to_string()))?
            .clone()
            .into_float()?;

        let projection = hashmap_xmatch
            .get("projection")
            .ok_or(BoomConfigError::MissingKeyError("projection".to_string()))?
            .clone()
            .into_table()?;

        let use_distance = match hashmap_xmatch.get("use_distance") {
            Some(use_distance) => use_distance.clone().into_bool()?,
            None => false,
        };

        let distance_key = match hashmap_xmatch.get("distance_key") {
            Some(distance_key) => Some(distance_key.clone().into_string()?),
            None => None,
        };

        let distance_max = match hashmap_xmatch.get("distance_max") {
            Some(distance_max) => Some(distance_max.clone().into_float()?),
            None => None,
        };

        let distance_max_near = match hashmap_xmatch.get("distance_max_near") {
            Some(distance_max_near) => Some(distance_max_near.clone().into_float()?),
            None => None,
        };

        // projection is a hashmap, we need to convert it to a Document
        let mut projection_doc = mongodb::bson::Document::new();
        for (key, value) in projection.iter() {
            let key = key.as_str();
            let value = value.clone().into_int()?;
            projection_doc.insert(key, value);
        }

        if use_distance {
            if distance_key.is_none() {
                panic!("must provide a distance_key if use_distance is true");
            }

            if distance_max.is_none() {
                panic!("must provide a distance_max if use_distance is true");
            }

            if distance_max_near.is_none() {
                panic!("must provide a distance_max_near if use_distance is true");
            }
        }

        Ok(CatalogXmatchConfig::new(
            &catalog,
            radius,
            projection_doc,
            use_distance,
            distance_key,
            distance_max,
            distance_max_near,
        ))
    }
}

// implement Deserialize for CatalogXmatchConfig
impl<'de> Deserialize<'de> for CatalogXmatchConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = Value::deserialize(deserializer).map_err(serde::de::Error::custom)?;
        CatalogXmatchConfig::from_config(v).map_err(serde::de::Error::custom)
    }
}

fn default_kafka_server() -> String {
    "localhost:9092".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConsumerConfig {
    #[serde(default = "default_kafka_server")]
    pub server: String, // URL of the Kafka broker
    pub group_id: String,                // Consumer group ID
    pub schema_registry: Option<String>, // URL of the schema registry (if any)
    pub username: Option<String>,        // Username for authentication (if any)
    pub password: Option<String>,        // Password for authentication (if any)
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct KafkaProducerConfig {
    #[serde(default = "default_kafka_server")]
    pub server: String, // URL of the Kafka broker
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub consumer: HashMap<Survey, KafkaConsumerConfig>,
    #[serde(default)]
    pub producer: KafkaProducerConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AuthConfig {
    pub secret_key: String,
    pub token_expiration: usize, // in seconds
    pub admin_username: String,
    pub admin_password: String,
    pub admin_email: String,
}

fn default_api_port() -> u16 {
    4000
}

#[derive(Deserialize, Debug, Clone)]
pub struct ApiConfig {
    pub auth: AuthConfig,
    #[serde(default = "default_api_port")]
    pub port: u16,
}

#[derive(Deserialize, Debug, Clone)]
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

#[derive(Deserialize, Debug, Clone)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
}

impl Default for RedisConfig {
    fn default() -> Self {
        RedisConfig {
            host: "localhost".to_string(),
            port: 6379,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct BabamulConfig {
    pub enabled: bool,
}

impl Default for BabamulConfig {
    fn default() -> Self {
        BabamulConfig { enabled: false }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct WorkerConfig {
    pub n_workers: usize,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SurveyWorkerConfig {
    pub command_interval: u64,
    pub alert: WorkerConfig,
    pub enrichment: WorkerConfig,
    pub filter: WorkerConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    pub api: ApiConfig,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub redis: RedisConfig,
    #[serde(default)]
    pub babamul: BabamulConfig,
    pub kafka: KafkaConfig,
    #[serde(default)]
    pub crossmatch: HashMap<Survey, Vec<CatalogXmatchConfig>>,
    #[serde(default)]
    pub workers: HashMap<Survey, SurveyWorkerConfig>,
}

impl AppConfig {
    pub fn from_default_path() -> Result<Self, BoomConfigError> {
        load_config(None)
    }

    pub fn from_path(config_path: &str) -> Result<Self, BoomConfigError> {
        load_config(Some(config_path))
    }

    pub fn from_test_config() -> Result<Self, BoomConfigError> {
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

pub fn load_config(config_path: Option<&str>) -> Result<AppConfig, BoomConfigError> {
    load_dotenv();

    let config_file = config_path.unwrap_or(DEFAULT_CONFIG_PATH);

    let config = load_raw_config(config_file)?;

    let app_config: AppConfig = config.try_deserialize()?;

    // Validate that required secrets are present
    if let Err(e) = app_config.validate_secrets() {
        return Err(BoomConfigError::InvalidSecretError(e));
    }

    info!("Configuration loaded successfully");
    debug!("Database host: {}", app_config.database.host);
    debug!("Database name: {}", app_config.database.name);
    debug!("Admin username: {}", app_config.api.auth.admin_username);
    debug!("Admin email: {}", app_config.api.auth.admin_email);
    debug!("API port: {}", app_config.api.port);
    debug!(
        "Token expiration: {} seconds",
        app_config.api.auth.token_expiration
    );

    Ok(app_config)
}

pub async fn get_test_db() -> Database {
    let config = AppConfig::from_test_config().expect("Failed to load test config");
    build_db(&config).await.unwrap()
}
