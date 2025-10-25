use crate::utils::{enums::Survey, o11y::logging::as_error};

// TODO: we do not want to get in the habit of making 3rd party types part of
// our public API. It's almost always asking for trouble.
use config::{Config, File, Value};
use dotenvy;
use serde::Deserialize;
use std::path::Path;
use tracing::{debug, error, info, instrument, warn};

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
pub async fn build_db(conf: &Config) -> Result<mongodb::Database, BoomConfigError> {
    let db_conf = conf.get_table("database")?;

    let host = match db_conf.get("host") {
        Some(host) => host.clone().into_string()?,
        None => "localhost".to_string(),
    };

    let port = match db_conf.get("port") {
        Some(port) => port.clone().into_int()? as u16,
        None => 27017,
    };

    let name = match db_conf.get("name") {
        Some(name) => name.clone().into_string()?,
        None => "boom".to_string(),
    };

    let max_pool_size = match db_conf.get("max_pool_size") {
        Some(max_pool_size) => Some(max_pool_size.clone().into_int()? as u32),
        None => None,
    };

    let replica_set = match db_conf.get("replica_set") {
        Some(replica_set) => {
            if replica_set.clone().into_string().is_ok() {
                Some(replica_set.clone().into_string()?)
            } else {
                None
            }
        }
        None => None,
    };

    let username = match db_conf.get("username") {
        Some(username) => {
            if username.clone().into_string().is_ok() {
                Some(username.clone().into_string()?)
            } else {
                None
            }
        }
        None => None,
    };

    let password = match db_conf.get("password") {
        Some(password) => {
            if password.clone().into_string().is_ok() {
                Some(password.clone().into_string()?)
            } else {
                None
            }
        }
        None => None,
    };

    // verify that if username or password is set, both are set
    if username.is_some() && password.is_none() {
        panic!("username is set but password is not set");
    }

    if password.is_some() && username.is_none() {
        panic!("password is set but username is not set");
    }

    let use_srv = match db_conf.get("srv") {
        Some(srv) => srv.clone().into_bool()?,
        None => false,
    };

    let prefix = match use_srv {
        true => "mongodb+srv://",
        false => "mongodb://",
    };

    let mut uri = prefix.to_string();

    let using_auth = username.is_some() && password.is_some();

    if using_auth {
        uri.push_str(&username.unwrap());
        uri.push_str(":");
        uri.push_str(&password.unwrap());
        uri.push_str("@");
    }

    uri.push_str(&host);
    uri.push_str(":");
    uri.push_str(&port.to_string());

    uri.push_str("/");
    uri.push_str(&name);

    uri.push_str("?directConnection=true");

    if using_auth {
        uri.push_str(&format!("&authSource=admin"));
    }

    if let Some(replica_set) = replica_set {
        uri.push_str(&format!("&replicaSet={}", replica_set));
    }

    if let Some(max_pool_size) = max_pool_size {
        uri.push_str(&format!("&maxPoolSize={}", max_pool_size));
    }

    let client_mongo = mongodb::Client::with_uri_str(&uri).await?;
    let db = client_mongo.database(&name);

    Ok(db)
}

#[instrument(skip_all, err)]
pub async fn build_redis(
    conf: &Config,
) -> Result<redis::aio::MultiplexedConnection, BoomConfigError> {
    let redis_conf = conf.get_table("redis")?;

    let host = match redis_conf.get("host") {
        Some(host) => host.clone().into_string()?,
        None => "localhost".to_string(),
    };

    let port = match redis_conf.get("port") {
        Some(port) => port.clone().into_int()? as u16,
        None => 6379,
    };

    let uri = format!("redis://{}:{}/", host, port);

    let client_redis =
        redis::Client::open(uri).inspect_err(as_error!("failed to connect to redis"))?;

    let con = client_redis
        .get_multiplexed_async_connection()
        .await
        .inspect_err(as_error!("failed to get multiplexed connection"))?;

    Ok(con)
}

#[derive(Debug)]
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

#[instrument(skip(conf), err)]
pub fn build_xmatch_configs(
    conf: &Config,
    survey_name: &Survey,
) -> Result<Vec<CatalogXmatchConfig>, BoomConfigError> {
    let crossmatches = conf.get_table("crossmatch")?;

    let crossmatches_stream = match crossmatches
        .get(&survey_name.to_string().to_lowercase())
        .cloned()
    {
        Some(x) => x,
        None => {
            return Ok(Vec::new());
        }
    };
    let mut catalog_xmatch_configs = Vec::new();

    for crossmatch in crossmatches_stream.into_array()? {
        let catalog_xmatch_config = CatalogXmatchConfig::from_config(crossmatch)?;
        catalog_xmatch_configs.push(catalog_xmatch_config);
    }

    Ok(catalog_xmatch_configs)
}

#[derive(Debug, Clone)]
pub struct KafkaConsumerConfig {
    pub server: String,                  // URL of the Kafka broker
    pub group_id: String,                // Consumer group ID
    pub schema_registry: Option<String>, // URL of the schema registry (if any)
    pub username: Option<String>,        // Username for authentication (if any)
    pub password: Option<String>,        // Password for authentication (if any)
}

#[derive(Debug, Clone)]
pub struct SurveyKafkaConfig {
    pub consumer: KafkaConsumerConfig, // Configuration for the Kafka consumer (filter worker input)
    pub producer: String, // URL of the Kafka broker for the producer (filter worker output)
}

impl SurveyKafkaConfig {
    pub fn from_config(
        conf: &Config,
        survey: &Survey,
    ) -> Result<SurveyKafkaConfig, BoomConfigError> {
        let kafka_conf = conf.get_table("kafka")?;

        // kafka section has a consumer and producer key
        // consumer has a key per survey, producer is global

        let consumer_config = kafka_conf
            .get("consumer")
            .cloned()
            .unwrap_or_default()
            .into_table()?
            .get(&survey.to_string().to_lowercase())
            .cloned()
            .unwrap_or_default()
            .into_table()?;

        let consumer_server = consumer_config
            .get("server")
            .and_then(|c| c.clone().into_string().ok())
            .ok_or(BoomConfigError::MissingKeyError("server".to_string()))?;

        // the schema registry is optional
        let consumer_schema_registry = consumer_config
            .get("schema_registry")
            .and_then(|c| c.clone().into_string().ok());

        let consumer_username = consumer_config
            .get("username")
            .and_then(|c| c.clone().into_string().ok());

        let consumer_password = consumer_config
            .get("password")
            .and_then(|c| c.clone().into_string().ok());

        // group_id defaults to boom-<survey>-consumer-group
        let consumer_group_id = consumer_config
            .get("group_id")
            .and_then(|c| c.clone().into_string().ok())
            .ok_or(BoomConfigError::MissingKeyError("group_id".to_string()))?;

        let consumer = KafkaConsumerConfig {
            server: consumer_server,
            group_id: consumer_group_id,
            schema_registry: consumer_schema_registry.clone(),
            username: consumer_username,
            password: consumer_password,
        };

        let producer = kafka_conf
            .get("producer")
            .and_then(|p| p.clone().into_string().ok())
            .unwrap_or_else(|| "localhost:9092".to_string());

        Ok(SurveyKafkaConfig { consumer, producer })
    }
}

#[instrument(skip_all, err)]
pub fn build_kafka_config(
    conf: &Config,
    survey: &Survey,
) -> Result<SurveyKafkaConfig, BoomConfigError> {
    SurveyKafkaConfig::from_config(conf, survey)
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

    let config = load_raw_config(config_file).unwrap();

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
