use crate::utils::{enums::Survey, o11y::logging::as_error};

use config::{Config, Value};
// TODO: we do not want to get in the habit of making 3rd party types part of
// our public API. It's almost always asking for trouble.
use config::File;
use dotenvy;
use regex::Regex;
use std::collections::HashMap;
use std::env;
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

#[instrument(err)]
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
            .ok_or(BoomConfigError::MissingKeyError)?
            .clone()
            .into_string()?;

        let radius = hashmap_xmatch
            .get("radius")
            .ok_or(BoomConfigError::MissingKeyError)?
            .clone()
            .into_float()?;

        let projection = hashmap_xmatch
            .get("projection")
            .ok_or(BoomConfigError::MissingKeyError)?
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
    stream_name: &str,
) -> Result<Vec<CatalogXmatchConfig>, BoomConfigError> {
    let crossmatches = conf.get_table("crossmatch")?;

    let crossmatches_stream = match crossmatches.get(stream_name).cloned() {
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
pub struct SurveyKafkaConfig {
    pub consumer: String, // URL of the Kafka broker for the consumer (alert worker input)
    pub producer: String, // URL of the Kafka broker for the producer (filter worker output)
    pub schema_registry: Option<String>, // URL of the schema registry (if any)
}

impl SurveyKafkaConfig {
    pub fn from_config(
        conf: &Config,
        survey: &Survey,
    ) -> Result<SurveyKafkaConfig, BoomConfigError> {
        let kafka_conf = conf.get_table("kafka")?;

        // kafka section has a consumer and producer key
        // consumer has a key per survey, producer is global

        let consumer = kafka_conf
            .get("consumer")
            .cloned()
            .unwrap_or_default()
            .into_table()?
            .get(&survey.to_string())
            .and_then(|c| c.clone().into_string().ok())
            .unwrap_or_else(|| "localhost:9092".to_string());

        let producer = kafka_conf
            .get("producer")
            .and_then(|p| p.clone().into_string().ok())
            .unwrap_or_else(|| "localhost:9092".to_string());

        let schema_registry = kafka_conf
            .get("schema_registry")
            .cloned()
            .unwrap_or_default()
            .into_table()?
            .get(&survey.to_string())
            .and_then(|sr| sr.clone().into_string().ok());

        Ok(SurveyKafkaConfig {
            consumer,
            producer,
            schema_registry,
        })
    }
}

#[instrument(skip_all, err)]
pub fn build_kafka_config(
    conf: &Config,
    survey: &Survey,
) -> Result<SurveyKafkaConfig, BoomConfigError> {
    SurveyKafkaConfig::from_config(conf, survey)
}
