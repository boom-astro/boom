use crate::utils::{enums::Survey, o11y::logging::as_error};

use config::{Config, Value};
// TODO: we do not want to get in the habit of making 3rd party types part of
// our public API. It's almost always asking for trouble.
use config::File;
use std::path::Path;
use tracing::{error, instrument};

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
}

#[instrument(err)]
pub fn load_config(filepath: &str) -> Result<Config, BoomConfigError> {
    let path = Path::new(filepath);

    if !path.exists() {
        return Err(BoomConfigError::ConfigFileNotFound);
    }

    let conf = Config::builder()
        .add_source(File::with_name(filepath))
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
    pub n_max: Option<i64>,                  // maximum number of objects to return
}

impl CatalogXmatchConfig {
    pub fn new(
        catalog: &str,
        radius: f64,
        projection: mongodb::bson::Document,
        distance_key: Option<String>,
        distance_max: Option<f64>,
        distance_max_near: Option<f64>,
        n_max: Option<i64>,
    ) -> CatalogXmatchConfig {
        let use_distance =
            distance_key.is_some() && distance_max.is_some() && distance_max_near.is_some();

        if distance_key.is_some() || distance_max.is_some() || distance_max_near.is_some() {
            if !use_distance {
                panic!("If any of distance_key, distance_max, or distance_max_near is set, all must be set (or none at all).");
            }
        }

        CatalogXmatchConfig {
            catalog: catalog.to_string(),
            radius: radius * std::f64::consts::PI / 180.0 / 3600.0, // convert arcsec to radians
            projection,
            use_distance,
            distance_key,
            distance_max,
            distance_max_near,
            n_max,
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

        let n_max = match hashmap_xmatch.get("n_max") {
            Some(n_max) => Some(n_max.clone().into_int()?),
            None => None,
        };

        // projection is a hashmap, we need to convert it to a Document
        let mut projection_doc = mongodb::bson::Document::new();
        for (key, value) in projection.iter() {
            let key = key.as_str();
            let value = value.clone().into_int()?;
            projection_doc.insert(key, value);
        }

        Ok(CatalogXmatchConfig::new(
            &catalog,
            radius,
            projection_doc,
            distance_key,
            distance_max,
            distance_max_near,
            n_max,
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
