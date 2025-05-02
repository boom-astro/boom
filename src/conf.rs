use config::{Config, Value};
// TODO: we do not want to get in the habit of making 3rd party types part of
// our public API. It's almost always asking for trouble.
use config::File;
use std::path::Path;
use tracing::error;

#[derive(thiserror::Error, Debug)]
pub enum BoomConfigError {
    #[error("could not find config file")]
    ConfigFileNotFound,
    #[error("failed to load config")]
    InvalidConfigError(#[from] config::ConfigError),
    #[error("missing key in config")]
    MissingKeyError,
    #[error("failed to connect to database using config")]
    ConnectMongoError(#[from] mongodb::error::Error),
    #[error("failed to connect to redis using config")]
    ConnectRedisError(#[source] redis::RedisError),
}

pub fn load_config(filepath: &str) -> Result<Config, BoomConfigError> {
    let path = Path::new(filepath);

    if !path.exists() {
        return Err(BoomConfigError::ConfigFileNotFound);
    }

    let conf = Config::builder()
        .add_source(File::with_name(filepath))
        .build()
        .map_err(BoomConfigError::InvalidConfigError)?;
    Ok(conf)
}

pub fn build_xmatch_configs(
    conf: &Config,
    stream_name: &str,
) -> Result<Vec<CatalogXmatchConfig>, BoomConfigError> {
    let crossmatches = conf
        .get_table("crossmatch")
        .map_err(BoomConfigError::InvalidConfigError)?;

    let crossmatches_stream = match crossmatches.get(stream_name).cloned() {
        Some(x) => x,
        None => {
            return Ok(Vec::new());
        }
    };
    let mut catalog_xmatch_configs = Vec::new();

    for crossmatch in crossmatches_stream
        .into_array()
        .map_err(BoomConfigError::InvalidConfigError)?
    {
        let catalog_xmatch_config = CatalogXmatchConfig::from_config(crossmatch)?;
        catalog_xmatch_configs.push(catalog_xmatch_config);
    }

    Ok(catalog_xmatch_configs)
}

pub async fn build_db(conf: &Config) -> Result<mongodb::Database, BoomConfigError> {
    let db_conf = conf
        .get_table("database")
        .map_err(BoomConfigError::InvalidConfigError)?;

    let host = match db_conf.get("host") {
        Some(host) => host
            .clone()
            .into_string()
            .map_err(BoomConfigError::InvalidConfigError)?,
        None => "localhost".to_string(),
    };

    let port = match db_conf.get("port") {
        Some(port) => port
            .clone()
            .into_int()
            .map_err(BoomConfigError::InvalidConfigError)? as u16,
        None => 27017,
    };

    let name = match db_conf.get("name") {
        Some(name) => name
            .clone()
            .into_string()
            .map_err(BoomConfigError::InvalidConfigError)?,
        None => "boom".to_string(),
    };

    let max_pool_size = match db_conf.get("max_pool_size") {
        Some(max_pool_size) => Some(
            max_pool_size
                .clone()
                .into_int()
                .map_err(BoomConfigError::InvalidConfigError)? as u32,
        ),
        None => None,
    };

    let replica_set = match db_conf.get("replica_set") {
        Some(replica_set) => {
            if replica_set.clone().into_string().is_ok() {
                Some(
                    replica_set
                        .clone()
                        .into_string()
                        .map_err(BoomConfigError::InvalidConfigError)?,
                )
            } else {
                None
            }
        }
        None => None,
    };

    let username = match db_conf.get("username") {
        Some(username) => {
            if username.clone().into_string().is_ok() {
                Some(
                    username
                        .clone()
                        .into_string()
                        .map_err(BoomConfigError::InvalidConfigError)?,
                )
            } else {
                None
            }
        }
        None => None,
    };

    let password = match db_conf.get("password") {
        Some(password) => {
            if password.clone().into_string().is_ok() {
                Some(
                    password
                        .clone()
                        .into_string()
                        .map_err(BoomConfigError::InvalidConfigError)?,
                )
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
        Some(srv) => srv
            .clone()
            .into_bool()
            .map_err(BoomConfigError::InvalidConfigError)?,
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

    let client_mongo = mongodb::Client::with_uri_str(&uri)
        .await
        .map_err(BoomConfigError::ConnectMongoError)?;
    let db = client_mongo.database(&name);

    Ok(db)
}

pub async fn build_redis(
    conf: &Config,
) -> Result<redis::aio::MultiplexedConnection, BoomConfigError> {
    let redis_conf = conf
        .get_table("redis")
        .map_err(BoomConfigError::InvalidConfigError)?;

    let host = match redis_conf.get("host") {
        Some(host) => host
            .clone()
            .into_string()
            .map_err(BoomConfigError::InvalidConfigError)?,
        None => "localhost".to_string(),
    };

    let port = match redis_conf.get("port") {
        Some(port) => port
            .clone()
            .into_int()
            .map_err(BoomConfigError::InvalidConfigError)? as u16,
        None => 6379,
    };

    let uri = format!("redis://{}:{}/", host, port);

    let client_redis = redis::Client::open(uri).map_err(BoomConfigError::ConnectRedisError)?;

    let con = client_redis
        .get_multiplexed_async_connection()
        .await
        .map_err(BoomConfigError::ConnectRedisError)?;

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
    pub fn from_config(config_value: Value) -> Result<CatalogXmatchConfig, BoomConfigError> {
        let hashmap_xmatch = config_value
            .into_table()
            .map_err(BoomConfigError::InvalidConfigError)?;

        let catalog = hashmap_xmatch
            .get("catalog")
            .ok_or(BoomConfigError::MissingKeyError)?
            .clone()
            .into_string()
            .map_err(BoomConfigError::InvalidConfigError)?;

        let radius = hashmap_xmatch
            .get("radius")
            .ok_or(BoomConfigError::MissingKeyError)?
            .clone()
            .into_float()
            .map_err(BoomConfigError::InvalidConfigError)?;

        let projection = hashmap_xmatch
            .get("projection")
            .ok_or(BoomConfigError::MissingKeyError)?
            .clone()
            .into_table()
            .map_err(BoomConfigError::InvalidConfigError)?;

        let use_distance = match hashmap_xmatch.get("use_distance") {
            Some(use_distance) => use_distance
                .clone()
                .into_bool()
                .map_err(BoomConfigError::InvalidConfigError)?,
            None => false,
        };

        let distance_key = match hashmap_xmatch.get("distance_key") {
            Some(distance_key) => Some(
                distance_key
                    .clone()
                    .into_string()
                    .map_err(BoomConfigError::InvalidConfigError)?,
            ),
            None => None,
        };

        let distance_max = match hashmap_xmatch.get("distance_max") {
            Some(distance_max) => Some(
                distance_max
                    .clone()
                    .into_float()
                    .map_err(BoomConfigError::InvalidConfigError)?,
            ),
            None => None,
        };

        let distance_max_near = match hashmap_xmatch.get("distance_max_near") {
            Some(distance_max_near) => Some(
                distance_max_near
                    .clone()
                    .into_float()
                    .map_err(BoomConfigError::InvalidConfigError)?,
            ),
            None => None,
        };

        // projection is a hashmap, we need to convert it to a Document
        let mut projection_doc = mongodb::bson::Document::new();
        for (key, value) in projection.iter() {
            let key = key.as_str();
            let value = value
                .clone()
                .into_int()
                .map_err(BoomConfigError::InvalidConfigError)?;
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
