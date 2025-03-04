use config::{Config, Value};
// TODO: we do not want to get in the habit of making 3rd party types part of
// our public API. It's almost always asking for trouble.
use config::File;
use std::path::Path;
use tracing::error;

pub use config::ConfigError;

pub fn load_config(filepath: &str) -> Result<Config, ConfigError> {
    let path = Path::new(filepath);

    if !path.exists() {
        error!("Config file {} does not exist.", path.display())
    }

    let conf = Config::builder()
        .add_source(File::with_name(filepath))
        .build()?;
    Ok(conf)
}

pub fn build_xmatch_configs(conf: &Config, stream_name: &str) -> Vec<CatalogXmatchConfig> {
    let crossmatches = conf
        .get_table("crossmatch")
        .expect("crossmatches key not found");
    let crossmatches_stream = crossmatches.get(stream_name).cloned();
    if crossmatches_stream.is_none() {
        return Vec::new();
    }
    let mut catalog_xmatch_configs = Vec::new();

    for crossmatch in crossmatches_stream.unwrap().clone().into_array().unwrap() {
        let catalog_xmatch_config = CatalogXmatchConfig::from_config(crossmatch);
        catalog_xmatch_configs.push(catalog_xmatch_config);
    }

    catalog_xmatch_configs
}

pub async fn build_db(conf: &Config) -> mongodb::Database {
    let db_conf = conf.get_table("database").unwrap();

    let host = {
        if let Some(host) = db_conf.get("host") {
            host.clone().into_string().unwrap()
        } else {
            "localhost".to_string()
        }
    };

    let port = {
        if let Some(port) = db_conf.get("port") {
            port.clone().into_int().unwrap() as u16
        } else {
            27017
        }
    };

    let name = {
        if let Some(name) = db_conf.get("name") {
            name.clone().into_string().unwrap()
        } else {
            "boom".to_string()
        }
    };

    let max_pool_size = {
        if let Some(max_pool_size) = db_conf.get("max_pool_size") {
            Some(max_pool_size.clone().into_int().unwrap() as u32)
        } else {
            None
        }
    };

    let replica_set = {
        if let Some(replica_set) = db_conf.get("replica_set") {
            // could be a string, or could be None
            if replica_set.clone().into_string().is_ok() {
                Some(replica_set.clone().into_string().unwrap())
            } else {
                None
            }
        } else {
            None
        }
    };

    let username = {
        if let Some(username) = db_conf.get("username") {
            if username.clone().into_string().is_ok() {
                Some(username.clone().into_string().unwrap())
            } else {
                None
            }
        } else {
            None
        }
    };

    let password = {
        if let Some(password) = db_conf.get("password") {
            if password.clone().into_string().is_ok() {
                Some(password.clone().into_string().unwrap())
            } else {
                None
            }
        } else {
            None
        }
    };

    // verify that if username or password is set, both are set
    if username.is_some() && password.is_none() {
        panic!("username is set but password is not set");
    }

    if password.is_some() && username.is_none() {
        panic!("password is set but username is not set");
    }

    // if srv is True, the uri will be mongodb+srv://
    let prefix = {
        if let Some(srv) = db_conf.get("srv") {
            if srv.clone().into_bool().unwrap() {
                "mongodb+srv://"
            } else {
                "mongodb://"
            }
        } else {
            "mongodb://"
        }
    };

    let mut uri = prefix.to_string();

    let using_auth = username.is_some() && password.is_some();

    if let Some(username) = username {
        uri.push_str(&username);
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

    let client_mongo = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client_mongo.database(&name);

    db
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
    pub fn from_config(config_value: Value) -> CatalogXmatchConfig {
        let hashmap_xmatch = config_value.into_table().unwrap();

        // any of the fields can be missing, so we need to carefully handle the Option type
        let catalog = {
            if let Some(catalog) = hashmap_xmatch.get("catalog") {
                catalog.clone().into_string().unwrap()
            } else {
                // raise an error
                panic!("catalog field is missing");
            }
        };

        let radius = {
            if let Some(radius) = hashmap_xmatch.get("radius") {
                radius.clone().into_float().unwrap()
            } else {
                panic!("radius field is missing");
            }
        };

        let projection = {
            if let Some(projection) = hashmap_xmatch.get("projection") {
                projection.clone().into_table().unwrap()
            } else {
                panic!("projection field is missing");
            }
        };

        let use_distance = {
            if let Some(use_distance) = hashmap_xmatch.get("use_distance") {
                use_distance.clone().into_bool().unwrap()
            } else {
                false
            }
        };

        let distance_key = {
            if let Some(distance_key) = hashmap_xmatch.get("distance_key") {
                Some(distance_key.clone().into_string().unwrap())
            } else {
                None
            }
        };

        let distance_max = {
            if let Some(distance_max) = hashmap_xmatch.get("distance_max") {
                Some(distance_max.clone().into_float().unwrap())
            } else {
                None
            }
        };

        let distance_max_near = {
            if let Some(distance_max_near) = hashmap_xmatch.get("distance_max_near") {
                Some(distance_max_near.clone().into_float().unwrap())
            } else {
                None
            }
        };

        // projection is a hashmap, we need to convert it to a Document
        let mut projection_doc = mongodb::bson::Document::new();
        for (key, value) in projection.iter() {
            let key = key.as_str();
            let value = value.clone().into_int().unwrap();
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

        CatalogXmatchConfig::new(
            &catalog,
            radius,
            projection_doc,
            use_distance,
            distance_key,
            distance_max,
            distance_max_near,
        )
    }
}
