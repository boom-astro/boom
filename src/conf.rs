use config::Config;
use config::ConfigError;
use config::File;
use std::path::Path;
use tracing::error;

use crate::types;

pub fn load_config(filepath: &str) -> Result<Config, ConfigError> {
    let path = Path::new(filepath);

    if !path.exists() {
        error!("Config file {} does not exist.", path.display())
    }

    let conf = Config::builder()
        .add_source(File::with_name(filepath))
        .build()
        .unwrap();
    Ok(conf)
}

pub fn build_xmatch_configs(conf: &Config, stream_name: &str) -> Vec<types::CatalogXmatchConfig> {
    let crossmatches = conf
        .get_table("crossmatch")
        .expect("crossmatches key not found");
    let crossmatches_stream = crossmatches.get(stream_name).cloned();
    if crossmatches_stream.is_none() {
        return Vec::new();
    }
    let mut catalog_xmatch_configs = Vec::new();

    for crossmatch in crossmatches_stream.unwrap().clone().into_array().unwrap() {
        let catalog_xmatch_config = types::CatalogXmatchConfig::from_config(crossmatch);
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
