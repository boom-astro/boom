use config::{Config, File};

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
            database: DatabaseConfig::default(),
        }
    }
}

pub fn load_config(config_path: Option<&str>) -> AppConfig {
    let config_fpath = config_path.unwrap_or("config.yaml");
    let default_config = AppConfig::default();
    let config = Config::builder()
        .add_source(File::with_name(config_fpath))
        .build()
        .expect("a config.yaml file should exist");
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
    {
        AppConfig {
            database: DatabaseConfig {
                name,
                host,
                port,
                username,
                password,
            },
        }
    }
}
