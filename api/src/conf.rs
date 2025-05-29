use config::{Config, File};

pub struct DatabaseConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
}

pub struct AppConfig {
    pub database: DatabaseConfig,
}

pub fn load_config() -> AppConfig {
    let config = Config::builder()
        .add_source(File::with_name("config.yaml"))
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
        None => "localhost".to_string(),
    };
    let port = match db_conf.get("port") {
        Some(port) => port
            .clone()
            .into_int()
            .unwrap_or_else(|e| panic!("Invalid port: {}", e)) as u16,
        None => 27017,
    };
    let name = match db_conf.get("name") {
        Some(name) => name
            .clone()
            .into_string()
            .unwrap_or_else(|e| panic!("Invalid name: {}", e)),
        None => "boom".to_string(),
    };
    let username = db_conf
        .get("username")
        .and_then(|username| username.clone().into_string().ok())
        .unwrap_or("mongoadmin".to_string());
    let password = db_conf
        .get("password")
        .and_then(|password| password.clone().into_string().ok())
        .unwrap_or("mongoadminsecret".to_string());
    {
        AppConfig {
            database: DatabaseConfig {
                name: name,
                host,
                port,
                username,
                password,
            },
        }
    }
}
