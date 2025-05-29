use boom_api::api;

use actix_web::{App, HttpServer, web};
use config::{Config, File};
use mongodb::Client;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Read the config file
    let config = Config::builder()
        .add_source(File::with_name("config.yaml"))
        .build()
        .expect("a config.yaml file should exist");
    let db_conf = config
        .get_table("database")
        .expect("a database table should exist in the config file");
    let host = match db_conf.get("host") {
        Some(host) => host.clone().into_string().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("Invalid host: {}", e))
        })?,
        None => "localhost".to_string(),
    };
    let port = match db_conf.get("port") {
        Some(port) => port.clone().into_int().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("Invalid port: {}", e))
        })? as u16,
        None => 27017,
    };
    let username = db_conf
        .get("username")
        .and_then(|username| username.clone().into_string().ok())
        .unwrap_or("mongoadmin".to_string());
    let password = db_conf
        .get("password")
        .and_then(|password| password.clone().into_string().ok())
        .unwrap_or("mongoadminsecret".to_string());
    let uri = std::env::var("MONGODB_URI").unwrap_or_else(|_| {
        format!("mongodb://{}:{}@{}:{}", username, password, host, port).into()
    });
    let client = Client::with_uri_str(uri).await.expect("failed to connect");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(client.clone()))
            .service(api::query::get_info)
            .service(api::query::sample)
            .service(api::query::cone_search)
            .service(api::query::count_documents)
            .service(api::query::find)
            .service(api::alerts::get_object)
            .service(api::filters::post_filter)
            .service(api::filters::add_filter_version)
    })
    .bind(("0.0.0.0", 4000))?
    .run()
    .await
}
