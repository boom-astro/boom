use boom_api::api;
use boom_api::conf::AppConfig;

use actix_web::{App, HttpServer, web};
use mongodb::{Client, Database};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Read the config file
    let config = AppConfig::from_default_path().database;
    let uri = std::env::var("MONGODB_URI").unwrap_or_else(|_| {
        format!(
            "mongodb://{}:{}@{}:{}",
            config.username, config.password, config.host, config.port
        )
        .into()
    });
    let client = Client::with_uri_str(uri).await.expect("failed to connect");
    let database: Database = client.database(&config.name);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(database.clone()))
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
