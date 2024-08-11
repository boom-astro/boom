mod query;
mod ztf_alerts;

use actix_web::{web, App, HttpServer};
use mongodb::Client;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let uri = std::env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into());

    let client = Client::with_uri_str(uri).await.expect("failed to connect");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(client.clone()))
            .service(query::query)
            .service(ztf_alerts::get_object)
    })
    .bind(("0.0.0.0", 4000))?
    .run()
    .await
}