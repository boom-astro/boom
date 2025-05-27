mod api;
mod models;

use actix_web::{App, HttpServer, web};
use mongodb::Client;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let user = "mongoadmin";
    let pass = "mongoadminsecret";
    let uri = std::env::var("MONGODB_URI")
        .unwrap_or_else(|_| format!("mongodb://{user}:{pass}@localhost:27017").into());
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
