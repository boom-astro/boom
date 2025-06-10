// Database related functionality
use crate::conf::{AppConfig, DatabaseConfig};

use mongodb::bson::doc;
use mongodb::{Client, Database};

async fn db_from_config(config: DatabaseConfig) -> Database {
    let uri = std::env::var("MONGODB_URI").unwrap_or_else(|_| {
        format!(
            "mongodb://{}:{}@{}:{}",
            config.username, config.password, config.host, config.port
        )
        .into()
    });
    let client = Client::with_uri_str(uri).await.expect("failed to connect");
    let db = client.database(&config.name);
    // Create a unique index for username and id in the users collection
    let username_index = mongodb::IndexModel::builder()
        .keys(doc! { "username": 1})
        .options(
            mongodb::options::IndexOptions::builder()
                .unique(true)
                .build(),
        )
        .build();
    let user_id_index = mongodb::IndexModel::builder()
        .keys(doc! { "id": 1})
        .options(
            mongodb::options::IndexOptions::builder()
                .unique(true)
                .build(),
        )
        .build();
    let _ = db
        .collection::<mongodb::bson::Document>("users")
        .create_index(username_index)
        .await
        .expect("failed to create username index on users collection");
    let _ = db
        .collection::<mongodb::bson::Document>("users")
        .create_index(user_id_index)
        .await
        .expect("failed to create id index on users collection");
    db
}

pub async fn get_db() -> Database {
    // Read the config file
    let config = AppConfig::from_default_path().database;
    db_from_config(config).await
}

pub async fn get_default_db() -> Database {
    let config = AppConfig::default().database;
    db_from_config(config).await
}
