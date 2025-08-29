use boom_config::{AppConfig, AuthConfig, BoomConfigError, DatabaseConfig, RedisConfig};
use mongodb::bson::doc;
use mongodb::{Client, Database};
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(thiserror::Error, Debug)]
pub enum BoomDbError {
    #[error("failed to connect to database using config")]
    ConnectMongoError(#[from] mongodb::error::Error),
    #[error("failed to connect to redis using config")]
    ConnectRedisError(#[from] redis::RedisError),
    #[error("configuration error")]
    ConfigError(#[from] BoomConfigError),
}

/// User model for database operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    #[serde(rename = "_id")]
    pub id: String,
    pub username: String,
    pub password: String, // This should be hashed
    pub email: String,
    pub is_admin: bool,
}

/// Protected names for operational data collections, which should not be used
/// for analytical data catalogs
pub const PROTECTED_COLLECTION_NAMES: [&str; 2] = ["users", "filters"];

#[instrument(skip_all, err)]
pub async fn build_db_from_config(config: &DatabaseConfig) -> Result<Database, BoomDbError> {
    let use_srv = config.srv.unwrap_or(false);
    let prefix = match use_srv {
        true => "mongodb+srv://",
        false => "mongodb://",
    };

    let mut uri = prefix.to_string();

    let using_auth = config.username.is_some() && config.password.is_some();

    if using_auth {
        uri.push_str(config.username.as_ref().unwrap());
        uri.push(':');
        uri.push_str(config.password.as_ref().unwrap());
        uri.push('@');
    }

    uri.push_str(&config.host);
    uri.push(':');
    uri.push_str(&config.port.to_string());

    uri.push('/');
    uri.push_str(&config.name);

    uri.push_str("?directConnection=true");

    if using_auth {
        uri.push_str("&authSource=admin");
    }

    if let Some(ref replica_set) = config.replica_set {
        uri.push_str(&format!("&replicaSet={}", replica_set));
    }

    if let Some(max_pool_size) = config.max_pool_size {
        uri.push_str(&format!("&maxPoolSize={}", max_pool_size));
    }

    let client_mongo = Client::with_uri_str(&uri).await?;
    let db = client_mongo.database(&config.name);

    Ok(db)
}

#[instrument(skip_all, err)]
pub async fn build_redis_from_config(
    config: &RedisConfig,
) -> Result<redis::aio::MultiplexedConnection, BoomDbError> {
    let uri = format!("redis://{}:{}/", config.host, config.port);

    let client_redis = redis::Client::open(uri)?;
    let con = client_redis.get_multiplexed_async_connection().await?;

    Ok(con)
}

/// Build database connection from app config
pub async fn build_db(app_config: &AppConfig) -> Result<Database, BoomDbError> {
    build_db_from_config(&app_config.database).await
}

/// Build Redis connection from app config (if Redis config exists)
pub async fn build_redis(
    app_config: &AppConfig,
) -> Result<Option<redis::aio::MultiplexedConnection>, BoomDbError> {
    if let Some(ref redis_config) = app_config.redis {
        Ok(Some(build_redis_from_config(redis_config).await?))
    } else {
        Ok(None)
    }
}

/// Convenience function to get database from default config path
pub async fn get_db() -> Result<Database, BoomDbError> {
    let config = AppConfig::from_default_path()?;
    build_db(&config).await
}

/// Convenience function to get database from specific config path
pub async fn get_db_from_path(config_path: &str) -> Result<Database, BoomDbError> {
    let config = AppConfig::from_path(config_path)?;
    build_db(&config).await
}

/// Initialize API admin user in the database
pub async fn init_api_admin_user(
    auth_config: &AuthConfig,
    users_collection: &mongodb::Collection<User>,
) -> Result<(), std::io::Error> {
    let username = auth_config.admin_username.clone();
    let email = auth_config.admin_email.clone();
    let password = auth_config.admin_password.clone();
    // Check if the admin user already exists
    let mut existing_user = users_collection
        .find_one(doc! { "username": &username })
        .await
        .expect("failed to query users collection");

    if existing_user.is_none() {
        // Create the admin user if it does not exist
        println!(
            "Admin user does not exist, creating a new one with username: {}",
            username
        );
        let admin_user = User {
            id: uuid::Uuid::new_v4().to_string(),
            username: username.clone(),
            password: bcrypt::hash(&password, bcrypt::DEFAULT_COST)
                .expect("failed to hash password"),
            email: email.clone(),
            is_admin: true, // Set the user as an admin
        };
        match users_collection.insert_one(admin_user).await {
            Ok(_) => {
                println!("Admin user created successfully.");
                return Ok(());
            }
            Err(e) => {
                // we could run into race conditions here, where multiple instances
                // try to create the admin user at the same time, so we check for
                // the specific error code for duplicate key errors
                // if the user already exists we just re-fetch the existing_user
                // and if that somehow fails, we return an error
                if !e.to_string().contains("E11000 duplicate key error") {
                    eprintln!("Failed to create admin user: {}", e);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to create admin user",
                    ));
                } else {
                    println!(
                        "Admin user already exists, but was created in another instance. Updating the user."
                    );
                    existing_user = users_collection
                        .find_one(doc! { "username": &username })
                        .await
                        .expect("failed to query users collection");
                    if existing_user.is_none() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Admin user not found after creation attempt",
                        ));
                    }
                }
            }
        }
    }

    // if the admin user exists, check that the password matches and email matches
    // if one of them does not, update the user
    let existing_user = existing_user.unwrap();
    if !bcrypt::verify(&password, &existing_user.password).unwrap_or(false)
        || existing_user.email != email
        || existing_user.is_admin != true
    {
        println!(
            "Admin user already exists, but password or email does not match with the one in the config. Updating the user."
        );
        // Update the existing user with the new password and email
        let updated_user = User {
            id: existing_user.id.clone(),
            username: existing_user.username,
            password: bcrypt::hash(&password, bcrypt::DEFAULT_COST)
                .expect("failed to hash password"),
            email: email,
            is_admin: true, // Ensure the user remains an admin
        };
        users_collection
            .replace_one(doc! { "_id": &existing_user.id }, updated_user)
            .await
            .expect("failed to update admin user");
    }

    Ok(())
}

/// Build database with API-specific initialization (users collection, admin user)
pub async fn db_from_config(config: &AppConfig) -> Result<Database, BoomDbError> {
    let db = build_db(config).await?;

    let users_collection: mongodb::Collection<User> = db.collection("users");
    // Create a unique index for username and id in the users collection
    let username_index = mongodb::IndexModel::builder()
        .keys(doc! { "username": 1})
        .options(
            mongodb::options::IndexOptions::builder()
                .unique(true)
                .build(),
        )
        .build();
    let _ = users_collection
        .create_index(username_index)
        .await
        .expect("failed to create username index on users collection");

    // Initialize the API admin user if it does not exist
    if let Some(ref auth_config) = config.auth {
        if let Err(e) = init_api_admin_user(auth_config, &users_collection).await {
            eprintln!("Failed to initialize API admin user: {}", e);
        }
    } else {
        println!("No auth config found - skipping admin user initialization");
    }

    Ok(db)
}

/// Get database from default config with API-specific initialization
pub async fn get_default_db() -> Result<Database, BoomDbError> {
    let config = AppConfig::from_default_path()?;
    db_from_config(&config).await
}
