// Database related functionality
use crate::{
    conf::{AppConfig, AuthConfig},
    routes::users::User,
};

use mongodb::bson::doc;
use mongodb::{Client, Database};

/// Protected names for operational data collections, which should not be used
/// for analytical data catalogs
pub const PROTECTED_COLLECTION_NAMES: [&str; 2] = ["users", "filters"];

async fn init_api_admin_user(
    auth_config: AuthConfig,
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
            .replace_one(doc! { "id": &existing_user.id }, updated_user)
            .await
            .expect("failed to update admin user");
    }

    Ok(())
}

async fn db_from_config(config: AppConfig) -> Database {
    let db_config = config.database;
    let uri = std::env::var("MONGODB_URI").unwrap_or_else(|_| {
        format!(
            "mongodb://{}:{}@{}:{}",
            db_config.username, db_config.password, db_config.host, db_config.port
        )
        .into()
    });
    let client = Client::with_uri_str(uri).await.expect("failed to connect");
    let db = client.database(&db_config.name);

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
    let user_id_index = mongodb::IndexModel::builder()
        .keys(doc! { "id": 1})
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
    let _ = users_collection
        .create_index(user_id_index)
        .await
        .expect("failed to create id index on users collection");

    // Initialize the API admin user if it does not exist
    if let Err(e) = init_api_admin_user(config.auth, &users_collection).await {
        eprintln!("Failed to initialize API admin user: {}", e);
    }

    db
}

pub async fn get_db() -> Database {
    // Read the config file
    let config = AppConfig::from_default_path();
    db_from_config(config).await
}

pub async fn get_default_db() -> Database {
    let config = AppConfig::default();
    db_from_config(config).await
}
