// Database related functionality
use crate::api::routes::users::User;
use crate::conf::{AppConfig, AuthConfig, BoomConfigError};

use mongodb::bson::doc;
use mongodb::Database;

/// Protected names for operational data collections, which should not be used
/// for analytical data catalogs
pub const PROTECTED_COLLECTION_NAMES: [&str; 3] = ["filters", "babamul_users", "users"];

async fn init_api_admin_user(
    auth_config: &AuthConfig,
    users_collection: &mongodb::Collection<User>,
) -> Result<(), std::io::Error> {
    let admin_username = auth_config.admin_username.clone();
    let admin_password = auth_config.admin_password.clone();
    let admin_email = auth_config.admin_email.clone();

    // Check if the admin user already exists
    let existing_user = users_collection
        .find_one(doc! { "username": &admin_username })
        .await
        .expect("failed to query users collection");

    if existing_user.is_none() {
        // Create the admin user if it does not exist
        tracing::info!(
            "Admin user does not exist, creating a new one with username: {}",
            admin_username
        );
        let admin_user = User {
            id: uuid::Uuid::new_v4().to_string(),
            username: admin_username.clone(),
            password: bcrypt::hash(&admin_password, bcrypt::DEFAULT_COST)
                .expect("failed to hash password"),
            email: admin_email.clone(),
            is_admin: true, // Set the user as an admin
        };
        match users_collection.insert_one(admin_user).await {
            Ok(_) => {
                tracing::info!("Admin user created successfully.");
                return Ok(());
            }
            Err(e) => {
                // we could run into race conditions here, where multiple instances
                // try to create the admin user at the same time, so we check for
                // the specific error code for duplicate key errors
                // if the user already exists we just re-fetch the existing_user
                // and if that somehow fails, we return an error
                if !e.to_string().contains("E11000 duplicate key error") {
                    tracing::error!("Failed to create admin user: {}", e);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to create admin user",
                    ));
                } else {
                    tracing::info!(
                        "Admin user already exists, but was created in another instance. Updating the user."
                    );
                    let existing_user = users_collection
                        .find_one(doc! { "username": &admin_username })
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
    if let Some(existing_user) = existing_user {
        if !bcrypt::verify(&admin_password, &existing_user.password).unwrap_or(false)
            || existing_user.email != admin_email
            || !existing_user.is_admin
        {
            tracing::info!(
                "Admin user already exists, but password or email does not match with the one in the config. Updating the user."
            );
            // Update the existing user with the new password and email
            let updated_user = User {
                id: existing_user.id.clone(),
                username: admin_username.clone(),
                password: bcrypt::hash(&admin_password, bcrypt::DEFAULT_COST)
                    .expect("failed to hash password"),
                email: admin_email.clone(),
                is_admin: true, // Ensure the user remains an admin
            };
            users_collection
                .replace_one(doc! { "_id": &existing_user.id }, updated_user)
                .await
                .expect("failed to update admin user");
        }
    }

    Ok(())
}

pub async fn build_db_api(conf: &AppConfig) -> Result<mongodb::Database, BoomConfigError> {
    let db = conf.build_db().await?;

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

    // Only create the babamul_users collection if Babamul is enabled
    if conf.babamul.enabled {
        // Create babamul_users collection with unique email index
        use crate::api::routes::babamul::BabamulUser;
        let babamul_users_collection: mongodb::Collection<BabamulUser> =
            db.collection("babamul_users");
        let email_index = mongodb::IndexModel::builder()
            .keys(doc! { "email": 1})
            .options(
                mongodb::options::IndexOptions::builder()
                    .unique(true)
                    .build(),
            )
            .build();
        let _ = babamul_users_collection
            .create_index(email_index)
            .await
            .expect("failed to create email index on babamul_users collection");

        // Index on tokens.token_hash for efficient lookup
        let token_hash_index = mongodb::IndexModel::builder()
            .keys(doc! { "tokens.token_hash": 1})
            .options(
                mongodb::options::IndexOptions::builder()
                    .unique(false)
                    .build(),
            )
            .build();
        let _ = babamul_users_collection
            .create_index(token_hash_index)
            .await
            .expect("failed to create token_hash index on babamul_users collection");

        // Unique index on the user id + tokens.name to prevent duplicate token names per user
        let token_name_index = mongodb::IndexModel::builder()
            .keys(doc! { "_id": 1, "tokens.name": 1})
            .options(
                mongodb::options::IndexOptions::builder()
                    .unique(true)
                    .build(),
            )
            .build();
        let _ = babamul_users_collection
            .create_index(token_name_index)
            .await
            .expect("failed to create tokens.name unique index on babamul_users collection");

        // Index on tokens.expires_at for efficient cleanup of expired tokens
        let expires_at_index = mongodb::IndexModel::builder()
            .keys(doc! { "tokens.expires_at": 1})
            .build();
        let _ = babamul_users_collection
            .create_index(expires_at_index)
            .await
            .expect("failed to create expires_at index on babamul_users collection");

        tracing::info!("Babamul database collections initialized");
    }

    // Initialize the API admin user if it does not exist
    if let Err(e) = init_api_admin_user(&conf.api.auth, &users_collection).await {
        tracing::error!("Failed to initialize API admin user: {}", e);
    }
    Ok(db)
}

pub async fn get_test_db_api() -> Database {
    let config = AppConfig::from_test_config().unwrap();
    build_db_api(&config).await.unwrap()
}
