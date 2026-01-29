/// Functionality for working with personal access tokens (PATs).
use crate::api::email::EmailService;
use crate::api::models::response;
use crate::api::{auth::AuthProvider, kafka::delete_kafka_credentials_and_acls};
use actix_web::{delete, get, post, web, HttpResponse};
use mongodb::bson::doc;
use mongodb::Database;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use std::process::Command;
use utoipa::ToSchema;

use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    AeadCore, Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose, Engine as _};

use crate::api::auth::BabamulUser;

#[derive(Deserialize, Clone, ToSchema)]
pub struct TokenPost {
    pub name: String,                 // User-defined name for the token
    pub expires_in_days: Option<u32>, // Optional expiration in days
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct TokenResponse {
    pub id: String,
    pub name: String,
    pub access_token: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub is_active: bool,
}

/// Create a new token for the authenticated user
#[utoipa::path(
    post,
    path = "/babamul/tokens",
    request_body = TokenPost,
    responses(
        (status = 200, description = "Token created successfully", body = TokenResponse),
        (status = 400, description = "Invalid request (e.g., empty name)"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error or Kafka configuration failed")
    ),
    tags=["Babamul"]
)]
#[post("/tokens")]
pub async fn post_token(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<BabamulUser>>,
    body: web::Json<TokenPost>,
    config: web::Data<crate::conf::AppConfig>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };

    let name = body.name.trim();
    if name.is_empty() {
        return response::bad_request("Token name cannot be empty");
    }

    // The token will have 3 parts: bbml_{id_string:7}{validation_string:32}

    // Generate randomized credentials
    let token_id = uuid::Uuid::new_v4().to_string();
    let unhashed_token = generate_random_string(36);

    // let's encrypt the password before storing it in the database
    let kafka_password_encrypted =
        match encrypt_password(&kafka_password, &config.api.auth.get_hashed_secret_key()) {
            Ok(enc) => enc,
            Err(e) => {
                eprintln!("Failed to encrypt Kafka password: {}", e);
                return response::internal_error("Failed to encrypt Kafka credential");
            }
        };

    let kafka_credential = KafkaCredentialEncrypted {
        id: credential_id.clone(),
        name: name.to_string(),
        kafka_username: kafka_username.clone(),
        kafka_password_encrypted: kafka_password_encrypted,
        created_at: flare::Time::now().to_utc().timestamp(),
    };

    // Create Kafka SCRAM user and ACLs
    if let Err(e) = create_kafka_user_and_acls(
        &kafka_username,
        &kafka_password,
        &config.kafka.producer.server,
    )
    .await
    {
        eprintln!(
            "Failed to create Kafka user/ACLs for {}: {}",
            kafka_username, e
        );
        return response::internal_error(
            "Failed to configure Kafka access. Please try again or contact support.",
        );
    }

    // Add credential to user's list in the database
    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");
    match babamul_users_collection
        .update_one(
            doc! { "_id": &current_user.id },
            doc! {
                "$push": {
                    "kafka_credentials": mongodb::bson::to_bson(&kafka_credential).unwrap()
                }
            },
        )
        .await
    {
        Ok(_) => HttpResponse::Ok().json(CreateKafkaCredentialResponse {
            message: "Kafka credential created successfully. Save the kafka_password - it can be retrieved later but should be stored securely.".to_string(),
            credential: KafkaCredential {
                id: kafka_credential.id,
                name: kafka_credential.name,
                kafka_username: kafka_credential.kafka_username,
                kafka_password,
                created_at: kafka_credential.created_at,
            },
        }),
        Err(e) => {
            eprintln!("Database error adding Kafka credential: {}", e);
            response::internal_error("Failed to save Kafka credential")
        }
    }
}
