/// Functionality for working with personal access tokens (PATs).
use crate::api::models::response;
use crate::api::routes::babamul::{generate_random_string, BabamulUser};
use actix_web::{get, post, web, HttpResponse};
use chrono::Utc;
use mongodb::bson::doc;
use mongodb::Database;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

#[derive(Deserialize, Clone, ToSchema)]
pub struct TokenPost {
    pub name: String,                 // User-defined name for the token
    pub expires_in_days: Option<u32>, // Optional expiration in days
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct TokenResponse {
    pub id: String,
    pub name: String,
    pub access_token: String,
    pub created_at: i64,
    pub expires_at: i64,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct TokenPublic {
    pub id: String,
    pub name: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub last_used_at: Option<i64>,
}

#[derive(Serialize, Deserialize)]
pub struct BabamulUserToken {
    pub _id: String,     // UUID for the token
    pub user_id: String, // Reference to babamul_user
    pub name: String,
    pub token_hash: String, // SHA256 hash of the token
    pub created_at: i64,
    pub expires_at: i64,
    pub last_used_at: Option<i64>,
}

impl BabamulUserToken {
    /// Convert to a TokenPublic (without exposing the token_hash)
    pub fn to_list_item(&self) -> TokenPublic {
        TokenPublic {
            id: self._id.clone(),
            name: self.name.clone(),
            created_at: self.created_at,
            expires_at: self.expires_at,
            last_used_at: self.last_used_at,
        }
    }
}

fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Get all tokens for the authenticated user
#[utoipa::path(
    get,
    path = "/babamul/tokens",
    responses(
        (status = 200, description = "Tokens retrieved successfully", body = Vec<TokenPublic>),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[get("/tokens")]
pub async fn get_tokens(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };

    let tokens_collection: mongodb::Collection<BabamulUserToken> =
        db.collection("babamul_user_tokens");

    match tokens_collection
        .find(doc! { "user_id": &current_user.id })
        .await
    {
        Ok(cursor) => {
            use futures::TryStreamExt;
            match cursor.try_collect::<Vec<_>>().await {
                Ok(tokens) => {
                    let token_list: Vec<TokenPublic> =
                        tokens.iter().map(|t| t.to_list_item()).collect();
                    HttpResponse::Ok().json(token_list)
                }
                Err(e) => {
                    eprintln!("Database error retrieving tokens: {}", e);
                    response::internal_error("Failed to retrieve tokens")
                }
            }
        }
        Err(e) => {
            eprintln!("Database error querying tokens: {}", e);
            response::internal_error("Failed to retrieve tokens")
        }
    }
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
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[post("/tokens")]
pub async fn post_token(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<BabamulUser>>,
    body: web::Json<TokenPost>,
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

    // Generate token: bbml_{36_random_chars}
    let token_secret = generate_random_string(36);
    let full_token = format!("bbml_{}", token_secret);

    // Hash the token for storage using SHA256
    let token_hash = hash_token(&token_secret);

    // Calculate expiration
    let now = Utc::now().timestamp();
    let default_expires_days = 365;
    let expires_at = body
        .expires_in_days
        .map(|days| now + (days as i64 * 86400))
        .unwrap_or(now + (default_expires_days * 86400));

    let token_id = uuid::Uuid::new_v4().to_string();
    let token_doc = BabamulUserToken {
        _id: token_id,
        user_id: current_user.id.clone(),
        name: name.to_string(),
        token_hash,
        created_at: now,
        expires_at,
        last_used_at: None,
    };

    // Store token in babamul_user_tokens collection
    let tokens_collection: mongodb::Collection<BabamulUserToken> =
        db.collection("babamul_user_tokens");
    match tokens_collection.insert_one(&token_doc).await {
        Ok(_) => HttpResponse::Ok().json(TokenResponse {
            id: token_doc._id,
            name: token_doc.name,
            access_token: full_token,
            created_at: token_doc.created_at,
            expires_at: token_doc.expires_at,
        }),
        Err(e) => {
            eprintln!("Database error creating token: {}", e);
            response::internal_error("Failed to create token")
        }
    }
}
