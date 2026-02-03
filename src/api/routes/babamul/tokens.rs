/// Functionality for working with personal access tokens (PATs).
use crate::api::models::response;
use crate::api::routes::babamul::{generate_random_string, BabamulUser, BabamulUserToken};
use crate::utils::db::mongify;
use actix_web::{delete, get, post, web, HttpResponse};
use flare::Time;
use mongodb::bson::doc;
use mongodb::Database;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

#[derive(Deserialize, Clone, ToSchema)]
pub struct TokenPost {
    pub name: String,                 // User-defined name for the token
    pub expires_in_days: Option<u32>, // Optional expiration in days (1-1095, default: 365)
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
pub async fn get_tokens(current_user: Option<web::ReqData<BabamulUser>>) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };

    let token_list: Vec<TokenPublic> = current_user
        .tokens
        .iter()
        .map(|t| TokenPublic {
            id: t.id.clone(),
            name: t.name.clone(),
            created_at: t.created_at,
            expires_at: t.expires_at,
            last_used_at: t.last_used_at,
        })
        .collect();
    HttpResponse::Ok().json(token_list)
}

/// Create a new token for the authenticated user
#[utoipa::path(
    post,
    path = "/babamul/tokens",
    request_body = TokenPost,
    responses(
        (status = 200, description = "Token created successfully", body = TokenResponse),
        (status = 400, description = "Invalid request (e.g., empty name, invalid expiration, token limit reached)"),
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

    // Validate expires_in_days if provided
    if let Some(days) = body.expires_in_days {
        if days == 0 {
            return response::bad_request("Token expiration must be at least 1 day");
        }
        if days > 1095 {
            // 3 years * 365 days
            return response::bad_request("Token expiration cannot exceed 3 years (1095 days)");
        }
    }

    // Check token limit (max 10 tokens per user)
    if current_user.tokens.len() >= 10 {
        return response::bad_request("Maximum number of tokens (10) reached. Please delete an existing token before creating a new one.");
    }

    // Generate token: bbml_{36_random_chars}
    let token_secret = generate_random_string(36);
    let full_token = format!("bbml_{}", token_secret);

    // Hash the token for storage using SHA256
    let token_hash = hash_token(&token_secret);

    // Calculate expiration
    let now = Time::now().to_utc().timestamp();
    let default_expires_days = 365;
    let expires_at = body
        .expires_in_days
        .map(|days| now + (days as i64 * 86400))
        .unwrap_or(now + (default_expires_days * 86400));

    let token_id = uuid::Uuid::new_v4().to_string();
    let token_doc = BabamulUserToken {
        id: token_id.clone(),
        name: name.to_string(),
        token_hash,
        created_at: now,
        expires_at,
        last_used_at: None,
    };

    let users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");
    // first, ensure no user has a token with the same hash (extremely unlikely, but just in case)
    match users_collection
        .find_one(doc! { "tokens.token_hash": &token_doc.token_hash })
        .await
    {
        Ok(None) => {} // No collision, proceed
        Ok(Some(_)) => {
            tracing::error!(
                "Token hash collision detected when creating token for user {}",
                current_user.id
            );
            return response::internal_error("Failed to create token");
        }
        Err(e) => {
            tracing::error!("Database error checking token hash uniqueness: {}", e);
            return response::internal_error("Failed to create token");
        }
    }

    // Push it to the current_user's tokens array, making sure
    // there are no duplicates (by id and name)
    match users_collection
        .update_one(
            doc! { "_id": &current_user.id, "tokens.name": { "$ne": name }, "tokens.id": { "$ne": &token_id } },
            doc! { "$push": { "tokens": mongify(&token_doc) } },
        )
        .await
    {
        Ok(update_result) => {
            if update_result.matched_count == 0 {
                return response::bad_request("A token with this name already exists");
            }
            HttpResponse::Ok().json(TokenResponse {
                id: token_doc.id,
                name: token_doc.name,
                access_token: full_token,
                created_at: token_doc.created_at,
                expires_at: token_doc.expires_at,
            })
        }
        Err(e) => {
            tracing::error!("Database error creating token: {}", e);
            response::internal_error("Failed to create token")
        }
    }
}

/// Delete a token by ID
#[utoipa::path(
    delete,
    path = "/babamul/tokens/{token_id}",
    params(
        ("token_id" = String, Path, description = "The ID of the token to delete")
    ),
    responses(
        (status = 200, description = "Token deleted successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Token not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[delete("/tokens/{token_id}")]
pub async fn delete_token(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<BabamulUser>>,
    token_id: web::Path<String>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };

    let token_id = token_id.into_inner();

    // Remove the token from the user's tokens array
    let users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");
    match users_collection
        .update_one(
            doc! { "_id": &current_user.id },
            doc! { "$pull": { "tokens": { "id": &token_id } } },
        )
        .await
    {
        Ok(update_result) => {
            if update_result.modified_count == 0 {
                return HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Token not found",
                }));
            }
            HttpResponse::Ok().json(serde_json::json!({
                "message": "Token deleted successfully"
            }))
        }
        Err(e) => {
            tracing::error!("Database error deleting token: {}", e);
            response::internal_error("Failed to delete token")
        }
    }
}
