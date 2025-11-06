use crate::api::auth::AuthProvider;
use crate::api::models::response;
use actix_web::{post, web, HttpResponse};
use mongodb::bson::doc;
use mongodb::Database;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use std::env;
use std::process::Command;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BabamulUser {
    // Save in the database as _id, but we want to rename on the way out
    #[serde(rename = "_id")]
    pub id: String,
    pub email: String,
    pub password_hash: String, // This will be the token, hashed for Kafka SCRAM auth
    pub activation_code: Option<String>,
    pub is_activated: bool,
    pub created_at: i64, // Unix timestamp
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct BabamulSignupPost {
    pub email: String,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Serialize, Clone, ToSchema)]
pub struct BabamulSignupResponse {
    pub message: String,
    pub token: String,
    pub expires_in: Option<usize>,
    pub activation_required: bool,
}

/// Babamul signup endpoint - creates a new Babamul user with email only
///
/// This endpoint is public and creates a new Babamul user account. The user
/// will receive a token that can be used both as a Bearer token for Babamul API
/// endpoints and as a password for Kafka stream access (username = email).
///
/// Babamul users are separate from main API users and have limited permissions:
/// - Can access Babamul-specific endpoints
/// - Can read from babamul.* Kafka topics
/// - Cannot access main API features like catalog queries
#[utoipa::path(
    post,
    path = "/babamul/signup",
    request_body = BabamulSignupPost,
    responses(
        (status = 200, description = "Signup successful", body = BabamulSignupResponse),
        (status = 409, description = "Email already exists"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[post("/babamul/signup")]
pub async fn post_babamul_signup(
    db: web::Data<Database>,
    auth: web::Data<AuthProvider>,
    body: web::Json<BabamulSignupPost>,
) -> HttpResponse {
    let email = body.email.trim().to_lowercase();

    // Basic email validation
    if !email.contains('@') || email.len() < 3 {
        return response::bad_request("Invalid email address");
    }

    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    // Check if email already exists
    match babamul_users_collection
        .find_one(doc! { "email": &email })
        .await
    {
        Ok(Some(_)) => {
            return HttpResponse::Conflict().json(serde_json::json!({
                "message": "Email already registered",
                "error": "DUPLICATE_EMAIL"
            }));
        }
        Ok(None) => {
            // Email doesn't exist, proceed with signup
        }
        Err(e) => {
            eprintln!("Database error checking email existence: {}", e);
            return response::internal_error("Database error");
        }
    }

    // Generate a unique token/password for the user
    let user_id = uuid::Uuid::new_v4().to_string();
    let token = uuid::Uuid::new_v4().to_string();

    // Hash the token for storage (used as Kafka password)
    let password_hash = match bcrypt::hash(&token, bcrypt::DEFAULT_COST) {
        Ok(hash) => hash,
        Err(e) => {
            eprintln!("Failed to hash password: {}", e);
            return response::internal_error("Failed to generate credentials");
        }
    };

    // TODO: Generate activation code if email verification is enabled
    let activation_code = Some(uuid::Uuid::new_v4().to_string());
    let is_activated = false; // Require activation

    let babamul_user = BabamulUser {
        id: user_id.clone(),
        email: email.clone(),
        password_hash: password_hash.clone(),
        activation_code: activation_code.clone(),
        is_activated,
        created_at: flare::Time::now().to_utc().timestamp(),
    };

    // Insert the user into the database
    match babamul_users_collection.insert_one(babamul_user).await {
        Ok(_) => {
            // Create Kafka SCRAM user and ACLs for babamul.* topics
            if let Err(e) = create_kafka_user_and_acls(&email, &token).await {
                eprintln!("Failed to create Kafka user/ACLs for {}: {}", email, e);
                // Continue anyway - user is created, Kafka access can be added later
            }

            // Generate JWT token for API access
            let (jwt_token, expires_in) = match create_babamul_jwt(&auth, &user_id).await {
                Ok(result) => result,
                Err(e) => {
                    eprintln!("Failed to create JWT token: {}", e);
                    return response::internal_error("Failed to generate API token");
                }
            };

            HttpResponse::Ok().json(BabamulSignupResponse {
                message: "Signup successful. Please check your email for activation instructions."
                    .to_string(),
                token: jwt_token,
                expires_in,
                activation_required: true,
            })
        }
        Err(e) => {
            eprintln!("Database error inserting babamul user: {}", e);
            if e.to_string().contains("E11000 duplicate key error") {
                HttpResponse::Conflict().json(serde_json::json!({
                    "message": "Email already registered",
                    "error": "DUPLICATE_EMAIL"
                }))
            } else {
                response::internal_error("Failed to create user")
            }
        }
    }
}

/// Create Kafka SCRAM user and ACLs to allow babamul user to read from babamul.* topics
async fn create_kafka_user_and_acls(email: &str, password: &str) -> Result<(), String> {
    // Determine broker and CLI paths
    let broker = env::var("KAFKA_INTERNAL_BROKER").unwrap_or_else(|_| "broker:29092".to_string());
    let configs_cli = "/opt/kafka/bin/kafka-configs.sh";
    let acls_cli = "/opt/kafka/bin/kafka-acls.sh";

    // First, create SCRAM user credentials
    let output = Command::new(configs_cli)
        .arg("--bootstrap-server")
        .arg(&broker)
        .arg("--alter")
        .arg("--entity-type")
        .arg("users")
        .arg("--entity-name")
        .arg(email)
        .arg("--add-config")
        .arg(format!("SCRAM-SHA-512=[password={}]", password))
        .output()
        .map_err(|e| format!("Failed to execute kafka-configs.sh: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to create SCRAM user: {}", stderr));
    }

    // Grant READ permission on babamul.* topics
    let output = Command::new(acls_cli)
        .arg("--bootstrap-server")
        .arg(&broker)
        .arg("--allow-principal")
        .arg(format!("User:{}", email))
        .arg("--add")
        .arg("--operation")
        .arg("READ")
        .arg("--topic")
        .arg("babamul.")
        .arg("--resource-pattern-type")
        .arg("prefixed")
        .output()
        .map_err(|e| format!("Failed to execute kafka-acls.sh: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to add READ ACL: {}", stderr));
    }

    // Grant DESCRIBE permission on babamul.* topics
    let output = Command::new(acls_cli)
        .arg("--bootstrap-server")
        .arg(&broker)
        .arg("--allow-principal")
        .arg(format!("User:{}", email))
        .arg("--add")
        .arg("--operation")
        .arg("DESCRIBE")
        .arg("--topic")
        .arg("babamul.")
        .arg("--resource-pattern-type")
        .arg("prefixed")
        .output()
        .map_err(|e| format!("Failed to execute kafka-acls.sh: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to add DESCRIBE ACL: {}", stderr));
    }

    // Grant READ permission on consumer groups (for offset commits)
    let output = Command::new(acls_cli)
        .arg("--bootstrap-server")
        .arg(&broker)
        .arg("--allow-principal")
        .arg(format!("User:{}", email))
        .arg("--add")
        .arg("--operation")
        .arg("READ")
        .arg("--group")
        .arg("babamul-")
        .arg("--resource-pattern-type")
        .arg("prefixed")
        .output()
        .map_err(|e| format!("Failed to execute kafka-acls.sh: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to add group READ ACL: {}", stderr));
    }

    Ok(())
}

/// Create a JWT token for a Babamul user
async fn create_babamul_jwt(
    auth: &AuthProvider,
    user_id: &str,
) -> Result<(String, Option<usize>), String> {
    use crate::api::auth::Claims;
    use jsonwebtoken::{encode, Header};

    let iat = flare::Time::now().to_utc().timestamp() as usize;
    let exp = iat + auth.token_expiration;

    // Add a "babamul:" prefix to distinguish babamul users in JWT claims
    let claims = Claims {
        sub: format!("babamul:{}", user_id),
        iat,
        exp,
    };

    let token = encode(&Header::default(), &claims, &auth.encoding_key)
        .map_err(|e| format!("JWT encoding failed: {}", e))?;

    Ok((
        token,
        if auth.token_expiration > 0 {
            Some(auth.token_expiration)
        } else {
            None
        },
    ))
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct BabamulActivatePost {
    pub email: String,
    pub activation_code: String,
}

#[derive(Serialize, Clone, ToSchema)]
pub struct BabamulActivateResponse {
    pub message: String,
    pub activated: bool,
}

/// Activate a Babamul user account
///
/// This endpoint allows users to activate their account using the activation
/// code sent to their email during signup.
#[utoipa::path(
    post,
    path = "/babamul/activate",
    request_body = BabamulActivatePost,
    responses(
        (status = 200, description = "Account activated successfully", body = BabamulActivateResponse),
        (status = 400, description = "Invalid activation code"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[post("/babamul/activate")]
pub async fn post_babamul_activate(
    db: web::Data<Database>,
    body: web::Json<BabamulActivatePost>,
) -> HttpResponse {
    let email = body.email.trim().to_lowercase();
    let activation_code = body.activation_code.trim();

    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    // Find user by email
    match babamul_users_collection
        .find_one(doc! { "email": &email })
        .await
    {
        Ok(Some(user)) => {
            // Check if already activated
            if user.is_activated {
                return HttpResponse::Ok().json(BabamulActivateResponse {
                    message: "Account is already activated".to_string(),
                    activated: true,
                });
            }

            // Verify activation code
            match &user.activation_code {
                Some(stored_code) if stored_code == activation_code => {
                    // Activate the user
                    match babamul_users_collection
                        .update_one(
                            doc! { "_id": &user.id },
                            doc! {
                                "$set": {
                                    "is_activated": true,
                                    "activation_code": mongodb::bson::Bson::Null
                                }
                            },
                        )
                        .await
                    {
                        Ok(_) => HttpResponse::Ok().json(BabamulActivateResponse {
                            message: "Account activated successfully".to_string(),
                            activated: true,
                        }),
                        Err(e) => {
                            eprintln!("Database error activating user: {}", e);
                            response::internal_error("Failed to activate account")
                        }
                    }
                }
                _ => response::bad_request("Invalid activation code"),
            }
        }
        Ok(None) => response::not_found("User not found"),
        Err(e) => {
            eprintln!("Database error fetching user: {}", e);
            response::internal_error("Database error")
        }
    }
}
