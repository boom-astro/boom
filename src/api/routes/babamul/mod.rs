pub mod surveys;

use crate::api::auth::AuthProvider;
use crate::api::email::EmailService;
use crate::api::models::response;
use actix_web::{get, post, web, HttpResponse};
use mongodb::bson::doc;
use mongodb::Database;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use std::process::Command;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BabamulUser {
    // Save in the database as _id, but we want to rename on the way out
    #[serde(rename = "_id")]
    pub id: String,
    pub username: String,
    pub email: String,
    pub password_hash: String, // Hashed password (for both Kafka SCRAM auth and API auth)
    pub activation_code: Option<String>,
    pub is_activated: bool,
    pub created_at: i64, // Unix timestamp
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BabamulUserPublic {
    // Save in the database as _id, but we want to rename on the way out
    #[serde(rename = "_id")]
    pub id: String,
    pub username: String,
    pub email: String,
    pub created_at: i64, // Unix timestamp
}

impl From<BabamulUser> for BabamulUserPublic {
    fn from(user: BabamulUser) -> Self {
        Self {
            id: user.id,
            username: user.username,
            email: user.email,
            created_at: user.created_at,
        }
    }
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
    pub activation_required: bool,
}

/// Sign up for a Babamul account using an email address
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
    email_service: web::Data<EmailService>,
    body: web::Json<BabamulSignupPost>,
    config: web::Data<crate::conf::AppConfig>,
) -> HttpResponse {
    let email = body.email.trim().to_lowercase();

    // Basic email validation (single '@', non-empty local part, domain contains a dot and at least two segments)
    if !is_valid_email(&email) {
        return response::bad_request("Invalid email address");
    }

    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    // Check if email already exists
    let user = match babamul_users_collection
        .find_one(doc! { "email": &email })
        .await
    {
        Ok(Some(mut existing_user)) => {
            if !existing_user.is_activated {
                // generate a new activation code
                let new_activation_code = Some(uuid::Uuid::new_v4().to_string());
                existing_user.activation_code = new_activation_code;
                // update the user in the database
                match babamul_users_collection
                    .update_one(
                        doc! { "_id": &existing_user.id },
                        doc! {
                            "$set": {
                                "activation_code": &existing_user.activation_code
                            }
                        },
                    )
                    .await
                {
                    Ok(_) => existing_user,
                    Err(e) => {
                        eprintln!("Database error updating activation code: {}", e);
                        return response::internal_error("Database error");
                    }
                }
            } else {
                return HttpResponse::Conflict().json(serde_json::json!({
                    "message": "Email already registered (and activated)",
                    "error": "DUPLICATE_EMAIL"
                }));
            }
        }
        Ok(None) => {
            // Email doesn't exist, proceed with signup
            // Generate user ID and password
            let user_id = uuid::Uuid::new_v4().to_string();

            // Generate a long random password (32 characters for good security)
            let password = generate_random_string(32);

            // Hash the password for storage (used for both Kafka SCRAM and API auth)
            let password_hash = match bcrypt::hash(&password, bcrypt::DEFAULT_COST) {
                Ok(hash) => hash,
                Err(e) => {
                    eprintln!("Failed to hash password: {}", e);
                    return response::internal_error("Failed to generate credentials");
                }
            };

            // Generate activation code - user must activate before getting their password
            let activation_code = Some(uuid::Uuid::new_v4().to_string());
            let is_activated = false; // Require activation

            // the username is the part before the @ in the email
            // that we sanitize to only allow alphanumeric characters, dots, underscores, and hyphens
            let username = email
                .split('@')
                .next()
                .unwrap_or("")
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '_' || *c == '-')
                .collect::<String>();
            if username.is_empty() {
                return response::bad_request("Invalid email address for username extraction");
            }

            let babamul_user = BabamulUser {
                id: user_id.clone(),
                username: username.clone(),
                email: email.clone(),
                password_hash: password_hash.clone(),
                activation_code: activation_code.clone(),
                is_activated,
                created_at: flare::Time::now().to_utc().timestamp(),
            };

            // Note: Kafka user/ACLs will be created upon activation, not signup
            match babamul_users_collection
                .insert_one(babamul_user.clone())
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Database error inserting babamul user: {}", e);
                    if e.to_string().contains("E11000 duplicate key error") {
                        return HttpResponse::Conflict().json(serde_json::json!({
                            "message": "Email already registered",
                            "error": "DUPLICATE_EMAIL"
                        }));
                    } else {
                        return response::internal_error("Failed to create user");
                    }
                }
            }
            babamul_user
        }
        Err(e) => {
            eprintln!("Database error checking email existence: {}", e);
            return response::internal_error("Database error");
        }
    };

    let activation_code = user.activation_code.clone().unwrap_or_default();

    // Try to send activation email if email service is enabled
    if email_service.is_enabled() {
        if let Err(e) = email_service.send_activation_email(
            &email,
            &activation_code,
            &config.babamul.webapp_url,
        ) {
            eprintln!("Failed to send activation email to {}: {}", email, e);
            // Don't fail the signup, just log the error
            // In production, you might want to queue this for retry
        }
    } else {
        if let Some(webapp_url) = &config.babamul.webapp_url {
            println!(
                "Email service disabled - activation code for {}: {} (link: {}/signup?email={}&activation_code={})",
                email, activation_code, webapp_url, email, activation_code
            );
        } else {
            println!(
                "Email service disabled - activation code for {}: {}",
                email, activation_code
            );
        }
    }

    HttpResponse::Ok().json(BabamulSignupResponse {
        message: format!(
            "Signup successful. An activation code has been sent to {}. Use the /babamul/activate endpoint to activate your account and receive your password.",
            email
        ),
        activation_required: true,
    })
}

/// Generate a random alphanumeric string of specified length
fn generate_random_string(length: usize) -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Generate a new password for a user (called during activation)
fn generate_password() -> String {
    generate_random_string(32)
}

/// Basic email validation tailored for activation flow (not full RFC compliance)
fn is_valid_email(email: &str) -> bool {
    // Must contain exactly one '@'
    let parts: Vec<&str> = email.split('@').collect();
    if parts.len() != 2 {
        return false;
    }
    let (local, domain) = (parts[0], parts[1]);
    if local.is_empty() || domain.is_empty() {
        return false;
    }
    // Domain must contain at least one dot and two non-empty labels
    let labels: Vec<&str> = domain.split('.').collect();
    if labels.len() < 2 || labels.iter().any(|l| l.is_empty()) {
        return false;
    }
    // Basic allowed character check (alphanumeric plus common symbols)
    let is_allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '+');
    if !local.chars().all(is_allowed) {
        return false;
    }
    if !domain
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
    {
        return false;
    }
    true
}

/// Create Kafka SCRAM user and ACLs to allow babamul user to read from babamul.* topics
/// This function is idempotent - it can be called multiple times safely.
/// kafka-acls --add operations will silently succeed if the ACL already exists.
async fn create_kafka_user_and_acls(
    email: &str,
    password: &str,
    broker: &str,
) -> Result<(), String> {
    // Try to find the right command names
    // Homebrew on macOS: kafka-configs, kafka-acls (no .sh)
    // Docker container: kafka-configs.sh, kafka-acls.sh (with .sh)
    let (configs_cli, acls_cli) = match which::which("kafka-configs") {
        Ok(_) => {
            // Found kafka-configs without .sh (Homebrew)
            ("kafka-configs", "kafka-acls")
        }
        Err(_) => {
            // Fall back to .sh version (Docker container)
            ("kafka-configs.sh", "kafka-acls.sh")
        }
    };

    // Create or update SCRAM user credentials (idempotent: --alter will create or update)
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
        .map_err(|e| format!("Failed to execute {}: {}", configs_cli, e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to create SCRAM user: {}", stderr));
    }

    // Grant READ permission on babamul.* topics (idempotent: kafka-acls --add ignores duplicates)
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
        .map_err(|e| format!("Failed to execute {}: {}", acls_cli, e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to add READ ACL: {}", stderr));
    }

    // Grant DESCRIBE permission on babamul.* topics (idempotent)
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
        .map_err(|e| format!("Failed to execute {}: {}", acls_cli, e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to add DESCRIBE ACL: {}", stderr));
    }

    // Grant READ permission on consumer groups (for offset commits, idempotent)
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
        .map_err(|e| format!("Failed to execute {}: {}", acls_cli, e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to add group READ ACL: {}", stderr));
    }

    Ok(())
}

/// Create a JWT token for a Babamul user
pub async fn create_babamul_jwt(
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
    pub username: String,
    pub email: String,
    pub password: Option<String>, // Only returned on successful activation (not if already activated)
}

/// Activate a Babamul user account
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
    config: web::Data<crate::conf::AppConfig>,
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
                    message: "Account is already activated. Your password was provided during initial activation.".to_string(),
                    activated: true,
                    username: user.username.clone(),
                    email: user.email.clone(),
                    password: None, // Don't return password again for security
                });
            }

            // Verify activation code
            match &user.activation_code {
                Some(stored_code) if stored_code == activation_code => {
                    // Generate a new password for the user
                    let password = generate_password();

                    // Hash the password
                    let password_hash = match bcrypt::hash(&password, bcrypt::DEFAULT_COST) {
                        Ok(hash) => hash,
                        Err(e) => {
                            eprintln!("Failed to hash password: {}", e);
                            return response::internal_error("Failed to generate password");
                        }
                    };

                    // CRITICAL: Create Kafka SCRAM user and ACLs BEFORE marking user as activated
                    // This ensures we don't activate a user who can't access Kafka
                    // The Kafka operations are idempotent, so retries are safe
                    if let Err(e) = create_kafka_user_and_acls(
                        &user.email,
                        &password,
                        &config.kafka.producer.server,
                    )
                    .await
                    {
                        eprintln!("Failed to create Kafka user/ACLs for {}: {}", user.email, e);
                        return response::internal_error(
                            "Failed to configure Kafka access. Please try again or contact support.",
                        );
                    }

                    // Now that Kafka is configured, mark the user as activated in the database
                    match babamul_users_collection
                        .update_one(
                            doc! { "_id": &user.id },
                            doc! {
                                "$set": {
                                    "is_activated": true,
                                    "activation_code": mongodb::bson::Bson::Null,
                                    "password_hash": password_hash
                                }
                            },
                        )
                        .await
                    {
                        Ok(_) => {
                            HttpResponse::Ok().json(BabamulActivateResponse {
                                message: "Account activated successfully. Save your password - it won't be shown again!".to_string(),
                                activated: true,
                                username: user.username.clone(),
                                email: user.email.clone(),
                                password: Some(password), // Return the password ONCE
                            })
                        }
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

#[derive(Deserialize, Clone, ToSchema)]
pub struct BabamulAuthPost {
    pub email: String,
    pub password: String,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Serialize, Clone, ToSchema)]
pub struct BabamulAuthResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: Option<usize>,
}

/// Authenticate a Babamul user and get a JWT token
#[utoipa::path(
    post,
    path = "/babamul/auth",
    request_body(content = BabamulAuthPost, content_type = "application/x-www-form-urlencoded"),
    responses(
        (status = 200, description = "Successful authentication", body = BabamulAuthResponse),
        (status = 401, description = "Invalid credentials or account not activated"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[post("/babamul/auth")]
pub async fn post_babamul_auth(
    db: web::Data<Database>,
    auth: web::Data<AuthProvider>,
    body: web::Form<BabamulAuthPost>,
) -> HttpResponse {
    let email = body.email.trim().to_lowercase();
    let password = &body.password;

    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    println!(
        "Authenticating Babamul user: {} (password: {})",
        email, password
    );

    // Find user by email
    match babamul_users_collection
        .find_one(doc! { "email": &email })
        .await
    {
        Ok(Some(user)) => {
            // Check if account is activated
            if !user.is_activated {
                return HttpResponse::Unauthorized().json(serde_json::json!({
                    "error": "Account not activated. Please activate your account first."
                }));
            }

            // Verify password
            match bcrypt::verify(password, &user.password_hash) {
                Ok(true) => {
                    // Generate JWT token
                    match create_babamul_jwt(&auth, &user.id).await {
                        Ok((token, expires_in)) => HttpResponse::Ok()
                            .insert_header(("Cache-Control", "no-store"))
                            .json(BabamulAuthResponse {
                                access_token: token,
                                token_type: "Bearer".into(),
                                expires_in,
                            }),
                        Err(e) => {
                            eprintln!("Failed to create JWT token: {}", e);
                            response::internal_error("Failed to generate token")
                        }
                    }
                }
                Ok(false) => HttpResponse::Unauthorized().json(serde_json::json!({
                    "error": "Invalid credentials"
                })),
                Err(e) => {
                    eprintln!("Password verification error: {}", e);
                    response::internal_error("Authentication error")
                }
            }
        }
        Ok(None) => HttpResponse::Unauthorized().json(serde_json::json!({
            "error": "Invalid credentials"
        })),
        Err(e) => {
            eprintln!("Database error fetching user: {}", e);
            response::internal_error("Database error")
        }
    }
}

// add a /profile route that returns the current user's info
/// Get current user's profile
#[utoipa::path(
    get,
    path = "/babamul/profile",
    responses(
        (status = 200, description = "User profile retrieved successfully", body = BabamulUserPublic),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[get("/babamul/profile")]
pub async fn get_babamul_profile(current_user: Option<web::ReqData<BabamulUser>>) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };
    let user_public = BabamulUserPublic::from(current_user.into_inner().clone());
    response::ok("success", serde_json::to_value(user_public).unwrap())
}
