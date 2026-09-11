pub mod oauth;
pub mod stats;
pub mod surveys;
pub mod tokens;

use crate::api::email::EmailService;
use crate::api::models::response;
use crate::api::{
    auth::{hash_token, AuthProvider},
    kafka::delete_kafka_credentials_and_acls,
};
use crate::conf::AppConfig;
use crate::utils::enums::Survey;
use actix_web::{delete, get, patch, post, web, HttpResponse};
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

/// Subset of [`Survey`] surfaced through the babamul API (no DECam).
#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum BabamulSurvey {
    #[serde(alias = "ZTF")]
    Ztf,
    #[serde(alias = "LSST")]
    Lsst,
}

impl From<BabamulSurvey> for Survey {
    fn from(s: BabamulSurvey) -> Self {
        match s {
            BabamulSurvey::Ztf => Survey::Ztf,
            BabamulSurvey::Lsst => Survey::Lsst,
        }
    }
}

fn validate_password_complexity(password: &str) -> Result<(), &'static str> {
    if password.len() < 12 {
        return Err("Password must be at least 12 characters long");
    }
    if !password.chars().any(|c| c.is_ascii_uppercase()) {
        return Err("Password must contain at least one uppercase letter");
    }
    if !password.chars().any(|c| c.is_ascii_lowercase()) {
        return Err("Password must contain at least one lowercase letter");
    }
    if !password.chars().any(|c| c.is_ascii_digit()) {
        return Err("Password must contain at least one digit");
    }
    if !password
        .chars()
        .any(|c| !c.is_ascii_alphanumeric() && c.is_ascii())
    {
        return Err("Password must contain at least one special character");
    }
    Ok(())
}

fn encrypt_password(
    password: &str,
    secret_key: &[u8; 32],
) -> Result<String, Box<dyn std::error::Error>> {
    let cipher = Aes256Gcm::new(secret_key.into());

    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

    let ciphertext = cipher
        .encrypt(&nonce, password.as_bytes())
        .map_err(|e| format!("Encryption failed: {}", e))?;

    // nonce || ciphertext: `decrypt_password` splits the first 12 bytes back off.
    let mut combined = nonce.to_vec();
    combined.extend_from_slice(&ciphertext);

    Ok(general_purpose::STANDARD.encode(combined))
}

fn decrypt_password(
    encrypted: &str,
    secret_key: &[u8; 32],
) -> Result<String, Box<dyn std::error::Error>> {
    let cipher = Aes256Gcm::new(secret_key.into());

    let combined = general_purpose::STANDARD.decode(encrypted)?;

    let (nonce_bytes, ciphertext) = combined.split_at(12);

    let nonce_array: &[u8; 12] = nonce_bytes.try_into()?;
    let nonce = Nonce::from(*nonce_array);

    let plaintext = cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|e| format!("Decryption failed: {}", e))?;

    Ok(String::from_utf8(plaintext)?)
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct KafkaCredentialEncrypted {
    pub id: String,
    pub name: String,
    pub kafka_username: String,
    pub kafka_password_encrypted: String,
    pub created_at: i64, // Unix timestamp
}

#[derive(Serialize, Clone, ToSchema)]
pub struct KafkaCredential {
    pub id: String,
    pub name: String,
    pub kafka_username: String,
    pub kafka_password: String, // Only returned on creation
    pub created_at: i64,
}

impl KafkaCredentialEncrypted {
    pub fn decrypt(
        &self,
        secret_key: &[u8; 32],
    ) -> Result<KafkaCredential, Box<dyn std::error::Error>> {
        let decrypted_password = decrypt_password(&self.kafka_password_encrypted, secret_key)?;
        Ok(KafkaCredential {
            id: self.id.clone(),
            name: self.name.clone(),
            kafka_username: self.kafka_username.clone(),
            kafka_password: decrypted_password,
            created_at: self.created_at,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BabamulUserToken {
    pub id: String,
    pub name: String,
    pub token_hash: String, // The token itself is never stored
    pub created_at: i64,
    pub expires_at: i64,
    pub last_used_at: Option<i64>,
}

/// An external account (Google / GitHub / ORCID) linked to a Babamul user.
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct LinkedIdentity {
    /// Provider slug: `google`, `github`, or `orcid`
    pub provider: String,
    /// Stable, provider-scoped user id — the join key for subsequent logins
    pub subject: String,
    /// Email the provider reported at link time (informational only)
    pub email: Option<String>,
    pub linked_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BabamulUser {
    #[serde(rename = "_id")]
    pub id: String,
    pub username: String,
    pub email: String,
    pub password_hash: String, // API auth only, not Kafka
    pub activation_code: Option<String>,
    pub is_activated: bool,
    pub created_at: i64, // Unix timestamp
    #[serde(default)]
    pub kafka_credentials: Vec<KafkaCredentialEncrypted>,
    pub tokens: Vec<BabamulUserToken>,
    pub password_reset_token_hash: Option<String>, // The token itself is never stored
    pub password_reset_token_expires_at: Option<i64>, // Unix timestamp
    pub password_last_changed_at: Option<i64>,     // Unix timestamp
    /// External accounts linked to this user, empty for password-only accounts
    #[serde(default)]
    pub identities: Vec<LinkedIdentity>,
    /// ORCID iD, set when the user has linked an ORCID account
    #[serde(default)]
    pub orcid_id: Option<String>,
    /// Full name for display. Seeded from the sign-in provider when there is
    /// one, editable by the user, never used to identify them — unlike
    /// `username` it is free text, optional, and not unique.
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BabamulUserPublic {
    /// The user id: Mongo storing it as `_id` is a storage detail, so the
    /// public shape sends `id`. The deserializer stays on `_id` so the type can
    /// still decode a document straight out of the collection.
    #[serde(rename(serialize = "id", deserialize = "_id"))]
    pub id: String,
    pub username: String,
    pub email: String,
    pub created_at: i64, // Unix timestamp
    /// Provider slugs the user can sign in with, e.g. `["google", "orcid"]`
    pub identity_providers: Vec<String>,
    pub orcid_id: Option<String>,
    /// Full name the user chose to display, if any
    pub name: Option<String>,
}

impl From<BabamulUser> for BabamulUserPublic {
    fn from(user: BabamulUser) -> Self {
        Self {
            id: user.id,
            username: user.username,
            email: user.email,
            created_at: user.created_at,
            identity_providers: user
                .identities
                .iter()
                .map(|identity| identity.provider.clone())
                .collect(),
            orcid_id: user.orcid_id,
            name: user.name,
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
        (status = 403, description = "This deployment is not creating new accounts"),
        (status = 409, description = "Email already exists"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[post("/signup")]
pub async fn post_babamul_signup(
    db: web::Data<Database>,
    email_service: web::Data<EmailService>,
    body: web::Json<BabamulSignupPost>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    // The UI only hides the sign-up link; this is what actually keeps the door shut.
    if !config.babamul.registration_enabled {
        return response::forbidden("New accounts aren't being created yet.");
    }

    let email = body.email.trim().to_lowercase();
    if !is_valid_email(&email) {
        return response::bad_request("Invalid email address");
    }

    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    let user = match babamul_users_collection
        .find_one(doc! { "email": &email })
        .await
    {
        Ok(Some(mut existing_user)) => {
            if existing_user.is_activated {
                return duplicate_email("Email already registered (and activated)");
            }
            existing_user.activation_code = Some(uuid::Uuid::new_v4().to_string());
            if let Err(e) = babamul_users_collection
                .update_one(
                    doc! { "_id": &existing_user.id },
                    doc! { "$set": { "activation_code": &existing_user.activation_code } },
                )
                .await
            {
                tracing::error!("Database error updating activation code: {}", e);
                return response::internal_error("Database error");
            }
            existing_user
        }
        Ok(None) => {
            let password_hash = match bcrypt::hash(generate_random_string(32), bcrypt::DEFAULT_COST)
            {
                Ok(hash) => hash,
                Err(e) => {
                    tracing::error!("Failed to hash password: {}", e);
                    return response::internal_error("Failed to generate credentials");
                }
            };

            let username: String = email
                .split('@')
                .next()
                .unwrap_or("")
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '_' || *c == '-')
                .collect();
            if username.is_empty() {
                return response::bad_request("Invalid email address for username extraction");
            }

            let babamul_user = BabamulUser {
                id: uuid::Uuid::new_v4().to_string(),
                username,
                email: email.clone(),
                password_hash,
                activation_code: Some(uuid::Uuid::new_v4().to_string()),
                is_activated: false,
                created_at: flare::Time::now().to_utc().timestamp(),
                kafka_credentials: Vec::new(), // Created on demand, not at sign-up
                tokens: Vec::new(),
                password_reset_token_hash: None,
                password_reset_token_expires_at: None,
                password_last_changed_at: None,
                identities: Vec::new(),
                orcid_id: None,
                name: None,
            };

            if let Err(e) = babamul_users_collection.insert_one(&babamul_user).await {
                tracing::error!("Database error inserting babamul user: {}", e);
                if e.to_string().contains("E11000 duplicate key error") {
                    return duplicate_email("Email already registered");
                }
                return response::internal_error("Failed to create user");
            }
            babamul_user
        }
        Err(e) => {
            tracing::error!("Database error checking email existence: {}", e);
            return response::internal_error("Database error");
        }
    };

    let activation_code = user.activation_code.unwrap_or_default();

    if email_service.is_enabled() {
        if let Err(e) = email_service.send_activation_email(
            &email,
            &activation_code,
            &config.api.domain,
            &config.babamul.webapp_url,
        ) {
            // Deliberately not fatal: the code can be resent from the sign-up form.
            tracing::error!("Failed to send activation email to {}: {}", email, e);
        }
    } else if let Some(webapp_url) = &config.babamul.webapp_url {
        tracing::info!(
            "Email service disabled - activation code for {}: {} (link: {}/signup?email={}&activation_code={})",
            email, activation_code, webapp_url, email, activation_code
        );
    } else {
        tracing::info!(
            "Email service disabled - activation code for {}: {}",
            email,
            activation_code
        );
    }

    HttpResponse::Ok().json(BabamulSignupResponse {
        message: format!(
            "Signup successful. An activation code has been sent to {}. Use the /babamul/activate endpoint to activate your account and receive your password.",
            email
        ),
        activation_required: true,
    })
}

fn unauthorized() -> HttpResponse {
    HttpResponse::Unauthorized().body("Unauthorized")
}

fn duplicate_email(message: &str) -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({
        "message": message,
        "error": "DUPLICATE_EMAIL"
    }))
}

pub fn generate_random_string(length: usize) -> String {
    use rand::RngExt;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Deliberately not full RFC compliance: enough to reject a typo before mailing it.
pub fn is_valid_email(email: &str) -> bool {
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    if local.is_empty() || domain.contains('@') {
        return false;
    }
    let mut labels = domain.split('.');
    if domain.split('.').count() < 2 || labels.any(str::is_empty) {
        return false;
    }
    local
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '+'))
        && domain
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
}

/// Run a Kafka CLI tool off the async runtime: `Command::output` blocks.
async fn run_kafka_cli(cli: &str, args: Vec<String>) -> Result<std::process::Output, String> {
    let program = cli.to_string();
    tokio::task::spawn_blocking(move || Command::new(&program).args(args).output())
        .await
        .map_err(|e| format!("Failed to join task: {}", e))?
        .map_err(|e| format!("Failed to execute {}: {}", cli, e))
}

/// `(operation, resource flag, resource name)` granted to every new credential.
const ACL_GRANTS: [(&str, &str, &str); 3] = [
    ("READ", "--topic", "babamul."),
    ("DESCRIBE", "--topic", "babamul."),
    ("READ", "--group", "babamul-"),
];

/// Idempotent: `--alter` and `--add` both succeed when the user or ACL already exists.
async fn create_kafka_user_and_acls(
    kafka_username: &str,
    kafka_password: &str,
    broker: &str,
) -> Result<(), String> {
    // Homebrew ships these without `.sh`, the Kafka Docker image with it.
    let (configs_cli, acls_cli) = match which::which("kafka-configs") {
        Ok(_) => ("kafka-configs", "kafka-acls"),
        Err(_) => ("kafka-configs.sh", "kafka-acls.sh"),
    };
    let user_entity = vec![
        "--bootstrap-server".to_string(),
        broker.to_string(),
        "--entity-type".to_string(),
        "users".to_string(),
        "--entity-name".to_string(),
        kafka_username.to_string(),
    ];

    let mut describe = vec!["--describe".to_string()];
    describe.extend(user_entity.iter().cloned());
    let output = run_kafka_cli(configs_cli, describe).await?;
    if String::from_utf8_lossy(&output.stdout).contains(&format!("User: {}", kafka_username)) {
        return Err(format!("Kafka user '{}' already exists", kafka_username));
    }

    let mut alter = vec!["--alter".to_string()];
    alter.extend(user_entity);
    alter.push("--add-config".to_string());
    alter.push(format!("SCRAM-SHA-512=[password={}]", kafka_password));
    let output = run_kafka_cli(configs_cli, alter).await?;
    if !output.status.success() {
        return Err(format!(
            "Failed to create SCRAM user: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    for (operation, resource_flag, resource_name) in ACL_GRANTS {
        let args = vec![
            "--bootstrap-server".to_string(),
            broker.to_string(),
            "--allow-principal".to_string(),
            format!("User:{}", kafka_username),
            "--add".to_string(),
            "--operation".to_string(),
            operation.to_string(),
            resource_flag.to_string(),
            resource_name.to_string(),
            "--resource-pattern-type".to_string(),
            "prefixed".to_string(),
        ];
        let output = run_kafka_cli(acls_cli, args).await?;
        if !output.status.success() {
            return Err(format!(
                "Failed to add {} ACL on {}: {}",
                operation,
                resource_flag.trim_start_matches("--"),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }

    Ok(())
}

pub async fn create_babamul_jwt(
    auth: &AuthProvider,
    user_id: &str,
) -> Result<(String, Option<usize>), String> {
    use crate::api::auth::Claims;
    use jsonwebtoken::{encode, Header};

    let iat = flare::Time::now().to_utc().timestamp() as usize;
    let exp = iat + auth.token_expiration;

    // The `babamul:` prefix is what `AuthProvider` reads the subject back as.
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
    pub password: Option<String>, // Only on successful activation, never again
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
#[post("/activate")]
pub async fn post_babamul_activate(
    db: web::Data<Database>,
    body: web::Json<BabamulActivatePost>,
) -> HttpResponse {
    let email = body.email.trim().to_lowercase();
    let activation_code = body.activation_code.trim();

    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    let user = match babamul_users_collection
        .find_one(doc! { "email": &email })
        .await
    {
        Ok(Some(user)) => user,
        Ok(None) => return response::not_found("User not found"),
        Err(e) => {
            tracing::error!("Database error fetching user: {}", e);
            return response::internal_error("Database error");
        }
    };

    if user.is_activated {
        return HttpResponse::Ok().json(BabamulActivateResponse {
            message: "Account is already activated. Your password was provided during initial activation.".to_string(),
            activated: true,
            username: user.username,
            email: user.email,
            password: None, // The password is only ever shown once
        });
    }

    if user.activation_code.as_deref() != Some(activation_code) {
        return response::bad_request("Invalid activation code");
    }

    let password = generate_random_string(32);
    let password_hash = match bcrypt::hash(&password, bcrypt::DEFAULT_COST) {
        Ok(hash) => hash,
        Err(e) => {
            tracing::error!("Failed to hash password: {}", e);
            return response::internal_error("Failed to generate password");
        }
    };

    if let Err(e) = babamul_users_collection
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
        tracing::error!("Database error activating user: {}", e);
        return response::internal_error("Failed to activate account");
    }

    HttpResponse::Ok().json(BabamulActivateResponse {
        message: "Account activated successfully. Save your password - it won't be shown again!"
            .to_string(),
        activated: true,
        username: user.username,
        email: user.email,
        password: Some(password),
    })
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct BabamulAuthPost {
    #[serde(alias = "username")]
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
#[post("/auth")]
pub async fn post_babamul_auth(
    db: web::Data<Database>,
    auth: web::Data<AuthProvider>,
    body: web::Form<BabamulAuthPost>,
) -> HttpResponse {
    let email = body.email.trim().to_lowercase();
    let password = &body.password;

    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    let invalid_credentials =
        || HttpResponse::Unauthorized().json(serde_json::json!({ "error": "Invalid credentials" }));

    let user = match babamul_users_collection
        .find_one(doc! { "email": &email })
        .await
    {
        Ok(Some(user)) => user,
        Ok(None) => return invalid_credentials(),
        Err(e) => {
            tracing::error!("Database error fetching user: {}", e);
            return response::internal_error("Database error");
        }
    };

    if !user.is_activated {
        return HttpResponse::Unauthorized().json(serde_json::json!({
            "error": "Account not activated. Please activate your account first."
        }));
    }

    match bcrypt::verify(password, &user.password_hash) {
        Ok(true) => {}
        Ok(false) => return invalid_credentials(),
        Err(e) => {
            tracing::error!("Password verification error: {}", e);
            return response::internal_error("Authentication error");
        }
    }

    match create_babamul_jwt(&auth, &user.id).await {
        Ok((token, expires_in)) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(BabamulAuthResponse {
                access_token: token,
                token_type: "Bearer".into(),
                expires_in,
            }),
        Err(e) => {
            tracing::error!("Failed to create JWT token: {}", e);
            response::internal_error("Failed to generate token")
        }
    }
}

/// `Some` while the account is inside `babamul.password_reset_cooldown_minutes`.
fn password_change_cooldown(
    user: &BabamulUser,
    config: &AppConfig,
    now: i64,
    message: &str,
) -> Option<HttpResponse> {
    let cooldown_secs = config.babamul.password_reset_cooldown_minutes as i64 * 60;
    let seconds_since = now - user.password_last_changed_at?;
    if seconds_since >= cooldown_secs {
        return None;
    }
    Some(
        HttpResponse::TooManyRequests()
            .insert_header(("Retry-After", (cooldown_secs - seconds_since).to_string()))
            .json(response::ApiResponseBody::error(message)),
    )
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct BabamulForgotPasswordPost {
    pub email: String,
}

#[derive(Serialize, Clone, ToSchema)]
pub struct BabamulForgotPasswordResponse {
    pub message: String,
}

/// Request a password reset link
///
/// Accepts an email address and sends a password-reset link to that address if
/// an activated account with that email exists. The response is always the same
/// regardless of whether the email is found – this prevents account enumeration.
#[utoipa::path(
    post,
    path = "/babamul/forgot-password",
    request_body = BabamulForgotPasswordPost,
    responses(
        (status = 200, description = "Reset email sent (or silently skipped)", body = BabamulForgotPasswordResponse),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[post("/forgot-password")]
pub async fn post_babamul_forgot_password(
    db: web::Data<Database>,
    email_service: web::Data<EmailService>,
    body: web::Json<BabamulForgotPasswordPost>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let email = body.email.trim().to_lowercase();

    // Always return the same generic message to prevent account enumeration.
    let generic_response = BabamulForgotPasswordResponse {
        message: "If an account with that email exists, a password reset link has been sent."
            .to_string(),
    };

    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    let user = match babamul_users_collection
        .find_one(doc! { "email": &email })
        .await
    {
        Ok(Some(u)) if u.is_activated => u,
        Ok(_) => return HttpResponse::Ok().json(generic_response),
        Err(e) => {
            tracing::error!("Database error during forgot-password lookup: {}", e);
            return response::internal_error("Database error");
        }
    };

    let now = flare::Time::now().to_utc().timestamp();
    if let Some(response) = password_change_cooldown(
        &user,
        &config,
        now,
        "Password was changed recently. Please wait 15 minutes before requesting another reset.",
    ) {
        return response;
    }

    let raw_token = generate_random_string(48);
    let token_hash = hash_token(&raw_token);

    if let Err(e) = babamul_users_collection
        .update_one(
            doc! { "_id": &user.id },
            doc! {
                "$set": {
                    "password_reset_token_hash": &token_hash,
                    "password_reset_token_expires_at": now + 3600
                }
            },
        )
        .await
    {
        tracing::error!("Database error storing reset token: {}", e);
        return response::internal_error("Database error");
    }

    // Fire-and-forget: the raw token must never reach the response.
    if email_service.is_enabled() {
        if let Err(e) = email_service.send_password_reset_email(
            &email,
            &raw_token,
            &config.api.domain,
            &config.babamul.webapp_url,
        ) {
            tracing::error!("Failed to send password reset email to {}: {}", email, e);
        }
    } else if let Some(webapp_url) = &config.babamul.webapp_url {
        tracing::info!(
            "Email service disabled – password reset link for {}: {}/reset-password?token={}&email={}",
            email, webapp_url, raw_token, email
        );
    } else {
        tracing::info!(
            "Email service disabled – password reset token for {}: {}",
            email,
            raw_token
        );
    }

    HttpResponse::Ok().json(generic_response)
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct BabamulResetPasswordPost {
    pub email: String,
    pub token: String,
    pub new_password: String,
}

#[derive(Serialize, Clone, ToSchema)]
pub struct BabamulResetPasswordResponse {
    pub message: String,
}

/// Reset a Babamul account password using a reset token
///
/// Validates the token, checks it has not expired, updates the password hash,
/// then invalidates the token so it cannot be reused.
#[utoipa::path(
    post,
    path = "/babamul/reset-password",
    request_body = BabamulResetPasswordPost,
    responses(
        (status = 200, description = "Password reset successfully", body = BabamulResetPasswordResponse),
        (status = 400, description = "Invalid or expired token, or password too short"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[post("/reset-password")]
pub async fn post_babamul_reset_password(
    db: web::Data<Database>,
    config: web::Data<AppConfig>,
    body: web::Json<BabamulResetPasswordPost>,
) -> HttpResponse {
    let raw_token = body.token.trim();
    let new_password = &body.new_password;

    if let Err(msg) = validate_password_complexity(new_password) {
        return response::bad_request(msg);
    }

    let token_hash = hash_token(raw_token);
    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    // One compound query: separate lookups would tell an attacker which part was wrong.
    let now = flare::Time::now().to_utc().timestamp();

    let user = match babamul_users_collection
        .find_one(doc! {
            "email": &body.email.trim().to_lowercase(),
            "password_reset_token_hash": &token_hash,
            "password_reset_token_expires_at": { "$gt": now }
        })
        .await
    {
        Ok(Some(u)) => u,
        Ok(None) => {
            return response::bad_request("Invalid or expired password reset token");
        }
        Err(e) => {
            tracing::error!("Database error during reset-password lookup: {}", e);
            return response::internal_error("Database error");
        }
    };

    if let Some(response) = password_change_cooldown(
        &user,
        &config,
        now,
        "Password was changed recently. Please wait 15 minutes before resetting again.",
    ) {
        return response;
    }

    let password_hash = match bcrypt::hash(new_password, bcrypt::DEFAULT_COST) {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("Failed to hash new password: {}", e);
            return response::internal_error("Failed to update password");
        }
    };

    // One update: a separate token wipe could fail and leave the token reusable.
    match babamul_users_collection
        .update_one(
            doc! { "_id": &user.id },
            doc! {
                "$set":   { "password_hash": &password_hash, "password_last_changed_at": now },
                "$unset": {
                    "password_reset_token_hash": "",
                    "password_reset_token_expires_at": ""
                }
            },
        )
        .await
    {
        Ok(_) => HttpResponse::Ok().json(BabamulResetPasswordResponse {
            message:
                "Password has been reset successfully. You can now log in with your new password."
                    .to_string(),
        }),
        Err(e) => {
            tracing::error!(
                "Database error resetting password for user {}: {}",
                user.id,
                e
            );
            response::internal_error("Failed to reset password")
        }
    }
}

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
#[get("/profile")]
pub async fn get_babamul_profile(current_user: Option<web::ReqData<BabamulUser>>) -> HttpResponse {
    let Some(current_user) = current_user else {
        return unauthorized();
    };
    response::ok_ser(
        "success",
        BabamulUserPublic::from(current_user.into_inner()),
    )
}

/// Long enough for any real name; short enough that the field is not free storage.
const MAX_NAME_LENGTH: usize = 100;

#[derive(Deserialize, Clone, ToSchema)]
pub struct UpdateProfilePatch {
    /// Full name to show on the profile. A blank or whitespace-only value
    /// clears it; omitting the field entirely leaves the current name alone.
    #[serde(default)]
    pub name: Option<String>,
}

/// Update the authenticated user's profile
///
/// Only the display name is editable. It is free text — not an identifier —
/// so it needs no uniqueness check, and clearing it is a normal thing to do.
#[utoipa::path(
    patch,
    path = "/babamul/profile",
    request_body = UpdateProfilePatch,
    responses(
        (status = 200, description = "Profile updated", body = BabamulUserPublic),
        (status = 400, description = "Name is too long or contains control characters"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[patch("/profile")]
pub async fn patch_babamul_profile(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<BabamulUser>>,
    body: web::Json<UpdateProfilePatch>,
) -> HttpResponse {
    let Some(current_user) = current_user else {
        return unauthorized();
    };
    let mut user = current_user.into_inner();

    let name = match &body.name {
        // Absent means "leave it as it is", so there is nothing to write.
        None => return response::ok_ser("success", BabamulUserPublic::from(user)),
        Some(name) => name.trim(),
    };

    // Characters, not bytes: `.len()` would cut a non-Latin name to a fraction.
    if name.chars().count() > MAX_NAME_LENGTH {
        return response::bad_request(&format!(
            "Name must be at most {} characters",
            MAX_NAME_LENGTH
        ));
    }
    // Rendered on one line: control characters only buy a caller newline smuggling.
    if name.chars().any(char::is_control) {
        return response::bad_request("Name cannot contain control characters");
    }

    // Unset, not `""`: a stored empty string is a name that renders blank.
    let stored = (!name.is_empty()).then(|| name.to_string());

    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");
    let update = match &stored {
        Some(name) => doc! { "$set": { "name": name } },
        None => doc! { "$unset": { "name": "" } },
    };
    if let Err(e) = babamul_users_collection
        .update_one(doc! { "_id": &user.id }, update)
        .await
    {
        tracing::error!("Failed to update profile for user {}: {}", user.id, e);
        return response::internal_error("Failed to update profile");
    }

    user.name = stored;
    response::ok_ser("success", BabamulUserPublic::from(user))
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct CreateKafkaCredentialPost {
    pub name: String,
}

#[derive(Serialize, Clone, ToSchema)]
pub struct CreateKafkaCredentialResponse {
    pub message: String,
    pub data: KafkaCredential, // Carries the decrypted password
}

/// Create a new Kafka credential for the authenticated user
#[utoipa::path(
    post,
    path = "/babamul/kafka-credentials",
    request_body = CreateKafkaCredentialPost,
    responses(
        (status = 200, description = "Kafka credential created successfully", body = CreateKafkaCredentialResponse),
        (status = 400, description = "Invalid request (e.g., empty name)"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error or Kafka configuration failed")
    ),
    tags=["Babamul"]
)]
#[post("/kafka-credentials")]
pub async fn post_kafka_credentials(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<BabamulUser>>,
    body: web::Json<CreateKafkaCredentialPost>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let Some(current_user) = current_user else {
        return unauthorized();
    };

    let name = body.name.trim();
    if name.is_empty() {
        return response::bad_request("Credential name cannot be empty");
    }

    let credential_id = uuid::Uuid::new_v4().to_string();
    let kafka_username = format!("babamul-{}", credential_id);
    let kafka_password = generate_random_string(32);

    let kafka_password_encrypted =
        match encrypt_password(&kafka_password, config.api.auth.get_hashed_secret_key()) {
            Ok(enc) => enc,
            Err(e) => {
                tracing::error!("Failed to encrypt Kafka password: {}", e);
                return response::internal_error("Failed to encrypt Kafka credential");
            }
        };

    let kafka_credential = KafkaCredentialEncrypted {
        id: credential_id,
        name: name.to_string(),
        kafka_username: kafka_username.clone(),
        kafka_password_encrypted,
        created_at: flare::Time::now().to_utc().timestamp(),
    };

    let kafka_credentials_bson = match mongodb::bson::to_bson(&kafka_credential) {
        Ok(bson) => bson,
        Err(e) => {
            tracing::error!("Failed to convert Kafka credential to BSON: {}", e);
            return response::internal_error("Failed to process Kafka credential");
        }
    };

    if let Err(e) = create_kafka_user_and_acls(
        &kafka_username,
        &kafka_password,
        &config.kafka.producer.server,
    )
    .await
    {
        tracing::error!(
            "Failed to create Kafka user/ACLs for {}: {}",
            kafka_username,
            e
        );
        return response::internal_error(
            "Failed to configure Kafka access. Please try again or contact support.",
        );
    }

    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");
    match babamul_users_collection
        .update_one(
            doc! { "_id": &current_user.id },
            doc! { "$push": { "kafka_credentials": kafka_credentials_bson } },
        )
        .await
    {
        Ok(_) => response::ok_ser(
            "Kafka credential created successfully. Save the kafka_password - it can be retrieved later but should be stored securely.",
            KafkaCredential {
                id: kafka_credential.id,
                name: kafka_credential.name,
                kafka_username: kafka_credential.kafka_username,
                kafka_password,
                created_at: kafka_credential.created_at,
            },
        ),
        Err(e) => {
            tracing::error!("Database error adding Kafka credential: {}", e);
            response::internal_error("Failed to save Kafka credential")
        }
    }
}

/// List all Kafka credentials for the authenticated user
#[utoipa::path(
    get,
    path = "/babamul/kafka-credentials",
    responses(
        (status = 200, description = "Kafka credentials retrieved successfully", body = Vec<KafkaCredential>),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[get("/kafka-credentials")]
pub async fn get_kafka_credentials(
    config: web::Data<AppConfig>,
    current_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    let Some(current_user) = current_user else {
        return unauthorized();
    };
    let mut decrypted_credentials = Vec::new();
    let secret_key = config.api.auth.get_hashed_secret_key();
    for cred in &current_user.kafka_credentials {
        match cred.decrypt(secret_key) {
            Ok(decrypted) => decrypted_credentials.push(decrypted),
            Err(e) => {
                tracing::error!("Failed to decrypt Kafka credential {}: {}", cred.id, e);
                return response::internal_error("Failed to decrypt Kafka credentials");
            }
        }
    }
    response::ok_ser("success", decrypted_credentials)
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct DeleteKafkaCredentialPath {
    pub credential_id: String,
}

#[derive(Serialize, Clone, ToSchema)]
pub struct DeleteKafkaCredentialResponse {
    pub message: String,
    pub deleted: bool,
}

/// Delete a Kafka credential for the authenticated user
/// This will disable the credential in Kafka and remove it from the user's credentials list
#[utoipa::path(
    delete,
    path = "/babamul/kafka-credentials/{credential_id}",
    responses(
        (status = 200, description = "Kafka credential deleted successfully", body = DeleteKafkaCredentialResponse),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Credential not found"),
        (status = 500, description = "Internal server error or Kafka revocation failed")
    ),
    params(
        ("credential_id" = String, Path, description = "ID of the Kafka credential to delete")
    ),
    tags=["Babamul"]
)]
#[delete("/kafka-credentials/{credential_id}")]
pub async fn delete_kafka_credential(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<BabamulUser>>,
    path: web::Path<DeleteKafkaCredentialPath>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let Some(current_user) = current_user else {
        return unauthorized();
    };

    let credential_id = &path.credential_id;
    let babamul_users_collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    let user = match babamul_users_collection
        .find_one(doc! { "_id": &current_user.id })
        .await
    {
        Ok(Some(user)) => user,
        Ok(None) => return response::not_found("User not found"),
        Err(e) => {
            tracing::error!("Database error fetching user: {}", e);
            return response::internal_error("Database error");
        }
    };

    let Some(credential) = user
        .kafka_credentials
        .iter()
        .find(|cred| cred.id == *credential_id)
    else {
        return response::not_found("Credential not found or does not belong to this user");
    };

    // Kafka first: a credential the database still lists but Kafka rejects is recoverable.
    if let Err(e) =
        delete_kafka_credentials_and_acls(&credential.kafka_username, &config.kafka.producer.server)
            .await
    {
        tracing::error!(
            "Failed to delete Kafka user/ACLs for {}: {}",
            credential.kafka_username,
            e
        );
        return response::internal_error(
            "Failed to revoke Kafka access. Please try again or contact support.",
        );
    }

    match babamul_users_collection
        .update_one(
            doc! { "_id": &current_user.id },
            doc! { "$pull": { "kafka_credentials": { "id": credential_id } } },
        )
        .await
    {
        Ok(_) => HttpResponse::Ok().json(DeleteKafkaCredentialResponse {
            message: format!(
                "Kafka credential '{}' has been deleted and revoked in Kafka.",
                credential.name
            ),
            deleted: true,
        }),
        Err(e) => {
            tracing::error!("Database error removing Kafka credential: {}", e);
            response::internal_error("Failed to remove credential from database")
        }
    }
}
