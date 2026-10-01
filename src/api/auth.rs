use crate::api::routes::babamul::BabamulUser;
use crate::api::routes::users::User;
use crate::conf::AppConfig;
use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{web, Error, HttpMessage};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use mongodb::bson::doc;
use mongodb::Database;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// sha2 0.11 moved to hybrid-array::Array which dropped the LowerHex impl that
// GenericArray had. Centralise the byte-level hex encoding here so callers
// don't need to know about the change.
pub fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iat: usize,
    pub exp: usize,
}

#[derive(Clone)]
pub struct AuthProvider {
    pub encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    validation: Validation,
    users_collection: mongodb::Collection<User>,
    pub token_expiration: usize,
}

impl AuthProvider {
    pub async fn new(config: &AppConfig, db: &Database) -> Result<Self, std::io::Error> {
        let auth_config = &config.api.auth;
        let encoding_key = EncodingKey::from_secret(auth_config.secret_key.as_bytes());
        let decoding_key = DecodingKey::from_secret(auth_config.secret_key.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = auth_config.token_expiration > 0; // Set to true if tokens should expire

        let users_collection: mongodb::Collection<User> = db.collection("users");

        Ok(AuthProvider {
            encoding_key,
            decoding_key,
            validation,
            users_collection,
            token_expiration: auth_config.token_expiration,
        })
    }

    pub async fn create_token(
        &self,
        user: &User,
    ) -> Result<(String, Option<usize>), jsonwebtoken::errors::Error> {
        let iat = flare::Time::now().to_utc().timestamp() as usize;
        let exp = iat + self.token_expiration;
        let claims = Claims {
            sub: user.id.clone(),
            iat,
            exp,
        };

        let token = encode(&Header::default(), &claims, &self.encoding_key)?;
        Ok((
            token,
            if self.token_expiration > 0 {
                Some(self.token_expiration)
            } else {
                None
            },
        ))
    }

    pub async fn decode_token(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        decode::<Claims>(token, &self.decoding_key, &self.validation).map(|data| data.claims)
    }

    pub async fn validate_token(&self, token: &str) -> Result<String, jsonwebtoken::errors::Error> {
        let claims = self.decode_token(token).await?;
        Ok(claims.sub)
    }

    pub async fn authenticate_user(&self, token: &str) -> Result<User, std::io::Error> {
        let user_id = self.validate_token(token).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("Incorrect JWT: {}", e))
        })?;

        // Check if this is a babamul user (has "babamul:" prefix)
        if user_id.starts_with("babamul:") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Babamul users cannot access main API endpoints",
            ));
        }

        // query the user
        let user = self
            .users_collection
            .find_one(doc! {"_id": &user_id})
            .await
            .map_err(|e| {
                tracing::error!(
                    "Database query failed when looking for user id {}: {}",
                    user_id,
                    e
                );
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Could not retrieve user with id {}", user_id),
                )
            })?;

        match user {
            Some(user) => return Ok(user),
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("User with id {} not found", user_id),
                ));
            }
        }
    }

    pub async fn create_token_for_user(
        &self,
        username: &str,
        password: &str,
    ) -> Result<(String, Option<usize>), std::io::Error> {
        let filter = mongodb::bson::doc! { "username": username };
        let user = self.users_collection.find_one(filter).await.map_err(|e| {
            eprint!(
                "Database query failed when looking for user {} (when creating token): {}",
                username, e
            );
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Could not retrieve user {}", username),
            )
        })?;

        // if the user exists and the password matches, create a token
        // otherwise return an error
        if let Some(user) = user {
            match bcrypt::verify(&password, &user.password) {
                Ok(true) => self.create_token(&user).await.map_err(|e| {
                    eprint!("Token creation failed: {}", e);
                    std::io::Error::new(std::io::ErrorKind::Other, format!("Token creation failed"))
                }),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid credentials",
                )),
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "User not found",
            ))
        }
    }
}

pub async fn get_auth(
    app_config: &AppConfig,
    db: &Database,
) -> Result<AuthProvider, std::io::Error> {
    AuthProvider::new(&app_config, &db).await
}

pub async fn get_test_auth(db: &Database) -> Result<AuthProvider, std::io::Error> {
    let app_config = AppConfig::from_test_config().unwrap();
    AuthProvider::new(&app_config, &db).await
}

pub const PUBLIC_ROUTES: &[&str] = &["/docs", "/auth", "/"];

/// Main-API routes a Babamul credential may authenticate on.
///
/// The admin surface is reached from the client, which holds a Babamul token,
/// and requiring a second login for it would mean two sessions in one page. It
/// is an allowlist rather than a blanket fallback because most of this API
/// authenticates by middleware alone: `GET /users` and the survey routes take
/// no user argument, so accepting a Babamul credential everywhere would hand
/// every BOOM account's email and every private cutout to anyone who signed
/// up, and Babamul signup is public. Every route here checks `is_admin` for
/// itself through `api::admin::require_admin`.
const BABAMUL_AUTHENTICATED_ROUTES: &[&str] = &[
    "/tasks",
    "/data/mutations",
    "/enrichment/status",
    "/enrichment/sets",
    "/catalogs/status",
    "/catalogs/exports",
];

// Listed path by path rather than by their parents, so that widening the admin
// page's surface is a deliberate edit here rather than a side effect of adding
// a route under one of these prefixes.

/// Whether a Babamul credential is accepted on this main-API path.
fn accepts_babamul_credentials(path: &str) -> bool {
    BABAMUL_AUTHENTICATED_ROUTES
        .iter()
        .any(|allowed| path == *allowed || path.starts_with(&format!("{allowed}/")))
}

pub async fn auth_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    // Allow public routes without authentication
    if PUBLIC_ROUTES.contains(&req.path()) {
        return next.call(req).await;
    }
    let auth_app_data: &web::Data<AuthProvider> = match req.app_data() {
        Some(data) => data,
        None => {
            return Err(actix_web::error::ErrorInternalServerError(
                "Unable to authenticate user",
            ));
        }
    };
    match req.headers().get("Authorization") {
        Some(auth_header) => {
            let token = match auth_header.to_str() {
                Ok(token) if token.starts_with("Bearer ") => token[7..].trim(),
                _ => {
                    return Err(actix_web::error::ErrorUnauthorized(
                        "Invalid Authorization header",
                    ));
                }
            };
            match auth_app_data.authenticate_user(token).await {
                Ok(user) => {
                    // inject the user in the request
                    req.extensions_mut().insert(user);
                }
                // Not a main-API credential. On the admin surface it may still
                // be a Babamul one; everywhere else the rejection stands, since
                // most routes here authenticate by middleware alone and would
                // otherwise accept anyone who signed up for Babamul.
                Err(_) if !accepts_babamul_credentials(req.path()) => {
                    return Err(actix_web::error::ErrorUnauthorized("Invalid token"));
                }
                Err(_) => {
                    let db_app_data: Option<&web::Data<mongodb::Database>> = req.app_data();
                    let Some(db) = db_app_data else {
                        return Err(actix_web::error::ErrorInternalServerError(
                            "Database connection not available",
                        ));
                    };
                    match resolve_babamul_user(db, auth_app_data, token).await? {
                        Some(user) => {
                            req.extensions_mut().insert(user);
                        }
                        None => {
                            return Err(actix_web::error::ErrorUnauthorized("Invalid token"));
                        }
                    }
                }
            }
        }
        _ => {
            return Err(actix_web::error::ErrorUnauthorized(
                "Missing or invalid Authorization header",
            ));
        }
    }
    next.call(req).await
}

/// Resolve a Babamul credential -- personal access token or JWT -- to its user.
///
/// Shared by the Babamul middleware and the main one, so the two cannot drift
/// on what counts as a valid Babamul credential or on the activation check.
/// `Ok(None)` means "not a Babamul credential"; an `Err` means it was one and
/// was rejected, which the caller must not paper over by falling through.
async fn resolve_babamul_user(
    db: &web::Data<mongodb::Database>,
    auth: &web::Data<AuthProvider>,
    token: &str,
) -> Result<Option<BabamulUser>, Error> {
    let collection: mongodb::Collection<BabamulUser> = db.collection("babamul_users");

    let user = if let Some(secret) = token.strip_prefix("bbml_") {
        // Expected format: the "bbml_" prefix plus a 36-char secret.
        if secret.len() != 36 {
            return Err(actix_web::error::ErrorUnauthorized(
                "Invalid Babamul personal access token",
            ));
        }
        let token_hash = hash_token(secret);
        let now = flare::Time::now().to_utc().timestamp();
        collection
            .find_one_and_update(
                doc! { "tokens.token_hash": &token_hash },
                doc! { "$set": { "tokens.$[token].last_used_at": now } },
            )
            .with_options(
                mongodb::options::FindOneAndUpdateOptions::builder()
                    .array_filters(vec![doc! { "token.token_hash": &token_hash }])
                    .build(),
            )
            .await
            .map_err(|e| {
                tracing::error!("Database error looking up token: {}", e);
                actix_web::error::ErrorInternalServerError("Database error")
            })?
    } else {
        let Ok(subject) = auth.validate_token(token).await else {
            return Ok(None);
        };
        let Some(user_id) = subject.strip_prefix("babamul:") else {
            return Ok(None);
        };
        collection
            .find_one(doc! { "_id": user_id })
            .await
            .map_err(|e| {
                tracing::error!("Database error fetching babamul user: {}", e);
                actix_web::error::ErrorInternalServerError("Database error")
            })?
    };

    match user {
        Some(user) if !user.is_activated => Err(actix_web::error::ErrorForbidden(
            "Account not activated. Please check your email for activation instructions.",
        )),
        other => Ok(other),
    }
}

const BABAMUL_PUBLIC_ROUTES: &[&str] = &[
    "/babamul/signup",
    "/babamul/activate",
    "/babamul/auth",
    "/babamul/forgot-password",
    "/babamul/reset-password",
    "/babamul/surveys/lsst/schemas",
    "/babamul/surveys/ztf/schemas",
    "/babamul/docs",
    "/babamul/stats/nightly",
    "/babamul/stats/collections",
    "/babamul/stats/kafka",
];

/// Middleware for authenticating Babamul users
///
/// This middleware validates JWT tokens with "babamul:" prefix in the subject claim.
/// It fetches the BabamulUser from the database and injects it into the request.
pub async fn babamul_auth_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    // Allow public routes without authentication. The social sign-in routes
    // carry a provider slug in the path, so they are matched by prefix rather
    // than listed individually — the whole point of those endpoints is to run
    // before the caller has a token.
    if BABAMUL_PUBLIC_ROUTES.contains(&req.path()) || req.path().starts_with("/babamul/oauth/") {
        return next.call(req).await;
    }

    let auth_app_data: &web::Data<AuthProvider> = match req.app_data() {
        Some(data) => data,
        None => {
            return Err(actix_web::error::ErrorInternalServerError(
                "Unable to authenticate user",
            ));
        }
    };

    let db_app_data: &web::Data<mongodb::Database> = match req.app_data() {
        Some(data) => data,
        None => {
            return Err(actix_web::error::ErrorInternalServerError(
                "Database connection not available",
            ));
        }
    };

    match req.headers().get("Authorization") {
        Some(auth_header) => {
            let token = match auth_header.to_str() {
                Ok(token) if token.starts_with("Bearer ") => token[7..].trim(),
                _ => {
                    return Err(actix_web::error::ErrorUnauthorized(
                        "Invalid Authorization header in Babamul middleware",
                    ));
                }
            };

            // Check if this is a personal access token (PAT)
            if token.starts_with("bbml_") {
                // Handle PAT authentication

                // Validate token length to prevent slicing panics.
                // Expected format: "bbml_" (5 chars) + 36-char secret = 41 chars total.
                if token.len() != 41 {
                    return Err(actix_web::error::ErrorUnauthorized(
                        "Invalid Babamul personal access token",
                    ));
                }
                // Extract the secret part after "bbml_"
                let token_secret = &token[5..];

                // Hash the token for comparison
                let token_hash = hash_token(token_secret);

                // Look up the token and update last_used_at in a single atomic operation
                // Use aggregation pipeline to update token and join with user in one operation
                let now = flare::Time::now().to_utc().timestamp();

                // instead, tokens are now an embedded array in the babamul_users collection
                let babamul_users_collection: mongodb::Collection<BabamulUser> =
                    db_app_data.collection("babamul_users");

                match babamul_users_collection
                    .find_one_and_update(
                        doc! { "tokens.token_hash": &token_hash },
                        doc! { "$set": { "tokens.$[token].last_used_at": now } },
                    )
                    .with_options(
                        mongodb::options::FindOneAndUpdateOptions::builder()
                            .array_filters(vec![doc! { "token.token_hash": &token_hash }])
                            .build(),
                    )
                    .await
                {
                    Ok(Some(user)) => {
                        // Check if user is activated
                        if !user.is_activated {
                            return Err(actix_web::error::ErrorForbidden(
                                "Account not activated. Please check your email for activation instructions.",
                            ));
                        }
                        // Inject the user in the request
                        req.extensions_mut().insert(user);
                    }
                    Ok(None) => {
                        return Err(actix_web::error::ErrorUnauthorized(
                            "Invalid personal access token",
                        ));
                    }
                    Err(e) => {
                        tracing::error!("Database error looking up token: {}", e);
                        return Err(actix_web::error::ErrorInternalServerError("Database error"));
                    }
                }
            } else {
                // Handle JWT authentication
                // Validate the token and extract user_id
                match auth_app_data.validate_token(token).await {
                    Ok(user_id) => {
                        // Check if this is a babamul user
                        if !user_id.starts_with("babamul:") {
                            return Err(actix_web::error::ErrorForbidden(
                                "Main API users cannot access Babamul endpoints",
                            ));
                        }

                        // Extract the actual user ID (remove "babamul:" prefix)
                        let actual_user_id = user_id.trim_start_matches("babamul:");

                        // Fetch the babamul user from the database
                        let babamul_users_collection: mongodb::Collection<BabamulUser> =
                            db_app_data.collection("babamul_users");

                        match babamul_users_collection
                            .find_one(doc! { "_id": actual_user_id })
                            .await
                        {
                            Ok(Some(user)) => {
                                // Check if user is activated
                                if !user.is_activated {
                                    return Err(actix_web::error::ErrorForbidden(
                                        "Account not activated. Please check your email for activation instructions.",
                                    ));
                                }
                                // Inject the user in the request
                                req.extensions_mut().insert(user);
                            }
                            Ok(None) => {
                                return Err(actix_web::error::ErrorUnauthorized(
                                    "Babamul user not found",
                                ));
                            }
                            Err(e) => {
                                tracing::error!("Database error fetching babamul user: {}", e);
                                return Err(actix_web::error::ErrorInternalServerError(
                                    "Database error",
                                ));
                            }
                        }
                    }
                    Err(_) => {
                        return Err(actix_web::error::ErrorUnauthorized("Invalid token"));
                    }
                }
            }
        }
        _ => {
            return Err(actix_web::error::ErrorUnauthorized(
                "Missing or invalid Authorization header",
            ));
        }
    }
    next.call(req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_admin_surface_accepts_a_babamul_credential() {
        // The reason the fallback exists: the client holds a Babamul token and
        // these are the routes it reaches on this scope.
        for path in [
            "/tasks",
            "/tasks/types",
            "/tasks/abc123",
            "/tasks/abc123/logs",
            "/tasks/abc123/cancel",
            "/data/mutations",
            "/enrichment/status",
            "/enrichment/sets/4/accept",
            "/catalogs/status",
            "/catalogs/exports",
            "/catalogs/exports/LSPSC/part-0000.jsonl.gz",
        ] {
            assert!(
                accepts_babamul_credentials(path),
                "{path} is part of the admin surface"
            );
        }
    }

    #[test]
    fn nothing_else_on_the_main_api_does() {
        // These authenticate by middleware alone -- they take no user argument
        // -- so accepting a Babamul credential here would hand every BOOM
        // account's email and every private cutout to anyone who signed up.
        for path in [
            // These four took no user argument, so the middleware was the only
            // thing standing in front of them.
            "/users",
            "/surveys/ztf/cutouts",
            "/surveys/ztf/tracks/BT000001",
            "/filters/schemas/ztf",
            // And these check a main-API user, but there is still no reason for
            // a Babamul credential to authenticate on them.
            "/users/someone",
            "/catalogs",
            "/filters",
            "/queries/count",
            "/",
        ] {
            assert!(
                !accepts_babamul_credentials(path),
                "{path} must not accept a Babamul credential"
            );
        }
    }

    #[test]
    fn a_prefix_is_not_a_path() {
        // `/tasksomething` starts with `/tasks` as a string but is not under
        // it, and a future `/catalogs/statuses` is not `/catalogs/status`.
        assert!(!accepts_babamul_credentials("/tasksomething"));
        assert!(!accepts_babamul_credentials("/catalogs/statuses"));
        assert!(!accepts_babamul_credentials("/catalogs"));
        assert!(!accepts_babamul_credentials("/enrichment"));
    }
}
