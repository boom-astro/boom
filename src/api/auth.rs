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

pub async fn auth_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
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
                Err(_) => {
                    return Err(actix_web::error::ErrorUnauthorized("Invalid token"));
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

/// Middleware for authenticating Babamul users
///
/// This middleware validates JWT tokens with "babamul:" prefix in the subject claim.
/// It fetches the BabamulUser from the database and injects it into the request.
pub async fn babamul_auth_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    println!("Babamul auth middleware invoked");
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
                        "Invalid Authorization header",
                    ));
                }
            };

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
        _ => {
            return Err(actix_web::error::ErrorUnauthorized(
                "Missing or invalid Authorization header",
            ));
        }
    }
    next.call(req).await
}
