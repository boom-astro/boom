use crate::{conf::AppConfig, conf::AuthConfig, routes::users::User};
use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{Error, HttpMessage, web};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    iat: usize,
    exp: usize,
}

#[derive(Clone)]
pub struct AuthProvider {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    validation: Validation,
    users_collection: mongodb::Collection<User>,
    pub token_expiration: usize,
}

impl AuthProvider {
    pub async fn new(config: AuthConfig, db: &mongodb::Database) -> Result<Self, std::io::Error> {
        let encoding_key = EncodingKey::from_secret(config.secret_key.as_bytes());
        let decoding_key = DecodingKey::from_secret(config.secret_key.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = config.token_expiration > 0; // Set to true if tokens should expire

        let users_collection: mongodb::Collection<User> = db.collection("users");

        Ok(AuthProvider {
            encoding_key,
            decoding_key,
            validation,
            users_collection,
            token_expiration: config.token_expiration,
        })
    }

    pub async fn create_token(
        &self,
        user: &User,
    ) -> Result<(String, Option<usize>), jsonwebtoken::errors::Error> {
        let iat = chrono::Utc::now().timestamp() as usize;
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

        // query the user
        let user = self
            .users_collection
            .find_one(doc! {"id": &user_id})
            .await
            .map_err(|e| {
                eprintln!(
                    "Database query failed when looking for user id {}: {}",
                    user_id, e
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

pub async fn get_auth(db: &mongodb::Database) -> Result<AuthProvider, std::io::Error> {
    let config = AppConfig::from_default_path().auth;
    AuthProvider::new(config, db).await
}

pub async fn get_default_auth(db: &mongodb::Database) -> Result<AuthProvider, std::io::Error> {
    let config = AppConfig::default().auth;
    AuthProvider::new(config, db).await
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
