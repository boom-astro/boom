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
    sub: String,
    iat: usize,
    exp: usize,
}

#[derive(Clone)]
pub struct AuthProvider {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    validation: Validation,
    users_collection: mongodb::Collection<User>,
    token_expiration: usize,
    admin_username: String,
    admin_password: String,
}

impl AuthProvider {
    pub async fn new(config: AuthConfig, db: &mongodb::Database) -> Result<Self, std::io::Error> {
        let encoding_key = EncodingKey::from_secret(config.secret_key.as_bytes());
        let decoding_key = DecodingKey::from_secret(config.secret_key.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = config.token_expiration > 0; // Set to true if tokens should expire

        let users_collection: mongodb::Collection<User> = db.collection("users");

        // always create the admin user if it doesn't exist,
        // if it does exist, check that the password matches
        let admin_username = config.admin_username.clone();
        let admin_password = config.admin_password.clone();

        match users_collection
            .find_one(doc! { "username": &admin_username })
            .await
        {
            Ok(Some(admin_user)) => {
                // Admin user already exists, check that the password matches
                if !bcrypt::verify(&config.admin_password, &admin_user.password).unwrap_or(false) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        "Admin user already exists, but password does not match with the one in the config",
                    ));
                }
            }
            Ok(None) => {
                // Admin user does not exist, create it
                let user_id = uuid::Uuid::new_v4().to_string();
                let hashed_admin_password = bcrypt::hash(&admin_password, bcrypt::DEFAULT_COST)
                    .expect("failed to hash admin password");
                let admin_user = User {
                    id: user_id,
                    username: admin_username.clone(),
                    email: "<admin_email@example.com>".to_string(),
                    password: hashed_admin_password,
                };
                match users_collection.insert_one(admin_user).await {
                    Ok(_) => {
                        println!("Admin user created successfully.");
                    }
                    Err(e) => {
                        // eprintln!("Failed to create admin user: {}", e);
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to create admin user: {}", e),
                        ));
                    }
                }
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to check for admin user: {}", e),
                ));
            }
        }

        Ok(AuthProvider {
            encoding_key,
            decoding_key,
            validation,
            users_collection,
            token_expiration: config.token_expiration,
            admin_username,
            admin_password,
        })
    }

    pub async fn create_token(&self, user: &User) -> Result<String, jsonwebtoken::errors::Error> {
        let iat = chrono::Utc::now().timestamp() as usize;
        let exp = iat + self.token_expiration;
        let claims = Claims {
            sub: user.id.clone(),
            iat,
            exp,
        };

        encode(&Header::default(), &claims, &self.encoding_key)
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
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Database query failed: {}", e),
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
    ) -> Result<String, std::io::Error> {
        let filter = mongodb::bson::doc! { "username": username };
        let user = self.users_collection.find_one(filter).await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Database query failed: {}", e),
            )
        })?;

        // if the user exists and the password matches, create a token
        // otherwise return an error
        if let Some(user) = user {
            match bcrypt::verify(&password, &user.password) {
                Ok(true) => self.create_token(&user).await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Token creation failed: {}", e),
                    )
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

    pub fn get_admin_credentials(&self) -> (String, String) {
        (self.admin_username.clone(), self.admin_password.clone())
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
