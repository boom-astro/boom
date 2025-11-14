use crate::api::auth::AuthProvider;
use crate::api::routes::babamul::BabamulUser;
use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{web, Error, HttpMessage};
use mongodb::bson::doc;

/// Middleware for authenticating Babamul users
///
/// This middleware validates JWT tokens with "babamul:" prefix in the subject claim.
/// It fetches the BabamulUser from the database and injects it into the request.
pub async fn babamul_auth_middleware(
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
                            eprintln!("Database error fetching babamul user: {}", e);
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
