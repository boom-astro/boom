use crate::auth::AuthProvider;
use actix_web::{HttpResponse, post, web};
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use utoipa::ToSchema;

#[derive(Deserialize, Clone, ToSchema)]
pub struct AuthPost {
    pub username: String,
    pub password: String,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Deserialize, Serialize, Clone, ToSchema)]
pub struct AuthResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: Option<usize>,
}

#[derive(Deserialize, Serialize, Clone, ToSchema)]
pub struct FailedAuthResponse {
    pub error: String,
    pub error_description: String,
}

/// Authenticate a user
#[utoipa::path(
    post,
    path = "/auth",
    request_body = AuthPost,
    responses(
        (status = 200, description = "Successful authentication", body = AuthResponse),
        (status = 401, description = "Invalid Client", body = FailedAuthResponse),
        (status = 400, description = "Invalid Request", body = FailedAuthResponse),
    ),
    tags=["Auth"]
)]
#[post("/auth")]
pub async fn post_auth(auth: web::Data<AuthProvider>, body: web::Json<AuthPost>) -> HttpResponse {
    // Check if the user exists and the password matches
    match auth
        .create_token_for_user(&body.username, &body.password)
        .await
    {
        Ok((token, expires_in)) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(AuthResponse {
                access_token: token,
                token_type: "Bearer".into(),
                expires_in,
            }),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound
                || e.kind() == std::io::ErrorKind::InvalidInput
            {
                // if the error is NotFound (username or password is incorrect), return a 401
                HttpResponse::Unauthorized().json(FailedAuthResponse {
                    error: "invalid_client".into(),
                    error_description: "Invalid username or password".into(),
                })
            } else {
                // for other errors, return a 400
                HttpResponse::BadRequest().json(FailedAuthResponse {
                    error: "invalid_request".into(),
                    error_description: format!("{}", e),
                })
            }
        }
    }
}
