use crate::auth::AuthProvider;
use actix_web::{HttpResponse, post, web};
use mongodb::bson::doc;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize, Clone)]
pub struct AuthPost {
    pub username: String,
    pub password: String,
}

#[post("/auth")]
pub async fn post_auth(auth: web::Data<AuthProvider>, body: web::Json<AuthPost>) -> HttpResponse {
    // Check if the user exists and the password matches
    match auth.authenticate_user(&body.username, &body.password).await {
        Ok(token) => HttpResponse::Ok().json(json!({
            "status": "success",
            "token": token,
            "message": "authentication successful"
        })),
        Err(e) => HttpResponse::Unauthorized().body(format!("authentication failed: {}", e)),
    }
}
