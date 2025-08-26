use actix_web::HttpResponse;
use serde_with::{serde_as, skip_serializing_none};

#[serde_as]
#[skip_serializing_none]
#[derive(serde::Serialize)]
pub struct ApiResponseBody {
    pub message: String,
    pub data: Option<serde_json::Value>,
}

// ApiResponse constructors
impl ApiResponseBody {
    pub fn ok(message: &str, data: serde_json::Value) -> Self {
        Self {
            message: message.to_string(),
            data: Some(data),
        }
    }
    pub fn error(message: &str) -> Self {
        Self {
            message: message.to_string(),
            data: None,
        }
    }
}

// builds an HttpResponse with an ApiResponseBody
pub fn ok(message: &str, data: serde_json::Value) -> HttpResponse {
    HttpResponse::Ok().json(ApiResponseBody::ok(message, data))
}

pub fn ok_no_data(message: &str) -> HttpResponse {
    HttpResponse::Ok().json(ApiResponseBody::ok(message, serde_json::Value::Null))
}

pub fn not_found(message: &str) -> HttpResponse {
    HttpResponse::NotFound().json(ApiResponseBody::error(message))
}

pub fn internal_error(message: &str) -> HttpResponse {
    HttpResponse::InternalServerError().json(ApiResponseBody::error(message))
}

pub fn bad_request(message: &str) -> HttpResponse {
    HttpResponse::BadRequest().json(ApiResponseBody::error(message))
}

pub fn forbidden(message: &str) -> HttpResponse {
    HttpResponse::Forbidden().json(ApiResponseBody::error(message))
}
