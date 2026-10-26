use actix_web::{http::header::ContentType, HttpResponse};
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

pub fn request_timeout(message: &str) -> HttpResponse {
    HttpResponse::RequestTimeout().json(ApiResponseBody::error(message))
}

pub fn service_unavailable(message: &str) -> HttpResponse {
    HttpResponse::ServiceUnavailable().json(ApiResponseBody::error(message))
}

/// A request that is well-formed and permitted but conflicts with the state it
/// found, so retrying it against fresh state is the right next move.
pub fn conflict(message: &str) -> HttpResponse {
    HttpResponse::Conflict().json(ApiResponseBody::error(message))
}

pub fn forbidden(message: &str) -> HttpResponse {
    HttpResponse::Forbidden().json(ApiResponseBody::error(message))
}

#[derive(serde::Serialize)]
struct SerializedResponseBody<'a, T> {
    message: &'a str,
    data: T,
}

/// Same body as `ok`, written straight from `data` without building a `serde_json::Value` first.
pub fn ok_ser<T: serde::Serialize>(message: &str, data: T) -> HttpResponse {
    match serde_json::to_vec(&SerializedResponseBody { message, data }) {
        Ok(body) => HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(body),
        Err(e) => {
            tracing::error!("Serialization error: {}", e);
            HttpResponse::InternalServerError()
                .json(ApiResponseBody::error("Failed to serialize response data"))
        }
    }
}
