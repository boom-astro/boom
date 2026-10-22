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

pub fn unauthorized(message: &str) -> HttpResponse {
    HttpResponse::Unauthorized().json(ApiResponseBody::error(message))
}

/// A refusal in the API's own JSON envelope, for the places that have to hand
/// back an `actix_web::Error` rather than an `HttpResponse` -- a `FromRequest`
/// extractor or a middleware.
///
/// `actix_web::error::ErrorForbidden` and its siblings send the message as
/// `text/plain`. A client that reads `message` out of the body finds nothing
/// there and falls back to the bare status code, so the same UI showed a
/// sentence for a refusal from one route and "failed: 403" for the next one --
/// and messages written to be read, like the one telling somebody their account
/// is not activated yet, never reached anybody.
#[derive(Debug)]
pub struct ApiError {
    status: actix_web::http::StatusCode,
    message: String,
}

impl ApiError {
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: actix_web::http::StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: actix_web::http::StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl actix_web::ResponseError for ApiError {
    fn status_code(&self) -> actix_web::http::StatusCode {
        self.status
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status).json(ApiResponseBody::error(&self.message))
    }
}

// have an ok_ser() that takes a serializable object and converts it to serde_json::Value
pub fn ok_ser<T: serde::Serialize>(message: &str, data: T) -> HttpResponse {
    match serde_json::to_value(data) {
        Ok(data_value) => HttpResponse::Ok().json(ApiResponseBody::ok(message, data_value)),
        Err(e) => {
            tracing::info!("Serialization error: {}", e); // TODO: replace with tracing once integrated
            HttpResponse::InternalServerError()
                .json(ApiResponseBody::error("Failed to serialize response data"))
        }
    }
}
