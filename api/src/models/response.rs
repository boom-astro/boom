use actix_web::HttpResponse;

#[derive(serde::Serialize)]
pub struct ApiResponseBody {
    pub status: String,
    pub message: String,
    pub data: serde_json::Value,
}

// ApiResponse constructors
impl ApiResponseBody {
    pub fn ok(message: &str, data: serde_json::Value) -> Self {
        Self {
            status: "success".to_string(),
            message: message.to_string(),
            data,
        }
    }
    pub fn error(message: &str) -> Self {
        Self {
            status: "error".to_string(),
            message: message.to_string(),
            data: serde_json::Value::Null,
        }
    }
}

// builds an HttpResponse with an ApiResponseBody
pub fn ok(message: &str, data: serde_json::Value) -> HttpResponse {
    HttpResponse::Ok().json(ApiResponseBody::ok(message, data))
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
