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
    pub fn internal_error(error_message: &str) -> Self {
        Self {
            status: "error".to_string(),
            message: error_message.to_string(),
            data: serde_json::Value::Null,
        }
    }
    pub fn bad_request(message: &str) -> Self {
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

pub fn internal_error(message: &str) -> HttpResponse {
    HttpResponse::InternalServerError().json(ApiResponseBody::internal_error(message))
}

pub fn bad_request(message: &str) -> HttpResponse {
    HttpResponse::BadRequest().json(ApiResponseBody::bad_request(message))
}
