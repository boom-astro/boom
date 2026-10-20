use crate::api::models::response;
use actix_web::HttpResponse;
use mongodb::Cursor;
use serde::de::DeserializeOwned;

pub const MAX_RESULT_BYTES: usize = 256 * 1024 * 1024;

pub enum ResultError {
    TooLarge,
    Database(mongodb::error::Error),
}

pub async fn next_result<T: DeserializeOwned>(
    cursor: &mut Cursor<T>,
    bytes_read: &mut usize,
) -> Result<Option<T>, ResultError> {
    if !cursor.advance().await.map_err(ResultError::Database)? {
        return Ok(None);
    }
    *bytes_read += cursor.current().as_bytes().len();
    if *bytes_read > MAX_RESULT_BYTES {
        return Err(ResultError::TooLarge);
    }
    cursor
        .deserialize_current()
        .map(Some)
        .map_err(ResultError::Database)
}

pub fn too_large() -> HttpResponse {
    response::bad_request(&format!(
        "Results exceed {} MB; add a limit and paginate with skip, narrow the query or project fewer fields",
        MAX_RESULT_BYTES / (1024 * 1024)
    ))
}

impl ResultError {
    pub fn into_response(self, context: &str) -> HttpResponse {
        match self {
            ResultError::TooLarge => too_large(),
            ResultError::Database(e) => response::internal_error(&format!("{}: {}", context, e)),
        }
    }
}
