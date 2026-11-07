pub mod cone_search;
pub mod count;
pub mod find;
pub mod pipeline;

pub use cone_search::post_cone_search_query;
pub use count::{post_count_query, post_estimated_count_query};
pub use find::post_find_query;
pub use pipeline::post_pipeline_query;

use crate::api::models::response;
use actix_web::HttpResponse;
use mongodb::error::{Error, ErrorKind};

/// MongoDB's code for an operation that ran past `maxTimeMS`.
const MAX_TIME_MS_EXPIRED: i32 = 50;

/// Turn a MongoDB failure on a user-supplied query into a response.
///
/// A `Command` error means the server understood the query and rejected it, so
/// the caller sent something wrong: an unknown operator, a malformed pipeline
/// stage, an argument of the wrong type. Reporting that as a 500 tells clients
/// the failure is transient, and a client that retries then runs the same bad
/// query several more times before giving up.
///
/// Anything else is ours: I/O, server selection, deserialization.
pub fn query_error(error: Error, context: &str) -> HttpResponse {
    match error.kind.as_ref() {
        ErrorKind::Command(command_error) => {
            if command_error.code == MAX_TIME_MS_EXPIRED {
                return response::request_timeout(
                    "query exceeded max_time_ms: narrow the filter, add a limit, or raise the budget",
                );
            }
            response::bad_request(&format!("{}: {}", context, command_error.message))
        }
        _ => response::internal_error(&format!("{}: {}", context, error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::StatusCode;
    use mongodb::bson::doc;
    use mongodb::error::CommandError;

    /// `CommandError` is non-exhaustive and holds a private field, so a test
    /// builds one the way the driver does, by deserializing a server reply.
    fn command_failure(code: i32, message: &str) -> Error {
        let command_error: CommandError = mongodb::bson::from_document(doc! {
            "code": code,
            "codeName": "",
            "errmsg": message,
            "topologyVersion": mongodb::bson::Bson::Null,
        })
        .expect("failed to build a CommandError");
        Error::from(ErrorKind::Command(command_error))
    }

    #[test]
    fn test_rejected_query_is_a_bad_request() {
        let response = query_error(
            command_failure(2, "unknown operator: $bogus"),
            "Error finding documents",
        );
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_exhausted_time_budget_is_a_timeout() {
        let response = query_error(
            command_failure(MAX_TIME_MS_EXPIRED, "operation exceeded time limit"),
            "Error finding documents",
        );
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    }

    #[test]
    fn test_other_failures_stay_internal() {
        let error = Error::from(std::io::Error::other("connection reset"));
        let response = query_error(error, "Error finding documents");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
