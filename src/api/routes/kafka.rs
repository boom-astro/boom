//! Routes for managing BOOM's Kafka cluster.

use crate::api::kafka::delete_acls_for_user;
use crate::api::models::response;
use crate::api::routes::users::User;
use actix_web::{delete, get, web, HttpResponse};
use std::str;

/// Get Kafka ACLs
#[utoipa::path(
    get,
    path = "/kafka/acls",
    responses(
        (status = 200, description = "Kafka ACLs retrieved successfully"),
    ),
    tags=["Kafka"]
)]
#[get("/kafka/acls")]
pub async fn get_kafka_acls(current_user: web::ReqData<User>) -> HttpResponse {
    // Only admins can access this endpoint
    if !current_user.is_admin {
        return response::forbidden("Access denied: Admins only");
    }
    match crate::api::kafka::get_acls() {
        Ok(entries) => response::ok(
            "Kafka ACLs retrieved successfully",
            serde_json::to_value(entries).unwrap(),
        ),
        Err(e) => response::internal_error(&format!("Error retrieving Kafka ACLs: {}", e)),
    }
}

/// Delete Kafka ACLs for a given user by email
#[utoipa::path(
    delete,
    path = "/kafka/acls/{user_email}",
    responses(
        (status = 200, description = "Kafka ACLs deleted successfully"),
    ),
    tags=["Kafka"]
)]
#[delete("/kafka/acls/{user_email}")]
pub async fn delete_kafka_acls_for_user(
    user_email: web::Path<String>,
    current_user: web::ReqData<User>,
) -> HttpResponse {
    // Only admins can access this endpoint
    if !current_user.is_admin {
        return response::forbidden("Access denied: Admins only");
    }

    let user_email = user_email.into_inner();

    // Call the function to delete ACLs for the user
    match delete_acls_for_user(&user_email) {
        Ok(_) => response::ok(
            "Kafka ACLs deleted successfully",
            serde_json::json!({ "user": user_email }),
        ),
        Err(e) => response::internal_error(&format!("Error deleting Kafka ACLs: {}", e)),
    }
}
