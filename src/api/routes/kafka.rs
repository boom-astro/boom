//! Routes for managing BOOM's Kafka cluster.

use crate::api::routes::users::User;
use crate::api::{kafka::delete_kafka_credentials_and_acls, models::response};
use actix_web::{delete, get, web, HttpResponse};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize)]
pub struct DeleteKafkaCredentialsPath {
    pub kafka_username: String,
}

#[derive(Serialize, Clone, ToSchema)]
pub struct DeleteKafkaCredentialsResponse {
    pub message: String,
    pub deleted: bool,
}

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
pub async fn get_kafka_acls(
    current_user: web::ReqData<User>,
    config: web::Data<crate::conf::AppConfig>,
) -> HttpResponse {
    // Only admins can access this endpoint
    if !current_user.is_admin {
        return response::forbidden("Access denied: Admins only");
    }
    match crate::api::kafka::get_acls(&config.kafka.producer.server) {
        Ok(entries) => response::ok(
            "Kafka ACLs retrieved successfully",
            serde_json::to_value(entries).unwrap(),
        ),
        Err(e) => response::internal_error(&format!("Error retrieving Kafka ACLs: {}", e)),
    }
}

/// Delete Kafka credentials for a given client ID
/// This will delete the SCRAM user credentials and remove all associated ACLs
#[utoipa::path(
    delete,
    path = "/kafka/credentials/{kafka_username}",
    responses(
        (status = 200, description = "Kafka credentials deleted successfully", body = DeleteKafkaCredentialsResponse),
        (status = 403, description = "Access denied: Admins only"),
        (status = 500, description = "Internal server error"),
    ),
    params(
        ("kafka_username" = String, Path, description = "Kafka username to delete credentials for")
    ),
    tags=["Kafka"]
)]
#[delete("/kafka/credentials/{kafka_username}")]
pub async fn delete_kafka_credentials(
    path: web::Path<DeleteKafkaCredentialsPath>,
    current_user: web::ReqData<User>,
    config: web::Data<crate::conf::AppConfig>,
) -> HttpResponse {
    // Only admins can access this endpoint
    if !current_user.is_admin {
        return response::forbidden("Access denied: Admins only");
    }

    let kafka_username = &path.kafka_username;

    // Delete Kafka credentials and ACLs
    if let Err(e) = delete_kafka_credentials_and_acls(kafka_username, &config.kafka.producer.server)
    {
        return response::internal_error(&format!("Error deleting Kafka credentials: {}", e));
    }

    response::ok_ser(
        "Kafka credentials deleted successfully",
        DeleteKafkaCredentialsResponse {
            message: format!(
                "Kafka credentials for user '{}' have been deleted and revoked.",
                kafka_username
            ),
            deleted: true,
        },
    )
}
