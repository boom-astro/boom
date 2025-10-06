/// Endpoints for executing analytical queries.
use crate::catalogs::catalog_exists;
use crate::filters::parse_filter;
use crate::models::response;

use actix_web::{HttpResponse, post, web};
use mongodb::{Database, bson::doc};
use utoipa::ToSchema;

#[derive(serde::Deserialize, Clone, ToSchema)]
struct CountQuery {
    catalog_name: String,
    filter: serde_json::Value,
}

/// Run a count query
#[utoipa::path(
    post,
    path = "/queries/count",
    request_body = CountQuery,
    responses(
        (status = 200, description = "Count of documents in the catalog", body = serde_json::Value),
        (status = 404, description = "Catalog does not exist"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Queries"]
)]
#[post("/queries/count")]
pub async fn post_count_query(
    db: web::Data<Database>,
    web::Json(query): web::Json<CountQuery>,
) -> HttpResponse {
    let catalog_name = query.catalog_name.trim();
    if !catalog_exists(&db, &catalog_name).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Count documents with optional filter
    let filter = match parse_filter(&query.filter) {
        Ok(f) => f,
        Err(e) => return response::bad_request(&format!("Invalid filter: {:?}", e)),
    };
    let count = match collection.count_documents(filter).await {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error counting documents: {:?}", e));
        }
    };
    // Return the count
    response::ok("success", serde_json::to_value(count).unwrap())
}

#[derive(serde::Deserialize, Clone, ToSchema)]
struct EstimatedCountQuery {
    catalog_name: String,
}

/// Run an estimated count query
#[utoipa::path(
    post,
    path = "/queries/estimated_count",
    request_body = EstimatedCountQuery,
    responses(
        (status = 200, description = "Approximately count documents in the catalog", body = serde_json::Value),
        (status = 404, description = "Catalog does not exist"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Queries"]
)]
#[post("/queries/estimated_count")]
pub async fn post_estimated_count_query(
    db: web::Data<Database>,
    web::Json(query): web::Json<EstimatedCountQuery>,
) -> HttpResponse {
    let catalog_name = query.catalog_name.trim();
    if !catalog_exists(&db, &catalog_name).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    let count = match collection.estimated_document_count().await {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error counting documents: {:?}", e));
        }
    };
    // Return the count
    response::ok("success", serde_json::to_value(count).unwrap())
}
