/// Endpoints for executing analytical queries.
use crate::api::catalogs::catalog_exists;
use crate::api::filters::parse_filter;
use crate::api::models::response;

use actix_web::{post, web, HttpResponse};
use futures::StreamExt;
use mongodb::{bson::doc, Database};
use utoipa::ToSchema;

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
struct FindQuery {
    catalog_name: String,
    filter: serde_json::Value,
    projection: Option<serde_json::Value>,
    limit: u32,
    skip: Option<u64>,
    sort: Option<serde_json::Value>,
    max_time_ms: Option<u64>,
}
impl FindQuery {
    /// Convert to MongoDB Find options
    fn to_find_options(&self) -> Result<mongodb::options::FindOptions, String> {
        let mut options = mongodb::options::FindOptions::default();
        if let Some(projection) = &self.projection {
            options.projection = match mongodb::bson::to_document(projection) {
                Ok(doc) => Some(doc),
                Err(e) => {
                    return Err(format!(
                        "Error converting projection to BSON document: {:?}",
                        e
                    ));
                }
            }
        }
        // assert that limit is a positive integer < 100_000
        if self.limit == 0 || self.limit > 100_000 {
            return Err(
                "Limit must be a positive integer less than or equal to 100,000".to_string(),
            );
        }
        options.limit = Some(self.limit as i64);
        if let Some(skip) = self.skip {
            options.skip = Some(skip);
        }
        if let Some(sort) = &self.sort {
            options.sort = match mongodb::bson::to_document(sort) {
                Ok(doc) => Some(doc),
                Err(e) => {
                    return Err(format!("Error converting sort to BSON document: {:?}", e));
                }
            }
        }
        if let Some(max_time_ms) = self.max_time_ms {
            options.max_time = Some(std::time::Duration::from_millis(max_time_ms));
        } else {
            options.max_time = Some(std::time::Duration::from_secs(30)); // Default max time
        }
        Ok(options)
    }
}

/// Run a find query on a catalog
#[utoipa::path(
    post,
    path = "/queries/find",
    request_body = FindQuery,
    responses(
        (status = 200, description = "Documents found in the catalog", body = serde_json::Value),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Queries"]
)]
#[post("/queries/find")]
pub async fn post_find_query(db: web::Data<Database>, body: web::Json<FindQuery>) -> HttpResponse {
    let catalog_name = body.catalog_name.trim();
    if !catalog_exists(&db, &catalog_name).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Find documents with the provided filter
    let filter = match parse_filter(&body.filter) {
        Ok(filter) => filter,
        Err(e) => return response::bad_request(&format!("Invalid filter: {}", e)),
    };
    let find_options = match body.to_find_options() {
        Ok(options) => options,
        Err(e) => return response::bad_request(&format!("Invalid find options: {}", e)),
    };
    let mut cursor = match collection.find(filter).with_options(find_options).await {
        Ok(cursor) => cursor,
        Err(e) => return response::internal_error(&format!("Error finding documents: {}", e)),
    };
    let mut docs = Vec::new();
    while let Some(result) = cursor.next().await {
        match result {
            Ok(doc) => docs.push(doc),
            Err(e) => {
                tracing::error!("Error retrieving document from the database: {}", e);
                return response::internal_error("Error retrieving document from the database");
            }
        }
    }
    response::ok_ser("success", &docs)
}
