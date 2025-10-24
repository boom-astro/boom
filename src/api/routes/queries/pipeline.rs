/// Endpoints for executing analytical queries.
use crate::api::catalogs::catalog_exists;
use crate::api::filters::parse_pipeline;
use crate::api::models::response;

use actix_web::{post, web, HttpResponse};
use futures::StreamExt;
use mongodb::{bson::doc, Database};
use utoipa::ToSchema;

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
struct PipelineQuery {
    catalog_name: String,
    pipeline: serde_json::Value,
    max_time_ms: Option<u64>,
}
impl PipelineQuery {
    /// Convert to MongoDB Find options
    fn to_pipeline_options(&self) -> mongodb::options::AggregateOptions {
        let mut options = mongodb::options::AggregateOptions::default();
        if let Some(max_time_ms) = self.max_time_ms {
            options.max_time = Some(std::time::Duration::from_millis(max_time_ms));
        }
        options
    }
}

/// Run a pipeline query on a catalog
#[utoipa::path(
    post,
    path = "/queries/pipeline",
    request_body = PipelineQuery,
    responses(
        (status = 200, description = "Documents retrieved by the pipeline", body = serde_json::Value),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Queries"]
)]
#[post("/queries/pipeline")]
pub async fn post_pipeline_query(
    db: web::Data<Database>,
    body: web::Json<PipelineQuery>,
) -> HttpResponse {
    let catalog_name = body.catalog_name.trim();
    if !catalog_exists(&db, &catalog_name).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Find documents with the provided filter
    let pipeline = match parse_pipeline(&body.pipeline) {
        Ok(pipeline) => pipeline,
        Err(e) => return response::bad_request(&format!("Invalid filter: {:?}", e)),
    };
    let pipeline_options = body.to_pipeline_options();
    match collection
        .aggregate(pipeline)
        .with_options(pipeline_options)
        .await
    {
        Ok(cursor) => {
            let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect::<Vec<_>>().await;
            response::ok("success", serde_json::to_value(docs).unwrap())
        }
        Err(e) => response::internal_error(&format!("Error finding documents: {:?}", e)),
    }
}
