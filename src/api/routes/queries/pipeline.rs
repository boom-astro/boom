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
    limit: u32,
    skip: Option<u64>,
    max_time_ms: Option<u64>,
}
impl PipelineQuery {
    /// Convert to MongoDB Aggregation options
    fn to_pipeline_options(&self) -> mongodb::options::AggregateOptions {
        let mut options = mongodb::options::AggregateOptions::default();
        if let Some(max_time_ms) = self.max_time_ms {
            options.max_time = Some(std::time::Duration::from_millis(max_time_ms));
        } else {
            options.max_time = Some(std::time::Duration::from_secs(30)); // Default max time
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
        Err(e) => return response::bad_request(&format!("Invalid pipeline: {}", e)),
    };
    let mut pipeline = pipeline;

    // add a skip stage to the pipeline if skip is set (must come before $limit)
    if let Some(skip) = body.skip {
        let skip_stage = doc! { "$skip": skip as i64 };
        pipeline.push(skip_stage);
    }
    // add a limit stage to the pipeline after validating that the limit is a positive integer < 100_000
    if body.limit == 0 || body.limit > 100_000 {
        return response::bad_request(
            "Limit must be a positive integer less than or equal to 100,000",
        );
    }

    let limit_stage = doc! { "$limit": body.limit };
    pipeline.push(limit_stage);

    let pipeline_options = body.to_pipeline_options();
    let mut cursor = match collection
        .aggregate(pipeline)
        .with_options(pipeline_options)
        .await
    {
        Ok(cursor) => cursor,
        Err(e) => return response::internal_error(&format!("Error executing pipeline: {}", e)),
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
