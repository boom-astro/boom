/// Endpoints for executing analytical queries.
use crate::{
    api::{
        catalogs::{catalog_accessible, is_catalog_queryable},
        filters::{joined_collections, parse_pipeline},
        models::response,
        routes::users::User,
    },
    conf::AppConfig,
};

use crate::api::limits::{
    large_query_slot, next_result, too_large, ResultError, DEFAULT_QUERY_MAX_TIME,
};
use actix_web::{post, web, HttpResponse};
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
        options.max_time = Some(
            self.max_time_ms
                .map(std::time::Duration::from_millis)
                .unwrap_or(DEFAULT_QUERY_MAX_TIME),
        );
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
    current_user: Option<web::ReqData<User>>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };
    let _slot = match large_query_slot().await {
        Ok(slot) => slot,
        Err(response) => return response,
    };
    let catalog_name = body.catalog_name.trim();
    if !catalog_accessible(&db, catalog_name, &current_user, &config).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Find documents with the provided filter
    let pipeline = match parse_pipeline(&body.pipeline) {
        Ok(pipeline) => pipeline,
        Err(e) => return response::bad_request(&format!("Invalid filter: {}", e)),
    };
    if pipeline
        .iter()
        .any(|stage| stage.contains_key("$out") || stage.contains_key("$merge"))
    {
        return response::bad_request("$out and $merge stages are not allowed");
    }
    let joined = match joined_collections(&body.pipeline) {
        Ok(names) => names,
        Err(e) => return response::bad_request(&format!("Invalid pipeline: {}", e)),
    };
    if let Some(name) = joined
        .into_iter()
        .find(|name| !is_catalog_queryable(name, &current_user, &config))
    {
        return response::not_found(&format!("Catalog {} does not exist", name));
    }
    let pipeline_options = body.to_pipeline_options();
    let mut cursor = match collection
        .aggregate(pipeline)
        .with_options(pipeline_options)
        .await
    {
        Ok(cursor) => cursor,
        Err(e) => return super::query_error(e, "Error executing pipeline"),
    };
    let mut docs = Vec::new();
    let mut bytes_read = 0;
    loop {
        match next_result(&mut cursor, &mut bytes_read).await {
            Ok(Some(doc)) => docs.push(doc),
            Ok(None) => break,
            Err(ResultError::TooLarge) => return too_large(),
            Err(ResultError::Database(e)) => {
                tracing::error!("Error retrieving document from the database: {}", e);
                return super::query_error(e, "Error executing pipeline");
            }
        }
    }
    response::ok_ser("success", &docs)
}
