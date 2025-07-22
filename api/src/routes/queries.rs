/// Endpoints for executing analytical queries.
use crate::catalogs::catalog_exists;
use crate::filters::{parse_filter, parse_optional_filter};
use crate::models::response;

use actix_web::{HttpResponse, post, web};
use futures::StreamExt;
use futures::TryStreamExt;
use mongodb::{Database, bson::doc};
use std::collections::HashMap;
use std::fmt;
use utoipa::openapi::RefOr;
use utoipa::openapi::schema::{ObjectBuilder, Schema};
use utoipa::{PartialSchema, ToSchema};

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
    path = "/queries/estimated-count",
    request_body = EstimatedCountQuery,
    responses(
        (status = 200, description = "Approximately count documents in the catalog", body = serde_json::Value),
        (status = 404, description = "Catalog does not exist"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Queries"]
)]
#[post("/queries/estimated-count")]
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

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
struct FindQuery {
    catalog_name: String,
    filter: serde_json::Value,
    projection: Option<serde_json::Value>,
    limit: Option<i64>,
    skip: Option<u64>,
    sort: Option<serde_json::Value>,
    max_time_ms: Option<u64>,
}
impl FindQuery {
    /// Convert to MongoDB Find options
    fn to_find_options(&self) -> mongodb::options::FindOptions {
        let mut options = mongodb::options::FindOptions::default();
        if let Some(projection) = &self.projection {
            options.projection = Some(mongodb::bson::to_document(projection).unwrap());
        }
        if let Some(limit) = self.limit {
            options.limit = Some(limit);
        }
        if let Some(skip) = self.skip {
            options.skip = Some(skip);
        }
        if let Some(sort) = &self.sort {
            options.sort = Some(mongodb::bson::to_document(sort).unwrap());
        }
        if let Some(max_time_ms) = self.max_time_ms {
            options.max_time = Some(std::time::Duration::from_millis(max_time_ms));
        }
        options
    }
}

/// Perform a find query on a catalog
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
        Err(e) => return response::bad_request(&format!("Invalid filter: {:?}", e)),
    };
    let find_options = body.to_find_options();
    match collection.find(filter).with_options(find_options).await {
        Ok(cursor) => {
            let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect::<Vec<_>>().await;
            response::ok("success", serde_json::to_value(docs).unwrap())
        }
        Err(e) => response::internal_error(&format!("Error finding documents: {:?}", e)),
    }
}

#[derive(serde::Deserialize, Clone)]
pub enum Unit {
    Degrees,
    Radians,
    Arcseconds,
    Arcminutes,
}
impl fmt::Debug for Unit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Unit::Degrees => {
                write!(f, "{}", "Degrees")
            }
            Unit::Radians => {
                write!(f, "{}", "Radians")
            }
            Unit::Arcseconds => {
                write!(f, "{}", "Arcseconds")
            }
            Unit::Arcminutes => {
                write!(f, "{}", "Arcminutes")
            }
        }
    }
}
impl ToSchema for Unit {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Unit")
    }
}
impl PartialSchema for Unit {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .schema_type(utoipa::openapi::Type::String)
                .enum_values(Some(vec![
                    "Degrees".to_string(),
                    "Radians".to_string(),
                    "Arcseconds".to_string(),
                    "Arcminutes".to_string(),
                ]))
                .build(),
        ))
    }
}

#[derive(serde::Deserialize, Clone, ToSchema)]
struct ConeSearchQuery {
    catalog_name: String,
    filter: Option<serde_json::Value>,
    projection: Option<serde_json::Value>,
    radius: f64,
    unit: Unit,
    object_coordinates: HashMap<String, [f64; 2]>, // Map of catalog name to coordinates [RA, Dec]
    limit: Option<i64>,
    skip: Option<u64>,
    sort: Option<serde_json::Value>,
    max_time_ms: Option<u64>,
}
impl ConeSearchQuery {
    /// Convert to MongoDB Find options
    fn to_find_options(&self) -> mongodb::options::FindOptions {
        let mut options = mongodb::options::FindOptions::default();
        if let Some(projection) = &self.projection {
            options.projection = Some(mongodb::bson::to_document(&projection).unwrap());
        }
        if let Some(limit) = self.limit {
            options.limit = Some(limit);
        }
        if let Some(skip) = self.skip {
            options.skip = Some(skip);
        }
        if let Some(sort) = &self.sort {
            options.sort = Some(mongodb::bson::to_document(&sort).unwrap());
        }
        if let Some(max_time_ms) = self.max_time_ms {
            options.max_time = Some(std::time::Duration::from_millis(max_time_ms));
        }
        options
    }
}

/// Perform a cone search query on a catalog
#[utoipa::path(
    post,
    path = "/queries/cone-search",
    request_body = ConeSearchQuery,
    responses(
        (status = 200, description = "Cone search results", body = serde_json::Value),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Queries"]
)]
#[post("/queries/cone-search")]
pub async fn post_cone_search_query(
    db: web::Data<Database>,
    body: web::Json<ConeSearchQuery>,
) -> HttpResponse {
    let catalog_name = body.catalog_name.trim();
    if !catalog_exists(&db, &catalog_name).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Perform cone search over each set of object coordinates
    let find_options = body.to_find_options();
    let mut radius = body.radius;
    let unit = body.unit.clone();
    // Convert radius to radians based on unit
    match unit {
        Unit::Degrees => radius = radius.to_radians(),
        Unit::Arcseconds => radius = radius.to_radians() / 3600.0,
        Unit::Arcminutes => radius = radius.to_radians() / 60.0,
        Unit::Radians => {}
    }
    let object_coordinates = &body.object_coordinates;
    let mut docs: HashMap<String, Vec<mongodb::bson::Document>> = HashMap::new();
    let filter = match parse_optional_filter(&body.filter) {
        Ok(f) => f,
        Err(e) => return response::bad_request(&format!("Invalid filter: {:?}", e)),
    };
    for (object_name, radec) in object_coordinates {
        let ra = radec[0] - 180.0;
        let dec = radec[1];
        let center_sphere = doc! {
            "$centerSphere": [[ra, dec], radius]
        };
        let geo_within = doc! {
            "$geoWithin": center_sphere
        };
        let mut conesearch_filter = filter.clone();
        conesearch_filter.insert("coordinates.radec_geojson", geo_within);
        let cursor = match collection
            .find(conesearch_filter)
            .with_options(find_options.clone())
            .await
        {
            Ok(c) => c,
            Err(e) => {
                return response::internal_error(&format!("Error finding documents: {:?}", e));
            }
        };
        // Create map entry for this object's cone search
        let data = match cursor.try_collect::<Vec<mongodb::bson::Document>>().await {
            Ok(d) => d,
            Err(e) => {
                return response::internal_error(&format!("Error collecting documents: {:?}", e));
            }
        };
        docs.insert(object_name.clone(), data);
    }
    return response::ok(
        &format!("Cone Search on {} completed", catalog_name),
        serde_json::json!(docs),
    );
}
