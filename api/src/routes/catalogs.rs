use crate::{
    bodies::{parse_filter, parse_optional_filter},
    models::response,
};

use actix_web::{HttpResponse, get, post, web};
use futures::StreamExt;
use futures::TryStreamExt;
use mongodb::{Database, bson::doc};
use std::collections::HashMap;
use std::fmt;
use utoipa::openapi::RefOr;
use utoipa::openapi::schema::{ObjectBuilder, Schema};
use utoipa::{PartialSchema, ToSchema};

/// Protected names for operational data collections, which should not be used
/// for analytical data catalogs
const PROTECTED_COLLECTION_NAMES: [&str; 2] = ["users", "filters"];

/// Check if a catalog exists
pub async fn catalog_exists(db: &Database, catalog_name: &str) -> bool {
    if catalog_name.is_empty() {
        return false; // Empty catalog names are not valid
    }
    if PROTECTED_COLLECTION_NAMES.contains(&catalog_name) {
        return false; // Protected names cannot be used as catalog names
    }
    // Get collection names in alphabetical order
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(_) => return false,
    };
    // Convert catalog name to collection name
    let collection_name = catalog_name.to_string();
    // Check if the collection exists
    collection_names.contains(&collection_name)
}

#[derive(serde::Deserialize)]
struct CatalogsQueryParams {
    get_details: bool,
}
impl Default for CatalogsQueryParams {
    fn default() -> Self {
        CatalogsQueryParams { get_details: false }
    }
}

/// Get a list of catalogs
#[utoipa::path(
    get,
    path = "/catalogs",
    params(
        ("get_details" = Option<bool>, Query, description = "Whether to include detailed information about each catalog")
    ),
    responses(
        (status = 200, description = "List of catalogs", body = Vec<serde_json::Value>),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/catalogs")]
pub async fn get_catalogs(
    db: web::Data<Database>,
    params: Option<web::Query<CatalogsQueryParams>>,
) -> HttpResponse {
    // Get collection names in alphabetical order
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error getting catalog info: {:?}", e));
        }
    };
    // Catalogs can't be part of the protected names and can't start with "system."
    let mut catalog_names = collection_names
        .into_iter()
        .filter(|name| {
            !PROTECTED_COLLECTION_NAMES.contains(&name.as_str()) && !name.starts_with("system.")
        })
        .collect::<Vec<String>>();
    catalog_names.sort();
    let mut catalogs = Vec::new();
    let params = params.map(|p| p.into_inner()).unwrap_or_default();
    if params.get_details {
        for catalog in catalog_names {
            match db
                .run_command(doc! {
                    "collstats": &catalog
                })
                .await
            {
                Ok(d) => catalogs.push(doc! {"name": catalog, "details": d}),
                Err(e) => {
                    return response::internal_error(&format!(
                        "Error getting catalog info: {:?}",
                        e
                    ));
                }
            };
        }
    } else {
        // If no details requested, just return the names
        for catalog in catalog_names {
            catalogs.push(doc! { "name": catalog });
        }
    }
    // Serialize catalogs
    match serde_json::to_value(&catalogs) {
        Ok(v) => return response::ok("success", v),
        Err(e) => {
            return response::internal_error(&format!("Error serializing catalog info: {:?}", e));
        }
    };
}

#[derive(serde::Deserialize, Clone, ToSchema)]
struct CountQuery {
    filter: Option<serde_json::Value>,
}

/// Run a count query on a catalog
#[utoipa::path(
    post,
    path = "/catalogs/{catalog_name}/count",
    params(
        ("catalog_name" = String, Path, description = "Name of the catalog, e.g., 'ZTF_alerts'"),
    ),
    request_body = CountQuery,
    responses(
        (status = 200, description = "Count of documents in the catalog", body = serde_json::Value),
        (status = 404, description = "Catalog does not exist"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/catalogs/{catalog_name}/count")]
pub async fn post_catalog_count_query(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
    web::Json(query): web::Json<CountQuery>,
) -> HttpResponse {
    if !catalog_exists(&db, &catalog_name).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Count documents with optional filter
    let filter = match parse_optional_filter(&query.filter) {
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

/// Get a catalog's indexes
#[utoipa::path(
    get,
    path = "/catalogs/{catalog_name}/indexes",
    params(
        ("catalog_name" = String, Path, description = "Name of the catalog (case insensitive), e.g., 'ztf'")
    ),
    responses(
        (status = 200, description = "List of indexes in the catalog", body = Vec<serde_json::Value>),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/catalogs/{catalog_name}/indexes")]
pub async fn get_catalog_indexes(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
) -> HttpResponse {
    if !catalog_exists(&db, &catalog_name).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Get index information
    match collection.list_indexes().await {
        Ok(indexes) => {
            let indexes: Vec<_> = indexes
                .map(|index| index.unwrap())
                .collect::<Vec<_>>()
                .await;
            response::ok("success", serde_json::to_value(indexes).unwrap())
        }
        Err(e) => response::internal_error(&format!("Error getting indexes: {:?}", e)),
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone)]
struct SampleQuery {
    size: Option<u16>,
}
impl Default for SampleQuery {
    fn default() -> Self {
        SampleQuery { size: Some(1) }
    }
}

/// Get a sample of data from a catalog
#[utoipa::path(
    get,
    path = "/catalogs/{catalog_name}/sample",
    params(
        ("catalog_name" = String, Path, description = "Name of the catalog (case insensitive), e.g., 'ztf'"),
        ("size" = Option<u16>, Query, description = "Number of sample records to return")
    ),
    responses(
        (status = 200, description = "Sample records from the catalog", body = Vec<serde_json::Value>),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/catalogs/{catalog_name}/sample")]
pub async fn get_catalog_sample(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
    params: web::Query<SampleQuery>,
) -> HttpResponse {
    if !catalog_exists(&db, &catalog_name).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);

    let size = params.size.unwrap_or(1) as i32;
    if size <= 0 || size > 1000 {
        return response::bad_request("Size must be between 1 and 1000");
    }

    // Get a sample of documents
    match collection
        .aggregate(vec![doc! { "$sample": { "size": size } }])
        .await
    {
        Ok(cursor) => {
            let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect::<Vec<_>>().await;
            response::ok("success", serde_json::to_value(docs).unwrap())
        }
        Err(e) => response::internal_error(&format!(
            "Error getting sample for catalog {}: {:?}",
            catalog_name, e
        )),
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
struct FindQuery {
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
    path = "/catalogs/{catalog_name}/find",
    params(
        ("catalog_name" = String, Path, description = "Name of the catalog (case insensitive), e.g., 'ztf'")
    ),
    request_body = FindQuery,
    responses(
        (status = 200, description = "Documents found in the catalog", body = serde_json::Value),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/catalogs/{catalog_name}/find")]
pub async fn post_catalog_find_query(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
    body: web::Json<FindQuery>,
) -> HttpResponse {
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
    path = "/catalogs/{catalog_name}/cone-search",
    params(
        ("catalog_name" = String, Path, description = "Name of the catalog (case insensitive), e.g., 'ztf'")
    ),
    request_body = ConeSearchQuery,
    responses(
        (status = 200, description = "Cone search results", body = serde_json::Value),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/catalogs/{catalog_name}/cone-search")]
pub async fn post_catalog_cone_search_query(
    db: web::Data<Database>,
    catalog_name: web::Path<String>,
    body: web::Json<ConeSearchQuery>,
) -> HttpResponse {
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
