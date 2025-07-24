/// Routes for data catalogs.
use crate::{catalogs::catalog_exists, db::PROTECTED_COLLECTION_NAMES, models::response};

use actix_web::{HttpResponse, get, web};
use futures::StreamExt;
use mongodb::{Database, bson::doc};

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
    ),
    tags=["Catalogs"]
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
    ),
    tags=["Catalogs"]
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
    ),
    tags=["Catalogs"]
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
