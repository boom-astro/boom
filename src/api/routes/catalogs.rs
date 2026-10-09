/// Routes for data catalogs.
use crate::{
    api::{
        catalogs::{catalog_accessible, is_catalog_queryable, is_reference_catalog},
        models::response,
        routes::users::User,
    },
    conf::AppConfig,
};

use actix_web::{get, web, HttpResponse};
use futures::StreamExt;
use mongodb::{bson::doc, Database};

#[derive(serde::Deserialize)]
struct CatalogsQueryParams {
    get_details: bool,
}
impl Default for CatalogsQueryParams {
    fn default() -> Self {
        CatalogsQueryParams { get_details: false }
    }
}

/// Get the catalogs the current user can query
///
/// Each entry has the catalog `name` and a boolean `crossmatch`, true for the reference
/// catalogs alerts are crossmatched against, plus `details` when `get_details` is set.
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
    current_user: Option<web::ReqData<User>>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };
    // Get collection names in alphabetical order
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error getting catalog info: {}", e));
        }
    };
    let mut catalog_names = collection_names
        .into_iter()
        .filter(|name| is_catalog_queryable(name, &current_user, &config))
        .collect::<Vec<String>>();
    catalog_names.sort();
    let mut catalogs = Vec::new();
    let params = params.map(|p| p.into_inner()).unwrap_or_default();
    if params.get_details {
        for catalog in catalog_names {
            let collection = db.collection::<mongodb::bson::Document>(&catalog);
            let mut cursor = match collection
                .aggregate(vec![doc! { "$collStats": { "storageStats": {} } }])
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    return response::internal_error(&format!("Error getting catalog info: {}", e));
                }
            };
            let stats = match cursor.next().await {
                Some(Ok(d)) => d,
                Some(Err(e)) => {
                    return response::internal_error(&format!("Error getting catalog info: {}", e));
                }
                None => doc! {},
            };
            let details = stats
                .get_document("storageStats")
                .cloned()
                .unwrap_or_default();
            let crossmatch = is_reference_catalog(&catalog, &config);
            catalogs.push(doc! {"name": catalog, "crossmatch": crossmatch, "details": details});
        }
    } else {
        for catalog in catalog_names {
            let crossmatch = is_reference_catalog(&catalog, &config);
            catalogs.push(doc! { "name": catalog, "crossmatch": crossmatch });
        }
    }
    // Serialize catalogs
    match serde_json::to_value(&catalogs) {
        Ok(v) => return response::ok("success", v),
        Err(e) => {
            return response::internal_error(&format!("Error serializing catalog info: {}", e));
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
    current_user: Option<web::ReqData<User>>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };
    if !catalog_accessible(&db, &catalog_name, &current_user, &config).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Get index information
    match collection.list_indexes().await {
        Ok(mut indexes) => {
            let mut index_list = Vec::new();
            while let Some(result) = indexes.next().await {
                match result {
                    Ok(i) => index_list.push(i),
                    Err(e) => {
                        return response::internal_error(&format!(
                            "Error retrieving index information: {}",
                            e
                        ));
                    }
                }
            }
            response::ok_ser("success", index_list)
        }
        Err(e) => response::internal_error(&format!("Error getting indexes: {}", e)),
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
    current_user: Option<web::ReqData<User>>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };
    if !catalog_accessible(&db, &catalog_name, &current_user, &config).await {
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
    let mut cursor = match collection
        .aggregate(vec![doc! { "$sample": { "size": size } }])
        .await
    {
        Ok(cursor) => cursor,
        Err(e) => {
            return response::internal_error(&format!(
                "Error getting sample for catalog {}: {}",
                catalog_name, e
            ))
        }
    };
    let mut docs = Vec::new();
    while let Some(result) = cursor.next().await {
        match result {
            Ok(doc) => docs.push(doc),
            Err(e) => {
                return response::internal_error(&format!(
                    "Error retrieving document for catalog {}: {}",
                    catalog_name, e
                ))
            }
        }
    }
    response::ok_ser("success", &docs)
}
