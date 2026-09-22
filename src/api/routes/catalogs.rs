/// Routes for data catalogs.
use crate::api::{
    catalogs::{catalog_accessible, is_catalog_name_visible},
    models::response,
    routes::users::User,
};

use crate::api::admin::require_admin;
use crate::api::routes::babamul::BabamulUser;
use crate::conf::AppConfig;

use actix_web::{get, web, HttpResponse};
use futures::StreamExt;
use mongodb::{bson::doc, Database};
use tokio::io::AsyncReadExt;

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
    current_user: Option<web::ReqData<User>>,
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
    // Filters out empty names, Mongo system.* internals, protected operational
    // collections, and watchlists the current user does not have access to.
    let mut catalog_names = collection_names
        .into_iter()
        .filter(|name| is_catalog_name_visible(name, Some(&current_user)))
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
            catalogs.push(doc! {"name": catalog, "details": details});
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
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };
    if !catalog_accessible(&db, &catalog_name, Some(&current_user)).await {
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
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };
    if !catalog_accessible(&db, &catalog_name, Some(&current_user)).await {
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

/// Report which declared catalogs are actually present
///
/// The admin page reads this to decide which catalogs to offer an ingest for.
/// It reports drift and never acts on it -- converging a catalog is an explicit,
/// attributed task, because it is hours to days of work and a typo in the
/// config must not be able to start one.
#[utoipa::path(
    get,
    path = "/catalogs/status",
    responses(
        (status = 200, description = "State of each declared catalog", body = Vec<serde_json::Value>),
        (status = 403, description = "Not an admin"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Catalogs"]
)]
#[get("/catalogs/status")]
pub async fn get_catalog_status(
    db: web::Data<Database>,
    config: web::Data<AppConfig>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    if let Err(e) = require_admin(&current_user, &babamul_user) {
        return e;
    }
    let declared = crate::catalogs::declared(&config);
    let crossmatched = crate::catalogs::crossmatched(&config);
    match crate::catalogs::status(&db, &declared, &crossmatched).await {
        Ok(statuses) => response::ok_ser("success", statuses),
        Err(e) => response::internal_error(&format!("failed to read catalog status: {e}")),
    }
}

/// Where `export_catalog` writes, and the only directory these routes serve.
fn export_root() -> std::path::PathBuf {
    std::path::PathBuf::from(
        std::env::var("BOOM_CATALOG_DATA_PATH").unwrap_or_else(|_| "data/catalogs".into()),
    )
    .join("export")
}

/// Whether a path component is safe to join onto the export root.
///
/// An allowlist rather than a check for `..`: these routes serve files off the
/// task worker's disk, and the only names that can appear there are collection
/// names and the files `export_catalog` writes.
fn safe_component(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        && !name.starts_with('.')
}

/// List catalog exports available for download
///
/// `export_catalog` writes these so a catalog BOOM cannot fetch again can be
/// published somewhere durable — an archive with a stable URL — and then
/// ingested as an ordinary download.
#[utoipa::path(
    get,
    path = "/catalogs/exports",
    responses(
        (status = 200, description = "Exports on this worker's disk", body = Vec<serde_json::Value>),
        (status = 403, description = "Not an admin")
    ),
    tags=["Catalogs"]
)]
#[get("/catalogs/exports")]
pub async fn get_catalog_exports(
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    if let Err(e) = require_admin(&current_user, &babamul_user) {
        return e;
    }
    let root = export_root();
    let mut exports = Vec::new();
    let Ok(dirs) = std::fs::read_dir(&root) else {
        // Nothing exported yet is not an error: it is the normal state.
        return response::ok_ser("success", exports);
    };
    for dir in dirs.flatten() {
        let name = dir.file_name().to_string_lossy().to_string();
        if !dir.path().is_dir() || !safe_component(&name) {
            continue;
        }
        let mut files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(dir.path()) {
            for entry in entries.flatten() {
                let file = entry.file_name().to_string_lossy().to_string();
                if !safe_component(&file) {
                    continue;
                }
                let bytes = entry.metadata().map(|m| m.len()).unwrap_or(0);
                files.push(serde_json::json!({ "name": file, "bytes": bytes }));
            }
        }
        files.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        let manifest = std::fs::read_to_string(dir.path().join("manifest.json"))
            .ok()
            .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok());
        exports.push(serde_json::json!({
            "collection": name,
            "files": files,
            "manifest": manifest,
        }));
    }
    exports.sort_by(|a, b| a["collection"].as_str().cmp(&b["collection"].as_str()));
    response::ok_ser("success", exports)
}

/// Download one exported catalog file
///
/// Streamed rather than buffered: an export chunk is hundreds of megabytes, and
/// these are meant to be moved to an archive, not opened in a browser tab.
#[utoipa::path(
    get,
    path = "/catalogs/exports/{collection}/{file}",
    params(
        ("collection" = String, Path, description = "Exported collection"),
        ("file" = String, Path, description = "File within that export")
    ),
    responses(
        (status = 200, description = "The file"),
        (status = 403, description = "Not an admin"),
        (status = 404, description = "No such export")
    ),
    tags=["Catalogs"]
)]
#[get("/catalogs/exports/{collection}/{file}")]
pub async fn download_catalog_export(
    path: web::Path<(String, String)>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    if let Err(e) = require_admin(&current_user, &babamul_user) {
        return e;
    }
    let (collection, file) = path.into_inner();
    if !safe_component(&collection) || !safe_component(&file) {
        return response::bad_request("invalid export path");
    }
    let candidate = export_root().join(&collection).join(&file);
    // Canonicalized and checked against the root, so a symlink inside the
    // export directory cannot reach outside it either.
    let (Ok(resolved), Ok(root)) = (candidate.canonicalize(), export_root().canonicalize()) else {
        return response::not_found("no such export");
    };
    if !resolved.starts_with(&root) || !resolved.is_file() {
        return response::not_found("no such export");
    }

    let Ok(file_handle) = tokio::fs::File::open(&resolved).await else {
        return response::not_found("no such export");
    };
    let stream = futures::stream::unfold(file_handle, |mut handle| async move {
        let mut buf = vec![0u8; 64 * 1024];
        match handle.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok::<_, actix_web::Error>(web::Bytes::from(buf)), handle))
            }
            Err(e) => Some((
                Err(actix_web::error::ErrorInternalServerError(e.to_string())),
                handle,
            )),
        }
    });
    HttpResponse::Ok()
        .content_type("application/octet-stream")
        .append_header((
            "content-disposition",
            format!("attachment; filename=\"{file}\""),
        ))
        .streaming(stream)
}
