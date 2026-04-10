use super::STATS_COLLECTION;
use crate::api::db::PROTECTED_COLLECTION_NAMES;
use crate::api::models::response;
use actix_web::{get, web, HttpResponse};
use chrono::Utc;
use mongodb::{bson::doc, Collection, Database};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const CATALOG_STATS_CACHE_KEY: &str = "catalog_stats";
/// Cache catalog stats for 5 days — reference catalogs rarely change.
const CATALOG_STATS_CACHE_SECS: f64 = 5.0 * 24.0 * 3600.0;

#[derive(Debug, Serialize, Deserialize)]
struct CatalogStatsCacheEntry {
    #[serde(rename = "_id")]
    id: String,
    n_catalogs: usize,
    catalogs: Vec<CatalogEntry>,
    updated_at: f64,
    cache_until: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CatalogEntry {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    /// Storage size in bytes (compressed, on-disk).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CatalogStatsQuery {
    /// Include document counts per catalog.
    pub count: Option<bool>,
    /// Include storage size per catalog.
    pub size: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CatalogStats {
    pub n_catalogs: usize,
    pub catalogs: Vec<CatalogEntry>,
}

/// Get catalog statistics.
///
/// By default returns just the list of catalog names. Use `count=true` and/or
/// `size=true` query parameters to include document counts and storage sizes.
/// Results with counts/sizes are cached for 5 days since reference catalogs rarely change.
#[utoipa::path(
    get,
    path = "/babamul/stats/catalogs",
    params(
        ("count" = Option<bool>, Query, description = "Include document counts per catalog."),
        ("size" = Option<bool>, Query, description = "Include storage size per catalog."),
    ),
    responses(
        (status = 200, description = "Catalog stats retrieved", body = CatalogStats),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Stats"]
)]
#[get("/stats/catalogs")]
pub async fn get_catalog_stats(
    query: web::Query<CatalogStatsQuery>,
    db: web::Data<Database>,
) -> HttpResponse {
    let include_count = query.count.unwrap_or(false);
    let include_size = query.size.unwrap_or(false);
    let now_ts = Utc::now().timestamp() as f64;

    // When extra details are requested, try the cache first
    if include_count || include_size {
        let stats_collection: Collection<CatalogStatsCacheEntry> = db.collection(STATS_COLLECTION);

        if let Ok(Some(cached)) = stats_collection
            .find_one(doc! { "_id": CATALOG_STATS_CACHE_KEY })
            .await
        {
            if cached.cache_until > now_ts {
                let catalogs = cached
                    .catalogs
                    .into_iter()
                    .map(|c| CatalogEntry {
                        name: c.name,
                        count: if include_count { c.count } else { None },
                        size_bytes: if include_size { c.size_bytes } else { None },
                    })
                    .collect::<Vec<_>>();
                let stats = CatalogStats {
                    n_catalogs: catalogs.len(),
                    catalogs,
                };
                return response::ok_ser("catalog stats (cached)", stats);
            }
        }
    }

    // List catalog names
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error listing collections: {}", e));
        }
    };

    let mut catalog_names: Vec<String> = collection_names
        .into_iter()
        .filter(|name| {
            !PROTECTED_COLLECTION_NAMES.contains(&name.as_str())
                && !name.starts_with("system.")
                && !name.starts_with("ZTF_")
                && !name.starts_with("LSST_")
        })
        .collect();
    catalog_names.sort();

    let mut catalogs = Vec::new();
    for name in &catalog_names {
        let count = if include_count {
            let collection = db.collection::<mongodb::bson::Document>(name);
            match collection.estimated_document_count().await {
                Ok(c) => Some(c),
                Err(e) => {
                    return response::internal_error(&format!(
                        "Error counting documents in {}: {}",
                        name, e
                    ));
                }
            }
        } else {
            None
        };

        let size_bytes = if include_size {
            match db.run_command(doc! { "collStats": name }).await {
                Ok(doc) => Some(match doc.get("storageSize") {
                    Some(bson) => bson
                        .as_i64()
                        .or_else(|| bson.as_i32().map(|i| i as i64))
                        .or_else(|| bson.as_f64().map(|f| f as i64))
                        .unwrap_or(0) as u64,
                    None => 0,
                }),
                Err(_) => Some(0),
            }
        } else {
            None
        };

        catalogs.push(CatalogEntry {
            name: name.clone(),
            count,
            size_bytes,
        });
    }

    // Upsert into cache when we computed counts/sizes
    if include_count || include_size {
        let stats_collection: Collection<CatalogStatsCacheEntry> = db.collection(STATS_COLLECTION);
        let cache_entry = CatalogStatsCacheEntry {
            id: CATALOG_STATS_CACHE_KEY.to_string(),
            n_catalogs: catalogs.len(),
            catalogs: catalogs.clone(),
            updated_at: now_ts,
            cache_until: now_ts + CATALOG_STATS_CACHE_SECS,
        };
        let _ = stats_collection
            .replace_one(doc! { "_id": CATALOG_STATS_CACHE_KEY }, &cache_entry)
            .upsert(true)
            .await;
    }

    let stats = CatalogStats {
        n_catalogs: catalogs.len(),
        catalogs,
    };

    response::ok_ser("catalog stats", stats)
}
