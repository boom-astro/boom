use super::STATS_COLLECTION;
use crate::api::db::PROTECTED_COLLECTION_NAMES;
use crate::api::models::response;
use crate::conf::AppConfig;
use actix_web::{get, web, HttpResponse};
use chrono::Utc;
use futures::StreamExt;
use mongodb::{bson::doc, Collection, Database};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use utoipa::ToSchema;

const CATALOG_STATS_CACHE_KEY: &str = "catalog_stats";
/// Cache catalog stats for 5 days
const CATALOG_STATS_CACHE_SECS: f64 = 5.0 * 24.0 * 3600.0;

/// MongoDB cache document storing the full catalog stats payload (with counts and sizes)
/// under a single well-known `_id`, along with the expiration timestamp.
#[derive(Debug, Serialize, Deserialize)]
struct CatalogStatsCacheEntry {
    #[serde(rename = "_id")]
    id: String,
    n_catalogs: usize,
    catalogs: Vec<CatalogEntry>,
    updated_at: f64,
    cache_until: f64,
}

/// Per-catalog stats entry. `count` and `size_bytes` are populated only when the
/// corresponding query flag is set; otherwise they are omitted from the response.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CatalogEntry {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    /// Storage size in bytes (compressed, on-disk).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// Query parameters controlling which optional details are included in the catalog
/// stats response.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CatalogStatsQuery {
    /// Include document counts per catalog.
    pub count: Option<bool>,
    /// Include storage size per catalog.
    pub size: Option<bool>,
}

/// Response payload for `/babamul/stats/catalogs`: the catalog count and per-catalog
/// entries.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CatalogStats {
    pub n_catalogs: usize,
    pub catalogs: Vec<CatalogEntry>,
}

/// Get catalog statistics.
///
/// By default, returns just the list of catalog names. Use `count=true` and/or
/// `size=true` query parameters to include document counts and storage sizes.
/// Results with counts/sizes are cached for 5 days since catalogs rarely change.
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
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let include_count = query.count.unwrap_or(false);
    let include_size = query.size.unwrap_or(false);
    let now_ts = Utc::now().timestamp() as f64;

    // Build the set of catalogs to expose:
    // configured crossmatch catalogs + survey alert collections (`ZTF_*` / `LSST_*`)
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(e) => {
            return response::internal_error(&format!("Error listing collections: {}", e));
        }
    };
    let is_safe = |name: &str| {
        !name.is_empty()
            && !name.starts_with("system.")
            && !PROTECTED_COLLECTION_NAMES.contains(&name)
    };
    let mut expected: HashSet<String> = config
        .crossmatch
        .values()
        .flat_map(|cats| cats.iter().map(|c| c.catalog.clone()))
        .filter(|name| is_safe(name))
        .collect();
    for name in &collection_names {
        if (name.starts_with("ZTF_") || name.starts_with("LSST_")) && is_safe(name) {
            expected.insert(name.clone());
        }
    }

    // When extra details are requested, try the cache
    // but only serve it if its set of catalog names matches what we expect now.
    // If catalogs were added or removed, fall through and refetch.
    if include_count || include_size {
        let stats_collection: Collection<CatalogStatsCacheEntry> = db.collection(STATS_COLLECTION);

        if let Ok(Some(cached)) = stats_collection
            .find_one(doc! {
                "_id": CATALOG_STATS_CACHE_KEY,
                "cache_until": { "$gt": now_ts },
            })
            .await
        {
            let cached_names: HashSet<String> =
                cached.catalogs.iter().map(|c| c.name.clone()).collect();
            if cached_names == expected {
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

    let mut catalog_names: Vec<String> = expected.into_iter().collect();
    catalog_names.sort();

    let fetch_details = include_count || include_size;
    let mut catalogs = Vec::new();
    for name in &catalog_names {
        let (count, size_bytes) = if fetch_details {
            let collection = db.collection::<mongodb::bson::Document>(name);
            let count = match collection.estimated_document_count().await {
                Ok(c) => Some(c),
                Err(e) => {
                    return response::internal_error(&format!(
                        "Error counting documents in {}: {}",
                        name, e
                    ));
                }
            };
            let size_bytes = match collection
                .aggregate(vec![doc! { "$collStats": { "storageStats": {} } }])
                .await
            {
                Ok(mut cursor) => match cursor.next().await {
                    Some(Ok(d)) => d
                        .get_document("storageStats")
                        .ok()
                        .and_then(|s| s.get("storageSize"))
                        .and_then(|bson| {
                            bson.as_i64()
                                .or_else(|| bson.as_i32().map(|i| i as i64))
                                .or_else(|| bson.as_f64().map(|f| f as i64))
                        })
                        .map(|v| v as u64)
                        .or_else(|| {
                            tracing::warn!("Missing or invalid storageSize for catalog {}", name);
                            None
                        }),
                    Some(Err(e)) => {
                        tracing::warn!("Error reading $collStats for catalog {}: {}", name, e);
                        None
                    }
                    None => {
                        tracing::warn!("Empty $collStats result for catalog {}", name);
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!("Error running $collStats on catalog {}: {}", name, e);
                    None
                }
            };
            (count, size_bytes)
        } else {
            (None, None)
        };

        catalogs.push(CatalogEntry {
            name: name.clone(),
            count,
            size_bytes,
        });
    }

    // Upsert full details into cache
    if fetch_details {
        let stats_collection: Collection<CatalogStatsCacheEntry> = db.collection(STATS_COLLECTION);
        let cache_entry = CatalogStatsCacheEntry {
            id: CATALOG_STATS_CACHE_KEY.to_string(),
            n_catalogs: catalogs.len(),
            catalogs: catalogs.clone(),
            updated_at: now_ts,
            cache_until: now_ts + CATALOG_STATS_CACHE_SECS,
        };
        if let Err(e) = stats_collection
            .replace_one(doc! { "_id": CATALOG_STATS_CACHE_KEY }, &cache_entry)
            .upsert(true)
            .await
        {
            tracing::warn!("Failed to upsert catalog stats cache: {}", e);
        }
    }

    let catalogs: Vec<CatalogEntry> = catalogs
        .into_iter()
        .map(|c| CatalogEntry {
            name: c.name,
            count: if include_count { c.count } else { None },
            size_bytes: if include_size { c.size_bytes } else { None },
        })
        .collect();

    let stats = CatalogStats {
        n_catalogs: catalogs.len(),
        catalogs,
    };

    response::ok_ser("catalog stats", stats)
}
