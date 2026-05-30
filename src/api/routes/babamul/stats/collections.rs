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

const COLLECTION_STATS_CACHE_KEY: &str = "collection_stats";
/// Cache collection stats for 5 days
const COLLECTION_STATS_CACHE_SECS: f64 = 5.0 * 24.0 * 3600.0;

/// MongoDB cache document storing the full collection stats payload (with counts and sizes)
/// under a single well-known `_id`, along with the expiration timestamp.
#[derive(Debug, Serialize, Deserialize)]
struct CollectionStatsCacheEntry {
    #[serde(rename = "_id")]
    id: String,
    n_collections: usize,
    collections: Vec<CollectionEntry>,
    updated_at: f64,
    cache_until: f64,
}

/// Per-collection stats entry. `count` and `size_bytes` are populated only when the
/// corresponding query flag is set; otherwise they are omitted from the response.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CollectionEntry {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    /// Storage size in bytes (compressed, on-disk).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// Query parameters controlling which optional details are included in the collection
/// stats response.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CollectionStatsQuery {
    /// Include document counts per collection.
    pub count: Option<bool>,
    /// Include storage size per collection.
    pub size: Option<bool>,
}

/// Response payload for `/babamul/stats/collections`: the collection count and per-collection
/// entries.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CollectionStats {
    pub n_collections: usize,
    pub collections: Vec<CollectionEntry>,
}

/// Get statistics for catalogs declared under `crossmatch` in the application config,
/// and survey alert collections matching `ZTF_*` / `LSST_*`.
/// Names matching `system.*` or any `PROTECTED_COLLECTION_NAMES` entry are
/// always excluded.
/// By default, returns just the list of collection names. Use `count=true`
/// and/or `size=true` query parameters to include document counts and storage
/// sizes. Results with counts/sizes are cached for 5 days; the cache is
/// auto-invalidated when the set of expected collections changes.
#[utoipa::path(
    get,
    path = "/babamul/stats/collections",
    params(
        ("count" = Option<bool>, Query, description = "Include document counts per collection."),
        ("size" = Option<bool>, Query, description = "Include storage size per collection."),
    ),
    responses(
        (status = 200, description = "Collection stats retrieved", body = CollectionStats),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Stats"]
)]
#[get("/stats/collections")]
pub async fn get_collection_stats(
    query: web::Query<CollectionStatsQuery>,
    db: web::Data<Database>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let include_count = query.count.unwrap_or(false);
    let include_size = query.size.unwrap_or(false);
    let now_ts = Utc::now().timestamp() as f64;

    // Build the set of collections to expose:
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
    // but only serve it if its set of collection names matches what we expect now.
    // If collections were added or removed, fall through and refetch.
    if include_count || include_size {
        let stats_collection: Collection<CollectionStatsCacheEntry> =
            db.collection(STATS_COLLECTION);

        if let Ok(Some(cached)) = stats_collection
            .find_one(doc! {
                "_id": COLLECTION_STATS_CACHE_KEY,
                "cache_until": { "$gt": now_ts },
            })
            .await
        {
            let cached_names: HashSet<String> =
                cached.collections.iter().map(|c| c.name.clone()).collect();
            if cached_names == expected {
                let collections = cached
                    .collections
                    .into_iter()
                    .map(|c| CollectionEntry {
                        name: c.name,
                        count: if include_count { c.count } else { None },
                        size_bytes: if include_size { c.size_bytes } else { None },
                    })
                    .collect::<Vec<_>>();
                let stats = CollectionStats {
                    n_collections: collections.len(),
                    collections,
                };
                return response::ok_ser("collection stats (cached)", stats);
            }
        }
    }

    let mut names: Vec<String> = expected.into_iter().collect();
    names.sort();

    let fetch_details = include_count || include_size;
    let mut collections = Vec::new();
    for name in &names {
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
                            tracing::warn!(
                                "Missing or invalid storageSize for collection {}",
                                name
                            );
                            None
                        }),
                    Some(Err(e)) => {
                        tracing::warn!("Error reading $collStats for collection {}: {}", name, e);
                        None
                    }
                    None => {
                        tracing::warn!("Empty $collStats result for collection {}", name);
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!("Error running $collStats on collection {}: {}", name, e);
                    None
                }
            };
            (count, size_bytes)
        } else {
            (None, None)
        };

        collections.push(CollectionEntry {
            name: name.clone(),
            count,
            size_bytes,
        });
    }

    // Upsert full details into cache
    if fetch_details {
        let stats_collection: Collection<CollectionStatsCacheEntry> =
            db.collection(STATS_COLLECTION);
        let cache_entry = CollectionStatsCacheEntry {
            id: COLLECTION_STATS_CACHE_KEY.to_string(),
            n_collections: collections.len(),
            collections: collections.clone(),
            updated_at: now_ts,
            cache_until: now_ts + COLLECTION_STATS_CACHE_SECS,
        };
        if let Err(e) = stats_collection
            .replace_one(doc! { "_id": COLLECTION_STATS_CACHE_KEY }, &cache_entry)
            .upsert(true)
            .await
        {
            tracing::warn!("Failed to upsert collection stats cache: {}", e);
        }
    }

    let collections: Vec<CollectionEntry> = collections
        .into_iter()
        .map(|c| CollectionEntry {
            name: c.name,
            count: if include_count { c.count } else { None },
            size_bytes: if include_size { c.size_bytes } else { None },
        })
        .collect();

    let stats = CollectionStats {
        n_collections: collections.len(),
        collections,
    };

    response::ok_ser("collection stats", stats)
}
