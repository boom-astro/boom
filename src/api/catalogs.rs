/// Functionality for working with analytical data catalogs.
use crate::api::{db::PROTECTED_COLLECTION_NAMES, routes::users::User};

use mongodb::Database;

/// Catalogs whose name starts with this prefix are watchlist, gated by per-user ACL.
pub const WATCHLIST_PREFIX: &str = "watchlist_";

/// Whether the name is allowed to refer to a catalog. False if empty names,
/// Mongo `system.*` internals or protected operational collections.
fn is_safe_catalog_name(catalog_name: &str) -> bool {
    !catalog_name.is_empty()
        && !catalog_name.starts_with("system.")
        && !PROTECTED_COLLECTION_NAMES.contains(&catalog_name)
}

async fn collection_exists(db: &Database, collection_name: &str) -> bool {
    match db.list_collection_names().await {
        Ok(names) => names.iter().any(|n| n == collection_name),
        Err(_) => false,
    }
}

/// Whether the catalog is visible to the user, without checking existence.
/// Watchlist catalogs are only visible to users with explicit access.
/// When `user` is `None`, watchlist catalogs are always rejected.
pub fn is_catalog_name_visible(catalog_name: &str, user: Option<&User>) -> bool {
    if !is_safe_catalog_name(catalog_name) {
        return false;
    }
    match user {
        Some(u) => u.can_access_catalog(catalog_name),
        None => !catalog_name.starts_with(WATCHLIST_PREFIX),
    }
}

/// Whether the catalog is visible to the user AND exists as a Mongo collection.
/// When `user` is `None`, watchlist catalogs are always rejected.
pub async fn catalog_accessible(db: &Database, catalog_name: &str, user: Option<&User>) -> bool {
    is_catalog_name_visible(catalog_name, user) && collection_exists(db, catalog_name).await
}

/// Whether the catalog exists as a Mongo collection, without checking user access.
pub async fn catalog_exists(db: &Database, catalog_name: &str) -> bool {
    is_safe_catalog_name(catalog_name) && collection_exists(db, catalog_name).await
}
