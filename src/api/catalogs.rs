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

/// Whether the name is a well-formed watchlist catalog name: it carries the
/// `watchlist_` prefix and passes the general catalog-name safety checks.
pub fn is_valid_watchlist_name(catalog_name: &str) -> bool {
    catalog_name.starts_with(WATCHLIST_PREFIX) && is_safe_catalog_name(catalog_name)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn user(is_admin: bool, watchlist_access: &[&str]) -> User {
        User {
            id: "u".to_string(),
            username: "u".to_string(),
            email: "u@example.com".to_string(),
            password: "x".to_string(),
            is_admin,
            watchlist_access: watchlist_access.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_is_valid_watchlist_name() {
        assert!(is_valid_watchlist_name("watchlist_foo"));
        assert!(!is_valid_watchlist_name("foo"));
        assert!(!is_valid_watchlist_name(""));
        assert!(!is_valid_watchlist_name("system.watchlist_foo"));
    }

    #[test]
    fn test_is_catalog_name_visible() {
        assert!(is_catalog_name_visible("public_cat", None));
        assert!(!is_catalog_name_visible("watchlist_foo", None));
        assert!(!is_catalog_name_visible("", None));
        assert!(!is_catalog_name_visible("system.users", None));

        let no_access = user(false, &[]);
        assert!(is_catalog_name_visible("public_cat", Some(&no_access)));
        assert!(!is_catalog_name_visible("watchlist_foo", Some(&no_access)));

        let with_access = user(false, &["watchlist_foo"]);
        assert!(is_catalog_name_visible("watchlist_foo", Some(&with_access)));
        assert!(!is_catalog_name_visible(
            "watchlist_bar",
            Some(&with_access)
        ));

        let admin = user(true, &[]);
        assert!(is_catalog_name_visible("watchlist_foo", Some(&admin)));
    }
}
