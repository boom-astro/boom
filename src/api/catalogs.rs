/// Functionality for working with analytical data catalogs.
use crate::{
    api::{db::PROTECTED_COLLECTION_NAMES, routes::users::User},
    conf::AppConfig,
    utils::enums::Survey,
};

use clap::ValueEnum;
use mongodb::Database;

/// Catalogs whose name starts with this prefix are watchlist, gated by per-user ACL.
pub const WATCHLIST_PREFIX: &str = "watchlist_";

const SURVEY_COLLECTION_SUFFIXES: [&str; 3] = ["alerts", "alerts_aux", "alerts_cutouts"];

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

fn survey_collection_suffix(catalog_name: &str) -> Option<&str> {
    Survey::value_variants().iter().find_map(|survey| {
        catalog_name
            .strip_prefix(survey.as_str())?
            .strip_prefix('_')
            .filter(|suffix| SURVEY_COLLECTION_SUFFIXES.contains(suffix))
    })
}

/// Whether the catalog is declared under `crossmatch`, for any survey.
pub fn is_reference_catalog(catalog_name: &str, config: &AppConfig) -> bool {
    // Watchlists are crossmatched too, but stay private behind their ACL.
    !catalog_name.starts_with(WATCHLIST_PREFIX)
        && config
            .crossmatch
            .values()
            .flatten()
            .any(|catalog| catalog.collection_name() == catalog_name)
}

/// Whether the catalog has coordinates to cone search, without checking user access.
pub fn is_cone_searchable(catalog_name: &str, config: &AppConfig) -> bool {
    catalog_name.starts_with(WATCHLIST_PREFIX)
        || matches!(
            survey_collection_suffix(catalog_name),
            Some("alerts" | "alerts_aux")
        )
        || is_reference_catalog(catalog_name, config)
}

/// Whether the user may query the catalog, without checking existence.
pub fn is_catalog_queryable(catalog_name: &str, user: &User, config: &AppConfig) -> bool {
    is_catalog_name_visible(catalog_name, Some(user))
        && (user.is_admin
            || catalog_name.starts_with(WATCHLIST_PREFIX)
            || survey_collection_suffix(catalog_name).is_some()
            || is_reference_catalog(catalog_name, config))
}

/// Whether the user may query the catalog AND it exists as a Mongo collection.
pub async fn catalog_accessible(
    db: &Database,
    catalog_name: &str,
    user: &User,
    config: &AppConfig,
) -> bool {
    is_catalog_queryable(catalog_name, user, config) && collection_exists(db, catalog_name).await
}

/// Whether the catalog exists as a Mongo collection, without checking user access.
pub async fn catalog_exists(db: &Database, catalog_name: &str) -> bool {
    is_safe_catalog_name(catalog_name) && collection_exists(db, catalog_name).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::test_utils::test_config_with_crossmatch;

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

    #[test]
    fn test_is_reference_catalog() {
        let config = test_config_with_crossmatch(&["css_ref", "watchlist_foo"]);
        assert!(is_reference_catalog("css_ref", &config));
        assert!(is_reference_catalog("Gaia_DR3", &config));
        assert!(!is_reference_catalog("watchlist_foo", &config));
        assert!(!is_reference_catalog("css_dets", &config));
        assert!(!is_reference_catalog("ZTF_alerts", &config));
    }

    #[test]
    fn test_is_catalog_queryable() {
        let config = test_config_with_crossmatch(&["watchlist_foo"]);
        let member = user(false, &["watchlist_foo"]);
        for name in [
            "ZTF_alerts",
            "LSST_alerts_aux",
            "DECAM_alerts_cutouts",
            "WINTER_alerts",
            "Gaia_DR3",
            "watchlist_foo",
        ] {
            assert!(is_catalog_queryable(name, &member, &config), "{name}");
        }
        for name in [
            "css_dets",
            "ZTF_alerts_cutouts_20260519",
            "ZTF_tracks",
            "ztf_alerts",
            "watchlist_bar",
            "users",
        ] {
            assert!(!is_catalog_queryable(name, &member, &config), "{name}");
        }

        let admin = user(true, &[]);
        assert!(is_catalog_queryable("css_dets", &admin, &config));
        assert!(is_catalog_queryable("watchlist_bar", &admin, &config));
        assert!(!is_catalog_queryable("users", &admin, &config));
    }

    #[test]
    fn test_is_cone_searchable() {
        let config = test_config_with_crossmatch(&[]);
        for name in ["ZTF_alerts", "LSST_alerts_aux", "Gaia_DR3", "watchlist_foo"] {
            assert!(is_cone_searchable(name, &config), "{name}");
        }
        for name in ["ZTF_alerts_cutouts", "css_dets", "ZTF_alerts_aux_20260519"] {
            assert!(!is_cone_searchable(name, &config), "{name}");
        }
    }

    #[test]
    fn test_protected_collections_are_never_visible() {
        let admin = user(true, &[]);
        for name in PROTECTED_COLLECTION_NAMES {
            assert!(!is_safe_catalog_name(name), "{name} is not protected");
            assert!(!is_catalog_name_visible(name, None));
            assert!(!is_catalog_name_visible(name, Some(&admin)));
        }
    }
}
