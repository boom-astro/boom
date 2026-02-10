/// Functionality for working with analytical data catalogs.
use crate::api::db::PROTECTED_COLLECTION_NAMES;

use mongodb::Database;
use tracing::debug;

/// Check if a catalog exists
#[tracing::instrument(name = "catalogs::catalog_exists", skip(db), fields(catalog_name = %catalog_name), ret)]
pub async fn catalog_exists(db: &Database, catalog_name: &str) -> bool {
    if catalog_name.is_empty() {
        debug!("empty catalog");
        return false; // Empty catalog names are not valid
    }
    if PROTECTED_COLLECTION_NAMES.contains(&catalog_name) {
        debug!("catalog name is in PROTECTED_COLLECTION_NAMES");
        return false; // Protected names cannot be used as catalog names
    }
    // Get collection names in alphabetical order
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(e) => {
            debug!(error = %e, "failed to list collection names from database");
            return false;
        }
    };
    // Convert catalog name to collection name
    let collection_name = catalog_name.to_string();
    // Check if the collection exists
    let exists = collection_names.contains(&collection_name);
    debug!(%exists, "catalog existence check complete");
    exists
}
