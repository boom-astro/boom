/// Functionality for working with analytical data catalogs.
use crate::db::PROTECTED_COLLECTION_NAMES;

use mongodb::Database;

/// Check if a catalog exists
pub async fn catalog_exists(db: &Database, catalog_name: &str) -> bool {
    if catalog_name.is_empty() {
        return false; // Empty catalog names are not valid
    }
    if PROTECTED_COLLECTION_NAMES.contains(&catalog_name) {
        return false; // Protected names cannot be used as catalog names
    }
    // Get collection names in alphabetical order
    let collection_names = match db.list_collection_names().await {
        Ok(c) => c,
        Err(_) => return false,
    };
    // Convert catalog name to collection name
    let collection_name = catalog_name.to_string();
    // Check if the collection exists
    collection_names.contains(&collection_name)
}
