//! Functionality for working with archival catalogs, which BOOM uses for
//! cross-matching incoming alerts to label them with IDs from their discovery
//! in other historical surveys.
#![allow(dead_code)] // TODO: remove once this module is finished

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::LazyLock;

const DOWNLOAD_DIR: &str = "~./boom/download"; // TODO: Get from config so we can use the correct volume on prod

/// Number of records to download and insert at a time
const CHUNK_SIZE: usize = 100_000; // TODO: tune, or get from config

/// A trait for implementing download logic for a catalog
trait Downloadable {
    fn download_chunk(&self, istart: usize, iend: usize) -> Result<usize>;
    // Get a vector of download URLs.
    fn get_download_urls(&self) -> Result<Vec<String>>;
}

pub struct Catalog {
    name: &'static str,
    download_url: &'static str,
    collection_name: &'static str,
    // Total number of records
    n_records: usize,
}

static CATALOGS: LazyLock<HashMap<&'static str, Catalog>> = LazyLock::new(|| {
    HashMap::from([(
        "2mass",
        Catalog {
            name: "2MASS",
            download_url: "https://TODO.com",
            collection_name: "",
            n_records: 0, // TODO
        },
    )])
});

/// Determine if the catalog exists in our database
fn catalog_exists_in_db(catalog: &Catalog) -> bool {
    todo!("fetch")
}

/// Add a catalog to the system
fn add(name: &str, drop_if_exists: bool) -> Result<()> {
    // First make sure the catalog exists in our definitions here and return
    // an error if not.
    let catalog: &Catalog = CATALOGS
        .get(name)
        .ok_or_else(|| anyhow!("unknown catalog: {name}"))?;
    let exists: bool = catalog_exists_in_db(catalog); // look up catalogs
    if exists && drop_if_exists {
        // Drop existing catalog
    }
    // Do this in chunks, picking up from where we left off, downloading and
    // inserting. Note that these must be done in order so we can resume.
    let n_existing: usize = todo!();
    let chunks = (n_existing..catalog.n_records).step_by(CHUNK_SIZE);
    for i_chunk in chunks {
        // Download a chunk
        // Run any necessary transformations
        // Insert the chunk
        // Record the insert in the analytics data changelog/ledger
        // Delete the downloaded chunk so we don't accumulate
    }
    // TODO: Add metadata to a catalogs collection so we aren't detecting
    // these implicitly?
    Ok(())
}
