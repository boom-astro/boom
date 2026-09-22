//! Export a catalog collection to files BOOM can ingest again.
//!
//! Some collections in BOOM came from somewhere that cannot be fetched again: a
//! service that answers one cone search at a time, a colleague's one-off load, a
//! dataset whose publisher never released a bulk copy. `LSPSC` is the worked
//! example — Liu et al. 2025 publish it as a query API, so the rows we hold are
//! the only bulk copy anyone has.
//!
//! Rather than reconstruct such a catalog by hammering someone's server, export
//! what we already have. The output is gzipped CSV chunks plus a manifest, which
//! is what a `Source::Staged` catalog ingests — so BOOM becomes the provenance
//! for its own copy, and the next deployment ingests it the ordinary way.
//!
//! **Read-only.** It writes files, never documents, so there is no ledger entry:
//! the ledger records mutations, and this mutates nothing.

use super::context::TaskContext;
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "export_catalog";

/// Where exports are written, under the shared catalog data path.
const EXPORT_DIR_ENV: &str = "BOOM_CATALOG_DATA_PATH";
const MAX_SHARDS: usize = 64;
const PROGRESS_EVERY: u64 = 500_000;

fn default_shards() -> usize {
    8
}

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExportCatalogParams {
    /// Collection to export, e.g. `LSPSC`. Must be a catalog: alert and user
    /// collections are refused.
    pub collection: String,
    /// Columns to write, in order. Required rather than inferred: a column list
    /// read off whichever document happened to be first is how an export ends
    /// up missing a field that only some rows carry.
    pub fields: Vec<String>,
    /// Output files. Each is written in one pass, so this also decides how much
    /// work a failed run repeats.
    #[serde(default = "default_shards")]
    pub shards: usize,
    /// Count the rows and write the manifest without writing any data.
    #[serde(default)]
    pub dry_run: bool,
}

impl ExportCatalogParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.collection.trim().is_empty() {
            return Err("collection is required".to_string());
        }
        if self.fields.is_empty() {
            return Err("fields is required: name the columns to export".to_string());
        }
        if self.shards == 0 || self.shards > MAX_SHARDS {
            return Err(format!("shards must be between 1 and {MAX_SHARDS}"));
        }
        // An alert or user collection is not a catalog, and exporting one to a
        // file on disk is a data exfiltration primitive, not a feature.
        if crate::api::db::PROTECTED_COLLECTION_NAMES.contains(&self.collection.as_str()) {
            return Err(format!("{} is a protected collection", self.collection));
        }
        let known = crate::catalogs::find_by_collection(&self.collection).is_some()
            || crate::catalogs::is_known_without_definition(&self.collection);
        if !known {
            return Err(format!(
                "{} is not a catalog this release knows about; catalogs are declared in \
                 CATALOGS or WITHOUT_DEFINITIONS in src/catalogs/mod.rs",
                self.collection
            ));
        }
        Ok(())
    }
}

/// One BSON value as a CSV cell.
///
/// Absent and null are both the empty string, which is what the CSV readers on
/// the ingest side treat as "no value".
fn cell(value: Option<&Bson>) -> String {
    match value {
        None | Some(Bson::Null) => String::new(),
        Some(Bson::Double(v)) => v.to_string(),
        Some(Bson::Int32(v)) => v.to_string(),
        Some(Bson::Int64(v)) => v.to_string(),
        Some(Bson::Boolean(v)) => v.to_string(),
        Some(Bson::String(v)) => v.clone(),
        Some(Bson::ObjectId(v)) => v.to_hex(),
        // Anything structured is written as its extended-JSON form rather than
        // silently flattened: a reader that meets one will fail loudly.
        Some(other) => other.to_string(),
    }
}

fn failed(e: impl std::fmt::Display) -> super::TaskError {
    super::TaskError::Failed(e.to_string())
}

/// Write one shard, returning how many rows it holds.
async fn write_shard(
    ctx: &TaskContext,
    collection: &mongodb::Collection<Document>,
    filter: Document,
    fields: &[String],
    path: &PathBuf,
    dry_run: bool,
    written_so_far: u64,
    estimated: u64,
) -> Result<u64, super::TaskError> {
    let mut projection = doc! {};
    for field in fields {
        projection.insert(field.as_str(), 1);
    }
    let mut cursor = collection
        .find(filter)
        .projection(projection)
        .no_cursor_timeout(true)
        .await
        .map_err(failed)?;

    let mut writer = if dry_run {
        None
    } else {
        let file = std::fs::File::create(path).map_err(failed)?;
        let gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut w = csv::Writer::from_writer(gz);
        w.write_record(fields).map_err(failed)?;
        Some(w)
    };

    let mut rows = 0u64;
    let mut last_reported = written_so_far;
    while let Some(doc) = cursor.try_next().await.map_err(failed)? {
        if let Some(w) = writer.as_mut() {
            let record: Vec<String> = fields.iter().map(|f| cell(doc.get(f))).collect();
            w.write_record(&record).map_err(failed)?;
        }
        rows += 1;

        if rows % 10_000 == 0 {
            // Checked between blocks rather than per row: a cancelled export
            // leaves a partial file, which the manifest's absence marks as
            // unusable.
            if ctx.is_canceled() {
                ctx.warn(format!(
                    "canceled after {rows} rows in {}; the partial file is not listed in a \
                     manifest, so it will not be ingested",
                    path.display()
                ));
                return Err(super::TaskError::Canceled);
            }
            let total = written_so_far + rows;
            if total - last_reported >= PROGRESS_EVERY {
                last_reported = total;
                ctx.progress(
                    total,
                    estimated.max(total),
                    format!("{total} rows exported"),
                )
                .await;
            }
        }
    }

    if let Some(w) = writer {
        w.into_inner().map_err(failed)?.finish().map_err(failed)?;
    }
    Ok(rows)
}

pub async fn run(
    ctx: &TaskContext,
    params: ExportCatalogParams,
) -> Result<serde_json::Value, super::TaskError> {
    params
        .validate_params()
        .map_err(super::TaskError::InvalidParams)?;

    let db = ctx.db().clone();
    let collection: mongodb::Collection<Document> = db.collection(&params.collection);
    let estimated = collection.estimated_document_count().await.unwrap_or(0);
    if estimated == 0 {
        return Err(super::TaskError::InvalidParams(format!(
            "{} is empty in this database; there is nothing to export",
            params.collection
        )));
    }

    let root =
        PathBuf::from(std::env::var(EXPORT_DIR_ENV).unwrap_or_else(|_| "data/catalogs".into()))
            .join("export")
            .join(&params.collection);
    std::fs::create_dir_all(&root).map_err(failed)?;

    let shard_key = crate::utils::db::shard_field(&collection).await;
    let shards =
        crate::utils::db::range_shards(&collection, params.shards, shard_key, &Document::new())
            .await;
    ctx.info(format!(
        "exporting ~{estimated} row(s) of {} into {} file(s) under {}{}",
        params.collection,
        shards.len(),
        root.display(),
        if params.dry_run { " (dry run)" } else { "" }
    ));

    let mut files = Vec::new();
    let mut total = 0u64;
    for (i, filter) in shards.iter().enumerate() {
        let name = format!("part-{i:04}.csv.gz");
        let path = root.join(&name);
        let rows = write_shard(
            ctx,
            &collection,
            filter.clone(),
            &params.fields,
            &path,
            params.dry_run,
            total,
            estimated,
        )
        .await?;
        total += rows;
        ctx.info(format!("{name}: {rows} row(s)"));
        files.push(serde_json::json!({ "file": name, "rows": rows }));
    }

    // The manifest is what makes the directory an artifact rather than a pile of
    // files: it names the columns, the row count, and the release that wrote it,
    // so whoever ingests it later can tell what they have.
    let manifest = serde_json::json!({
        "collection": params.collection,
        "fields": params.fields,
        "rows": total,
        "files": files,
        "exported_at": super::models::now(),
        "exported_by": ctx.run_id(),
        "code_version": super::ledger::CodeVersion::current(),
        "source_database": db.name(),
    });
    if !params.dry_run {
        std::fs::write(
            root.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).map_err(failed)?,
        )
        .map_err(failed)?;
    }

    ctx.progress(total, total.max(1), format!("{total} rows exported"))
        .await;
    Ok(serde_json::json!({
        "collection": params.collection,
        "rows": total,
        "files": files.len(),
        "directory": root.display().to_string(),
        "dry_run": params.dry_run,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(collection: &str) -> ExportCatalogParams {
        ExportCatalogParams {
            collection: collection.to_string(),
            fields: vec!["_id".into(), "ra".into(), "dec".into()],
            shards: default_shards(),
            dry_run: false,
        }
    }

    #[test]
    fn a_catalog_collection_is_exportable() {
        assert!(params("LSPSC").validate_params().is_ok());
        assert!(params("NED").validate_params().is_ok());
    }

    #[test]
    fn alert_and_user_collections_are_not() {
        // Exporting one of these to a file is exfiltration wearing a task's
        // clothes, so the allowlist is catalogs rather than a denylist.
        assert!(params("ZTF_alerts").validate_params().is_err());
        assert!(params("babamul_users").validate_params().is_err());
        assert!(params("filters").validate_params().is_err());
    }

    #[test]
    fn the_column_list_is_required() {
        let mut p = params("LSPSC");
        p.fields.clear();
        assert!(
            p.validate_params().is_err(),
            "inferring columns from the first document silently drops fields that only \
             some rows carry"
        );
    }

    #[test]
    fn absent_and_null_both_write_an_empty_cell() {
        let doc = doc! { "a": 1.5, "b": Bson::Null, "s": "x", "t": true };
        assert_eq!(cell(doc.get("a")), "1.5");
        assert_eq!(cell(doc.get("b")), "");
        assert_eq!(cell(doc.get("missing")), "");
        assert_eq!(cell(doc.get("s")), "x");
        assert_eq!(cell(doc.get("t")), "true");
    }

    #[test]
    fn single_flight_is_keyed_by_collection() {
        let a = crate::tasks::single_flight_key(
            TASK_TYPE,
            &serde_json::json!({ "collection": "LSPSC" }),
        );
        let b =
            crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({ "collection": "NED" }));
        assert!(a.is_some());
        assert_ne!(a, b, "two collections must not block each other");
    }
}
