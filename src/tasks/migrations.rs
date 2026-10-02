//! Which migrations this deployment has already run.
//!
//! A [`TaskKind::Migration`](super::TaskKind::Migration) runs once per
//! deployment. The ledger cannot answer whether it has: the ledger records
//! mutations, and a migration that finds nothing left to change writes none,
//! so an absent row means "nothing to do" and "never ran" alike.
//!
//! This is a fact about the deployment rather than about the data, so it lives
//! on its own: one document per migration that has succeeded here, written
//! when the run finishes. That is what lets the admin page separate pending
//! from applied, what makes submitting an applied one need `rerun`, and what
//! makes deleting a migration's module a decision somebody can defend -- the
//! row outlives the code that produced it.

use super::ledger::CodeVersion;
use super::models::now;
use mongodb::bson::doc;
use mongodb::Database;
use serde::{Deserialize, Serialize};

pub const COLLECTION: &str = "task_migrations";

/// One migration, as applied on this deployment.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AppliedMigration {
    /// The task type, which is what makes this idempotent to record.
    #[serde(rename = "_id")]
    pub task_type: String,
    pub applied_at: f64,
    /// The run that applied it, so its parameters and logs can be read back.
    pub run_id: String,
    /// The release it ran under, for the same reason the ledger records one.
    pub code_version: CodeVersion,
}

/// Record that a migration succeeded here.
///
/// Keyed on the task type and upserted, so re-running a migration deliberately
/// moves the record forward rather than adding a second one. Best-effort: the
/// run has already succeeded and failing to note it is not worth reporting the
/// run as failed, but it is worth a warning, because the next person to look
/// will be told the migration is still pending.
pub async fn record_applied(db: &Database, task_type: &str, run_id: &str) {
    let applied = AppliedMigration {
        task_type: task_type.to_string(),
        applied_at: now(),
        run_id: run_id.to_string(),
        code_version: CodeVersion::current(),
    };
    let doc = match mongodb::bson::to_document(&applied) {
        Ok(doc) => doc,
        Err(e) => {
            tracing::warn!(task_type, "could not encode the migration record: {}", e);
            return;
        }
    };
    if let Err(e) = db
        .collection::<AppliedMigration>(COLLECTION)
        .update_one(doc! { "_id": task_type }, doc! { "$set": doc })
        .upsert(true)
        .await
    {
        tracing::warn!(
            task_type,
            run_id,
            "the migration succeeded but recording it did not: {}",
            e
        );
    }
}

/// Every migration applied here, newest first.
pub async fn applied(db: &Database) -> Result<Vec<AppliedMigration>, mongodb::error::Error> {
    use futures::TryStreamExt;
    db.collection::<AppliedMigration>(COLLECTION)
        .find(doc! {})
        .sort(doc! { "applied_at": -1 })
        .await?
        .try_collect()
        .await
}

/// When this migration was applied here, if it has been.
pub async fn applied_one(
    db: &Database,
    task_type: &str,
) -> Result<Option<AppliedMigration>, mongodb::error::Error> {
    db.collection::<AppliedMigration>(COLLECTION)
        .find_one(doc! { "_id": task_type })
        .await
}
