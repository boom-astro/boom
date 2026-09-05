//! The append-only record of what has been done to the data.
//!
//! BOOM's scientific artifacts -- filtered streams, crossmatch tables,
//! enrichment scores -- are a function of the *current state* of the database,
//! and that state is the raw alert stream plus a sequence of out-of-band
//! mutations. If those mutations are only in someone's shell history, the
//! artifacts derived from them cannot be reasoned about, reproduced, or cited.
//!
//! So every task that changes data appends here, and the collection is
//! **append-only**: entries are never updated after the producing operation
//! finishes and never deleted. A record that can be edited answers a different,
//! much weaker question than "what happened".
//!
//! See [`docs/task-system.md`](../../docs/task-system.md).

use super::models::{now, Actor, Trigger};
use mongodb::bson::{doc, Document};
use mongodb::Database;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const MUTATIONS_COLLECTION: &str = "data_mutations";

/// What produced a mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    /// A task run, kicked off from the admin page or the API.
    Task,
    /// A startup schema migration. Not implemented yet.
    Migration,
    /// The live alert pipeline.
    Pipeline,
}

/// What kind of change was made.
///
/// Coarse on purpose: the point is to make a timeline readable at a glance,
/// with the specifics in `details`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// New data written where there was none.
    Ingest,
    /// Existing documents filled in from a source.
    Backfill,
    /// Existing documents recomputed from data already stored.
    Recompute,
    /// Documents removed.
    Delete,
    /// Indexes created or dropped.
    Index,
    /// A whole collection removed.
    Drop,
}

/// What the mutation acted on.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MutationTarget {
    pub database: String,
    pub collection: String,
    /// Catalog slug, when the target is an archival catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    /// Survey, when the target is survey data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub survey: Option<String>,
}

/// The release the mutation ran under.
///
/// `git_sha` is compiled in from `BOOM_GIT_SHA` when the build sets it, and is
/// absent otherwise. Recorded as an explicit `None` rather than a placeholder:
/// "we do not know which commit did this" is a real and important answer, and a
/// fabricated one would be worse than none.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CodeVersion {
    pub package_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_sha: Option<String>,
}

impl CodeVersion {
    pub fn current() -> Self {
        Self {
            package_version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: option_env!("BOOM_GIT_SHA").map(str::to_string),
        }
    }
}

/// One entry in the ledger.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MutationRecord {
    #[serde(rename = "_id")]
    pub id: String,
    pub source_kind: SourceKind,
    /// Task run id, migration version, or scheduler instance.
    pub source_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_type: Option<String>,
    pub actor: Actor,
    pub trigger: Trigger,
    pub target: MutationTarget,
    pub operation: Operation,
    /// Row counts and anything else worth reading back, task-specific.
    ///
    /// A free-form BSON document rather than a typed field: what is worth
    /// recording differs per task, and pinning a schema here would mean
    /// changing this type every time a task learns something new to report.
    #[schema(value_type = Object)]
    pub details: Document,
    pub recorded_at: f64,
}

#[derive(thiserror::Error, Debug)]
#[error("failed to record a data mutation")]
pub struct LedgerError(#[from] mongodb::error::Error);

/// Append one entry.
///
/// Deliberately takes an owned record and returns nothing to update: there is
/// no API here for changing an entry once written.
pub async fn record(db: &Database, entry: MutationRecord) -> Result<(), LedgerError> {
    db.collection::<MutationRecord>(MUTATIONS_COLLECTION)
        .insert_one(entry)
        .await?;
    Ok(())
}

/// Build an entry for a task that changed one collection.
#[allow(clippy::too_many_arguments)]
pub fn for_task(
    run_id: &str,
    task_type: &str,
    actor: &Actor,
    trigger: Trigger,
    target: MutationTarget,
    operation: Operation,
    details: Document,
) -> MutationRecord {
    MutationRecord {
        id: uuid::Uuid::new_v4().to_string(),
        source_kind: SourceKind::Task,
        source_id: run_id.to_string(),
        task_type: Some(task_type.to_string()),
        actor: actor.clone(),
        trigger,
        target,
        operation,
        details,
        recorded_at: now(),
    }
}

/// What has been done to a collection, most recent first.
pub async fn history(
    db: &Database,
    collection: Option<&str>,
    limit: i64,
) -> Result<Vec<MutationRecord>, LedgerError> {
    use futures::TryStreamExt;
    let filter = match collection {
        Some(name) => doc! { "target.collection": name },
        None => doc! {},
    };
    let cursor = db
        .collection::<MutationRecord>(MUTATIONS_COLLECTION)
        .find(filter)
        .sort(doc! { "recorded_at": -1 })
        .limit(limit)
        .await?;
    Ok(cursor.try_collect().await?)
}

/// Indexes the ledger's read paths depend on.
pub async fn initialize_indexes(db: &Database) -> Result<(), mongodb::error::Error> {
    let collection = db.collection::<Document>(MUTATIONS_COLLECTION);
    // "What has been done to this collection?" is the question the ledger
    // exists to answer, so it gets the compound index.
    collection
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "target.collection": 1, "recorded_at": -1 })
                .build(),
        )
        .await?;
    collection
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "recorded_at": -1 })
                .build(),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor() -> Actor {
        Actor {
            user_id: "babamul:abc".into(),
            username: "pete".into(),
        }
    }

    fn target(collection: &str) -> MutationTarget {
        MutationTarget {
            database: "boom_test".into(),
            collection: collection.to_string(),
            catalog: Some("milliquas".into()),
            survey: None,
        }
    }

    async fn cleanup(db: &Database, collection: &str) {
        let _ = db
            .collection::<MutationRecord>(MUTATIONS_COLLECTION)
            .delete_many(doc! { "target.collection": collection })
            .await;
    }

    #[tokio::test]
    async fn an_entry_records_who_changed_what_under_which_release() {
        let db = crate::conf::get_test_db().await;
        let collection = format!("ledger_test_{}", uuid::Uuid::new_v4().simple());

        record(
            &db,
            for_task(
                "run-1",
                "catalog_ingest",
                &actor(),
                Trigger::Api,
                target(&collection),
                Operation::Ingest,
                doc! { "records_inserted": 1_021_800i64 },
            ),
        )
        .await
        .unwrap();

        let entries = history(&db, Some(&collection), 10).await.unwrap();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.source_kind, SourceKind::Task);
        assert_eq!(entry.source_id, "run-1");
        assert_eq!(entry.task_type.as_deref(), Some("catalog_ingest"));
        // Realm-qualified, because the two id spaces are unrelated and the
        // point of recording an actor is looking them up later.
        assert_eq!(entry.actor.user_id, "babamul:abc");
        assert_eq!(entry.operation, Operation::Ingest);
        assert_eq!(
            entry.details.get_i64("records_inserted").unwrap(),
            1_021_800
        );
        cleanup(&db, &collection).await;
    }

    #[tokio::test]
    async fn history_is_newest_first_and_scoped_to_one_collection() {
        // "What has been done to this collection" is the question the ledger
        // exists to answer, so it must not return another collection's history.
        let db = crate::conf::get_test_db().await;
        let mine = format!("ledger_test_{}", uuid::Uuid::new_v4().simple());
        let theirs = format!("ledger_test_{}", uuid::Uuid::new_v4().simple());

        for (collection, op) in [
            (&mine, Operation::Ingest),
            (&theirs, Operation::Ingest),
            (&mine, Operation::Recompute),
        ] {
            record(
                &db,
                for_task(
                    "run",
                    "catalog_ingest",
                    &actor(),
                    Trigger::Api,
                    target(collection),
                    op,
                    doc! {},
                ),
            )
            .await
            .unwrap();
        }

        let entries = history(&db, Some(&mine), 10).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].operation, Operation::Recompute, "newest first");
        cleanup(&db, &mine).await;
        cleanup(&db, &theirs).await;
    }

    #[tokio::test]
    async fn a_scheduled_run_is_distinguishable_from_one_a_person_asked_for() {
        // The reason `trigger` and the system actor are on the record from the
        // start: adding them later would leave every historical entry unable to
        // say where it came from.
        let db = crate::conf::get_test_db().await;
        let collection = format!("ledger_test_{}", uuid::Uuid::new_v4().simple());

        record(
            &db,
            for_task(
                "run",
                "catalog_ingest",
                &Actor::system(),
                Trigger::Schedule,
                target(&collection),
                Operation::Ingest,
                doc! {},
            ),
        )
        .await
        .unwrap();

        let entry = &history(&db, Some(&collection), 1).await.unwrap()[0];
        assert_eq!(entry.trigger, Trigger::Schedule);
        assert_eq!(entry.actor.username, "system");
        cleanup(&db, &collection).await;
    }

    #[test]
    fn an_unknown_commit_is_recorded_as_absent_rather_than_invented() {
        // A fabricated sha would be worse than none: it would make the ledger
        // confidently wrong about which code produced the data.
        let version = CodeVersion::current();
        assert_eq!(version.package_version, env!("CARGO_PKG_VERSION"));
        // Whatever the build knew, the record must agree with it exactly --
        // never a placeholder standing in for a commit.
        assert_eq!(version.git_sha.as_deref(), option_env!("BOOM_GIT_SHA"));
        assert_ne!(version.git_sha.as_deref(), Some("unknown"));
        assert_ne!(version.git_sha.as_deref(), Some(""));
    }
}
