//! The async (typically long-running) task system used to do things like
//! reprocess alerts, download and insert new archival catalogs, etc.
//!
//! See [`docs/task-system.md`](../docs/task-system.md). Nothing here runs yet --
//! this is a placeholder so the module compiles.

/// An index of possible task types to run, which must be kicked off by
/// an admin calling the API.
///
/// TODO: this is a list of names standing in for the real registry, which needs
/// a params schema, a required role, and idempotence/destructiveness flags per
/// entry. [`crate::catalogs::add_catalog`] is deliberately written as a plain
/// async fn over an `AddCatalogParams` struct, so it can become the first real
/// entry without another refactor.
pub const TASKS: &[&str] = &["add_catalog"];
