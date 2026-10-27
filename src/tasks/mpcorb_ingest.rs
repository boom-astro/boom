//! The `mpcorb_ingest` task: refresh the local copy of MPC orbital elements.
//!
//! Solar system geometry is derived from these orbits, so they go stale. The
//! ZTF scheduler already refreshes them on its own (`mpcorb::keep_orbits_fresh`),
//! which means this task is for the cases the automatic refresh does not cover:
//! forcing one after an upstream correction, pointing at a different MPCORB
//! mirror, or validating a parse with `dry_run` before letting it write.
//!
//! **Idempotent.** The parse builds a staging collection and swaps it in
//! atomically, so a run either replaces the catalogue wholesale or leaves the
//! previous one untouched. Re-running produces the same result.
//!
//! The work lives in [`crate::utils::mpcorb::refresh_orbits`], which the
//! scheduler also calls -- this task is the attributed, monitored way to invoke
//! the same code by hand.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::mpcorb::{refresh_orbits, DEFAULT_MPCORB_URL, ORBITS_COLLECTION};
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "mpcorb_ingest";

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MpcorbIngestParams {
    /// Where to fetch MPCORB from. Defaults to the Minor Planet Center.
    #[serde(default = "default_url")]
    pub url: String,
    /// Orbits per insert batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Parse and report without writing. Useful for checking an unfamiliar
    /// mirror before letting it replace the catalogue.
    #[serde(default)]
    pub dry_run: bool,
}

fn default_url() -> String {
    DEFAULT_MPCORB_URL.to_string()
}

fn default_batch_size() -> usize {
    10_000
}

const MAX_BATCH_SIZE: usize = 100_000;

impl MpcorbIngestParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        // A client choosing where BOOM downloads from is a wide door: this is
        // the whole solar system catalogue, and a bad mirror would silently
        // replace it. Restricting to https means a URL cannot be used to make
        // the worker fetch from an unencrypted or non-web location.
        if !self.url.starts_with("https://") {
            return Err("url must be an https:// address".to_string());
        }
        Ok(())
    }
}

/// Refresh the orbit catalogue.
pub async fn run(
    ctx: &TaskContext,
    params: MpcorbIngestParams,
) -> Result<serde_json::Value, super::TaskError> {
    // Checked before the download, which is the only cheap place to stop: the
    // refresh builds a staging collection and swaps it in, and there is no safe
    // point to abandon partway that would leave a usable catalogue.
    if ctx.is_canceled() {
        return Err(super::TaskError::Canceled);
    }

    ctx.info(format!("downloading MPCORB from {}", params.url));
    let now = chrono::Utc::now().timestamp() as f64;
    let db = (!params.dry_run).then(|| ctx.db().clone());

    let report = refresh_orbits(
        db.as_ref(),
        &params.url,
        params.batch_size,
        now,
        // A progress bar suits a terminal; this run is watched through its log.
        false,
    )
    .await
    .map_err(|e| super::TaskError::Failed(e.to_string()))?;

    ctx.info(format!(
        "read {} lines: {} orbits parsed ({} comets), {} skipped",
        report.lines, report.parsed, report.comets, report.skipped
    ));
    for sample in &report.rejected_samples {
        // Always empty in a healthy run, so anything here is data being dropped.
        ctx.warn(format!("rejected record-shaped line: {sample}"));
    }

    if params.dry_run {
        ctx.info(format!("dry run: {ORBITS_COLLECTION} not modified"));
    } else {
        ctx.info(format!(
            "{ORBITS_COLLECTION} refreshed with {} orbits",
            report.parsed
        ));
        ctx.record_mutation(
            MutationTarget {
                database: ctx.db().name().to_string(),
                collection: ORBITS_COLLECTION.to_string(),
                catalog: None,
                survey: None,
            },
            // The collection is replaced wholesale from an upstream file.
            Operation::Ingest,
            doc! {
                "source_url": &params.url,
                "orbits": report.parsed as i64,
                "comets": report.comets as i64,
                "lines_read": report.lines as i64,
                "skipped": report.skipped as i64,
                "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                    .unwrap_or(mongodb::bson::Bson::Null),
            },
        )
        .await;
    }

    Ok(serde_json::json!({
        "collection": ORBITS_COLLECTION,
        "orbits": report.parsed,
        "comets": report.comets,
        "lines_read": report.lines,
        "skipped": report.skipped,
        "dry_run": params.dry_run,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> MpcorbIngestParams {
        MpcorbIngestParams {
            url: default_url(),
            batch_size: default_batch_size(),
            dry_run: false,
        }
    }

    #[test]
    fn it_defaults_to_the_minor_planet_center() {
        let parsed: MpcorbIngestParams =
            serde_json::from_value(serde_json::json!({})).expect("defaults");
        assert_eq!(parsed.url, DEFAULT_MPCORB_URL);
        assert!(!parsed.dry_run);
        assert!(parsed.validate_params().is_ok());
    }

    #[test]
    fn a_non_https_source_is_refused() {
        // This replaces the whole solar system catalogue; letting a client name
        // an arbitrary scheme is a wider door than the task needs.
        for url in [
            "http://example.org/MPCORB.DAT",
            "file:///etc/passwd",
            "ftp://x/y",
        ] {
            let mut p = params();
            p.url = url.to_string();
            assert!(p.validate_params().is_err(), "{url} should be refused");
        }
    }

    #[test]
    fn batch_size_is_bounded() {
        let mut p = params();
        p.batch_size = 0;
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn the_task_is_registered_and_retryable() {
        // The refresh stages and swaps atomically, so a resumed run replaces
        // the catalogue wholesale rather than half-applying.
        assert!(crate::tasks::is_retryable(TASK_TYPE));
    }

    #[test]
    fn only_one_refresh_runs_at_a_time() {
        // Two would download the same file and race on the staging collection.
        assert_eq!(
            crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({})),
            Some(mongodb::bson::doc! {})
        );
    }
}
