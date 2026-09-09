//! The async (typically long-running) task system used to do things like
//! reprocess alerts, download and insert new archival catalogs, etc.
//!
//! See [`docs/task-system.md`](../../docs/task-system.md).
//!
//! Anything that mutates BOOM's data outside the live alert pipeline runs here
//! rather than as a binary someone starts over SSH. That is not only about
//! access: these jobs run for hours to days, so they have to survive a deploy,
//! report their logs while running, and be cancellable, and they have to leave
//! a record of who ran what, when, and with which parameters.
//!
//! A run is submitted through the API, written to `task_runs` with
//! `status: queued`, and claimed by the task worker, which holds a lease on it
//! and renews it with a heartbeat. A run whose lease lapses -- because the
//! worker was deployed over or killed -- is requeued and picked up again. Task
//! bodies are therefore written to be **resumable**: re-running one continues
//! rather than repeating.

pub mod batch;
pub mod catalog_ingest;
pub mod context;
pub mod copy_cutouts;
pub mod enrich_reprocess;
pub mod ledger;
pub mod logs;
pub mod migrate_fp_flux;
pub mod migrate_snr;
pub mod models;
pub mod mpcorb_ingest;
pub mod prepare_catalog;
pub mod queue;
pub mod redact;
pub mod reprocess_crossmatch;
pub mod sso_baselines;
pub mod stream_kowalski_alerts;

pub use context::TaskContext;
pub use models::{Actor, TaskRun, TaskStatus, Trigger};

use mongodb::bson::doc;
use serde::Deserialize;

#[derive(thiserror::Error, Debug)]
pub enum TaskError {
    #[error("{0}")]
    Failed(String),
    #[error("canceled")]
    Canceled,
    #[error("unknown task type {id:?}; known types are {known}")]
    UnknownType { id: String, known: String },
    #[error("invalid parameters: {0}")]
    InvalidParams(String),
}

/// A declared kind of work.
///
/// In code rather than in the database: a task type is a piece of the release,
/// and pinning the code version pins what the task does. See the "Concepts"
/// section of the design doc.
#[derive(Debug, Clone, Copy)]
pub struct TaskSpec {
    /// Stable identifier. Never changes -- historical runs are read back by it.
    pub id: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    /// Whether running it twice with the same parameters leaves the same state.
    /// Only an idempotent task can be safely requeued after a lost lease.
    pub idempotent: bool,
    /// Whether it can destroy data, and so needs the client to confirm.
    pub destructive: bool,
}

// TODO: port `stream_kowalski_alerts`, the last data-mutating binary. It is a
// worker pool over an external Kowalski deployment rather than a batch job, so
// it wants the same populate-and-drain treatment `enrich_reprocess` got. Each becomes a body plus an
// arm in `dispatch`; their existing Valkey work queues already give them the
// resumability a task needs, so what they mainly want is the params struct and
// a cancellation check in their batch loop.
//
// TODO: recurring runs. A scheduled task needs an enqueue loop that submits
// with `Trigger::Schedule` and `Actor::system()`, plus a cron expression on
// TaskSpec; `single_flight_key` already prevents a schedule from stacking runs
// up when one is still going. Wanted for periodic maintenance work such as
// trimming old LSST cutouts (#518) -- though for that specific case a TTL index
// on the cutout documents does the job without a task at all, and only the
// one-off backfill of the existing rows needs to run here.

/// Every task type this release knows how to run.
pub const TASKS: &[TaskSpec] = &[
    TaskSpec {
        id: catalog_ingest::TASK_TYPE,
        title: "Ingest an archival catalog",
        description: "Download an archival catalog and insert it into MongoDB, one chunk at a \
                      time. Resumable: re-running continues from the last completed chunk.",
        idempotent: true,
        // Only with drop_existing, which the client has to ask for explicitly.
        destructive: true,
    },
    TaskSpec {
        id: stream_kowalski_alerts::TASK_TYPE,
        title: "Back-fill BOOM from a Kowalski deployment",
        description: "Stream Kowalski's ZTF_alerts into BOOM, importing alerts for objects \
                      BOOM already knows and fetching cutouts only for what was new.",
        // Unordered inserts skipping duplicates, so re-running resumes.
        idempotent: true,
        destructive: false,
    },
    TaskSpec {
        id: copy_cutouts::TASK_TYPE,
        title: "Copy alert cutouts between deployments",
        description: "Copy a survey's cutout collection from one MongoDB to another, \
                      typically before repointing BOOM at new storage. Re-run with \
                      min_candid to catch up on what arrived during the first pass.",
        // Keyed on candid; duplicates are counted rather than fatal.
        idempotent: true,
        destructive: false,
    },
    TaskSpec {
        id: sso_baselines::TASK_TYPE,
        title: "Fit solar system phase-curve baselines",
        description: "Fit a phase curve per object per band from ZTF detections, giving \
                      the baseline brightness that outburst detection is judged against.",
        // Upserts keyed on designation, refit from the same detections.
        idempotent: true,
        destructive: false,
    },
    TaskSpec {
        id: mpcorb_ingest::TASK_TYPE,
        title: "Refresh MPC orbital elements",
        description: "Re-download MPCORB and swap it into MPC_orbits. The scheduler does \
                      this on its own; use this to force a refresh or validate a parse.",
        // Staged and swapped atomically, so a rerun replaces wholesale.
        idempotent: true,
        destructive: false,
    },
    TaskSpec {
        id: enrich_reprocess::TASK_TYPE,
        title: "Re-run enrichment over a selection of alerts",
        description: "Select alerts, queue them, and run enrichment workers over them. \
                      Babamul is disabled and nothing is forwarded to the filter queue, \
                      so reprocessing does not re-alert anyone.",
        // Scores are recomputed from the stored alert, so re-running converges.
        idempotent: true,
        destructive: false,
    },
    TaskSpec {
        id: prepare_catalog::TASK_TYPE,
        title: "Prepare an imported collection for crossmatching",
        description: "Add ra/dec, the GeoJSON point, galactic coordinates and a 2dsphere \
                      index to a collection imported from a file, so it can be used as a \
                      crossmatch catalog.",
        // Documents that already carry coordinates are skipped unless forced,
        // and index creation is a no-op when it already exists.
        idempotent: true,
        destructive: false,
    },
    TaskSpec {
        id: reprocess_crossmatch::TASK_TYPE,
        title: "Reprocess crossmatches against archival catalogs",
        description: "Fill in or refresh crossmatches on a survey's alerts_aux records. \
                      Needed after adding a catalog to crossmatch config, since the \
                      scheduler only crossmatches at first insert.",
        // Each write recomputes a record's matches from the catalog as it
        // stands; watchlists use $addToSet, which is idempotent by construction.
        idempotent: true,
        destructive: false,
    },
    TaskSpec {
        id: migrate_snr::TASK_TYPE,
        title: "Recompute signal-to-noise for ZTF and LSST",
        description: "Recompute snr_psf, snr_ap and (for ZTF) apFlux/apFluxErr on alerts \
                      and their lightcurves, from the stored photometry.",
        // Derived from stored photometry, never from a previous run's output.
        idempotent: true,
        destructive: false,
    },
    TaskSpec {
        id: migrate_fp_flux::TASK_TYPE,
        title: "Migrate ZTF forced photometry to a fixed zeropoint",
        description: "Recompute psfFlux and psfFluxErr in ZTF_alerts_aux from the raw IPAC \
                      flux fields, at the fixed ZTF_ZP zeropoint.",
        // Always recomputed from the raw fields, never from the previous
        // result, so re-running converges on the same values.
        idempotent: true,
        // It overwrites derived values, but the inputs it derives from are
        // untouched, so nothing is lost that cannot be recomputed.
        destructive: false,
    },
];

pub fn find(id: &str) -> Option<&'static TaskSpec> {
    TASKS.iter().find(|t| t.id == id)
}

/// Whether a run of this type may be retried automatically.
///
/// An unknown type is treated as **not** idempotent. A run can outlive the
/// release that created it -- a task type removed or renamed in a later version
/// still has rows in `task_runs` -- and re-running something this build cannot
/// even describe is exactly the case to be conservative about.
pub fn is_retryable(task_type: &str) -> bool {
    find(task_type).is_some_and(|spec| spec.idempotent)
}

/// Task types safe to requeue after a lost lease.
pub fn retryable_task_types() -> Vec<&'static str> {
    TASKS
        .iter()
        .filter(|spec| spec.idempotent)
        .map(|spec| spec.id)
        .collect()
}

fn known_types() -> String {
    TASKS.iter().map(|t| t.id).collect::<Vec<_>>().join(", ")
}

/// Check parameters against the task type, without running anything.
///
/// Called by the API at submit time so a malformed request is a 400 rather than
/// a run that fails minutes later on a worker.
pub fn validate_params(task_type: &str, params: &serde_json::Value) -> Result<(), TaskError> {
    match task_type {
        catalog_ingest::TASK_TYPE => {
            let parsed: catalog_ingest::CatalogIngestParams =
                serde_json::from_value(params.clone())
                    .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed
                .validate()
                .map(|_| ())
                .map_err(|e| TaskError::InvalidParams(e.to_string()))
        }
        stream_kowalski_alerts::TASK_TYPE => {
            let parsed: stream_kowalski_alerts::StreamKowalskiParams =
                serde_json::from_value(params.clone())
                    .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed.validate_params().map_err(TaskError::InvalidParams)
        }
        copy_cutouts::TASK_TYPE => {
            let parsed: copy_cutouts::CopyCutoutsParams = serde_json::from_value(params.clone())
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed.validate_params().map_err(TaskError::InvalidParams)
        }
        sso_baselines::TASK_TYPE => {
            let parsed: sso_baselines::SsoBaselinesParams = serde_json::from_value(params.clone())
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed.validate_params().map_err(TaskError::InvalidParams)
        }
        mpcorb_ingest::TASK_TYPE => {
            let parsed: mpcorb_ingest::MpcorbIngestParams = serde_json::from_value(params.clone())
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed.validate_params().map_err(TaskError::InvalidParams)
        }
        enrich_reprocess::TASK_TYPE => {
            let parsed: enrich_reprocess::EnrichReprocessParams =
                serde_json::from_value(params.clone())
                    .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed.validate_params().map_err(TaskError::InvalidParams)
        }
        prepare_catalog::TASK_TYPE => {
            let parsed: prepare_catalog::PrepareCatalogParams =
                serde_json::from_value(params.clone())
                    .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed.validate_params().map_err(TaskError::InvalidParams)
        }
        reprocess_crossmatch::TASK_TYPE => {
            let parsed: reprocess_crossmatch::ReprocessCrossmatchParams =
                serde_json::from_value(params.clone())
                    .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed.validate_params().map_err(TaskError::InvalidParams)
        }
        migrate_snr::TASK_TYPE => {
            let parsed: migrate_snr::MigrateSnrParams = serde_json::from_value(params.clone())
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed.validate_params().map_err(TaskError::InvalidParams)
        }
        migrate_fp_flux::TASK_TYPE => {
            let parsed: migrate_fp_flux::MigrateFpFluxParams =
                serde_json::from_value(params.clone())
                    .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            parsed.validate_params().map_err(TaskError::InvalidParams)
        }
        other => Err(TaskError::UnknownType {
            id: other.to_string(),
            known: known_types(),
        }),
    }
}

/// Params that must not be concurrently active for a new run of this type.
///
/// Two ingests of the same catalog would race on the same collection and the
/// same chunk state, so submission is single-flight per catalog rather than per
/// task type -- ingesting 2MASS should not block ingesting NED.
pub fn single_flight_key(
    task_type: &str,
    params: &serde_json::Value,
) -> Option<mongodb::bson::Document> {
    match task_type {
        catalog_ingest::TASK_TYPE => params
            .get("catalog")
            .and_then(|c| c.as_str())
            .map(|catalog| doc! { "catalog": catalog }),
        // One migration of a collection at a time: two concurrent runs would
        // rewrite the same documents with the same pipeline, wasting a large
        // amount of write throughput for no benefit.
        migrate_fp_flux::TASK_TYPE => Some(doc! {}),
        // Keyed by survey: migrating ZTF and LSST at once is fine, but two runs
        // over the same survey would rewrite the same documents.
        // Two imports into one BOOM would duplicate the whole stream's work.
        stream_kowalski_alerts::TASK_TYPE => params
            .get("boom_uri")
            .and_then(|v| v.as_str())
            .map(|uri| doc! { "boom_uri": uri }),
        // Keyed by destination and survey: two copies into one collection would
        // race, but different surveys or deployments are independent.
        copy_cutouts::TASK_TYPE => {
            let dst = params.get("dst_uri").and_then(|v| v.as_str());
            let survey = params.get("survey").and_then(|v| v.as_str());
            match (dst, survey) {
                (Some(dst), Some(survey)) => Some(doc! { "dst_uri": dst, "survey": survey }),
                _ => Some(doc! {}),
            }
        }
        // Two would upsert the same baselines from the same detections.
        sso_baselines::TASK_TYPE => Some(doc! {}),
        // One refresh at a time: two would download the same file and race on
        // the staging collection.
        mpcorb_ingest::TASK_TYPE => Some(doc! {}),
        // Keyed by survey: two reprocesses of one survey would contend for the
        // same enrichment workers and GPU, but ZTF and LSST are independent.
        enrich_reprocess::TASK_TYPE => Some(
            params
                .get("survey")
                .and_then(|s| s.as_str())
                .map(|survey| doc! { "survey": survey })
                .unwrap_or_default(),
        ),
        // One preparation of a collection at a time; two would rewrite the same
        // documents and race on creating the index.
        prepare_catalog::TASK_TYPE => params
            .get("catalog")
            .and_then(|c| c.as_str())
            .map(|catalog| doc! { "catalog": catalog }),
        // Keyed by survey: two runs over the same alerts_aux would fight over
        // the same records, but reprocessing ZTF and LSST at once is fine.
        reprocess_crossmatch::TASK_TYPE => Some(
            params
                .get("survey")
                .and_then(|s| s.as_str())
                .map(|survey| doc! { "survey": survey })
                .unwrap_or_default(),
        ),
        migrate_snr::TASK_TYPE => Some(
            params
                .get("survey")
                .and_then(|s| s.as_str())
                .map(|survey| doc! { "survey": survey })
                .unwrap_or_default(),
        ),
        _ => None,
    }
}

/// Run a task body by type.
///
/// The one place a task type turns into work. Adding a task means a body, an
/// arm here, an arm in [`validate_params`], and an entry in [`TASKS`].
pub async fn dispatch(
    ctx: &TaskContext,
    task_type: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, TaskError> {
    match task_type {
        catalog_ingest::TASK_TYPE => {
            let params = catalog_ingest::CatalogIngestParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            catalog_ingest::run(ctx, params).await
        }
        stream_kowalski_alerts::TASK_TYPE => {
            let params = stream_kowalski_alerts::StreamKowalskiParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            stream_kowalski_alerts::run(ctx, params).await
        }
        copy_cutouts::TASK_TYPE => {
            let params = copy_cutouts::CopyCutoutsParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            copy_cutouts::run(ctx, params).await
        }
        sso_baselines::TASK_TYPE => {
            let params = sso_baselines::SsoBaselinesParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            sso_baselines::run(ctx, params).await
        }
        mpcorb_ingest::TASK_TYPE => {
            let params = mpcorb_ingest::MpcorbIngestParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            mpcorb_ingest::run(ctx, params).await
        }
        enrich_reprocess::TASK_TYPE => {
            let params = enrich_reprocess::EnrichReprocessParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            enrich_reprocess::run(ctx, params).await
        }
        prepare_catalog::TASK_TYPE => {
            let params = prepare_catalog::PrepareCatalogParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            prepare_catalog::run(ctx, params).await
        }
        reprocess_crossmatch::TASK_TYPE => {
            let params = reprocess_crossmatch::ReprocessCrossmatchParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            reprocess_crossmatch::run(ctx, params).await
        }
        migrate_snr::TASK_TYPE => {
            let params = migrate_snr::MigrateSnrParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            migrate_snr::run(ctx, params).await
        }
        migrate_fp_flux::TASK_TYPE => {
            let params = migrate_fp_flux::MigrateFpFluxParams::deserialize(params)
                .map_err(|e| TaskError::InvalidParams(e.to_string()))?;
            migrate_fp_flux::run(ctx, params).await
        }
        other => Err(TaskError::UnknownType {
            id: other.to_string(),
            known: known_types(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_registered_task_is_idempotent() {
        // Not a rule of the system -- the queue handles a non-idempotent task
        // by failing it rather than retrying -- but it is the property that
        // makes a task survive a deploy, which is most of the point. Adding one
        // without it is a deliberate choice, so make it a deliberate edit here.
        let not: Vec<&str> = TASKS
            .iter()
            .filter(|spec| !spec.idempotent)
            .map(|spec| spec.id)
            .collect();
        assert!(
            not.is_empty(),
            "these task types would be failed rather than resumed when a worker \
             goes away: {not:?}. If that is intended, update this test and say why."
        );
    }

    #[test]
    fn an_unknown_task_type_is_not_retryable() {
        // A run can outlive the release that registered its type.
        assert!(!is_retryable("a_type_from_some_future_release"));
    }

    #[test]
    fn task_ids_are_unique_and_stable_looking() {
        let mut ids: Vec<&str> = TASKS.iter().map(|t| t.id).collect();
        let count = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), count, "duplicate task id");
        for id in &ids {
            // Historical runs are read back by this string, so it wants to look
            // like an identifier rather than a sentence.
            assert!(
                id.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{id} is not a snake_case identifier"
            );
        }
    }

    #[test]
    fn every_registered_task_validates_and_dispatches() {
        // A type in TASKS with no arm in validate_params is a task the admin
        // page offers and the API rejects.
        for spec in TASKS {
            let err = validate_params(spec.id, &serde_json::json!({})).err();
            assert!(
                !matches!(err, Some(TaskError::UnknownType { .. })),
                "{} is registered but validate_params does not know it",
                spec.id
            );
        }
    }
}
