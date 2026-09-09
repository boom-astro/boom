//! The `enrich_reprocess` task: re-run enrichment over a selection of alerts.
//!
//! Enrichment normally happens once, as an alert is ingested. Re-running it --
//! after a new classifier lands, or after importing historical alerts -- means
//! feeding candids back through the enrichment workers.
//!
//! The binary this replaces was only half the operation: it drained a Redis
//! queue that something else had to fill, so it had no natural completion and
//! sat running against a possibly-empty queue. This **populates the queue and
//! then drains it**, which is what an operator actually wants ("reprocess these
//! alerts", not "run some workers") and which makes completion unambiguous:
//! because the task owns the queue, empty means done.
//!
//! The queue is scoped to the run (`<survey>_enrichment_queue_reprocess_<run>`)
//! precisely so that is true -- a shared queue someone else is also filling
//! would make "empty" meaningless. `input_queue` overrides it for the case
//! where a queue was populated out of band, and then the task only drains.
//!
//! **Idempotent.** Enrichment recomputes scores and photometric properties from
//! the stored alert, so running it again over the same candids converges on the
//! same values.
//!
//! Two behaviours are inherited from the binary and matter: Babamul is forcibly
//! disabled regardless of config, and processed alerts are **not** forwarded to
//! the filter queue -- reprocessing must not re-alert anyone.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::conf::AppConfig;
use crate::enrichment::{
    models::{SharedModelPool, SharedModels},
    EnrichmentWorker, EnrichmentWorkerError, LsstEnrichmentWorker, ZtfEnrichmentWorker,
};
use crate::utils::{
    enums::Survey,
    o11y::logging::as_error,
    worker::{should_terminate, WorkerCmd},
};
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::{num::NonZero, sync::Arc, thread, time::Duration};
use tokio::sync::mpsc;
use tracing::{debug, span, Level};
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "enrich_reprocess";

/// Candids pushed to the queue per round trip.
const PUSH_BATCH: usize = 1_000;

/// How often to check whether the queue has drained.
const DRAIN_POLL: Duration = Duration::from_secs(5);

/// Which alerts to reprocess.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Selection {
    /// Every alert in the survey. Honest about what it is: a full reprocess of
    /// hundreds of millions of alerts, not a quick fix.
    All,
    /// Alerts missing a field, e.g. `classifications.acai_h` after adding a
    /// classifier. The usual case, and the cheapest.
    MissingField { field: String },
    /// A candid range, for reprocessing a known import.
    CandidRange { from: i64, to: i64 },
    /// Everything not enriched by what this release runs.
    ///
    /// The selection to reach for after changing a model or a derivation: it is
    /// exact, where `MissingField` only finds alerts that never had the field
    /// at all and silently misses every alert holding a stale value.
    Stale,
}

impl Selection {
    /// The query that picks the alerts out of `<survey>_alerts`.
    ///
    /// `current_set` is the set this release would produce; only `Stale` uses
    /// it. Alerts with no `enrichment_set` at all are included, because they
    /// were enriched before stamping existed and cannot be shown to be current.
    fn filter(&self, current_set: i64) -> Document {
        match self {
            Selection::All => doc! {},
            Selection::MissingField { field } => doc! { field: { "$exists": false } },
            Selection::CandidRange { from, to } => {
                doc! { "_id": { "$gte": from, "$lte": to } }
            }
            Selection::Stale => doc! { "enrichment_set": { "$ne": current_set } },
        }
    }

    fn describe(&self) -> String {
        match self {
            Selection::All => "every alert".to_string(),
            Selection::MissingField { field } => format!("alerts missing {field}"),
            Selection::CandidRange { from, to } => format!("candids {from}..={to}"),
            Selection::Stale => "alerts not enriched by the current set".to_string(),
        }
    }
}

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnrichReprocessParams {
    pub survey: Survey,
    pub selection: Selection,
    /// Enrichment workers to spawn. Defaults to the survey's configured count.
    #[serde(default)]
    pub n_workers: Option<usize>,
    /// Drain this queue instead of populating a run-scoped one. For a queue
    /// filled out of band; the task will not add to it or delete it.
    #[serde(default)]
    pub input_queue: Option<String>,
}

/// More than this and the workers contend for the connection pool and, on ZTF,
/// for GPU memory.
const MAX_WORKERS: usize = 32;

impl EnrichReprocessParams {
    pub fn validate_params(&self) -> Result<(), String> {
        // Only these two have enrichment workers; the others would fail after
        // the queue had already been filled.
        if !matches!(self.survey, Survey::Ztf | Survey::Lsst) {
            return Err(format!(
                "enrichment reprocessing is not supported for {}",
                self.survey
            ));
        }
        if let Some(n) = self.n_workers {
            if n == 0 || n > MAX_WORKERS {
                return Err(format!("n_workers must be between 1 and {MAX_WORKERS}"));
            }
        }
        if let Selection::MissingField { field } = &self.selection {
            if field.trim().is_empty() {
                return Err("selection.field is required".to_string());
            }
        }
        if let Selection::CandidRange { from, to } = &self.selection {
            if from > to {
                return Err("selection.from must not exceed selection.to".to_string());
            }
        }
        Ok(())
    }
}

/// Fill the queue with the candids to reprocess.
///
/// Returns how many were pushed. Streams rather than collecting: a full
/// reprocess selects hundreds of millions of candids, which will not fit in
/// memory as one vector.
async fn populate(
    ctx: &TaskContext,
    survey: &Survey,
    selection: &Selection,
    current_set: i64,
    queue: &str,
) -> Result<u64, super::TaskError> {
    let failed = |e: String| super::TaskError::Failed(e);
    let alerts = ctx.db().collection::<Document>(&format!("{survey}_alerts"));

    let mut con = ctx
        .config()
        .build_redis()
        .await
        .map_err(|e| failed(e.to_string()))?;

    let mut cursor = alerts
        .find(selection.filter(current_set))
        .projection(doc! { "_id": 1 })
        .no_cursor_timeout(true)
        .await
        .map_err(|e| failed(e.to_string()))?;

    let mut batch: Vec<i64> = Vec::with_capacity(PUSH_BATCH);
    let mut pushed: u64 = 0;

    while let Some(doc) = cursor.try_next().await.map_err(|e| failed(e.to_string()))? {
        if ctx.is_canceled() {
            ctx.warn(format!(
                "canceled while filling the queue after {pushed} candids"
            ));
            return Err(super::TaskError::Canceled);
        }
        // An alert whose _id is not an integer candid cannot be enriched;
        // skipping beats pushing something the workers will choke on.
        let Ok(candid) = doc.get_i64("_id") else {
            continue;
        };
        batch.push(candid);
        if batch.len() >= PUSH_BATCH {
            let n = batch.len() as u64;
            con.lpush::<&str, &Vec<i64>, ()>(queue, &batch)
                .await
                .map_err(|e| failed(e.to_string()))?;
            batch.clear();
            pushed += n;
            ctx.progress(0, pushed, format!("queued {pushed} alerts"))
                .await;
        }
    }
    if !batch.is_empty() {
        pushed += batch.len() as u64;
        con.lpush::<&str, &Vec<i64>, ()>(queue, &batch)
            .await
            .map_err(|e| failed(e.to_string()))?;
    }
    Ok(pushed)
}

/// Run the reprocess.
pub async fn run(
    ctx: &TaskContext,
    params: EnrichReprocessParams,
) -> Result<serde_json::Value, super::TaskError> {
    let failed = |e: String| super::TaskError::Failed(e);
    let config = ctx.config();

    #[cfg(target_os = "linux")]
    crate::utils::gpu::validate_gpu_configuration_for_survey(&params.survey, config)
        .map_err(|e| super::TaskError::InvalidParams(e.to_string()))?;

    let worker_config = config.workers.get(&params.survey).ok_or_else(|| {
        super::TaskError::InvalidParams(format!("no worker config for {}", params.survey))
    })?;
    let n_workers = params
        .n_workers
        .unwrap_or(worker_config.enrichment.n_workers)
        .clamp(1, MAX_WORKERS);

    // What this release would produce, so `Stale` can ask for everything else.
    // Resolved before the workers start: they intern the same set, and running
    // the query against a set that does not exist yet would select every alert.
    let current_set = crate::enrichment::version::resolve_current_set(
        ctx.db(),
        &params.survey.to_string().to_lowercase(),
        crate::enrichment::version::ZTF_MODELS,
    )
    .await
    .map_err(|e| failed(e.to_string()))?;
    let current_set_id = current_set.id;
    ctx.info(format!("current enrichment set is {current_set_id}"));

    // Owning the queue is what makes "empty" mean "done". A caller-supplied one
    // may still be filling, so the task only drains it and never deletes it.
    let owned = params.input_queue.is_none();
    let queue = params.input_queue.clone().unwrap_or_else(|| {
        format!(
            "{}_enrichment_queue_reprocess_{}",
            params.survey,
            ctx.run_id()
        )
    });

    let queued = if owned {
        ctx.info(format!(
            "selecting {} for {} reprocessing",
            params.selection.describe(),
            params.survey
        ));
        let n = populate(
            ctx,
            &params.survey,
            &params.selection,
            current_set_id,
            &queue,
        )
        .await?;
        ctx.info(format!("queued {n} alerts on {queue}"));
        if n == 0 {
            // Nothing to do is a success, not a failure, but it is worth saying
            // out loud -- an empty selection usually means the filter was wrong.
            ctx.warn("the selection matched no alerts; nothing to reprocess");
            return Ok(serde_json::json!({
                "survey": params.survey.to_string(),
                "selection": params.selection.describe(),
                "queued": 0,
                "processed": 0,
            }));
        }
        n
    } else {
        ctx.info(format!("draining externally populated queue {queue}"));
        0
    };

    let shared_model_pool: Option<Arc<SharedModelPool>> =
        if matches!(params.survey, Survey::Ztf) && config.gpu.is_active() {
            Some(
                SharedModelPool::load(&config.gpu.device_ids)
                    .map_err(|e| failed(format!("failed to load ONNX models on GPU: {e}")))?,
            )
        } else {
            None
        };

    ctx.info(format!(
        "starting {n_workers} enrichment worker(s) (Babamul disabled, no filter forwarding)"
    ));

    let mut workers: Vec<(thread::JoinHandle<()>, mpsc::Sender<WorkerCmd>)> =
        Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let (sender, receiver) = mpsc::channel(1);
        let config_path = ctx.config_path().to_string();
        let queue = queue.clone();
        let survey = params.survey.clone();
        let shared_models = shared_model_pool.as_ref().map(|pool| pool.next_model_set());

        let handle = thread::spawn(move || {
            let tid = std::thread::current().id();
            span!(Level::INFO, "enrich-only worker", ?tid, ?survey).in_scope(|| {
                let result = match survey {
                    Survey::Ztf => run_enrich_only::<ZtfEnrichmentWorker>(
                        receiver,
                        &config_path,
                        shared_models,
                        queue,
                    ),
                    Survey::Lsst => run_enrich_only::<LsstEnrichmentWorker>(
                        receiver,
                        &config_path,
                        shared_models,
                        queue,
                    ),
                    _ => unreachable!("survey validated at submit time"),
                };
                result.unwrap_or_else(as_error!("enrichment worker failed"));
            })
        });
        workers.push((handle, sender));
    }

    let outcome = drain(ctx, &queue, queued, &workers).await;

    // Stop the workers however we got here. A worker finishes the batch it has
    // already popped before noticing, so nothing is left half-enriched.
    for (_, sender) in &workers {
        let _ = sender.send(WorkerCmd::TERM).await;
    }
    let mut panicked = 0;
    for (handle, _) in workers {
        if handle.join().is_err() {
            panicked += 1;
        }
    }
    if panicked > 0 {
        ctx.error(format!("{panicked} enrichment worker(s) panicked"));
    }

    if owned {
        // The run-scoped queue is ours; leaving it behind would accumulate keys
        // in Valkey that nothing will ever read.
        if let Ok(mut con) = config.build_redis().await {
            let _ = con.del::<&str, ()>(&queue).await;
        }
    }

    let processed = outcome?;

    ctx.record_mutation(
        MutationTarget {
            database: ctx.db().name().to_string(),
            collection: format!("{}_alerts", params.survey),
            catalog: None,
            survey: Some(params.survey.to_string().to_lowercase()),
        },
        // Scores are recomputed from the stored alert, with no external source.
        Operation::Recompute,
        doc! {
            "selection": params.selection.describe(),
            "enrichment_set": current_set_id,
            "queued": queued as i64,
            "workers": n_workers as i64,
            "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                .unwrap_or(mongodb::bson::Bson::Null),
        },
    )
    .await;

    Ok(serde_json::json!({
        "survey": params.survey.to_string(),
        "selection": params.selection.describe(),
        "queued": queued,
        "processed": processed,
    }))
}

/// Wait for the queue to drain, reporting progress.
///
/// Returns how many were consumed. Completion is `LLEN == 0` observed twice in
/// a row: a worker pops a batch before processing it, so a single zero can be
/// seen while a batch is still in flight, and terminating there would count
/// those alerts as done before they were.
async fn drain(
    ctx: &TaskContext,
    queue: &str,
    queued: u64,
    workers: &[(thread::JoinHandle<()>, mpsc::Sender<WorkerCmd>)],
) -> Result<u64, super::TaskError> {
    let mut con = ctx
        .config()
        .build_redis()
        .await
        .map_err(|e| super::TaskError::Failed(e.to_string()))?;

    let mut empty_streak = 0;
    loop {
        tokio::time::sleep(DRAIN_POLL).await;

        if ctx.is_canceled() {
            ctx.warn("cancellation requested; stopping the workers");
            return Err(super::TaskError::Canceled);
        }

        // Nothing is draining the queue any more, so waiting would hang the run.
        if workers.iter().all(|(h, _)| h.is_finished()) {
            return Err(super::TaskError::Failed(
                "every enrichment worker exited before the queue drained".to_string(),
            ));
        }

        let remaining: u64 = con
            .llen::<&str, u64>(queue)
            .await
            .map_err(|e| super::TaskError::Failed(e.to_string()))?;

        let done = queued.saturating_sub(remaining);
        ctx.progress(done, queued.max(done), format!("{remaining} alerts left"))
            .await;

        if remaining == 0 {
            empty_streak += 1;
            if empty_streak >= 2 {
                ctx.info(format!("queue drained: {done} alerts reprocessed"));
                return Ok(done);
            }
        } else {
            empty_streak = 0;
        }
    }
}

/// One enrichment worker, on its own thread with its own runtime.
///
/// `#[tokio::main]` here is deliberate and safe: `thread::spawn` gives this its
/// own thread, so the runtime it builds does not nest inside the task worker's.
///
/// Differs from the normal enrichment loop in two ways that matter for
/// reprocessing: Babamul is disabled after construction, and processed alerts
/// are not forwarded to the filter queue -- re-running enrichment must not
/// re-alert anyone.
// The error type is large, but this returns once per worker thread rather than
// in a hot path, so boxing it would trade clarity for nothing.
#[allow(clippy::result_large_err)]
#[tokio::main]
async fn run_enrich_only<T: EnrichmentWorker>(
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
    shared_models: Option<Arc<SharedModels>>,
    input_queue: String,
) -> Result<(), EnrichmentWorkerError> {
    debug!(?config_path);
    let mut worker = T::new(config_path, shared_models).await?;
    worker.disable_babamul();

    let config = AppConfig::from_path(config_path)?;
    let survey = T::survey();
    let worker_config = config
        .workers
        .get(&survey)
        .ok_or(EnrichmentWorkerError::WorkerConfigMissing(survey))?;

    let mut con = config.build_redis().await?;

    let command_interval = worker_config.command_interval;
    let mut command_check_countdown = command_interval;

    loop {
        if command_check_countdown == 0 {
            if should_terminate(&mut receiver) {
                break;
            }
            command_check_countdown = command_interval;
        }

        let candids: Vec<i64> = con
            .rpop::<&str, Vec<i64>>(&input_queue, NonZero::new(1000))
            .await?;

        if candids.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            command_check_countdown = 0;
            continue;
        }

        command_check_countdown = command_check_countdown.saturating_sub(candids.len());

        // Return value dropped on purpose: nothing is pushed to an output queue.
        worker.process_alerts(&candids).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(survey: Survey, selection: Selection) -> EnrichReprocessParams {
        EnrichReprocessParams {
            survey,
            selection,
            n_workers: None,
            input_queue: None,
        }
    }

    #[test]
    fn only_surveys_with_enrichment_workers_are_accepted() {
        // Rejected at submit rather than after the queue is already full.
        assert!(params(Survey::Ztf, Selection::All)
            .validate_params()
            .is_ok());
        assert!(params(Survey::Lsst, Selection::All)
            .validate_params()
            .is_ok());
        assert!(params(Survey::Decam, Selection::All)
            .validate_params()
            .is_err());
    }

    #[test]
    fn a_missing_field_selection_needs_a_field() {
        let blank = Selection::MissingField {
            field: "  ".to_string(),
        };
        assert!(params(Survey::Ztf, blank).validate_params().is_err());
    }

    #[test]
    fn an_inverted_candid_range_is_rejected() {
        // Would silently match nothing and report success.
        let backwards = Selection::CandidRange { from: 20, to: 10 };
        assert!(params(Survey::Ztf, backwards).validate_params().is_err());
    }

    #[test]
    fn worker_count_is_bounded() {
        let mut p = params(Survey::Ztf, Selection::All);
        p.n_workers = Some(0);
        assert!(p.validate_params().is_err());
        p.n_workers = Some(MAX_WORKERS + 1);
        assert!(p.validate_params().is_err());
        p.n_workers = Some(MAX_WORKERS);
        assert!(p.validate_params().is_ok());
    }

    #[test]
    fn each_selection_builds_the_query_it_describes() {
        assert_eq!(Selection::All.filter(7), doc! {});
        assert_eq!(
            Selection::MissingField {
                field: "classifications.acai_h".into()
            }
            .filter(7),
            doc! { "classifications.acai_h": { "$exists": false } }
        );
        assert_eq!(
            Selection::CandidRange { from: 1, to: 9 }.filter(7),
            doc! { "_id": { "$gte": 1i64, "$lte": 9i64 } }
        );
    }

    #[test]
    fn stale_selects_everything_not_enriched_by_the_current_set() {
        // Including alerts with no stamp at all: they were enriched before
        // stamping existed, so they cannot be shown to be current.
        assert_eq!(
            Selection::Stale.filter(7),
            doc! { "enrichment_set": { "$ne": 7i64 } }
        );
    }

    #[test]
    fn stale_is_the_selection_that_finds_a_changed_model() {
        // MissingField only finds alerts that never had the field. After a
        // model changes, every alert still has `classifications.btsbot` -- the
        // value is just stale -- so MissingField would select none of them.
        let changed_model = Selection::MissingField {
            field: "classifications.btsbot".into(),
        };
        assert_eq!(
            changed_model.filter(7),
            doc! { "classifications.btsbot": { "$exists": false } },
            "MissingField cannot express staleness, which is why Stale exists"
        );
        assert!(Selection::Stale.describe().contains("current set"));
    }

    #[test]
    fn selections_round_trip_through_the_api_shape() {
        // The admin page and curl both submit these as JSON.
        let parsed: EnrichReprocessParams = serde_json::from_value(serde_json::json!({
            "survey": "ztf",
            "selection": { "kind": "missing_field", "field": "classifications.acai_h" },
        }))
        .expect("deserializes");
        assert!(matches!(parsed.selection, Selection::MissingField { .. }));
        assert!(
            parsed.input_queue.is_none(),
            "defaults to a run-scoped queue"
        );
        assert!(parsed.validate_params().is_ok());
    }

    #[test]
    fn the_task_is_registered_and_retryable() {
        // Enrichment recomputes from the stored alert, so a resumed run
        // reprocesses the same candids to the same values.
        assert!(crate::tasks::is_retryable(TASK_TYPE));
    }

    #[test]
    fn single_flight_is_keyed_by_survey() {
        assert_eq!(
            crate::tasks::single_flight_key(
                TASK_TYPE,
                &serde_json::json!({ "survey": "ztf", "selection": { "kind": "all" } })
            ),
            Some(mongodb::bson::doc! { "survey": "ztf" })
        );
    }
}
