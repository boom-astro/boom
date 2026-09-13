//! Routes for the task system.
//!
//! Submitting a run is how data-mutating work gets started -- there is
//! deliberately no binary an operator can run over SSH. See
//! [`docs/task-system.md`](../../../docs/task-system.md).

use crate::api::{
    admin::require_admin,
    models::response,
    routes::{babamul::BabamulUser, users::User},
};
use crate::tasks::{
    self,
    models::{now, TaskRun, TaskStatus, Trigger},
    queue, redact,
};

use actix_web::{get, post, web, HttpResponse};
use serde::Deserialize;
use utoipa::{IntoParams, ToSchema};

/// Runs returned by a list request without an explicit limit.
const DEFAULT_LIST_LIMIT: i64 = 50;
const MAX_LIST_LIMIT: i64 = 500;

#[derive(Debug, Deserialize, ToSchema)]
pub struct SubmitTaskBody {
    /// Task type id, e.g. `catalog_ingest`.
    pub task_type: String,
    /// Parameters for that task type, validated here rather than on the worker.
    pub params: serde_json::Value,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ListTasksParams {
    /// Only runs of this task type.
    pub task_type: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct MutationsParams {
    /// Only mutations of this collection.
    pub collection: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct LogsParams {
    /// Return only chunks after this sequence number, for tailing.
    pub after_seq: Option<u64>,
}

/// Mask connection credentials before a run leaves the API.
///
/// The worker reads the real parameters straight from `task_runs`; nothing that
/// renders them needs the password, and the admin page is the most likely place
/// for one to end up on a screen or in a screenshot.
fn redacted(mut run: TaskRun) -> TaskRun {
    run.params = redact::redact_params(&run.params);
    run
}

/// List the task types this release can run
#[utoipa::path(
    get,
    path = "/tasks/types",
    responses(
        (status = 200, description = "Available task types", body = Vec<serde_json::Value>),
        (status = 403, description = "Not an admin")
    ),
    tags=["Tasks"]
)]
#[get("/tasks/types")]
pub async fn get_task_types(
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    if let Err(e) = require_admin(&current_user, &babamul_user) {
        return e;
    }
    let types: Vec<serde_json::Value> = tasks::TASKS
        .iter()
        .map(|spec| {
            serde_json::json!({
                "id": spec.id,
                "title": spec.title,
                "description": spec.description,
                "idempotent": spec.idempotent,
                "destructive": spec.destructive,
                // The client renders its submission form from this, so the form
                // and what the API accepts come from one definition.
                "params_schema": (spec.params_schema)(),
            })
        })
        .collect();
    response::ok_ser("success", types)
}

/// Submit a task run
#[utoipa::path(
    post,
    path = "/tasks",
    request_body = SubmitTaskBody,
    responses(
        (status = 200, description = "The queued run", body = TaskRun),
        (status = 400, description = "Unknown task type or invalid parameters"),
        (status = 403, description = "Not an admin"),
        (status = 409, description = "An equivalent run is already queued or running")
    ),
    tags=["Tasks"]
)]
#[post("/tasks")]
pub async fn submit_task(
    db: web::Data<mongodb::Database>,
    body: web::Json<SubmitTaskBody>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    let admin = match require_admin(&current_user, &babamul_user) {
        Ok(admin) => admin,
        Err(e) => return e,
    };
    let body = body.into_inner();

    // Validated here so a typo comes back as a 400 the client can act on,
    // rather than as a run that fails on a worker minutes later.
    if let Err(e) = tasks::validate_params(&body.task_type, &body.params) {
        return response::bad_request(&e.to_string());
    }

    // Single-flight: two ingests of the same catalog would race on the same
    // collection and the same chunk state. Returning the existing run rather
    // than a bare error lets the client jump straight to watching it.
    if let Some(key) = tasks::single_flight_key(&body.task_type, &body.params) {
        match queue::find_active(&db, &body.task_type, key).await {
            Ok(Some(existing)) => {
                return HttpResponse::Conflict().json(response::ApiResponseBody::ok(
                    "an equivalent run is already queued or running",
                    serde_json::to_value(redacted(existing)).unwrap_or_default(),
                ));
            }
            Ok(None) => {}
            Err(e) => {
                return response::internal_error(&format!("failed to check for active runs: {e}"))
            }
        }
    }

    let run = TaskRun {
        id: uuid::Uuid::new_v4().to_string(),
        task_type: body.task_type,
        params: body.params,
        status: TaskStatus::Queued,
        actor: admin.as_task_actor(),
        trigger: Trigger::Api,
        requested_at: now(),
        started_at: None,
        finished_at: None,
        progress: Default::default(),
        worker: None,
        lease_expires_at: None,
        cancel_requested: false,
        error: None,
        attempts: 0,
    };

    match queue::submit(&db, &run).await {
        Ok(()) => {
            tracing::info!(
                run_id = %run.id,
                task_type = %run.task_type,
                "queued a run for {}",
                run.actor.username
            );
            response::ok_ser("success", redacted(run))
        }
        Err(e) => response::internal_error(&format!("failed to queue the run: {e}")),
    }
}

/// List task runs, most recent first
#[utoipa::path(
    get,
    path = "/tasks",
    params(ListTasksParams),
    responses(
        (status = 200, description = "Task runs", body = Vec<TaskRun>),
        (status = 403, description = "Not an admin")
    ),
    tags=["Tasks"]
)]
#[get("/tasks")]
pub async fn get_tasks(
    db: web::Data<mongodb::Database>,
    params: web::Query<ListTasksParams>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    if let Err(e) = require_admin(&current_user, &babamul_user) {
        return e;
    }
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    match queue::list(&db, params.task_type.as_deref(), limit).await {
        Ok(runs) => response::ok_ser(
            "success",
            runs.into_iter().map(redacted).collect::<Vec<_>>(),
        ),
        Err(e) => response::internal_error(&format!("failed to list runs: {e}")),
    }
}

/// Get one task run
#[utoipa::path(
    get,
    path = "/tasks/{run_id}",
    params(("run_id" = String, Path, description = "Task run id")),
    responses(
        (status = 200, description = "The run", body = TaskRun),
        (status = 403, description = "Not an admin"),
        (status = 404, description = "No such run")
    ),
    tags=["Tasks"]
)]
#[get("/tasks/{run_id}")]
pub async fn get_task(
    db: web::Data<mongodb::Database>,
    run_id: web::Path<String>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    if let Err(e) = require_admin(&current_user, &babamul_user) {
        return e;
    }
    match queue::get(&db, &run_id).await {
        Ok(Some(run)) => response::ok_ser("success", redacted(run)),
        Ok(None) => response::not_found("no such run"),
        Err(e) => response::internal_error(&format!("failed to read the run: {e}")),
    }
}

/// Tail a task run's logs
#[utoipa::path(
    get,
    path = "/tasks/{run_id}/logs",
    params(
        ("run_id" = String, Path, description = "Task run id"),
        LogsParams
    ),
    responses(
        (status = 200, description = "Log chunks after after_seq", body = Vec<serde_json::Value>),
        (status = 403, description = "Not an admin")
    ),
    tags=["Tasks"]
)]
#[get("/tasks/{run_id}/logs")]
pub async fn get_task_logs(
    db: web::Data<mongodb::Database>,
    run_id: web::Path<String>,
    params: web::Query<LogsParams>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    if let Err(e) = require_admin(&current_user, &babamul_user) {
        return e;
    }
    match tasks::logs::read_after(&db, &run_id, params.after_seq).await {
        Ok(chunks) => response::ok_ser("success", chunks),
        Err(e) => response::internal_error(&format!("failed to read logs: {e}")),
    }
}

/// Request cancellation of a task run
#[utoipa::path(
    post,
    path = "/tasks/{run_id}/cancel",
    params(("run_id" = String, Path, description = "Task run id")),
    responses(
        (status = 200, description = "Cancellation requested or already terminal"),
        (status = 403, description = "Not an admin"),
        (status = 404, description = "No such run")
    ),
    tags=["Tasks"]
)]
#[post("/tasks/{run_id}/cancel")]
pub async fn cancel_task(
    db: web::Data<mongodb::Database>,
    run_id: web::Path<String>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    let admin = match require_admin(&current_user, &babamul_user) {
        Ok(admin) => admin,
        Err(e) => return e,
    };
    match queue::request_cancel(&db, &run_id).await {
        Ok(None) => response::not_found("no such run"),
        Ok(Some(status)) => {
            tracing::info!(run_id = %*run_id, "cancel requested by {}", admin.username);
            let message = match status {
                // Running tasks stop at their next safe point rather than being
                // killed, so this is a request, not a completed action.
                TaskStatus::Running => {
                    "cancellation requested; the run will stop at its next safe point"
                }
                TaskStatus::Canceled => "run canceled",
                _ => "run had already finished",
            };
            response::ok_ser(message, serde_json::json!({ "status": status }))
        }
        Err(e) => response::internal_error(&format!("failed to request cancellation: {e}")),
    }
}

/// Read the record of what has been done to the data
///
/// Append-only: entries are written when a mutation finishes and are never
/// edited or removed. This is what makes "what has been done to this
/// collection, by whom, under which release" an answerable question rather than
/// a matter of shell history.
#[utoipa::path(
    get,
    path = "/data/mutations",
    params(MutationsParams),
    responses(
        (status = 200, description = "Mutations, most recent first", body = Vec<serde_json::Value>),
        (status = 403, description = "Not an admin")
    ),
    tags=["Tasks"]
)]
#[get("/data/mutations")]
pub async fn get_data_mutations(
    db: web::Data<mongodb::Database>,
    params: web::Query<MutationsParams>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    if let Err(e) = require_admin(&current_user, &babamul_user) {
        return e;
    }
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    match tasks::ledger::history(&db, params.collection.as_deref(), limit).await {
        Ok(entries) => response::ok_ser("success", entries),
        Err(e) => response::internal_error(&format!("failed to read the ledger: {e}")),
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct AcceptSetBody {
    /// Why this set is acceptable despite not being current. Required: "this is
    /// fine" is only useful to the next person if it says on what grounds.
    pub reason: String,
}

/// Report which enrichment each survey's alerts were produced by
///
/// The enrichment analogue of `/catalogs/status`: it reports drift and never
/// acts on it. Re-enriching an archive is days of work, so starting one stays an
/// explicit, attributed decision.
#[utoipa::path(
    get,
    path = "/enrichment/status",
    responses(
        (status = 200, description = "Drift per survey", body = Vec<serde_json::Value>),
        (status = 403, description = "Not an admin")
    ),
    tags=["Tasks"]
)]
#[get("/enrichment/status")]
pub async fn get_enrichment_status(
    db: web::Data<mongodb::Database>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    if let Err(e) = require_admin(&current_user, &babamul_user) {
        return e;
    }
    // Only ZTF has enrichment models declared today; LSST joins the list when
    // it does, rather than reporting an empty status that reads as "no drift".
    match crate::enrichment::version::drift_status(&db, "ztf").await {
        Ok(status) => response::ok_ser("success", vec![status]),
        Err(e) => response::internal_error(&format!("failed to read enrichment status: {e}")),
    }
}

/// Accept a non-current enrichment set, so it stops being reported as drift
///
/// For when the difference does not matter for the alerts already scored — a
/// derivation version bumped for a change that cannot affect them, say. It
/// records the decision **against the set**; no alert is rewritten, so every
/// alert keeps saying exactly which enrichment produced it.
#[utoipa::path(
    post,
    path = "/enrichment/sets/{set_id}/accept",
    params(("set_id" = i64, Path, description = "The set to accept")),
    request_body = AcceptSetBody,
    responses(
        (status = 200, description = "Accepted"),
        (status = 400, description = "No reason given"),
        (status = 403, description = "Not an admin")
    ),
    tags=["Tasks"]
)]
#[post("/enrichment/sets/{set_id}/accept")]
pub async fn accept_enrichment_set(
    db: web::Data<mongodb::Database>,
    set_id: web::Path<i64>,
    body: web::Json<AcceptSetBody>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    let admin = match require_admin(&current_user, &babamul_user) {
        Ok(admin) => admin,
        Err(e) => return e,
    };
    let reason = body.reason.trim();
    if reason.is_empty() {
        return response::bad_request("a reason is required to accept a set");
    }

    let current = match crate::enrichment::version::resolve_current_set(
        &db,
        "ztf",
        crate::enrichment::version::ZTF_MODELS,
    )
    .await
    {
        Ok(set) => set,
        Err(e) => return response::internal_error(&format!("failed to resolve the set: {e}")),
    };

    let actor = admin.as_task_actor();
    if let Err(e) =
        crate::enrichment::version::accept_set(&db, *set_id, current.id, &actor.user_id, reason)
            .await
    {
        return response::internal_error(&format!("failed to accept the set: {e}"));
    }

    // The ledger is where "what has been done to this data, and by whom" lives,
    // and deciding not to reprocess is such a decision.
    let entry = tasks::ledger::MutationRecord {
        id: uuid::Uuid::new_v4().to_string(),
        source_kind: tasks::ledger::SourceKind::Task,
        source_id: format!("enrichment-accept-{}", *set_id),
        task_type: None,
        actor,
        trigger: Trigger::Api,
        target: tasks::ledger::MutationTarget {
            database: db.name().to_string(),
            collection: "ZTF_alerts".to_string(),
            catalog: None,
            survey: Some("ztf".to_string()),
        },
        // No document changed; what changed is whether these alerts are
        // considered to need reprocessing.
        operation: tasks::ledger::Operation::Index,
        details: mongodb::bson::doc! {
            "accepted_set": *set_id,
            "against_current_set": current.id,
            "reason": reason,
        },
        recorded_at: now(),
    };
    if let Err(e) = tasks::ledger::record(&db, entry).await {
        tracing::warn!("failed to record the acceptance in the ledger: {}", e);
    }

    tracing::info!(
        set_id = *set_id,
        "enrichment set accepted by {}: {}",
        admin.username,
        reason
    );
    response::ok_no_data("set accepted")
}

/// Withdraw an acceptance, so the set is reported as drift again
#[utoipa::path(
    post,
    path = "/enrichment/sets/{set_id}/unaccept",
    params(("set_id" = i64, Path, description = "The set to stop accepting")),
    responses(
        (status = 200, description = "Acceptance withdrawn"),
        (status = 403, description = "Not an admin")
    ),
    tags=["Tasks"]
)]
#[post("/enrichment/sets/{set_id}/unaccept")]
pub async fn unaccept_enrichment_set(
    db: web::Data<mongodb::Database>,
    set_id: web::Path<i64>,
    current_user: Option<web::ReqData<User>>,
    babamul_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    if let Err(e) = require_admin(&current_user, &babamul_user) {
        return e;
    }
    match crate::enrichment::version::unaccept_set(&db, *set_id).await {
        Ok(()) => response::ok_no_data("acceptance withdrawn"),
        Err(e) => response::internal_error(&format!("failed to withdraw: {e}")),
    }
}
