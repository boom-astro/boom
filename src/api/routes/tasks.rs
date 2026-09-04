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
    queue,
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
pub struct LogsParams {
    /// Return only chunks after this sequence number, for tailing.
    pub after_seq: Option<u64>,
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
                    serde_json::to_value(&existing).unwrap_or_default(),
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
            response::ok_ser("success", &run)
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
        Ok(runs) => response::ok_ser("success", runs),
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
        Ok(Some(run)) => response::ok_ser("success", run),
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
