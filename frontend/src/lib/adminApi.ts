// Client for BOOM's admin surface: catalog drift and the task system.
//
// These routes live on the main API scope (`/api/...`) rather than under
// `/api/babamul`, but they accept the same token the web app already holds --
// the API middleware resolves either realm, so the admin page needs no second
// login. See docs/task-system.md.
import { fetchWithAuth, parseResponseJson, unwrapData } from "./api";

const API_BASE = "/api";

/** How a declared catalog compares to what is actually in the database. */
export type CatalogHealth = "present" | "missing" | "partial" | "undeclared";

export type CatalogStatus = {
  id: string;
  collection: string | null;
  title: string | null;
  health: CatalogHealth;
  chunks_done: number;
  chunks_total: number;
  n_records: number;
};

/** One field of a task's parameters, from its JSON Schema. */
export type SchemaField = {
  /** "string" | "integer" | "number" | "boolean", or a pair with "null" for an
   *  optional field. */
  type?: string | string[];
  description?: string;
  /** Present when the field is a closed set, e.g. a survey. */
  enum?: string[];
  minimum?: number;
  maximum?: number;
  /** Present for tagged unions, e.g. enrich_reprocess's selection. */
  oneOf?: SchemaField[];
  properties?: Record<string, SchemaField>;
  required?: string[];
};

export type TaskType = {
  id: string;
  title: string;
  description: string;
  idempotent: boolean;
  destructive: boolean;
  params_schema: {
    properties?: Record<string, SchemaField>;
    required?: string[];
  };
};

export type TaskStatus = "queued" | "running" | "succeeded" | "failed" | "canceled";

export type TaskRun = {
  _id: string;
  task_type: string;
  params: Record<string, unknown>;
  status: TaskStatus;
  actor: { user_id: string; username: string };
  trigger: string;
  requested_at: number;
  started_at?: number | null;
  finished_at?: number | null;
  progress: { done: number; total: number; message: string };
  worker?: string | null;
  error?: string | null;
  attempts: number;
};

export type TaskLogLine = { ts: number; level: string; message: string };
export type TaskLogChunk = { run_id: string; seq: number; ts: number; lines: TaskLogLine[] };

/** A run that is not finished, and so is worth polling. */
export function isActive(run: TaskRun): boolean {
  return run.status === "queued" || run.status === "running";
}

async function request<T>(path: string, init: RequestInit | undefined, fallback: T): Promise<T> {
  const res = await fetchWithAuth(`${API_BASE}${path}`, init);
  const body = await parseResponseJson(res).catch(() => null);
  if (!res.ok) {
    // The API puts the reason in `message` -- an unknown catalog or a bad
    // parameter comes back as a 400 saying which, and that is worth showing
    // rather than replacing with a status code.
    const message =
      body && typeof body === "object" && "message" in body
        ? String((body as { message?: unknown }).message)
        : `${path} failed: ${res.status}`;
    throw new Error(message);
  }
  return unwrapData<T>(body, fallback);
}

export function fetchTaskTypes(): Promise<TaskType[]> {
  return request<TaskType[]>("/tasks/types", undefined, []);
}

/** Submit any task type. */
export function submitTask(
  taskType: string,
  params: Record<string, unknown>,
): Promise<TaskRun> {
  return request<TaskRun>(
    "/tasks",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ task_type: taskType, params }),
    },
    {} as TaskRun,
  );
}

export function fetchCatalogStatus(): Promise<CatalogStatus[]> {
  return request<CatalogStatus[]>("/catalogs/status", undefined, []);
}

export function fetchTaskRuns(taskType?: string, limit = 25): Promise<TaskRun[]> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (taskType) params.set("task_type", taskType);
  return request<TaskRun[]>(`/tasks?${params}`, undefined, []);
}

export function fetchTaskRun(runId: string): Promise<TaskRun> {
  return request<TaskRun>(`/tasks/${encodeURIComponent(runId)}`, undefined, {} as TaskRun);
}

/**
 * Tail a run's logs.
 *
 * `afterSeq` is the last chunk sequence already seen; the server returns only
 * what came after it, which is stable under concurrent writes in a way a
 * timestamp cursor is not.
 */
export function fetchTaskLogs(runId: string, afterSeq?: number): Promise<TaskLogChunk[]> {
  const params = new URLSearchParams();
  if (afterSeq !== undefined) params.set("after_seq", String(afterSeq));
  const query = params.toString();
  return request<TaskLogChunk[]>(
    `/tasks/${encodeURIComponent(runId)}/logs${query ? `?${query}` : ""}`,
    undefined,
    [],
  );
}

export function submitCatalogIngest(catalog: string, dropExisting = false): Promise<TaskRun> {
  return request<TaskRun>(
    "/tasks",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        task_type: "catalog_ingest",
        params: { catalog, drop_existing: dropExisting },
      }),
    },
    {} as TaskRun,
  );
}

export function cancelTaskRun(runId: string): Promise<unknown> {
  return request<unknown>(
    `/tasks/${encodeURIComponent(runId)}/cancel`,
    { method: "POST" },
    null,
  );
}
