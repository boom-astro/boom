// Client for BOOM's admin surface: catalog drift and the task system.
//
// These routes live on the main API scope (`/api/...`) rather than under
// `/api/babamul`, but they accept the same token the Client already holds --
// the API middleware resolves either realm, so the admin page needs no second
// login. See docs/task-system.md.
import { fetchWithAuth, parseResponseJson, unwrapData } from "./api";

const API_BASE = "/api";

/** How a declared catalog compares to what is actually in the database.
 *
 *  "partial" and "ingesting" are both "not every chunk is in", kept apart by
 *  whether a run is working on it: one is work that stopped and the other is
 *  work in progress, and only the first is something to go and do.
 *
 *  "undeclared" is a name config asks for that this release has no definition
 *  for -- almost always a typo. "external" is one it deliberately has no
 *  definition for, because the collection is populated outside the ingest path;
 *  its `title` carries the reason. */
export type CatalogHealth =
  | "present"
  | "missing"
  | "partial"
  | "ingesting"
  | "undeclared"
  | "external";

export type CatalogStatus = {
  id: string;
  collection: string | null;
  title: string | null;
  health: CatalogHealth;
  chunks_done: number;
  chunks_total: number;
  n_records: number;
  /** Whether crossmatch config names this catalog. Only these turn a missing
   *  ingest into a problem: the pipeline queries them on every alert and gets
   *  nothing back. */
  crossmatched: boolean;
};

/** One field of a task's parameters, from its JSON Schema. */
export type SchemaField = {
  /** "string" | "integer" | "number" | "boolean", or a pair with "null" for an
   *  optional field. */
  type?: string | string[];
  description?: string;
  /** A closed set of values, e.g. a survey. */
  enum?: string[];
  minimum?: number;
  maximum?: number;
  /** The element schema of an array field. */
  items?: SchemaField;
  /** A reference to a definition the task's schema does not carry: `schema_of`
   *  serializes one type and keeps no registry, so a struct or enum parameter
   *  arrives as a `$ref` that cannot be followed. Such a field has no `type`
   *  either, so it has to be recognized by this rather than rendered as the
   *  string the fallback would make it. */
  $ref?: string;
  /** Tagged unions, and optional fields utoipa cannot express as a type pair. */
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

/** One enrichment set alerts are sitting at that is neither current nor accepted. */
export type StaleSet = {
  id: number;
  /** Which models or derivations differ from the current set. */
  changed: string[];
};

export type EnrichmentDrift = {
  survey: string;
  /** null until an enrichment worker publishes a set. */
  current_set: number | null;
  stale_sets: StaleSet[];
  accepted_sets: number[];
  /** Alerts enriched before stamping existed. */
  has_unstamped: boolean;
  /** The drift query timed out: an empty `stale_sets` means nothing then. */
  drift_unknown: boolean;
};

export type TaskStatus = "queued" | "running" | "succeeded" | "failed" | "canceled";

export type Task = {
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
  /** Set by a cancel request. The worker stops at its next safe point, so a
   *  run can be flagged for cancellation and still be running. */
  cancel_requested?: boolean;
  error?: string | null;
  attempts: number;
};

export type TaskLogLine = { ts: number; level: string; message: string };
export type TaskLogChunk = { task_id: string; seq: number; ts: number; lines: TaskLogLine[] };

/** A run that is not finished, and so is worth polling. */
export function isActive(run: Task): boolean {
  return run.status === "queued" || run.status === "running";
}

/**
 * A request the API refused, with what it sent back.
 *
 * Both halves matter to a caller. The status tells a poll whether to keep
 * going -- a 404 means the run is not coming back -- and the payload is
 * sometimes the answer: a 409 from a task submit carries the run that is
 * already in flight, which the server puts there so the client can jump
 * straight to watching it instead of telling somebody to go and find it.
 */
export class ApiRequestError extends Error {
  readonly status: number;
  readonly data: unknown;

  constructor(message: string, status: number, data: unknown) {
    super(message);
    this.name = "ApiRequestError";
    this.status = status;
    this.data = data;
  }
}

/** The API's envelope: a message written to be shown, and the payload. */
export type ApiEnvelope<T> = { message: string; data: T };

function messageOf(body: unknown): string | undefined {
  if (!body || typeof body !== "object" || !("message" in body)) return undefined;
  const message = (body as { message?: unknown }).message;
  return typeof message === "string" && message ? message : undefined;
}

async function send<T>(
  path: string,
  init: RequestInit | undefined,
  fallback: T,
): Promise<ApiEnvelope<T>> {
  const res = await fetchWithAuth(`${API_BASE}${path}`, init);
  const body = await parseResponseJson(res).catch(() => null);
  // The API puts the reason in `message` -- an unknown catalog or a bad
  // parameter comes back as a 400 saying which, and that is worth showing
  // rather than replacing with a status code.
  const message = messageOf(body);
  if (!res.ok) {
    throw new ApiRequestError(
      message ?? `${path} failed: ${res.status}`,
      res.status,
      unwrapData<unknown>(body, null),
    );
  }
  return { message: message ?? "", data: unwrapData<T>(body, fallback) };
}

async function request<T>(path: string, init: RequestInit | undefined, fallback: T): Promise<T> {
  return (await send(path, init, fallback)).data;
}

export function fetchTaskTypes(): Promise<TaskType[]> {
  return request<TaskType[]>("/task-types", undefined, []);
}

/** Submit any task type. */
export function submitTask(
  taskType: string,
  params: Record<string, unknown>,
): Promise<Task> {
  return request<Task>(
    "/tasks",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ task_type: taskType, params }),
    },
    {} as Task,
  );
}

export function fetchEnrichmentStatus(): Promise<EnrichmentDrift[]> {
  return request<EnrichmentDrift[]>("/enrichment/status", undefined, []);
}

/** Marks a non-current set as not needing reprocessing, without touching alerts. */
export function acceptEnrichmentSet(setId: number, reason: string): Promise<unknown> {
  return request<unknown>(
    `/enrichment/sets/${setId}/accept`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason }),
    },
    null,
  );
}

export function unacceptEnrichmentSet(setId: number): Promise<unknown> {
  return request<unknown>(`/enrichment/sets/${setId}/unaccept`, { method: "POST" }, null);
}

/** A catalog export on the task worker's disk, waiting to be published. */
export type CatalogExport = {
  collection: string;
  files: { name: string; bytes: number }[];
  manifest: {
    rows?: number;
    fields?: string[];
    source_database?: string;
    code_version?: { package_version: string; git_sha?: string };
  } | null;
};

export function fetchCatalogExports(): Promise<CatalogExport[]> {
  return request<CatalogExport[]>("/catalogs/exports", undefined, []);
}

export function fetchCatalogStatus(): Promise<CatalogStatus[]> {
  return request<CatalogStatus[]>("/catalogs/status", undefined, []);
}

export function fetchTasks(taskType?: string, limit = 25): Promise<Task[]> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (taskType) params.set("task_type", taskType);
  return request<Task[]>(`/tasks?${params}`, undefined, []);
}

export function fetchTaskRun(runId: string): Promise<Task> {
  return request<Task>(`/tasks/${encodeURIComponent(runId)}`, undefined, {} as Task);
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

export function submitCatalogIngest(catalog: string, dropExisting = false): Promise<Task> {
  return request<Task>(
    "/tasks",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        task_type: "catalog_ingest",
        params: { catalog, drop_existing: dropExisting },
      }),
    },
    {} as Task,
  );
}

/**
 * Ask for a run to be canceled.
 *
 * Returns the server's message, which is the whole point of showing anything:
 * a running task stops at its next safe point rather than being killed, so
 * "cancellation requested" and "run canceled" are different outcomes and the
 * operator needs to know which one they got.
 */
export function cancelTaskRun(runId: string): Promise<string> {
  return send<unknown>(
    `/tasks/${encodeURIComponent(runId)}/cancel`,
    { method: "POST" },
    null,
  ).then((envelope) => envelope.message || "Cancellation requested.");
}
