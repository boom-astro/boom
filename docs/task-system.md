# Task system

Design for running one-off, data-mutating jobs (catalog ingestion, backfills,
migrations, reprocessing) as tracked async tasks kicked off from an admin page
in the web app, instead of by SSH-ing to the production host.

Tracks [#471](https://github.com/boom-astro/boom/issues/471).

## Motivation

Today, anything that mutates BOOM's data outside the live alert pipeline is a
binary someone runs by hand over SSH:

- In this repo: `reprocess_crossmatch`, `migrate_snr`, `migrate_fp_flux`,
  `enrich_reprocess`, `copy_cutouts`, `add_filter`, `stream_kowalski_alerts`.
- In [boom-catalogs](https://github.com/boom-astro/boom-catalogs): the
  `add_{csv,ascii,parquet,fits}_catalog` binaries, the Python `downloaders/`,
  and the `minifiers/`.

This has three problems, in increasing order of severity:

1. **Access.** Mutating production data requires shell access to the production
   host, so the set of people who can do it is the set of people we're willing
   to hand root-adjacent access to.
2. **Safety.** Nothing prevents two people from running `reprocess_crossmatch`
   on the same collection at once, running an ingest with
   `--drop-existing-collection` against the wrong name, or running a binary
   built from an unmerged branch.
3. **Provenance.** This is the real problem. BOOM's scientific artifacts —
   filtered alert streams, crossmatch tables, enrichment scores — are functions
   of the *current state* of the data, and that state is the raw alert stream
   plus an unrecorded sequence of out-of-band mutations. Right now the only
   record that `ZTF_alerts_aux` was backfilled against a new catalog on some
   date, with some radius, using some commit, is shell history and human
   memory. We cannot answer "what has been done to this collection, and with
   what code?" — which means we cannot fully reason about, reproduce, or cite
   the artifacts derived from it.

The goal is not just to move a shell prompt into a browser. It is to make every
mutation of the data system a **recorded, attributed, parameterized,
reproducible event**, and to make the accumulated history of those events
queryable.

## Goals

- Admins launch a fixed, declared set of jobs from the web app; the API records
  who, what, when, with which parameters, and against which code and config.
- Jobs run asynchronously in a dedicated service, survive an API restart, and
  report status and logs back to the UI while running.
- Every job that mutates data appends to an **append-only mutation ledger**, so
  the state of any collection can be reconstructed as `raw ingest + ordered list
  of mutations`.
- Dangerous jobs support a dry run that reports what *would* change.
- No SSH required for routine data operations.

## Non-goals

- Not a general remote-shell. There is no "run arbitrary command" task type.
  The set of runnable operations is a compiled-in registry with typed
  parameters; anything new requires a PR and a deploy. That constraint is the
  security model.
- Not a workflow engine. Tasks do not form a DAG and do not have ordering
  dependencies (per the discussion on #471, they should each be idempotent
  instead). If we later need chaining, it can be layered on.
- Not a replacement for the alert-processing scheduler. That is a separate,
  always-on pipeline; the task system is for out-of-band work.
- No automatic rollback in the first version. See
  [Reversibility](#reversibility).

## Concepts

**Task type** — a declared kind of work, e.g. `reprocess_crossmatch` or
`ingest_catalog`. Identified by a stable string, carries a typed parameter
schema, a human description, a declaration of what it mutates, and flags for
idempotence and destructiveness. Lives in code, versioned with the repo.

**Task run** — one execution of a task type with concrete parameters. Has a
lifecycle (`queued → running → succeeded | failed | cancelled`), an owner, a
timeline, captured logs, and the code/config fingerprint it ran under.

**Mutation record** — an entry in the append-only ledger describing a change
applied to one target (a collection, or a catalog within one). Produced by a
task run; a single run may produce several. This is the provenance primitive.

## Architecture

```
        browser (admin page)
              │  POST /tasks   GET /tasks/:id   GET /tasks/:id/logs
              ▼
        ┌───────────┐        validates params against the registry,
        │  boom-api │        checks authz, writes a `task_runs` doc
        └─────┬─────┘        with status=queued. Returns immediately.
              │
              ▼
        ┌──────────────┐     MongoDB is both the queue and the
        │   mongodb    │     source of truth for run state.
        └─────┬────────┘
              │  findOneAndUpdate(status=queued → running, lease)
              ▼
        ┌──────────────────┐
        │ boom-task-worker │  claims one run at a time, executes it,
        │  (new service)   │  streams logs + progress back to mongo,
        └─────┬────────────┘  renews its lease, writes mutation records
              │
              ├──► data collections (the actual mutation)
              └──► data_mutations (the ledger)
```

### Why Mongo as the queue rather than Valkey

Valkey is already in the stack and is what the alert scheduler uses, so it's the
obvious first thought. But the alert pipeline moves millions of items a day and
needs the throughput; the task system will see a few runs a week. At that
volume, an atomic `findOneAndUpdate` on `task_runs` gives us claiming, leasing,
and status in a single store, with no dual-write to keep consistent between the
queue and the run record. Poll interval of a couple of seconds is imperceptible
for a job that runs for hours.

If task volume ever justifies it, swapping the claim step for a Valkey list is a
contained change.

### The worker service

A new `task-worker` service in `docker-compose.yaml`, built from the same image
as everything else (so it has every binary and every model available), on the
`boom` network, with:

- Its own memory limit — these jobs are heavy, and they should not be able to
  starve the API or the schedulers.
- The config bind mount, like every other service.
- A bind mount for the catalog data directory (a new `BOOM_CATALOG_DATA_PATH`),
  since ingestion reads large files from disk.
- `restart: always`, and a heartbeat so a run orphaned by a crash is detected
  and marked `failed` (or requeued, if the task type is idempotent) rather than
  sitting in `running` forever.

Default concurrency is 1. Concurrency > 1 is a config knob, gated by the lock
keys described in [Concurrency](#concurrency-and-locking).

### How a task actually executes

Two execution kinds, both driven from the same registry:

**`Native`** (preferred). The task body is an `async fn` in `src/tasks/<name>.rs`
taking a typed params struct, and `src/bin/<name>.rs` becomes a thin `clap`
wrapper over that same function. The worker calls it in-process.

This is worth the refactor because it gives us, for free:

- One definition of the parameters — the serde struct is what the API validates,
  what the UI renders a form from, and what the CLI parses into. No drift.
- Structured progress and log events instead of scraped stdout, and `tracing`
  spans that already flow to Tempo/Loki through the existing subscriber.
- Real cancellation, via a cancellation token checked in the batch loop, rather
  than signalling a child process.
- No argv construction from user input at all.

The existing binaries are already close to this shape — most of the logic in
`reprocess_crossmatch.rs` and `migrate_snr.rs` is free functions over a
`Database` — so the port is mostly moving code and defining a params struct.

**`Command`** (escape hatch). Runs an allowlisted binary with argv *constructed
by the registry* from typed params — never a string from the client. This exists
so we can put boom-catalogs' ingestion and the Python downloaders behind the UI
before porting them, and so we aren't blocked on the migration described in
[boom-catalogs](#absorbing-boom-catalogs). Stdout/stderr are captured line-wise
into the run's logs.

### Task registry sketch

```rust
pub struct TaskSpec {
    /// Stable identifier, e.g. "reprocess_crossmatch". Never changes.
    pub id: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    /// JSON Schema for the params, derived from the params struct.
    /// The admin UI renders its form from this.
    pub params_schema: fn() -> serde_json::Value,
    /// Running it twice with the same params leaves the same state.
    pub idempotent: bool,
    /// Requires the admin to type the target name to confirm (e.g. anything
    /// that drops a collection).
    pub destructive: bool,
    /// Whether the task supports `dry_run`.
    pub supports_dry_run: bool,
    /// Which subtrees of config.yaml change this task's behavior, so we can
    /// fingerprint them into the run record.
    pub config_deps: &'static [&'static str],
    pub kind: TaskKind,
}

pub enum TaskKind {
    Native(/* boxed constructor for the task impl */),
    Command { binary: &'static str, argv: fn(&serde_json::Value) -> Vec<String> },
}

#[async_trait]
pub trait Task {
    /// Deserialized, validated params.
    type Params: DeserializeOwned + Serialize + JsonSchema;

    /// Locks this run needs, derived from params, e.g.
    /// ["collection:ZTF_alerts_aux"]. Held for the duration.
    fn locks(params: &Self::Params) -> Vec<String>;

    /// Targets this run may mutate, recorded in the ledger up front.
    fn targets(params: &Self::Params) -> Vec<MutationTarget>;

    async fn run(&self, ctx: &TaskContext, params: Self::Params)
        -> Result<Vec<MutationRecord>, TaskError>;
}
```

`TaskContext` carries the database handle, the config, the run ID, a
cancellation token, a `progress(done, total)` sink, and a `log(level, msg)`
sink, plus the `dry_run` flag.

## Data model

Three new collections.

### `task_runs`

```jsonc
{
  "_id": "9f1c…",              // uuid
  "task_type": "reprocess_crossmatch",
  "params": { "survey": "ztf", "catalogs": ["Gaia_DR3"], "batch_size": 5000 },
  "dry_run": false,
  "status": "running",         // queued | running | succeeded | failed | cancelled
  "requested_by": { "user_id": "…", "username": "pete" },
  "requested_at": 1765000000,
  "started_at": 1765000012,
  "finished_at": null,
  "progress": { "done": 412000, "total": 9100000, "message": "batch 83/1820" },
  "locks": ["collection:ZTF_alerts_aux"],
  "worker": { "instance_id": "…", "hostname": "task-worker" },
  "lease_expires_at": 1765000072,   // renewed by heartbeat; reaper uses this
  "code_version": {
    "git_sha": "cc1d42a…",
    "tag": "v0.4.1",
    "image_digest": "sha256:…"
  },
  "config_fingerprint": {
    "sha256": "…",                  // hash of the config subtrees in config_deps
    "subtrees": { "crossmatch.ztf": { /* snapshot */ } }
  },
  "error": null,
  "cancel_requested": false
}
```

Indexes: `{status: 1, requested_at: 1}` for the claim query,
`{requested_at: -1}` for the UI list, and a partial unique index on
`{locks: 1}` where `status: "running"` to enforce single-flight.

### `task_logs`

One document per chunk of lines, not one per line, to keep write volume sane:

```jsonc
{
  "run_id": "9f1c…",
  "seq": 42,
  "ts": 1765000031,
  "lines": [{ "level": "info", "ts": 1765000030, "msg": "…" }]
}
```

Index on `{run_id: 1, seq: 1}`; the UI tails by polling for `seq > last_seen`.
A TTL index expires logs after some retention window (90 days?), and each run
has a hard byte cap after which lines are dropped with a marker — a runaway
job must not fill the disk. The full firehose still goes to Loki via the normal
container-log path; `task_logs` is the convenience copy the UI reads.

### `data_mutations` — the ledger

Append-only. Never updated after the run finishes, never deleted.

```jsonc
{
  "_id": "…",
  "run_id": "9f1c…",
  "task_type": "reprocess_crossmatch",
  "target": { "db": "boom", "collection": "ZTF_alerts_aux", "survey": "ztf",
              "catalog": "Gaia_DR3" },
  "operation": "backfill",     // ingest | backfill | recompute | delete | index | drop
  "scope": {
    "description": "aux records missing cross_matches.Gaia_DR3",
    "filter": { "cross_matches.Gaia_DR3": { "$exists": false } }
  },
  "counts": { "matched": 9100000, "modified": 9099873, "inserted": 0, "deleted": 0 },
  "started_at": 1765000012,
  "finished_at": 1765021400,
  "idempotent": true,
  "reversible": false,
  "code_version": { "git_sha": "cc1d42a…", "tag": "v0.4.1" },
  "config_fingerprint": { "sha256": "…", "subtrees": { "crossmatch.ztf": {} } },
  "params": { /* the run params, denormalized so the ledger stands alone */ },
  "status": "succeeded"        // failed/partial runs are recorded too — a
                               // half-applied mutation is exactly the thing
                               // you need a record of
}
```

Indexes: `{"target.collection": 1, "finished_at": -1}` and `{run_id: 1}`.

The ledger is denormalized on purpose. It has to stay meaningful years later,
after the task type has been renamed, the config has changed, and the run
document has been pruned.

## Provenance

The ledger is the point of the whole exercise, so it deserves its own
requirements rather than being a side effect of the job runner.

**Every mutation is recorded, including failures.** A run that dies halfway
through leaves the data in a state nobody planned; that is precisely when the
record matters most. Records are written with `status: "running"` when the run
claims its targets and closed out at the end, so a crash leaves a visible
partial record rather than nothing.

**Code version is captured, not assumed.** The git SHA and image digest are
baked into the image at build time (via build args in `Dockerfile`, populated by
the deploy workflow) and read from the environment at runtime. "Which commit
produced this data" must not depend on anyone remembering what was deployed.

**Config is part of the input.** Crossmatch radii, projections, and enrichment
model paths live in `config.yaml`, so the same task with the same params against
a different config produces different data. Each task type declares which config
subtrees it depends on; the worker snapshots and hashes those into both the run
and the mutation records.

**Live ingestion is part of the state too.** The out-of-band mutations are only
half the story — the rest is which version of the pipeline wrote the raw
records. Each scheduler writes a lightweight `pipeline_version` record on
startup (`{survey, git_sha, tag, config_fingerprint, active_from}`), so the
ledger query for a collection returns a complete timeline: which pipeline
versions were writing to it when, interleaved with every out-of-band mutation.

**The state of a collection is a query, and it's exposed.**
`GET /data/state?collection=ZTF_alerts_aux` returns the ordered timeline. A
paper or data release can cite a *provenance snapshot* — a stable hash over the
set of mutation IDs applied to the collections an artifact depends on — so
"reproduce figure 3" becomes a well-defined request.

**Idempotence over ordering.** Per the discussion on #471, tasks are expected to
be idempotent and independent rather than an ordered migration chain à la
Alembic. Each task type asserts `idempotent`, and the assertion is testable: run
it twice in the integration test and assert the second run's `modified` count is
zero (or that the resulting documents are unchanged). The ledger still records
the actual order things happened in, which is what you need for forensics, but
correctness must not depend on it.

### Reversibility

The issue floats per-task `undo` operations, Alembic-style. Deliberately out of
scope for the first version: for the mutations we actually run, the inverse
either doesn't exist (a recompute overwrites the prior value, which was never
stored) or is a restore-from-backup, which is a different tool. Task types
declare `reversible: false`, and the honest recovery story for a bad mutation is
"fix the code, re-run the idempotent task" or "restore from backup and replay."
If a specific task type gets a genuine inverse later, the ledger already has the
parameters needed to construct it.

## API

New routes on the main API, all admin-only.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/tasks/types` | The registry: id, title, description, JSON Schema, flags. Drives the UI form. |
| `POST` | `/tasks` | Submit a run. Validates params against the schema, checks lock availability, writes `queued`. |
| `GET` | `/tasks` | List runs, filterable by status/type/user, paginated. |
| `GET` | `/tasks/{id}` | One run, with progress. |
| `GET` | `/tasks/{id}/logs?since_seq=` | Tail logs. |
| `POST` | `/tasks/{id}/cancel` | Set `cancel_requested`; the worker's token observes it. |
| `GET` | `/data/state` | Ledger timeline, filterable by collection/survey/catalog. |
| `GET` | `/data/state/snapshot` | Citable provenance hash for a set of collections. |

`POST /tasks` on a `destructive` type requires a `confirm` field echoing the
target name, mirroring the "type the repo name to delete" pattern. The
confirmation is stored on the run.

### Authorization

`is_admin` already exists on the main API's `User`
(`src/api/routes/users.rs`), and the bootstrap admin comes from
`api.auth.admin_username` in `config.yaml`, so the main API has the concept we
need. The web app, however, authenticates against the Babamul API
(`frontend/src/lib/api.ts` points at `/api/babamul`), and `BabamulUser` has no
admin flag.

The frontend's nginx proxies all of `/api/` to the API origin, so a page in the
SPA *can* call the main API's `/tasks` directly — but it would need a second
login against `/auth`, which is a poor experience and a second credential to
manage.

Recommendation: add `is_admin: bool` (defaulting to false) to `BabamulUser`,
seeded from `babamul.admin_username`, and mount the task routes under
`/babamul/admin/*` behind the existing `babamul_auth_middleware` plus an admin
check. One login, one session, one place where admin is defined for the web app.
The main-API `/tasks` routes can exist in parallel for scripted use by holders
of main-API admin accounts, sharing the same handlers.

This is the one decision here that touches an existing auth model, so it's worth
settling before the API work starts.

## Frontend

A new admin-only section, `/admin/tasks`, hidden from the nav for
non-admins and guarded by a route wrapper (in addition to server-side authz —
the route guard is UX, not security).

- **Run list.** Table of recent runs: type, status badge, who, started, duration,
  progress bar for running ones. Auto-refreshes.
- **New run.** Pick a task type, get a form generated from its JSON Schema
  (react-jsonschema-form, or a small renderer over the handful of field types we
  actually use — the schemas are shallow). Dry-run checkbox where supported.
  Destructive types show a confirmation input.
- **Run detail.** Params, code version, config fingerprint, live log tail,
  cancel button, and the mutation records it produced.
- **Data state.** Per-collection timeline of mutations and pipeline versions —
  the human-readable answer to "what has been done to this data?"

## Concurrency and locking

Each task type derives lock keys from its params, e.g.
`collection:ZTF_alerts_aux` or `catalog:Gaia_DR3`. A run cannot be claimed while
another running run holds an overlapping lock; the partial unique index on
`task_runs` makes this an atomic property of the claim, not an application-level
check with a race in it.

Submissions that conflict are accepted and stay `queued` (with the UI showing
what they're waiting on) rather than being rejected — the common case is an
admin queueing the next backfill while one is running.

Leases: the worker renews `lease_expires_at` on a heartbeat. A reaper (in the
worker, or the API) marks runs whose lease has expired as `failed` with a
"worker lost" error, and requeues them only if the task type is `idempotent`.

## Absorbing boom-catalogs

Catalog ingestion is the main thing people are SSH-ing to do, and #471's first
comment explicitly wants it inside BOOM. The end state is that the ingestion
code lives here — either merged into `src/` or vendored as a workspace member —
so it shares the `TaskSpec`/`Task` machinery, the config, the observability
stack, and the ledger.

The Python `downloaders/` are a separate question. They fetch large files over
HTTP with retries and parallelism, which is a handful of lines with `reqwest`
(already a dependency), so porting the download step to a native task is likely
easier than standing up a Python worker. The `minifiers/` are pandas-shaped and
harder; if we find we need Python for real, a Python worker consuming the same
Mongo queue is a clean addition — but we shouldn't build it speculatively.

Interim: run the boom-catalogs binaries via the `Command` kind, with the
binaries added to the image. That unblocks the UI without waiting on the port.

## Implementation phases

Each phase is meant to be a shippable PR or small stack of them.

**1. Skeleton and one real task.**
`task_runs` + `task_logs` collections, the `TaskSpec`/`Task` traits and
registry, the `task-worker` service in compose, claim/lease/heartbeat, and the
API routes for submit/list/get/logs/cancel. Port exactly one task —
`migrate_snr` is a good first choice: self-contained, idempotent, already
batched — and prove the whole path end to end.

**2. Provenance.**
`data_mutations`, code-version and config-fingerprint capture (including the
`Dockerfile` build args and deploy workflow changes), the `pipeline_version`
record on scheduler startup, and `GET /data/state`. Backfill a ledger entry by
hand for anything we already know was run, so the timeline doesn't start empty
and misleading.

**3. Admin UI.**
The `is_admin` decision on `BabamulUser`, the route guard, run list, schema-driven
submit form, run detail with log tail. This is the phase that actually removes
the need to SSH.

**4. Port the remaining boom tasks.**
`reprocess_crossmatch`, `migrate_fp_flux`, `enrich_reprocess`, `copy_cutouts`,
`add_filter`. Each becomes `src/tasks/<name>.rs` plus a thin CLI wrapper, with a
dry-run mode and an idempotence test.

**5. Catalog ingestion.**
boom-catalogs behind the `Command` kind first, then absorbed into the registry
as native tasks. Catalog download as a native task writing to the shared data
volume.

**6. Polish.**
Provenance snapshot endpoint, Grafana panel and alert for failed/stuck runs
(the metrics plumbing already exists), log retention tuning, and the data-state
view in the UI.

## Alternatives considered

**Have the API spawn the job itself.** Simplest possible thing, and wrong: an
API restart or deploy kills a multi-hour backfill, a memory-hungry ingest
competes with request serving, and the API container's resource limits are sized
for request serving.

**Celery / rusty-celery** (floated on the issue). It brings a broker protocol, a
result backend, and a dependency on a project whose Rust client is much less
maintained than its Python one — in exchange for features (routing, chords,
retries with backoff, beat scheduling) we don't need for a few admin jobs a
week. The Mongo-claim approach is maybe 200 lines and leaves the run record as
the single source of truth. If we later want Python workers for ML, they can
consume the same Mongo queue, which is the actual motivation behind the Celery
suggestion.

**One-off containers via the Docker socket.** Real isolation and per-task
resource limits, and there's already a `docker-socket-proxy` in the stack. But
handing a service the ability to start containers is a serious privilege
escalation surface, and log/status plumbing gets harder, not easier. Worth
revisiting if a task ever needs a genuinely different environment (a GPU, a
different base image); not worth it for the current set.

**Kubernetes Jobs.** We're a single-node Docker Compose deployment. No.

**Ledger in a separate store (Postgres, a file, git).** An append-only ledger in
a document DB relies on discipline rather than enforcement, and a separate store
would give stronger guarantees. But it adds a database to the stack for one
collection, and the mutations that matter are written by our own worker, which
is the same trust boundary either way. A periodic export of the ledger to a
signed/append-only location is a cheap hedge if we want one later.

## Open questions

- **Admin identity** — `is_admin` on `BabamulUser` versus a second login against
  the main API. Recommendation above, but it needs a decision before phase 3.
- **Log retention** — how long to keep `task_logs`, and whether the UI should
  read from Loki instead of a Mongo copy. Mongo is proposed for simplicity;
  Loki is where the data already goes.
- **Where large input files come from.** Downloads land on a bind-mounted volume
  today. Do admins ever need to *upload* a catalog through the UI, or is
  "download from a URL" always sufficient? Assuming the latter for now.
- **Ledger granularity for the live pipeline.** One record per scheduler start
  is cheap and probably enough. If deploys become frequent this gets noisy and
  we'd want to collapse consecutive identical versions.
- **Whether to keep the CLI wrappers.** Keeping `src/bin/*` as thin wrappers
  preserves local development and emergency access, but an emergency `docker
  exec` bypasses the ledger. Proposal: keep them, and have the shared task
  function write the mutation record regardless of who invoked it — so even a
  hand-run binary lands in the ledger, flagged as `cli` rather than `ui`.
