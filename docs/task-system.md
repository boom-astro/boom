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

## Two kinds of mutation

Before designing anything, it's worth separating two things that both look like
"changing the data" and want opposite mechanisms.

**Analytical mutations** act on the science data — alert collections, aux
collections, catalogs, cutouts. They are human-initiated, long-running (hours to
days), parameterized, and should be *idempotent and independent* rather than
ordered. Re-running one is a normal thing to do. These are what #471 is about,
and they are what the rest of this document calls **tasks**.

**Operational mutations** act on the application's own bookkeeping —
`users`, `babamul_users`, `filters`, `task_runs`. Adding `is_admin` to
`babamul_users`, dropping a key that's no longer read, renaming a field. These
are release-coupled, fast, bounded, and must run **exactly once, in order, at
startup**, with no human in the loop. That is Alembic-shaped, and it is the
precise opposite of the task model.

They should not share a mechanism. Forcing them into one abstraction produces
something wrong for both: startup migrations don't want a UI, an approval, or a
parameter form, and tasks don't want a version pointer or a strict ordering.

They *must* share the provenance ledger, though. Both change data that
scientific artifacts and user-facing behavior depend on, and "what has been done
to this database" has to have one answer, not two.

Conveniently, the line between them already exists in the code:
`PROTECTED_COLLECTION_NAMES` in `src/api/db.rs` names exactly the operational
collections, specifically to keep them distinct from analytical data catalogs.
That constant becomes the enforced boundary — see
[Operational schema migrations](#operational-schema-migrations).

## Goals

- Admins launch a declared set of jobs from the web app; the API records who,
  what, when, with which parameters, and against which code and config.
- Jobs run asynchronously in a dedicated service, survive an API restart, and
  report status and logs back to the UI while running.
- Every mutation of data — task, startup migration, or live pipeline — appends
  to an **append-only ledger**, so the state of any collection can be
  reconstructed as `raw ingest + ordered list of mutations`.
- Ingesting or updating a catalog is a one-click operation naming only the
  catalog ("NED"); how to do it is declared in this repo and shipped with the
  release, and the deployment converges its data to match.
- Dangerous jobs support a dry run that reports what *would* change.
- No SSH required for routine data operations.

## Non-goals

- Not a workflow engine. Tasks do not form a DAG and do not have ordering
  dependencies (per the discussion on #471, they should each be idempotent
  instead). If we later need chaining, it can be layered on.
- Not a replacement for the alert-processing scheduler. That is a separate,
  always-on pipeline; the task system is for out-of-band work.
- No automatic rollback. See [Reversibility](#reversibility).
- **Arbitrary code execution is not part of the default capability.** Running
  code from an arbitrary git rev is a real requirement for the long tail (see
  [External code tasks](#external-code-tasks)), but it is a separately-granted
  capability with its own controls, added late, and explicitly *not* how the
  common cases are meant to be served.

## Concepts

**Task type** — a declared kind of work, e.g. `reprocess_crossmatch` or
`ingest_catalog`. Identified by a stable string, carries a typed parameter
schema, a human description, a declaration of what it mutates, and flags for
idempotence and destructiveness. Lives in code, versioned with the repo.

**Task run** — one execution of a task type with concrete parameters. Has a
lifecycle (`queued → running → succeeded | failed | cancelled`), an actor, a
timeline, captured logs, and the code/config fingerprint it ran under.

**Mutation record** — an entry in the append-only ledger describing a change
applied to one target. Produced by a task run, a startup migration, or a
pipeline version. This is the provenance primitive, and the one thing every
mechanism in this document shares.

**Catalog definition** — an in-repo, versioned declaration of how a given
astronomical catalog is sourced, transformed, and indexed. Shipped with the
release; the deployment converges its collections to match. See
[Catalogs](#catalogs-declared-definitions-and-convergence).

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
queue and the run record. A poll interval of a couple of seconds is
imperceptible for a job that runs for hours.

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

### How a task executes

Two execution kinds, both driven from the same registry. (A third,
`External`, is described in [External code tasks](#external-code-tasks) and is
deliberately not part of the initial build.)

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

**`Command`** (escape hatch). Runs an allowlisted binary *already present in the
image* with argv constructed by the registry from typed params — never a string
from the client. This exists so we can put boom-catalogs' ingestion behind the
UI before porting it. Stdout/stderr are captured line-wise into the run's logs.

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
    /// Capability required to submit this type.
    pub required_role: Role,
    /// Running it twice with the same params leaves the same state.
    pub idempotent: bool,
    /// Requires the actor to type the target name to confirm (e.g. anything
    /// that drops a collection).
    pub destructive: bool,
    pub supports_dry_run: bool,
    /// Which subtrees of config.yaml change this task's behavior, so we can
    /// fingerprint them into the run record.
    pub config_deps: &'static [&'static str],
    pub kind: TaskKind,
}

pub enum TaskKind {
    Native(/* boxed constructor for the task impl */),
    Command { binary: &'static str, argv: fn(&serde_json::Value) -> Vec<String> },
    External,  // see "External code tasks" — phase 8, gated
}

#[async_trait]
pub trait Task {
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

Three new collections (plus `schema_migrations`, below).

### `task_runs`

```jsonc
{
  "_id": "9f1c…",              // uuid
  "task_type": "reprocess_crossmatch",
  "params": { "survey": "ztf", "catalogs": ["Gaia_DR3"], "batch_size": 5000 },
  "dry_run": false,
  "status": "running",         // queued | running | succeeded | failed | cancelled
  "actor": { "realm": "babamul", "user_id": "…", "username": "pete" },
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
{ "run_id": "9f1c…", "seq": 42, "ts": 1765000031,
  "lines": [{ "level": "info", "ts": 1765000030, "msg": "…" }] }
```

Index on `{run_id: 1, seq: 1}`; the UI tails by polling for `seq > last_seen`.
A TTL index expires logs after some retention window, and each run has a hard
byte cap after which lines are dropped with a marker — a runaway job must not
fill the disk. The full firehose still goes to Loki via the normal container-log
path; `task_logs` is the convenience copy the UI reads.

### `task_partitions`

Units of restartable work for long tasks, described in
[Restartability](#restartability-partitioned-work). Written at plan time,
claimed with the same atomic `findOneAndUpdate` pattern as runs:

```jsonc
{ "run_id": "9f1c…", "partition": 412,
  "range": { "field": "candid", "from": …, "to": … },
  "status": "done", "counts": { "matched": 98211, "modified": 98211 },
  "attempts": 1, "lease_expires_at": null }
```

Indexes: `{run_id: 1, status: 1}` for claiming and for the `done / total`
progress count, and `{run_id: 1, partition: 1}` unique. Expired with the run.

### `data_mutations` — the ledger

Append-only. Never updated after the producing operation finishes, never
deleted. Written by tasks, startup migrations, and pipeline startups alike.

```jsonc
{
  "_id": "…",
  "source": {                  // what produced this mutation
    "kind": "task",            // task | migration | pipeline
    "run_id": "9f1c…",         // task run id, migration version, or scheduler instance
    "task_type": "reprocess_crossmatch"
  },
  "actor": { "realm": "babamul", "user_id": "…", "username": "pete" },
                               // realm: babamul | boom | cli | system
  "trigger": "ui",             // ui | api | cli | startup | deploy
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
  "params": { /* denormalized so the ledger stands alone */ },
  "status": "succeeded"        // failed/partial runs are recorded too — a
                               // half-applied mutation is exactly the thing
                               // you need a record of
}
```

Indexes: `{"target.collection": 1, "finished_at": -1}` and
`{"source.run_id": 1}`.

The ledger is denormalized on purpose. It has to stay meaningful years later,
after the task type has been renamed, the config has changed, and the run
document has been pruned.

The `actor.realm` field is what lets a single ledger span two identity systems
without waiting for them to merge — see
[Identity and roles](#identity-and-roles).

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
records. Each scheduler writes a `pipeline` mutation record on startup
(`{survey, git_sha, tag, config_fingerprint, active_from}`), so the ledger query
for a collection returns a complete timeline: which pipeline versions were
writing to it when, interleaved with every out-of-band mutation.

**The state of a collection is a query, and it's exposed.**
`GET /data/state?collection=ZTF_alerts_aux` returns the ordered timeline. A
paper or data release can cite a *provenance snapshot* — a stable hash over the
set of mutation IDs applied to the collections an artifact depends on — so
"reproduce figure 3" becomes a well-defined request.

**Idempotence over ordering.** Per the discussion on #471, tasks are expected to
be idempotent and independent rather than an ordered migration chain. Each task
type asserts `idempotent`, and the assertion is testable: run it twice in the
integration test and assert the second run's `modified` count is zero. The
ledger still records the actual order things happened in, which is what you need
for forensics, but correctness must not depend on it. (Startup migrations are
the explicit exception, and they carry a version pointer instead.)

### Reversibility

The issue floats per-task `undo` operations, Alembic-style. Deliberately out of
scope: for the mutations we actually run, the inverse either doesn't exist (a
recompute overwrites the prior value, which was never stored) or is a
restore-from-backup, which is a different tool. Task types declare
`reversible: false`, and the honest recovery story for a bad mutation is "fix
the code, re-run the idempotent task" or "restore from backup and replay." If a
specific task type gets a genuine inverse later, the ledger already has the
parameters needed to construct it.

## Identity and roles

BOOM is one application serving two populations, and the auth model reflects
that:

- **boom API** (`src/api/routes/`) — SkyPortal-side users. `User` has
  `is_admin` and `watchlist_access`; the bootstrap admin is created at startup
  by `init_api_admin_user` from `api.auth.admin_username`.
- **babamul API** (`src/api/routes/babamul/`) — the public querying and Kafka
  stream users. `BabamulUser` has activation state, personal access tokens, and
  Kafka credentials, and **no admin concept at all**. JWTs are distinguished by
  a `babamul:` prefix on the subject claim, and the two middlewares actively
  refuse each other's tokens.

The web app (`frontend/src/lib/api.ts`) authenticates against
`/api/babamul`. Its nginx proxies all of `/api/` to the API origin, so a page in
the SPA *could* call the boom API's routes directly — but it would need a second
login and a second credential to manage, which is a bad experience for what is,
to the user, one application.

### Recommendation: don't merge the logins yet; unify roles and the actor now

A full merge — one user collection, one middleware, one token format — is the
right end state, but it is its own project (two document shapes, two token
conventions, two OpenAPI surfaces, an activation flow on one side only, a
migration of live credentials) and the task system should not be blocked behind
it. Three steps instead, in order of urgency:

1. **Unify the actor in the ledger, immediately.** `actor.realm` distinguishes
   `babamul` / `boom` / `cli` / `system`. This costs nothing and it is the part
   that actually matters for your provenance concern: both populations draw from
   the same data, so "who changed this" must have one answer even while "where do
   they log in" has two. Do this in phase 2 regardless of what happens to auth.

2. **Introduce roles instead of a second boolean.** The immediate need is
   "certain babamul users can be admins," and the reflex is to add
   `is_admin: bool` to `BabamulUser`. Resist it: the moment there are two admin
   booleans in two collections, they drift, and this system needs finer grain
   than one bit anyway — running a backfill, approving someone else's run, and
   running arbitrary external code are three different levels of trust held by
   different people. Add instead:

   ```rust
   #[serde(default)]
   pub roles: Vec<Role>,   // task:run, task:approve, task:run_external, user:admin, …
   ```

   to *both* user types, with `is_admin` on `User` mapping to the full set for
   backward compatibility. `#[serde(default)]` means existing documents need no
   migration (see [the note below](#most-schema-changes-arent-migrations)).
   Seed babamul admins from a `babamul.admin_username` config key, mirroring
   what `init_api_admin_user` already does.

3. **Revisit the merge later, as its own project.** Step 2 is not throwaway work
   if we do: converging both user types on a shared role vocabulary and a shared
   authorization check is the first step of the merge, and it makes the eventual
   collapse mostly mechanical.

The task routes then mount at `/babamul/admin/*` behind the existing
`babamul_auth_middleware` plus a role check, and in parallel on the boom API
behind `auth_middleware` plus the same check, sharing handlers. One set of
handlers, two doors, one ledger.

## API

All routes require a role; the specific role varies by task type.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/tasks/types` | The registry: id, title, description, JSON Schema, flags. Drives the UI form. |
| `POST` | `/tasks` | Submit a run. Validates params against the schema, checks role + lock availability, writes `queued`. |
| `GET` | `/tasks` | List runs, filterable by status/type/actor, paginated. |
| `GET` | `/tasks/{id}` | One run, with progress. |
| `GET` | `/tasks/{id}/logs?since_seq=` | Tail logs. |
| `POST` | `/tasks/{id}/cancel` | Set `cancel_requested`; the worker's token observes it. |
| `POST` | `/tasks/{id}/approve` | Second-person approval, for types that require it. |
| `GET` | `/catalogs/definitions` | The catalog definitions the running release declares. |
| `GET` | `/catalogs/drift` | Three-way drift: config inventory vs release definitions vs actual state, including undeclared collections. |
| `POST` | `/catalogs/{id}/plan` | What `ensure_catalog` would do, without doing it. |
| `GET` | `/data/state` | Ledger timeline, filterable by collection/survey/catalog. |
| `GET` | `/data/state/snapshot` | Citable provenance hash for a set of collections. |

`POST /tasks` on a `destructive` type requires a `confirm` field echoing the
target name, mirroring the "type the repo name to delete" pattern. The
confirmation is stored on the run.

## Frontend

A new admin-only section, `/admin`, hidden from the nav for users without the
role and guarded by a route wrapper (in addition to server-side authz — the
route guard is UX, not security).

- **Run list.** Table of recent runs: type, status badge, actor, started,
  duration, progress bar for running ones. Auto-refreshes.
- **New run.** Pick a task type, get a form generated from its JSON Schema
  (react-jsonschema-form, or a small renderer over the handful of field types we
  actually use — the schemas are shallow). Dry-run checkbox where supported.
  Destructive types show a confirmation input.
- **Run detail.** Params, code version, config fingerprint, live log tail,
  cancel button, and the mutation records it produced.
- **Catalogs.** The drift table: every catalog the config inventory declares,
  its actual state (absent, current, stale definition, new source release
  available), plus any undeclared collections, and a plan/apply button per row.
  This is probably the single most useful screen in the admin UI — it's the
  answer to "is my data what this deployment says it should be?" at a glance.
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

Leases: the worker renews `lease_expires_at` on a heartbeat. A reaper marks runs
whose lease has expired as `failed` with a "worker lost" error, and requeues
them only if the task type is `idempotent`.

## Restartability: partitioned work

The jobs that matter most here run for days. They *will* be interrupted — by a
reboot, and more routinely by every deploy, since `docker compose up -d`
recreates containers and sends `SIGTERM`. A backfill that has to start over
because we shipped a frontend change is not viable, so restartability is a core
property of the task system rather than a per-task concern.

### Why `enrich_reprocess` uses a queue, and what's wrong with it

The current design — push candids into a Redis list, have workers consume it —
isn't arbitrary. The enrichment workers already consume from Redis in the live
pipeline, so reprocessing reuses the existing worker architecture instead of
growing a second one. And pop-and-process does give you interruption tolerance:
whatever is left in the list survives a restart.

Two things are wrong with it, and neither is the queue itself:

- **The work set is materialized up front.** Enqueueing every candid that needs
  reprocessing means billions of entries in an in-memory store. At ~50 bytes per
  Redis list entry that's tens of gigabytes of RAM to describe work whose
  definition is a one-line query. Valkey is sharing a box with two MongoDB
  instances and the alert pipeline; this isn't a tuning problem, it just doesn't
  fit.
- **Populating it is a separate manual step.** The binary is only the worker
  half. Working out what needs reprocessing and getting it into the queue is
  left to a human at a shell, which is most of why this operation requires SSH
  today. That step disappears into the admin page: the queueing is something the
  task does when an admin submits it, not something an admin does before running
  the task.

### Partition the work, not the items

Rather than enumerating documents, enumerate **ranges** of them. At plan time
the task divides its target into partitions over a stable indexed key — candid
or jd ranges — and records those:

```jsonc
{ "run_id": "9f1c…", "partition": 412,
  "range": { "field": "candid", "from": 2……, "to": 2……9 },
  "status": "done",             // pending | running | done | failed
  "counts": { "matched": 98211, "modified": 98211 },
  "attempts": 1, "lease_expires_at": null }
```

For a billion alerts in partitions of ~100k documents, that's ten thousand small
documents instead of a billion queue entries — bounded state that lives in the
same store as the run, and is therefore durable across reboots by construction.

What this buys, beyond fitting in memory:

- **Restart is re-claiming, not re-doing.** A worker coming back after a deploy
  claims `pending` partitions and partitions whose lease expired mid-flight. At
  worst one partition's work is repeated, which is safe because tasks are
  idempotent.
- **Honest progress.** `done / total` partitions is a real percentage and a real
  ETA, which a queue depth isn't once the queue is the whole job.
- **Parallelism without coordination.** Multiple workers claim different
  partitions with the same atomic `findOneAndUpdate` used for runs.
- **Failures are localized.** One bad partition fails and retries, or is left
  `failed` with the rest of the run continuing and the count surfaced — rather
  than poisoning the whole job.
- **The work set is closed.** Partitions cover what existed at plan time. Alerts
  arriving during a multi-day run are written by the live pipeline with current
  enrichments and current stamps, so they're already correct and fall outside
  the ranges. No moving target.

### Redis holds in-flight work; Mongo holds progress

For enrichment specifically, the Redis queue stays — it's the transport to the
existing worker pool, and rebuilding that would be gratuitous. It just becomes a
**sliding window**: the task claims a partition, pushes that partition's candids
(a hundred thousand, not a billion), waits for them to drain, marks the
partition done, claims the next.

The division of responsibility is the point. Redis carries work that is in
flight; MongoDB carries what has been completed. Losing Valkey costs the
partition currently in flight, not the run — which is the property the current
design lacks, since today the queue *is* the durable record of remaining work.

### Shutting down cleanly

On `SIGTERM` the worker stops claiming new partitions and lets the current one
finish if it can within the shutdown grace period, releasing it otherwise. The
machinery for this already exists — `WorkerCmd::TERM` and `should_terminate` in
`src/utils/worker.rs` are exactly this pattern for the pipeline workers — so the
task worker should reuse it rather than invent a second convention. Anything not
released cleanly is covered by lease expiry, so an abrupt power loss degrades to
the same path, just slower.

Partition size is a tradeoff between redo cost and bookkeeping overhead, and is
better expressed as a target duration (a few minutes) than a fixed document
count, since per-document cost varies by orders of magnitude between a field
recompute and six ONNX inferences.

### This is general, not enrichment-specific

`reprocess_crossmatch`, `migrate_snr`, and `migrate_fp_flux` all have the same
shape: iterate a huge collection in batches, update in place. They're all
currently a bare loop that restarts from the beginning. Partitioning belongs to
the task framework — a helper that a task declares a key and a range against —
so every long task gets restartability, progress, and parallelism without
implementing them again.

## Catalogs: declared definitions and convergence

The set of catalogs BOOM crossmatches against — NED, Gaia, ALLWISE, Milliquas,
VSX, PS1, LS, DESI — is essentially the same on every BOOM deployment, and the
knowledge of how to ingest each one (where it lives, how its columns map, what
the units are, which quirks to work around) is developed once and shared. That
knowledge belongs in this repo, written by the people who understand it, in
PRs, with tests.

So the model is **not** "the admin authors an ingestion spec in the UI." It is:

> The repo declares a set of catalog definitions. A deployment's job is to
> **converge** its collections to the definitions the running release declares.
> The admin picks a catalog — "NED" — and the system works out what that
> requires.

That's a desired-state model rather than a run-a-script model, and framing it
that way answers the "what if the definition changes?" question structurally
instead of case by case. Convergence is not a special event; it is the only
operation. Ingesting a catalog for the first time is just convergence from
"absent."

> **This supersedes the user-authored-spec design in the previous revision of
> this document.** In-repo definitions are better on every axis that matters
> here — see [Why in-repo](#why-definitions-belong-in-the-repo).

### The deployment declares an inventory

If the repo says *how* to build each catalog, the only deployment-specific
decision left is *which* ones this instance should hold. That's a list of names,
and it belongs in `config.yaml` alongside everything else that describes what
this deployment is.

That splits the problem three ways:

| Where | What it declares | Changes via |
| --- | --- | --- |
| `catalogs/<id>.yaml` in this repo | **how** to build each catalog | PR + release |
| `catalogs:` in `config.yaml` | **which** ones this deployment holds | PR to the config |
| MongoDB | what's **actually** there | `ensure_catalog` |

Drift is then a three-way comparison, not a two-way one, and every one of the
three is version-controlled: prod config already lives in the repo at
`config/prod/<deployment>/config.yaml` and is synced by a workflow, so the
inventory gets the same review as the definitions do.

**`config.yaml` already half-does this.** `crossmatch.<survey>` is, in effect, a
catalog list today — it names every catalog the pipeline expects to match
against. It's just aspirational: nothing checks that a named catalog was ever
ingested. The evidence that this is a real gap is already in the tree —
`warn_if_missing_crossmatches` in `src/bin/scheduler.rs` samples a single random
aux record at startup and warns if a configured catalog has no crossmatches on
it, which is a hand-rolled, probabilistic drift detector for exactly this
problem. An explicit inventory plus a real drift check replaces that with an
exact answer.

A minimal shape, with per-deployment overrides optional:

```yaml
catalogs:
  - id: ned                          # usually all you need
  - id: gaia_dr3
    source_release: "2023-12"        # pin; default is the definition's release
  - id: allwise
    source_dir: /mnt/local/allwise   # files already staged; skip the download
```

Keep it a separate top-level key rather than deriving the inventory from the
union of `crossmatch.*`: catalogs can be held for direct querying without being
crossmatch targets (`get_catalogs` already serves those), so the crossmatch
config is a subset, not the whole. What it *should* get is a startup validation
that every catalog named under `crossmatch.<survey>` also appears in `catalogs`
— a cheap check that converts a silent misconfiguration into a startup error.

**Declaring is not converging.** It's tempting, once config states desired state
and the system can compute drift, to reconcile automatically on deploy. Don't:
ingesting NED is hours and hundreds of GB, and making that an implicit side
effect of a `git push` would undo the attribution and approval properties this
whole system exists to provide. A typo'd config entry should not be able to
rebuild a catalog.

The rule is **detect always, act never**. On startup the API computes drift and
logs it, exports it as a gauge (so Grafana can alert when drift persists), and
surfaces it in the admin drift table. Converging stays an explicit, attributed,
human-initiated task. That keeps the existing `warn_if_missing_crossmatches`
behavior — warn loudly about the gap — while making the warning exact and the
remedy one click away.

One arguable exception: creating a *missing* index is bounded and
non-destructive, and there's precedent in `initialize_survey_indexes` doing
exactly that at scheduler startup. Auto-creating missing indexes is probably
fine; auto-*dropping* extra ones is not, since someone may have added one
deliberately for a query workload. Worth deciding explicitly rather than letting
it fall out of the implementation.

**Drift runs in both directions.** A collection that exists but isn't declared —
an orphan from an old release, a manual experiment, a renamed catalog — is
reported as undeclared and never auto-removed. Removing one is a separate,
destructive, confirmation-gated `drop_catalog` task. For the provenance story to
hold, "what is in this database and why" has to have no unexplained entries, and
that means naming the orphans rather than quietly tolerating or deleting them.

**Config declares intent; the ledger records what happened.** These are
complementary, not alternatives, and it's worth being explicit that the
inventory does not replace `data_mutations`. Config says NED should be here at
release 2024-06; the ledger says it was ingested on this date by this person
under this commit, recomputed in place three months later when the definition
changed, and swept of 12,000 retracted records. Desired state can't answer any
of the questions in [Provenance](#provenance) — it has no history, and it is
edited in place. The `catalogs` collection sits between them as the cached
*actual* state, so drift is a cheap comparison instead of a scan.

Since config subtrees are already fingerprinted into run and mutation records,
`catalogs:` composes into that for free: a run's record carries the inventory it
was executed against.

### What a definition contains

Definitions live in `catalogs/<id>.yaml` in this repo and ship inside the image.
The declarative form covers the common case; a definition may additionally name
a Rust transform hook for anything that can't be expressed declaratively.

```yaml
id: ned
version: 4                     # bumped when output changes; hash-checked (below)
collection: NED
title: NASA/IPAC Extragalactic Database
source:
  release: "2024-06"           # the upstream data release, distinct from `version`
  urls: [ "https://…/ned_2024-06.parquet.tar.gz" ]
  format: parquet
id_from: [ prefname ]          # REQUIRED — see below
fields:
  - { from: ra,       to: ra,   type: f64 }
  - { from: dec,      to: dec,  type: f64 }
  - { from: z,        to: redshift, type: f64, null_if: NaN }
  - { from: objtype,  to: type, type: string }
coordinates: { ra: ra, dec: dec, add_healpix: true }
indexes:
  - { keys: { "coordinates.radec_geojson": "2dsphere" } }
transform: null                # or a named Rust hook, e.g. `ned::normalize_z`
convergence_from:              # how to get here from each earlier version
  3: recompute_in_place
  2: reingest_from_source
```

`version` is bumped by hand, but a content hash of the definition is computed at
build time and stored alongside it — so a changed definition with a forgotten
version bump is caught by a test, not discovered in production six months later.

### The linchpin: deterministic document IDs

`id_from` is required, and it is what makes any of this work. If a catalog's
`_id` is auto-generated, "re-run the ingest" is a duplicate factory rather than
an idempotent operation, and every convergence strategy below collapses to
"drop and rebuild." With `_id` derived deterministically from stable source
fields, re-ingestion is an upsert and is idempotent by construction.

This is cheap to require now and effectively impossible to retrofit later —
once a collection exists with synthetic IDs, there is no way to match new source
records to existing documents. It should be a hard validation error for a
definition to omit it.

The matching requirement for **deletions**: a source release that drops records
leaves orphans behind, because upserts never delete. So each convergence run
stamps a generation marker on every document it writes and sweeps anything left
carrying an older marker. Mark-and-sweep, and without it "idempotent reingest"
quietly accumulates records that upstream has retracted — which for a
crossmatch catalog means alerts matching against objects that no longer exist.

### One task: `ensure_catalog`

A single compiled-in task type, parameterized by catalog ID, with a `plan` mode
and an `apply` mode. Not a new task type per catalog, and not a new task type
per definition change — the registry has to stay comprehensible, and the
definition version is data, not a type.

`plan` compares declared state against actual state and reports what apply would
do:

| Actual | Declared | Plan |
| --- | --- | --- |
| absent | v4 | download, ingest, build indexes |
| v4, release 2024-06 | v4, release 2024-06 | no-op |
| v4, indexes differ | v4 | create/drop indexes only |
| v3 | v4 (`recompute_in_place`) | update every document from stored fields |
| v2 | v4 (`reingest_from_source`) | re-read source files on disk, upsert, sweep |
| v4, release 2024-06 | v4, release 2025-01 | download new release, upsert, sweep |

Note that the naming matters: "reingest" is what the user asks for, but it
describes what the system usually *won't* do. `ensure_catalog` says what's
actually being requested, and `plan` makes the difference visible before
anything runs.

Much of the comparison is already available: `get_catalog_indexes` in
`src/api/routes/catalogs.rs` introspects actual indexes today, and
`catalog_exists` and `get_catalog_sample` cover the rest of the "what's actually
there" side. The declared side is a file in the image. The missing piece is a
small `catalogs` collection recording, per ingested catalog:

```jsonc
{ "_id": "NED", "definition_id": "ned", "definition_version": 4,
  "definition_hash": "…", "source_release": "2024-06", "generation": 7,
  "document_count": 12400000, "ingested_at": …, "code_version": { … } }
```

### Convergence strategies are declared, not inferred

When a definition changes, *something* has to know whether the new output is
derivable from what's already stored or requires going back to source. Three
ways to decide:

- **Infer it by diffing the definitions.** Attractive and brittle: as soon as a
  transform hook is involved, the diff is meaningless, and a wrong inference
  silently produces wrong data.
- **Always re-ingest from source.** Simple and always correct, but a full Gaia
  re-ingest is days of work and hundreds of GB of download for what might be a
  changed unit conversion.
- **The dev who changes the definition declares it.** Explicit, reviewable in
  the same PR that makes the change, and the person writing it is exactly the
  person who knows whether the new field is computable from stored data.

The third, with the second as the always-available fallback. The strategies are
totally ordered by cost:

```rust
pub enum Convergence {
    /// Only indexes or metadata changed.
    IndexesOnly,
    /// New/changed fields are computable from fields already on each document.
    RecomputeInPlace,
    /// Requires re-reading source files, which are still on disk.
    ReingestFromSource,
    /// Requires re-downloading the source.
    RedownloadAndReingest,
}
```

Because they're ordered, a deployment that skipped releases — sitting on v2 when
the repo declares v4 — takes the **max** over the strategies for every version
in the gap. That case is real (not every BOOM instance updates promptly) and
this handles it without a migration chain.

`RecomputeInPlace` is not a hypothetical shape: `migrate_snr` is exactly that
task, recomputing derived values from stored inputs in batches, and it already
works. The strategy enum is mostly a way of saying which existing pattern
applies.

### Converging safely on a live system

Catalogs are read by the live crossmatch path, so convergence can't take the
collection away mid-flight:

- **Definition changes** converge **in place** — upsert plus generation sweep.
  Readers see a mix of old and new documents during the run, which is acceptable
  for the field-level changes this covers.
- **Source release changes** build into a temporary collection and **swap
  atomically** with `renameCollection`. Drop-then-rebuild would leave a window
  where crossmatches silently return nothing, which is worse than a stale
  catalog: it produces alerts that look confidently unmatched.

### Downstream staleness

Changing a catalog makes every crossmatch against it stale. `ensure_catalog`
knows which surveys reference the catalog (from `crossmatch.<survey>` in
`config.yaml`), so after a successful apply it **surfaces the affected
`reprocess_crossmatch` runs as suggested follow-ups** in the UI, pre-filled and
one click from submission.

Suggested, not automatic. Auto-chaining would make this a workflow engine, which
is an explicit non-goal, and a crossmatch reprocess over billions of alerts is
not something to kick off as a side effect of someone updating a catalog. But
leaving the user to remember it is how data quietly goes stale, so the system
should say so loudly.

### Why definitions belong in the repo

Compared to the user-authored specs proposed in the previous revision:

- **They're shared.** The same definitions are correct on every deployment, so
  authoring them per-deployment is duplicated work that will drift.
- **They get review and tests.** A wrong unit conversion in a catalog
  transformation is a science bug that propagates into every crossmatch and
  every artifact downstream. That deserves a PR, not a form.
- **Provenance gets simpler, not just better.** If definitions ship with the
  release, `code_version` in the ledger already pins the definition — there's no
  second, independently-mutable input to fingerprint. A DB-stored spec would
  have needed its own version, its own immutability rules, and its own audit
  trail.

The honest cost: this puts a deploy back on the critical path for adding a
*new* catalog, which I earlier cited as much of why people SSH. Two things make
that acceptable. Adding a catalog becomes a PR containing a YAML file — a fast,
low-risk review, and releases are already automated, so it is a very different
proposition from "SSH in and run a script." And if release cadence turns out to
be the real blocker, definitions can also be loaded from a config-mounted
directory as a deployment-local override, which is a pressure-release valve
without a code deploy. Start without it; add it if the need is demonstrated
rather than assumed.

### What's left over

The long tail a declarative definition can't express — multi-file joins, the
`minifiers/` pandas transforms, genuinely novel formats — is first the transform
hook (still a PR to this repo), and only then
[external code tasks](#external-code-tasks). This model shrinks the case for
external code considerably: "devs develop the ingestion procedures as PRs" *is*
the well-lit path, and the residual need is narrowed to "someone wants a catalog
we haven't defined and can't wait for a release."

The Python `downloaders/` fold into the definition's `source.urls`: fetching
files over HTTP with retries and parallelism is a small amount of `reqwest`
(already a dependency), so a native downloader driven by the definition is less
work than standing up a Python worker.

Interim, before any of this exists: run the boom-catalogs binaries via the
`Command` kind, with those binaries added to the image. That unblocks the UI
without waiting on the definition engine.

## Enrichments: the same pattern, applied to models

A new ML classifier — or a new version of an existing one — needs to be run
against every alert already in the database. This is the same shape as the
catalog case, and it's worth saying so explicitly rather than treating it as a
separate feature.

Crossmatch catalogs and ML models are both **ingest-time enrichments**: declared
centrally, applied by the scheduler at first insert, and therefore silently
absent or stale on every pre-existing record whenever the declaration changes.
`reprocess_crossmatch` exists precisely because "the live pipeline only
crossmatches at first insert"; `enrich_reprocess` exists for the identical
reason on the model side. Both want a declaration, a version, a drift query, and
a convergence task.

So `ensure_enrichment` is `ensure_catalog` with a different body, and everything
already described — plan/apply, drift, the ledger record, suggested follow-ups —
carries over.

### The blocking problem: scores carry no version

`ZtfAlertClassifications` (`src/enrichment/ztf.rs`) is a flat struct of six
`f32`s — `acai_h`, `acai_n`, `acai_v`, `acai_o`, `acai_b`, `btsbot` — written to
the alert document under `classifications`. **Nothing records which model
produced any of them.** The model files are hardcoded paths in
`SharedModels::load` (`data/models/acai_h.d1_dnn_20201130.onnx` and friends),
not config, so the version exists only in a filename inside the image.

This is worse than the equivalent catalog problem, because it makes drift
detection *impossible* rather than merely awkward. For a catalog you can ask
whether `cross_matches.Gaia_DR3` exists. For a model, `classifications.acai_h`
exists both before and after a model swap — the field is present, the value is
just stale, and nothing distinguishes the two. There is no query that returns
"alerts needing rescoring."

The consequences compound:

- You can't tell which alerts have been reprocessed, so a run that dies at 40%
  can't be resumed — only restarted from zero, over billions of alerts.
- You can't reproduce a filter result from six months ago, because you can't
  determine which classifier produced the score it triggered on.
- You can't compare model versions, or roll one back with any confidence.
- A reprocessing run silently overwrites the old scores with no record that the
  values changed or what they were.

Like `id_from` for catalogs, this is **cheap to add now and impossible to
retrofit**: once billions of alerts carry unversioned scores, no amount of later
work can establish which model produced them. It should land before any
reprocessing task is built, and arguably before the next classifier change
regardless of this system.

### Stamping without paying for it six times per alert

The obvious fix — a version string per score — costs real money at BOOM's
scale. Six extra fields across ~10⁹ alerts is on the order of 60 GB of pure
bookkeeping.

Instead, intern the combination. A `model_sets` collection records each distinct
set of model versions the deployment has run:

```jsonc
{ "_id": 7, "survey": "ztf",
  "models": { "acai_h": "d1_dnn_20201130", "acai_n": "d1_dnn_20201130",
              "btsbot": "v1.0.1" },
  "active_from": 1765000000, "code_version": { … } }
```

and each alert carries a single integer, `classifications_set: 7`. One int per
alert rather than six strings, and the drift query is
`{ classifications_set: { $ne: <current> } }` — indexable, exact, and cheap.

Selective reprocessing still works, and this is the point of interning rather
than using an opaque generation counter: if sets 7 and 8 differ only in
`btsbot`, then alerts at set 7 need only `btsbot` re-run, not all six models.
That comparison happens once against the registry, not per document.

### What the task actually does

`enrich_reprocess` today reads candids from a Redis queue that **you have to
populate yourself** — the binary is only the worker half, and the "work out what
needs reprocessing and enqueue it" half is manual. That's a large part of why
this operation currently requires a shell.

`ensure_enrichment` closes the loop: partition the target range, drive the Redis
queue a partition at a time (see
[Restartability](#restartability-partitioned-work)), track progress against a
known total, write the ledger record, and stamp the new set ID as it goes. The
enqueueing becomes an implementation detail of submitting the task from the
admin page rather than a step someone performs beforehand.

Two properties matter more here than anywhere else in this document, because
this is the largest job BOOM will ever run:

- **The stamp makes restart correct; partitions make it cheap.** Partitions are
  how a run resumes after a deploy without redoing finished work. The version
  stamp is what makes that safe — a partition replayed after an unclean stop is
  idempotent, and a final drift query catches anything the partitioning missed.
  Neither substitutes for the other: without the stamp you can't verify
  completion, and without partitions you re-scan a billion documents to find
  where you left off.
- **Live ingest wins.** Rescoring the archive competes with the real-time
  pipeline for the same GPU and the same model mutexes. The task needs a
  throttle (a configurable rate or worker count, adjustable mid-run) and must be
  cancellable, and it should default to conservative rather than fast. A
  backfill that takes a week and doesn't delay tonight's alerts is strictly
  better than one that takes two days and does.

### Declaring models like catalogs

`SharedModels` being a fixed six-field struct with hardcoded paths means adding
a classifier is a struct change, a worker change, and a deploy — and that the
set of models a deployment runs isn't declared anywhere inspectable.

The catalog treatment applies directly: a `models:` list in `config.yaml`
naming which models this deployment runs and at which version, with the
per-model input adapter (ONNX input shapes and metadata differ) remaining a Rust
implementation in the repo, exactly as the catalog transform hook does. That
turns `SharedModels` into a map built from the declaration, makes the drift
check a config-vs-database comparison like every other, and lets a deployment
run a subset of models without a code change.

This is a bigger refactor than the catalog case and doesn't have to land at the
same time — but the version stamp does, because it's the part that can't be
added retroactively.

### The staleness chain

Worth naming explicitly, because it's the structure the whole provenance effort
is trying to capture:

```
raw alert  →  crossmatches  →  enrichment scores  →  filter results  →  artifacts
```

Each stage is computed at ingest from the stage before it, so a change anywhere
stales everything downstream. Updating a catalog stales crossmatches; rescoring
with a new classifier stales every filter result that read a classification.

The task system covers the first three links. **It does not currently cover the
last one**: filters also run at ingest, and there is no `reprocess_filters` —
so after a reprocessing run, filter results reflect the old scores with nothing
recording the discrepancy. That's a real gap in the artifact-provenance story
rather than an oversight in this design, and it deserves its own issue. At
minimum, `ensure_enrichment` should surface the affected filters as a warning
the way `ensure_catalog` surfaces affected crossmatches.

## Worked example: host-galaxy association (#566)

[#566](https://github.com/boom-astro/boom/pull/566) is a good stress test for
everything above, because it is the first feature in a while that moves *all
three* links of the staleness chain at once: it needs catalog columns BOOM does
not currently store, computes a new derived field from them at ingest, and
surfaces a new property on enriched alerts. Walking through what a dev and an
admin actually do after it merges is a better check on this design than more
abstraction would be — and it turns up five things the design as written does
not handle.

### What the PR introduces

- `src/utils/host/` — DLR-based association. A pure function of a transient's
  position, its cross-match documents, and a config struct.
- A call in each survey's alert worker, storing the result at
  `aux.host_galaxy`.
- A `host_galaxy:` block in `config.yaml`, `enabled: false`, carrying the
  algorithm's tuning constants (`max_dlr`, the REX rejection thresholds,
  `isophote_mag`, …).
- A new `NED_LVS` cross-match entry, reading the *existing* `NED` collection
  under a new key with an angular-size-driven radius and a projection that
  includes `Diam`, `Diam_ba`, `Diam_pa`, `Diam_survey`, `DistMpc`.
- A dependency on an `LSDR10` collection carrying Tractor shape columns
  (`shape_r`, `shape_e1`, `shape_e2`, `objtype`, `flux_r`, and optionally
  `sersic`, `flux_ivar_r`, `fracflux_r`).
- `initialize_angular_size_indexes` at scheduler startup, because an
  angular-size match without that index is unusably slow.
- `hosted: Option<bool>` on `ZtfAlertProperties`, three-state on purpose.

**Merging it changes nothing**, because `enabled` defaults to false. That is the
right default and worth stating as a convention: a feature that depends on data
the deployment may not have **lands dark**, and switching it on is a config
change with a reviewer and an actor, not a merge.

What follows is the sequence from there.

### Stage 1 — the catalogs (dev writes a PR; admin runs `ensure_catalog`)

This stage settles the how-do-we-add-a-catalog question: **no, a new catalog
does not mean a new Rust ingest module.** The split is the one described in
[Catalogs](#catalogs-declared-definitions-and-convergence) — how to build a
catalog is a declaration in this repo, which catalogs a deployment holds is its
`catalogs:` inventory, and Rust enters only through a named transform hook when
the declarative fields genuinely cannot express the source. #566 needs both
halves of that, in two different modes.

**NED needs more columns, not a new catalog.** `Diam`, `Diam_ba`, `Diam_pa` and
`Diam_survey` come from the NED-LVS source table; if the current ingest dropped
them, they are not in the collection and no amount of reprocessing conjures them
back. So this is a definition change:

```yaml
# catalogs/ned.yaml
version: 5                       # was 4
fields:
  # …
  - { from: Diam,        to: Diam,        type: f64, null_if: NaN }
  - { from: Diam_ba,     to: Diam_ba,     type: f64, null_if: NaN }
  - { from: Diam_pa,     to: Diam_pa,     type: f64, null_if: NaN }
  - { from: Diam_survey, to: Diam_survey, type: string }
convergence_from:
  4: reingest_from_source        # NOT recompute_in_place
```

`reingest_from_source`, not `recompute_in_place`, and that distinction is the
whole reason the strategy is declared rather than inferred. A diff of the two
definitions shows four added fields and says nothing about whether they are
derivable; the person adding them knows they are not, and says so in the same
PR that adds them.

**`LSDR10` may not exist at all.** The PR's own config comment — "collection
name as ingested; UMN uses LSDR10" — is a per-deployment fact, which is exactly
what the inventory is for. A deployment that wants host association adds a
`catalogs/ls_dr10.yaml` definition (one PR, shared by everyone) and then, in its
own `config/prod/<deployment>/config.yaml`:

```yaml
catalogs:
  - id: ned
  - id: ls_dr10
```

Drift appears on the admin page. Someone opens `ensure_catalog`, runs `plan`,
reads "absent → download, ingest, build indexes; ~N hours, ~M GB", and applies
it. For NED the same plan reads "v4 → v5 (`reingest_from_source`): re-read
source files on disk, upsert, sweep" — visibly a different and much cheaper
operation than the LSDR10 one, which is the point of showing a plan at all.

**Interim, before phase 7a exists**, both of those are the boom-catalogs
`add_fits_catalog` binary run through the `Command` kind. The dev-facing
workflow is the same either way — the ingest procedure is code in a repo, not a
form someone fills in — and the admin-facing workflow is identical, because both
end as an attributed run with a ledger record. Only the plumbing behind it
changes when 7a lands.

**One wrinkle: the index is not a property of the catalog alone.**
`initialize_angular_size_indexes` derives the index it needs from
`crossmatch.<survey>` — specifically from `angular_size_key` — not from anything
in the catalog definition. So the required index set for `NED` is a function of
the definition *and* of how each survey matches against it. The three-way drift
comparison in [The deployment declares an inventory](#the-deployment-declares-an-inventory)
is really four-way, and `ensure_catalog`'s index planning has to read the
crossmatch config too. Not hard, but it does not fall out of the model as
written.

### Stage 2 — turn it on (config PR; effective at scheduler restart)

Two edits to the deployment config: `host_galaxy.enabled: true`, and the
`NED_LVS` and `LSDR10` entries under `crossmatch.<survey>` so the shape columns
are actually projected into the match documents. Schedulers restart, and each
writes a `pipeline` mutation record — which is what makes the moment host
association started being computed a fact in the ledger rather than a memory.
From that instant, **new** alerts get `aux.host_galaxy`; nothing already in the
database changes.

**This stage is where the design has a real hole.** #566 reads Legacy Survey
shapes from the collection named by `host_galaxy.ls_dr10_catalog`, but the
reference `config.yaml` ships no `LSDR10` cross-match entry at all. A deployment
that flips `enabled: true` today gets NED-LVS-only association, produces
plausible-looking results, and reports no error — because
`collect_galaxies` finds no `LSDR10` key in the cross-match map and simply
returns the NED-LVS candidates. The same failure happens one level down: a key
missing from the projection reads as "this galaxy has no diameter", which is
indistinguishable from the ~19% of NED-LVS rows that genuinely have none.

The PR does defend against part of this — there is a test asserting that every
key `galaxy_from_ned_lvs` reads appears in `config.yaml`'s projection. But that
test covers the reference config, not `config/prod/*/overrides.yaml`, and no
test can see whether the deployed `NED` collection actually *has* `Diam`, which
is the thing stage 1 was about.

The generalization is a **feature preflight**: a feature that depends on data
declares that dependency — config keys, catalog IDs, and the projected fields it
reads — and the dependency is checked at startup against the inventory, against
the crossmatch config, and against a sample of the actual collections. A
deployment enabling host association without `LSDR10` should get a startup error
naming exactly what is missing, or an explicit `partial: true` acknowledgement
in config, rather than quietly degraded science. This is the same check already
proposed for `crossmatch.<survey>` ⊆ `catalogs:`, one level more specific, and
it is what finally retires `warn_if_missing_crossmatches`.

### Stage 3 — the backfill (three separate admin decisions)

Nothing here happens automatically. Each step is a task submitted from the admin
page, and each is a real decision rather than a rubber stamp, because they
differ in cost by two orders of magnitude.

| # | Task | What it does | Cost | Drift query |
| --- | --- | --- | --- | --- |
| 1 | `reprocess_crossmatch` | fills `cross_matches.NED_LVS` / `.LSDR10` on existing aux docs | one pass over aux, DB-bound | `{"cross_matches.NED_LVS": {"$exists": false}}` |
| 2 | `ensure_host_association` | computes `aux.host_galaxy` from stored cross-matches | one pass over aux, CPU-only | `{"host_galaxy": {"$exists": false}}` |
| 3 | `ensure_enrichment` | stamps `hosted` onto alert properties | billions of alerts, contends for the GPU | `{"properties.hosted": null}` |

**Step 1 is exactly backfillable, and only because #566 added a key rather than
widening one.** The PR keeps `NED_LVS` as a separate cross-match entry against
the same `NED` collection, and justifies it on matching-semantics grounds — but
the provenance argument is at least as strong. Had it instead widened the
existing `NED` entry's projection in place, `cross_matches.NED` would exist both
before and after, with the diameters present in some documents and not others,
and there would be **no query that finds the un-widened ones** — the same
failure as unversioned classification scores, on a different collection. Worth a
rule: *widening a projection in place is not backfillable; adding a key is.*

**Step 2 is a third kind of convergence.** It needs no source files, no network,
and no GPU — it is a pure recompute from fields already on the document, the
`RecomputeInPlace` shape applied to an alert collection instead of a catalog.
That suggests two things:

- `reprocess_crossmatch` should run the downstream derivation in the same pass,
  so the common case is one scan instead of two, and so the association is
  computed from the cross-matches that pass just wrote. This mirrors the live
  path, where association runs immediately after the cross-match in the same
  function.
- `ensure_host_association` should *also* exist standalone, because retuning
  `max_dlr` or the REX thresholds changes every stored association without
  changing a single cross-match, and that will happen far more often than a
  re-crossmatch. Coupling them would make a cheap config-tuning pass pay for the
  expensive step every time.

**Step 3 is the one that genuinely needs a human to say no.** `hosted` is a
convenience boolean on the enriched alert; the underlying association is already
readable at `aux.host_galaxy.best_host.d_dlr`, which is what the filters cut on
anyway. So re-enriching the archive buys a queryable flag on historical alerts,
and costs a week of contention with the live pipeline. That is a scientific and
operational judgement, it belongs to whoever is accountable for the instrument's
throughput, and the system's job is to present it as a suggested follow-up with
the cost attached — not to chain it automatically after step 2. This is the
concrete shape of "gated by an admin's decision": the gate is not ceremony, it
is a decision with a defensible answer of "not yet."

**Step 4 does not exist.** Filter results computed before host association
existed are not re-run, and there is no `reprocess_filters` to run them —
[the staleness chain](#the-staleness-chain) again, and #566 is a good argument
for giving that gap its own issue rather than continuing to note it.

### Stage 4 — what the ledger says afterwards

```
GET /data/state?collection=ZTF_alerts_aux

2026-04-02  ingest      NED       ensure_catalog v4→v5, reingest_from_source
                                  pete · ui · git 3f2a91c · 12.4M matched
2026-04-02  ingest      LSDR10    ensure_catalog absent→v1
                                  pete · ui · git 3f2a91c · 1.9B inserted
2026-04-03  pipeline    ztf       scheduler start · git 3f2a91c
                                  crossmatch.ztf + host_galaxy fingerprint a1b2…
2026-04-05  backfill    NED_LVS   reprocess_crossmatch · 811M modified
2026-04-09  recompute   —         ensure_host_association · 811M modified
```

That timeline is the payoff. A paper saying "host associations were computed
for ZTF objects" can cite a provenance snapshot over those five records, and
"against which catalog release, at which `max_dlr`, under which commit" has an
answer that does not depend on anyone's memory.

### What this example changes about the design

1. **Stored derived results need a config stamp, and this one needs it before
   the first backfill.** `aux.host_galaxy` is a function of `max_dlr`, the REX
   thresholds and `isophote_mag` as much as of the cross-matches, and changing
   any of them leaves every stored association stale with nothing marking it —
   [the classifications problem](#the-blocking-problem-scores-carry-no-version)
   exactly. It is much cheaper to fix here: aux documents are per *object*, not
   per alert, so an interned `host_config_set` integer (or even a short
   fingerprint string) is affordable at this scale. Add it while the field is
   new; it is unretrofittable in the same way and for the same reason.
2. **There is a third convergence family: recompute-from-stored.**
   `ensure_catalog` goes back to source, `ensure_enrichment` needs models and a
   GPU, and this needs neither. Proposal: hand-write them until there are three,
   then decide whether a generic `ensure_derived` parameterized by a declared
   derivation is worth having.
3. **Index drift is four-way, not three-way.** Definitions, inventory,
   *crossmatch config*, and actual state — because `angular_size_key` in a
   survey's crossmatch block creates an index requirement on a catalog.
4. **Three-state derived booleans are mandatory, not stylistic.**
   `hosted: Option<bool>` — absent means never evaluated, `Some(false)` means
   evaluated and nothing found — is the only reason step 3 above has a drift
   query at all, and the only reason a filter cutting on `hosted == false` does
   not silently sweep in every alert enriched before the feature existed. This
   belongs in `CONTRIBUTING.md` as a rule for any new derived field, alongside
   expand/contract.
5. **Features declare their data dependencies, and startup checks them.** The
   preflight described in stage 2. A feature that can run degraded when its
   inputs are missing must not do so silently.

## External code tasks

The requirement: point at a git repo, a rev, and a path, and run it, given a
properly-defined environment. This is a genuine need for the long tail, and it
is also, stated plainly, **remote code execution against the production database
as a product feature**. Both things are true and the design has to hold both.

### What actually changes about the threat model

It's tempting to say "the people who'd use this already have SSH, so this grants
nothing new," and for the *authorized* case that's correct — it's strictly less
privilege than a shell, and strictly more provenance, since the rev is recorded.

What changes is the *credential*. Today, mutating production requires an SSH key
on a specific machine. Afterwards, it requires a web session — which is
phishable, XSS-reachable, and shares a browser with the rest of the internet. So
the control that matters is not "prevent admins from running code," it's
**"prevent a single compromised web session from being sufficient."**

Concretely, that means:

- **A repo allowlist in `config.yaml`**, not free-form URLs — e.g.
  `boom-astro/*` plus a named list. Cheapest, highest-value control by a wide
  margin: it turns "run anything" into "run something a maintainer could already
  merge."
- **Immutable revs.** Refs are resolved to a commit SHA at submit time and the
  SHA is what runs and what's recorded. Never run `main`; a mutable ref is
  unrecordable provenance and a TOCTOU hole at the same time.
- **A separate role**, `task:run_external`, granted to fewer people than
  `task:run`.
- **Second-person approval** before an external run is claimed — a
  `pending_approval` status and a `POST /tasks/{id}/approve` from a *different*
  actor holding `task:approve`. This is the standard control for this class of
  action and it's a few lines given the state machine already exists. It is what
  makes one stolen session insufficient.
- **A scoped database credential** for external runs, rather than the worker's
  own. Mongo roles can restrict to the analytical collections and deny writes to
  `users`, `babamul_users`, and the ledger itself. A ledger the running code can
  rewrite is not a ledger.

### On isolation, honestly

On a single-node Compose deployment there is no configuration of
docker-in-docker or a mounted Docker socket that is meaningfully sandboxed:
both are root-equivalent on the host. So the design should not pretend
otherwise. What it can do is contain the blast radius:

- The build/run privilege lives in **one small service** with a narrow
  interface, never in the API. The API server must not have the socket.
- The *task container* gets none of it: no socket, `--user` non-root, dropped
  capabilities, read-only rootfs except the data mount, and a network attached
  only to what it needs (mongo, and egress only if the task declares it).
- Build rootless (BuildKit rootless) if we build at all.

### Prefer declared environments over arbitrary Dockerfiles

Rather than building whatever `Dockerfile` the repo contains, the repo declares
a runtime from a **small set of prebuilt base images** we maintain
(`boom-task-rust`, `boom-task-python`) plus a lockfile (`Cargo.lock` /
`requirements.txt`) that the builder installs into it. This is what "so long as
the environment is properly defined" should mean in practice:

- A much narrower surface than arbitrary `FROM` and arbitrary `RUN`.
- Far faster, because the expensive layers are pre-built and shared.
- More reproducible, because the base is one of ours and pinned by digest.

Builds are cached by `(repo, sha, runtime, lockfile hash)` → image digest, and
the resulting digest goes in the ledger alongside the commit SHA.

### Sequencing

This is **phase 8**, after the catalog spec engine. That ordering is deliberate:
if we build the escape hatch first, it becomes the path of least resistance for
everything, and we end up with the same unreviewed-code provenance problem we
started with — just with better logging. Build the well-lit path first, then see
what's actually left over. It's possible the answer is "the minifiers, twice a
year," which might not justify the machinery at all.

## Operational schema migrations

The third case: adding `is_admin` to `babamul_users`, dropping a key,
renaming a field. Release-coupled, must run exactly once, must not wait for a
human. As argued in [Two kinds of mutation](#two-kinds-of-mutation), this gets
its own mechanism and shares only the ledger.

### Most "schema changes" aren't migrations

Worth saying first, because it's the reason BOOM has needed so few: in MongoDB,
an additive field with `#[serde(default)]` requires no migration at all. A
missing field deserializes to the default, and documents pick up the field the
next time they're written. `User::watchlist_access` and
`BabamulUser::kafka_credentials` already work this way.

So the motivating example — adding `is_admin` (or `roles`) to `babamul_users` —
**needs no migration**. That's not a dodge; it's the correct answer, and the
system should be designed so that reaching for a migration is rare.

Real migrations are for the cases where documents must actually be rewritten:
renaming a field that existing code reads, backfilling a value that can't be
computed lazily, deleting a key to reclaim space or to remove a secret, or
changing an index in a way that requires a rebuild.

### Mechanism

An ordered list of migrations in code, each with a version number and an `up`
function, plus a `schema_migrations` collection recording
`{version, name, applied_at, code_version, duration_ms}`. On startup, apply
every migration whose version is absent, in order.

This slots into machinery that already exists: `build_db_api` in
`src/api/db.rs` is already an idempotent startup routine that creates indexes
and bootstraps the admin user, and it already handles the concurrent-instance
race by catching `E11000` on a unique index. Migrations get the same treatment:
a unique index on `schema_migrations.version` makes double-application a
duplicate-key error rather than a race, so multiple services booting at once is
safe without a separate lock. Alternatively, a one-shot `migrate` service in
Compose that other services `depend_on` gives a single designated runner; the
unique index is simpler and needs no compose changes.

Each applied migration writes a `data_mutations` record with
`source.kind: "migration"`, `actor.realm: "system"`, `trigger: "startup"` — so
the ledger's answer to "what has been done to `babamul_users`" is complete.

### Two rules that keep this safe

**Operational collections only.** Startup migrations may only touch the
collections in `PROTECTED_COLLECTION_NAMES` (`users`, `babamul_users`,
`filters`, the stats collection) plus the task system's own. Anything touching
alert or catalog collections is a **task**, not a startup migration, even when
it looks like a schema change — because those collections have billions of
documents and blocking API startup on them is an outage. Enforce this in the
migration harness: assert the target is in the protected set. That single
assertion is what keeps the two mechanisms from bleeding into each other.

**Expand/contract, always.** A migration must leave the database working for the
*previous* release, because a deploy can be rolled back and because services
restart at different times. Field deletions and renames therefore span two
releases: first ship code that stops reading the old field, then in a later
release delete it. The migration harness can't enforce this, so it goes in
`CONTRIBUTING.md` and in review.

## Implementation phases

Each phase is meant to be a shippable PR or small stack of them.
[The #566 worked example](#worked-example-host-galaxy-association-566) cuts
across phases 5, 7a and 7b and is a reasonable acceptance test for them: when
shipping host association end to end requires no shell, the phases have landed.

**1. Skeleton and one real task.**
`task_runs` + `task_logs`, the `TaskSpec`/`Task` traits and registry, the
`task-worker` service in compose, claim/lease/heartbeat, and the API routes for
submit/list/get/logs/cancel. Port exactly one task — `migrate_snr` is a good
first choice: self-contained, idempotent, already batched — and prove the whole
path end to end.

**1b. Partitioned execution.**
`task_partitions`, plan-time range splitting over a declared key, partition
claiming and leases, `SIGTERM` handling reusing `should_terminate`, and
`done / total` progress. Part of proving the path end to end, since a task that
can't survive a deploy isn't finished. Retrofit `migrate_snr` onto it as the
worked example.

**2. Provenance.**
`data_mutations` with the unified `actor`/`source`/`trigger` shape,
code-version and config-fingerprint capture (including `Dockerfile` build args
and deploy workflow changes), the `pipeline` record on scheduler startup, and
`GET /data/state`. Backfill ledger entries by hand for anything we already know
was run, so the timeline doesn't start empty and misleading.

**3. Roles.**
`roles: Vec<Role>` on both `User` and `BabamulUser` with `#[serde(default)]`,
`is_admin` mapped to the full set, babamul admin seeding from config, and a
shared authorization check used by both middlewares.

**4. Admin UI.**
Route guard, run list, schema-driven submit form, run detail with log tail.
This is the phase that actually removes the need to SSH.

**5. Port the remaining boom tasks.**
`reprocess_crossmatch`, `migrate_fp_flux`, `enrich_reprocess`, `copy_cutouts`,
`add_filter`. Each becomes `src/tasks/<name>.rs` plus a thin CLI wrapper, with a
dry-run mode and an idempotence test. boom-catalogs' binaries go into the image
behind the `Command` kind as an interim.

**5b. Version the classification scores.** Out of phase order deliberately: the
`model_sets` collection and the `classifications_set` stamp on alert documents,
written by the live enrichment worker. This is the one item in this document
that cannot be added retroactively — every alert ingested before it lands is
permanently unattributable to a model version — so it should go in as early as
it can be reviewed, ahead of the tasks that depend on it. Includes a one-time
backfill stamping existing alerts with the set they were (believed to be)
scored under.

**6. Startup migrations.**
The migration harness, `schema_migrations`, the protected-collection assertion,
ledger integration, and the expand/contract note in `CONTRIBUTING.md`. Small,
and independent enough to land any time after phase 2.

**7. Catalog definitions and convergence.**
The largest phase, and the one that delivers "the admin just picks NED." Best
split in two:

- **7a. Definitions, inventory, and first ingest.** The definition format, the
  in-repo `catalogs/` directory, the loader and content-hash test, the
  `catalogs:` inventory in `config.yaml` with the crossmatch cross-check at
  startup, the `catalogs` state collection, and `ensure_catalog` covering
  absent → ingested (download, `id_from` upserts, generation stamping, index
  creation). Port two or three real catalogs — one easy, one awkward — to
  validate the format before porting the rest.
- **7b. Convergence and drift.** `plan` mode, the `Convergence` strategy enum
  and the max-over-the-gap rule, generation sweep for deletions,
  build-then-swap for source-release changes, the three-way drift endpoint and
  drift table UI, the startup drift check and gauge (retiring
  `warn_if_missing_crossmatches`), `drop_catalog` for undeclared collections,
  and suggested `reprocess_crossmatch` follow-ups.

**7c. Enrichment convergence.**
`ensure_enrichment` built on the phase 5b stamp: drift query, queue population
(closing the loop `enrich_reprocess` leaves open), throttling against live
ingest, resumability, and ledger records. Optionally the `models:` config
declaration, which can follow later — only the stamp is order-critical.

**7d. Derived-field convergence.**
The third family named in
[the worked example](#what-this-example-changes-about-the-design): a recompute
over an alert collection from fields already stored on it, needing neither
source files nor a GPU. `ensure_host_association` is the first instance;
`reprocess_crossmatch` also grows the ability to run the derivation in the same
pass. Small, and only worth generalizing once there is a third one.

**8. External code tasks.**
Repo allowlist, SHA resolution, the `task:run_external` role, second-person
approval, the declared-runtime builder, the scoped DB credential. Only if
phase 7 leaves enough of a long tail to justify it — and phase 7 is designed to
make that unlikely, since "devs write ingestion procedures as PRs" is exactly
the path this would otherwise route around.

**9. Polish.**
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
week. The Mongo-claim approach is a couple hundred lines and leaves the run
record as the single source of truth. If we later want Python workers for ML,
they can consume the same Mongo queue, which is the actual motivation behind the
Celery suggestion.

**One mechanism for tasks and startup migrations.** Covered above: opposite
requirements on triggering, ordering, re-runnability, and blocking. Shared
ledger, separate mechanisms.

**User-authored catalog specs stored in the database** (this document's previous
revision). Rejected in favor of in-repo definitions: catalog ingestion knowledge
is identical across deployments and deserves review and tests, and shipping
definitions with the release means `code_version` already pins them instead of
introducing a second independently-mutable input to fingerprint. See
[Why definitions belong in the repo](#why-definitions-belong-in-the-repo) for
the cost this accepts.

**A distinct task type per catalog, or per definition change.** "Ingest NED" as
its own registry entry reads nicely, but the registry then grows with every
catalog and every revision, the UI becomes a list of dozens of near-identical
types, and the definition version — which is data — gets encoded in a type name.
One `ensure_catalog` parameterized by catalog ID, with `plan` showing what it
will actually do, gets the same "the user just picks NED" experience without
that.

**Merging the boom and babamul auth systems now.** The right end state, but it's
a project with a live-credential migration in it, and the task system doesn't
need it — a unified `actor` in the ledger and a shared role vocabulary get us
the properties that matter, and are the first step of the merge anyway.

**One-off containers via the Docker socket, for everything.** Real isolation and
per-task resource limits, and there's already a `docker-socket-proxy` in the
stack. But socket access is root-equivalent on a single-node deploy, and
log/status plumbing gets harder, not easier. Reserved for external code tasks,
where the tradeoff is forced.

**Kubernetes Jobs.** We're a single-node Docker Compose deployment. No.

**Ledger in a separate store (Postgres, a file, git).** An append-only ledger in
a document DB relies on discipline rather than enforcement. But a separate store
adds a database to the stack for one collection, and the mutations that matter
are written by our own worker — the same trust boundary either way. The cheap
hedge, if we want one, is denying ledger writes to the scoped credential used by
external tasks (already proposed) and periodically exporting to append-only
storage.

## Open questions

- **Roles vocabulary.** Proposed: `task:run`, `task:approve`,
  `task:run_external`, `user:admin`. Is that the right granularity, and does
  anything else in the app want roles at the same time (filter authorship,
  watchlist administration)?
- **Log retention** — how long to keep `task_logs`, and whether the UI should
  read from Loki instead of a Mongo copy. Mongo is proposed for simplicity;
  Loki is where the data already goes.
- **Where large input files come from.** Downloads land on a bind-mounted volume
  today. Do admins ever need to *upload* a catalog through the UI, or is
  "download from a URL" always sufficient? Assuming the latter.
- **How far the declarative definition stretches.** Worth validating against the
  three or four gnarliest existing catalogs before committing to the format in
  phase 7a — if half of them need a transform hook, the format is probably
  trying to do too much and should shrink to the parts that are genuinely
  common.
- **What set do existing alerts get stamped with?** Backfilling
  `classifications_set` onto already-scored alerts means asserting which model
  versions produced them, which we only know from deploy history and filenames.
  Proposal: one `model_sets` entry marked `inferred: true`, honest about being a
  reconstruction rather than a record. Alerts predating it stay unstamped and
  therefore always appear as drift — which is correct, if inconvenient.
- **Does rescoring overwrite, or accumulate?** Overwriting loses the old score;
  keeping both doubles the field count on the hottest collection in the
  database. Overwrite is proposed, on the grounds that the ledger records the
  transition and the old score is reproducible from the archived model — but
  that assumes we archive superseded ONNX files, which should be an explicit
  commitment rather than an assumption.
- **Does a feature get to run degraded?** Host association with `LSDR10` absent
  produces NED-LVS-only results and no error (see
  [stage 2](#stage-2--turn-it-on-config-pr-effective-at-scheduler-restart)).
  Proposal: a declared data dependency, checked at startup, that refuses to
  start unless the deployment either satisfies it or explicitly acknowledges
  running partial. The counter-argument is that some features are genuinely
  useful degraded and a hard failure is worse than a warning — but then the
  degradation should be recorded on the output, not left implicit.
- **How wide is the config-stamp requirement?** `classifications_set` is
  proposed for models and a `host_config` stamp for host association, and the
  same argument applies to anything derived from config at ingest time. If it
  applies broadly, a single per-document "which pipeline config produced this"
  stamp may be cheaper and more general than one stamp per feature — but it is
  coarser, so any config change stales everything at once.
- **`reprocess_filters`.** Named as a gap in
  [the staleness chain](#the-staleness-chain), not designed here. Needs its own
  issue: filter results are the actual scientific artifacts, and they're
  currently the one link with no reprocessing path at all.
- **Do any existing catalog collections have synthetic `_id`s?** If so, they
  can't be converged in place and need a one-time rebuild with deterministic IDs
  before any of this applies to them. Worth auditing early, because it's a
  latent constraint on the whole convergence model rather than a detail.
- **Should `catalogs:` subsume the crossmatch config rather than sit beside it?**
  Proposed: a separate top-level list, with startup validation that
  `crossmatch.<survey>` only names catalogs in it. Folding the per-survey
  matching params into the inventory entries would remove the need for the
  cross-check, but it restructures a config shape that prod deployments already
  use and that `CatalogXmatchConfig` deserializes directly.
- **Auto-create missing indexes at startup, or leave it to `ensure_catalog`?**
  Precedent exists (`initialize_survey_indexes`), it's non-destructive, and it
  removes a common cause of drift — but building a 2dsphere index over a
  multi-million-document catalog during startup is not free.
- **Source releases as versions, or as separate catalogs?** Treating
  "NED 2024-06" and "NED 2025-01" as the same catalog at different releases
  (proposed) means an artifact citing "NED" needs the ledger to say which
  release it matched against. Keeping them as distinct collections would make
  that self-evident but complicates the crossmatch config and doubles storage.
- **Who bumps `version`, and how do we keep the hash test from being noise?**
  A content hash catches a forgotten bump, but it also fires on comment and
  formatting changes unless the hash is computed over the parsed, normalized
  definition. Worth getting right or the test gets disabled.
- **Ledger granularity for the live pipeline.** One record per scheduler start
  is cheap and probably enough. If deploys become frequent this gets noisy and
  we'd want to collapse consecutive identical versions.
- **Whether to keep the CLI wrappers.** Keeping `src/bin/*` as thin wrappers
  preserves local development and emergency access, but an emergency `docker
  exec` bypasses the UI. Proposal: keep them, and have the shared task function
  write the mutation record regardless of who invoked it — so even a hand-run
  binary lands in the ledger with `trigger: "cli"`.
