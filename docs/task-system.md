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
- New catalogs can be ingested without writing code, from the UI.
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
| `GET` | `/catalog-specs` … | CRUD for declarative catalog specs (see below). |
| `POST` | `/catalog-specs/{id}/preview` | Parse the first N rows and return sample output documents. |
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
- **Catalog specs.** The editor described below — the piece that turns "write a
  Rust struct and SSH in" into "fill in a form and click preview."
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

## Catalog ingestion as data, not code

This is the main thing people SSH to do, and it's worth solving properly rather
than wrapping the status quo.

Looking at what actually varies between boom-catalogs' ingestion runs: the file
format (csv / ascii / parquet / fits), a per-catalog Rust struct used to
deserialize each row, the column-to-field mapping and types, which columns are
RA/Dec, the output collection name, and the index specification. Only one of
those is genuinely code-shaped — the per-catalog struct — and it is code only
because a typed deserializer was the convenient way to express a mapping.

So: **make the catalog a declarative spec, stored as a document, and make
ingestion a single compiled-in task type that interprets it.**

```jsonc
{
  "_id": "gaia_dr3",
  "version": 3,                       // bumped on every edit; old versions kept
  "collection": "Gaia_DR3",
  "format": "parquet",
  "source": { "glob": "/data/gaia_dr3/**/*.parquet" },
  "fields": [
    { "from": "source_id",   "to": "_id",  "type": "i64" },
    { "from": "ra",          "to": "ra",   "type": "f64" },
    { "from": "dec",         "to": "dec",  "type": "f64" },
    { "from": "phot_g_mean_mag", "to": "Gmag", "type": "f32", "null_if": "NaN" }
  ],
  "coordinates": { "ra": "ra", "dec": "dec", "add_healpix": true },
  "indexes": [ { "keys": { "coordinates.radec_geojson": "2dsphere" } } ],
  "created_by": { "realm": "babamul", "username": "pete" }
}
```

Why this is better than BYO code, and not just easier:

- **It is self-describing provenance.** "This collection was built from spec
  `gaia_dr3` v3" tells you what the data *means*. "This collection was built by
  running commit `a3f9e1` of some repo" requires reading the code to find out.
  The ledger references the spec ID and version, and the spec document is
  immutable once used.
- **Dev work moves to the frontend for real**, which is the outcome you're
  after. The spec editor gets a **preview** action: parse the first N rows,
  show the inferred source schema, the sample output documents, the index plan,
  and any collision with an existing collection — before anything is written.
  That feedback loop is what makes a form a substitute for a script.
- **It removes the deploy from the critical path.** Adding a catalog stops
  being a PR-and-release, which is most of why people SSH.

The long tail that a spec can't express — multi-file joins, the `minifiers/`
pandas transforms, genuinely novel formats — is what
[External code tasks](#external-code-tasks) are for. My expectation is that the
spec covers most catalogs and the escape hatch is rare, which is the ratio that
justifies building the spec first.

The Python `downloaders/` are a separate concern: they fetch large files over
HTTP with retries and parallelism, which is a small amount of `reqwest` (already
a dependency), so a native `download_catalog` task taking a URL list is likely
less work than standing up a Python worker.

Interim, before the spec engine exists: run the boom-catalogs binaries via the
`Command` kind, with those binaries added to the image. That unblocks the UI
without waiting on any of this.

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

**1. Skeleton and one real task.**
`task_runs` + `task_logs`, the `TaskSpec`/`Task` traits and registry, the
`task-worker` service in compose, claim/lease/heartbeat, and the API routes for
submit/list/get/logs/cancel. Port exactly one task — `migrate_snr` is a good
first choice: self-contained, idempotent, already batched — and prove the whole
path end to end.

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

**6. Startup migrations.**
The migration harness, `schema_migrations`, the protected-collection assertion,
ledger integration, and the expand/contract note in `CONTRIBUTING.md`. Small,
and independent enough to land any time after phase 2.

**7. Catalog specs.**
The spec document, the interpreter task, the preview endpoint, and the spec
editor UI. The largest phase, and the one that delivers "do the dev work in the
frontend."

**8. External code tasks.**
Repo allowlist, SHA resolution, the `task:run_external` role, second-person
approval, the declared-runtime builder, the scoped DB credential. Only if
phase 7 leaves enough of a long tail to justify it.

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
- **How far the catalog spec stretches.** Worth validating against the three or
  four gnarliest existing catalogs before committing to phase 7 — if it only
  covers half of them, the balance between phases 7 and 8 changes.
- **Ledger granularity for the live pipeline.** One record per scheduler start
  is cheap and probably enough. If deploys become frequent this gets noisy and
  we'd want to collapse consecutive identical versions.
- **Whether to keep the CLI wrappers.** Keeping `src/bin/*` as thin wrappers
  preserves local development and emergency access, but an emergency `docker
  exec` bypasses the UI. Proposal: keep them, and have the shared task function
  write the mutation record regardless of who invoked it — so even a hand-run
  binary lands in the ledger with `trigger: "cli"`.
