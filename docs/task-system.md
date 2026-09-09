# Task system

Operating a BOOM system typically involves adding new catalogs,
changing the schema of alerts and object already in the database,
and reprocessing alerts already saved in the database, e.g., when a new
catalog or enrichment step, e.g., ML model classifier, is added.
It is important for us to be able to track what mutations were done to the
data when.

BOOM's task system allows kicking off, monitoring, and querying the history
of these tasks from the admin section of the front end.
All tasks report what they've done to mutate the data system, and this
changelog can be viewed from the admin page.

## Why not just run a binary

Because the properties that matter here are not available to a process someone
starts over SSH:

- **It has to survive a deploy.** A catalog ingest is hours to days. A run whose
  worker is replaced mid-flight goes back on the queue and is picked up again.
- **Its logs have to be watchable while it runs**, by whoever started it, without
  shell access to the production host.
- **It has to be cancellable** — cleanly, at a point the task chooses, not by
  killing a process partway through a batch.
- **It has to leave a record.** Who ran what, with which parameters, against
  which release. BOOM's scientific artifacts are a function of the current state
  of the data, and that state is the raw stream plus a sequence of mutations. If
  those mutations are only in someone's shell history, the artifacts derived
  from them can't be reasoned about or reproduced.

Access control is the fourth reason and the least interesting one: mutating
production data should not require handing out root-adjacent shell access.

## How a run flows

```text
   admin page ──POST /tasks──▶ boom-api ──▶ task_runs (status: queued)
                                                 │
                                    findOneAndUpdate(queued → running, +lease)
                                                 ▼
                                          boom task-worker
                                                 │
                    ┌────────────────────────────┼───────────────────────┐
                    ▼                            ▼                       ▼
             the data itself              task_runs.progress         task_logs
                                          (+ lease heartbeat)     (tailed by the UI)
```

MongoDB is both the queue and the record. An atomic `find_one_and_update` moves
a run from `queued` to `running` and stamps a lease in one operation: two
workers racing both match the filter, but only one update sees `queued`.

**Why not Valkey**, which is already in the stack and is what the alert
scheduler uses: the run record has to live in Mongo regardless — status, params,
actor, progress and history are what the admin page and the provenance story
read. Putting the queue elsewhere makes every state transition a dual write to
two stores, and reconciling those when one fails mid-transition is a real source
of lost or duplicated runs. Valkey's advantage is throughput, which the alert
pipeline needs and this does not: the task system sees a few runs a week, and
the claim is one indexed lookup every couple of seconds.

It also does not contend with alert writes. WiredTiger takes only *intent* locks
at the database and collection level, and those are mutually compatible, so
writes to `task_runs` never block writes to an alert collection. The thing that
*does* contend is a large ingest itself — which is why `catalog_ingest` exposes
`num_workers` and `batch_size`, so a run can be turned down when it is hurting
the pipeline.

## Leases, and surviving a deploy

A claimed run carries `lease_expires_at`, renewed by the worker's heartbeat
every 20 seconds against a 60-second lease. Two things use it:

- **A worker shutting down cleanly** (SIGTERM, i.e. a deploy) sets the running
  task's cancellation flag, waits for it to stop at its next safe point, and
  puts the run back on the queue as `queued`. The replacement worker picks it up
  immediately rather than waiting out the lease.
- **A worker that dies** renews nothing. The next worker to poll requeues any
  run whose lease has lapsed.

Both rely on task bodies being **resumable**: re-running one continues rather
than repeating. `catalog_ingest` records each completed chunk, so a resumed run
skips what is already in and costs one chunk, not the whole catalog.

**Only tasks that declare `idempotent` are retried.** Resuming is safe exactly
when re-running produces the same state — a chunked ingest skips what it
recorded, a recompute derives from untouched inputs. Anything else is **failed**
instead, with an error saying why, and left for a person to look at. Quietly
doing half the work twice is the one outcome nobody could detect afterwards.

The same rule applies on clean shutdown, not just to a lapsed lease: a deploy
must not silently re-run something that cannot be re-run safely.

A task type this build does not recognize is treated as **not** retryable. A run
can outlive the release that created it, and re-running something the code
cannot even describe is precisely the case to be conservative about.

Every task registered today is idempotent, and a test asserts it — not because
the system requires it, but because it is the property that makes a task survive
a deploy, which is most of the point. Registering one without it should be a
deliberate edit to that test.

The heartbeat also carries cancellation in the other direction. `POST
/tasks/{id}/cancel` sets `cancel_requested`; the heartbeat mirrors it into a
flag the task polls. A queued run is canceled outright, since nothing started.

## Task types

Declared in code, in `TASKS` in [`src/tasks/mod.rs`](../src/tasks/mod.rs) —
a task type is part of the release, so pinning the code version pins what it
does. Parameters are validated by the API at submit time, so a bad request is a
400 the client can act on rather than a run that fails on a worker minutes
later.

| Task | What it does |
| --- | --- |
| `catalog_ingest` | Download an archival catalog and insert it. See [catalogs.md](./catalogs.md). |
| `migrate_fp_flux` | Recompute ZTF forced-photometry flux in `ZTF_alerts_aux` at a fixed zeropoint. |
| `migrate_snr` | Recompute `snr_psf`, `snr_ap` and ZTF `apFlux` across alerts and lightcurves. |
| `reprocess_crossmatch` | Fill in or refresh crossmatches on a survey's `alerts_aux` records. |
| `prepare_catalog` | Add spatial fields and a 2dsphere index to a hand-imported collection. |
| `enrich_reprocess` | Select alerts, queue them, and re-run enrichment over them. See [alert-processing.md](./alert-processing.md#re-enriching-alerts-after-a-change). |
| `mpcorb_ingest` | Re-download MPC orbital elements and swap them into `MPC_orbits`. |
| `sso_baselines` | Fit solar system phase-curve baselines from ZTF detections. |
| `copy_cutouts` | Copy a survey's cutout collection between MongoDB deployments. |
| `stream_kowalski_alerts` | Back-fill BOOM by streaming a Kowalski deployment's alerts. |

Submission is single-flight per target, not per type: two ingests of the same
catalog would race on the same collection and chunk state, but ingesting 2MASS
should not block ingesting NED.

**Every data-mutating binary is now a task.** `src/bin/` holds the services
(`api`, `scheduler`, `kafka_consumer`, `kafka_producer`, `task_worker`) and two
tools that change nothing (`check_config`, `add_filter`).

That is the point the system was built for: there is no longer a binary an
operator can run over SSH that mutates data without a record of who ran it, with
what, under which release.

### Credentials in parameters

A task may take a connection URI — a copy between two clusters has to name both
ends somehow. But parameters are stored on the run, rendered on the admin page,
and copied into the ledger, so a URI carries a password into all three.

The worker reads the real parameters from `task_runs`. Everywhere they are read
*back* they are redacted first: every API response, and the ledger, which is
append-only and would otherwise archive a password permanently.

Redaction masks the password and leaves the rest — `mongodb://alice:***@host/db`
— because which host and database a run touched is most of why anyone reads the
parameters back. It keys on the field *name* (`*_uri`, `uri`), not on whether a
value looks like a URI, so a catalog source URL stays readable in full.

`enrich_reprocess` **populates the queue and then drains it**, rather than only
draining one something else filled. That is what closes the loop the binary left
open, and it is also what makes completion well-defined: because the task owns
the queue, `LLEN == 0` means done rather than "nobody has pushed anything yet".

The queue is scoped to the run — `<survey>_enrichment_queue_reprocess_<run_id>`
— precisely so that holds. An `input_queue` parameter overrides it for a queue
filled out of band; the task then only drains, and never deletes it.

Completion needs `LLEN == 0` twice in a row. A worker pops a batch of up to
1000 before processing it, so a single zero can be observed while a batch is
still in flight, and stopping there would count those alerts as reprocessed
before they were.

Selection is explicit rather than inferred: everything not enriched by the
current set (`stale`, the one to use after changing a model or a formula),
alerts missing a field, a candid range, or everything. See
[alert-processing.md](./alert-processing.md#re-enriching-alerts-after-a-change)
for how staleness is recorded and why `missing_field` cannot express it. Each
needs a params struct, an arm in `dispatch`, and a cancellation check in its
batch loop; the ones that drive their work through Valkey already have the
resumability a task needs.

`migrate_fp_flux` shows the shape. Three things change when a one-shot binary
becomes a task, and all three are about no longer owning the process:

- **`process::exit` becomes an error.** Exiting would kill the worker and every
  other run on it, and leave this run holding a lease until it expired.
- **The batch loop checks for cancellation**, at batch boundaries — an
  `update_many` is atomic per document, so a boundary is the only point where
  stopping leaves a state that is easy to describe.
- **Progress goes to the run**, not to a terminal progress bar nobody is
  watching.

A panicking task fails its own run rather than taking the worker down: the
worker catches unwinds at the dispatch boundary. That matters precisely because
these bodies come from binaries where an `unwrap` on unexpected data was a
reasonable way to stop.

**A ported task leaves no binary behind.** `migrate_fp_flux` and `migrate_snr`
were briefly kept as thin wrappers and are now gone, the same way `add_catalog`
was: a binary is precisely the thing this system exists to replace, and leaving
one available means the untracked path stays the easy one.

That does mean these can only be run against a database the API and a worker can
both reach. The escape hatch, if a migration ever has to run somewhere the task
system cannot, is a task body called from a one-off binary — but that should be
a deliberate, temporary addition rather than a standing wrapper.

## Running it in dev

`make dev` brings up a `task-worker` alongside the API, under cargo-watch like
the other services. It shares the dev MongoDB, so a run kicked off from the
admin page is picked up within a couple of seconds.

Two things about the dev container specifically:

- It runs `uv sync --project /app/boompy --frozen` before starting, so boompy's
  environment exists before the first ingest asks for it. The venv lives in its
  own volume rather than being bind-mounted from the host — a host venv would
  carry macOS wheels into a Debian image.
- It shares the `target` volume with the api and scheduler containers, so all of
  them serialize on one cargo build lock. A source edit therefore costs several
  sequential rebuilds, and the API can be briefly unavailable while they drain.

Catalog chunks are staged in the `catalog_data` volume, mounted at
`/app/data/catalogs`.

## Collections

| Collection | Holds |
| --- | --- |
| `task_runs` | One document per run: params, status, actor, progress, lease, error. Also the queue. |
| `task_logs` | Log lines, batched — one document per flush, not per line. The UI tails by asking for `seq` greater than the last it saw. |
| `data_mutations` | The append-only ledger: what changed, who changed it, and under which release. |

`task_logs` is a convenience copy for the UI; the full firehose still reaches
Loki through the normal container-log path. It is capped per run so a task
logging in a loop cannot fill the disk.

## The ledger

BOOM's scientific artifacts are a function of the *current state* of the
database, and that state is the raw alert stream plus a sequence of out-of-band
mutations. If those mutations live only in shell history, the artifacts derived
from them cannot be reasoned about or reproduced. So every task that changes
data appends to `data_mutations`, and `GET /data/mutations?collection=NED` reads
it back, newest first.

It is **append-only**. Entries are written when a mutation finishes, and there
is no code path that updates or deletes one — a record that can be edited
answers a much weaker question than "what happened".

Each entry names the source (task run, migration, or pipeline), the actor
qualified by realm, the trigger, the target collection, a coarse operation
(`ingest`, `backfill`, `recompute`, `delete`, `index`, `drop`), and a free-form
`details` document for row counts.

`code_version` carries the package version and the commit, the latter compiled
in from `BOOM_GIT_SHA`. When the build does not set it the field is **absent**
rather than a placeholder: "we do not know which commit did this" is a real
answer, and a fabricated one would make the ledger confidently wrong. Set it
with `BOOM_GIT_SHA=$(git rev-parse HEAD)`; the Dockerfile takes it as a build
arg.

Writing a ledger entry is best-effort. A task that mutated data has already
done so, and failing the run because the bookkeeping write failed would leave
the data changed *and* the run marked failed — the worst of both. The failure is
logged loudly instead.

## The submission form

The admin page renders a form for each task from the `params_schema` on
`/tasks/types`. That schema is derived from the params struct's `ToSchema`
derive, so it cannot drift from what the API will accept, and the help text
under each field is the doc comment written on it — which is a reason to write
them well.

Adding a parameter to a task therefore puts it on the form with no frontend
change. Two conventions the form depends on:

- **A blank field is omitted, not sent as zero or null.** The server then
  applies the struct's `#[serde(default)]`. An empty number box becoming `0`
  would be rejected by nearly every `validate_params` here, and confusingly so.
- **A closed set renders as a dropdown.** `survey`, and anything else that is a
  Rust enum, arrives in the schema as `enum` and cannot be mistyped.

A task whose parameters are a tagged union — `enrich_reprocess`'s `selection` —
falls back to a JSON field rather than a half-working widget that would need to
guess at the variants.

## Access

Admin-only, and both login realms are accepted: the BOOM API's `users` and
Babamul's `babamul_users` both carry `is_admin`, and
[`src/api/admin.rs`](../src/api/admin.rs) is the one check. Babamul admins are
declared in config as `babamul.admin_emails` and reconciled onto accounts at API
startup — in both directions, so removing someone from the list revokes their
access on the next restart rather than leaving a grant nobody remembers making.

The actor recorded on a run is qualified by realm (`babamul:<id>`), because the
two id spaces are unrelated and the point of recording it is being able to look
the person up later.

## API

| Route | |
| --- | --- |
| `GET /tasks/types` | What this release can run, with a JSON Schema per task |
| `POST /tasks` | Submit a run |
| `GET /tasks` | Runs, most recent first |
| `GET /tasks/{id}` | One run |
| `GET /tasks/{id}/logs?after_seq=` | Tail its logs |
| `POST /tasks/{id}/cancel` | Request cancellation |
| `GET /data/mutations?collection=&limit=` | What has been done to the data |

## Not yet built

- **Recurring runs.** The run record already distinguishes `trigger` (`api` vs
  `schedule`) and has a `system` actor, so a schedule needs an enqueue loop and
  a cron expression on `TaskSpec`; `single_flight_key` already stops a schedule
  stacking runs up. Worth noting that the first candidate — trimming old LSST
  cutouts ([#518](https://github.com/boom-astro/boom/issues/518)) — is better
  served by a TTL index on the cutout documents than by a task, with only the
  one-off backfill of existing rows running here.
- **Ledger coverage beyond tasks.** `data_mutations` records task runs today.
  Startup migrations and the live pipeline have `SourceKind` variants reserved
  but do not write to it yet.
- **Partitioned execution**, for tasks whose unit of work is a key range rather
  than a chunk.
