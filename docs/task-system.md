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

Submission is single-flight per target, not per type: two ingests of the same
catalog would race on the same collection and chunk state, but ingesting 2MASS
should not block ingesting NED.

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

## Running it in dev

`make dev` brings up a `task-worker` alongside the API, under cargo-watch like
the other services. It shares the dev MongoDB, so a run kicked off from the
admin page is picked up within a couple of seconds.

It shares the `target` volume with the api and scheduler containers, so all of
them serialize on one cargo build lock. A source edit therefore costs several
sequential rebuilds, and the API can be briefly unavailable while they drain.

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

## Scheduled tasks, when we build them

The first candidate is the LSST cutout retention policy
([#518](https://github.com/boom-astro/boom/issues/518)). Nothing here is built
yet; this records what the current design already supports so that whoever
picks it up does not have to re-derive it.

**What is already in place.** A run carries `trigger` (`api` or `schedule`) and
`Actor::system()`, both on the document from the start, so a scheduled run is
distinguishable from one a person submitted rather than being attributed to
whoever configured the schedule. Everything else a run gets — the lease and
heartbeat, requeue after a lost worker, cancellation, streamed logs, progress,
the `data_mutations` ledger entry — is keyed off the run rather than off what
triggered it, so a scheduled run inherits all of it unchanged.

**What is missing** is where a schedule is declared and the loop that fires it,
plus two things worth getting right the first time.

*Firing exactly once per tick.* Every task-worker in the fleet wakes at the same
cron instant. `single_flight_key` does not solve this: it is a check-then-insert
in the API handler, not an invariant of `queue::submit`, so a scheduler that
enqueues directly bypasses it, and two schedulers racing would both pass the
check anyway. The fix needs no leader election and no new index — give a
scheduled run a deterministic id, `sched:{schedule}:{unix_fire_time}`, and let
the `_id` uniqueness Mongo already enforces settle it. One worker inserts, the
rest take a duplicate-key error and move on.

*Missed ticks.* If the fleet was down across a fire time, maintenance work wants
skip-to-next rather than one backfilled run per missed tick. The work is
cumulative — a single run catches up on everything that accrued.

**Where the schedule lives** is the open design question. Declaring it on
`TaskSpec` keeps "may this run unattended" in code review; storing it in a
collection lets an operator change it from the admin page, which is more in
keeping with the rest of this system. The likely answer is both: the spec says a
task is schedulable and gives a default cron, and the collection holds whether
it is enabled and any override.

**On #518 specifically:** it asks to *offload* cutouts to S3 after ~30 days, not
to delete them — Babamul is meant to read the archived ones back. A TTL index
would quietly destroy exactly the data the issue wants kept, so this is a task
body rather than an index: a chunked, resumable copy-then-delete of the same
shape as `catalog_ingest`, where each chunk is durable before the source rows go.
`CutoutStorage` and the existing `copy_cutouts` task already cover most of the
moving part. The scheduling is the small half; the offload is the work.

## Not yet built

- **Recurring runs.** See [Scheduled tasks](#scheduled-tasks-when-we-build-them)
  above — the run record is ready for them, the loop that fires them is not.
- **Ledger coverage beyond tasks.** `data_mutations` records task runs today.
  Startup migrations and the live pipeline have `SourceKind` variants reserved
  but do not write to it yet.
- **Partitioned execution**, for tasks whose unit of work is a key range rather
  than a chunk.
