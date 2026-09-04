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

Submission is single-flight per target, not per type: two ingests of the same
catalog would race on the same collection and chunk state, but ingesting 2MASS
should not block ingesting NED.

Still to port, so that the last reasons to SSH in go away: `enrich_reprocess`,
`migrate_fp_flux`, `migrate_snr`, `reprocess_crossmatch`, `copy_cutouts`,
`prepare_catalog`. Each needs a params struct, an arm in `dispatch`, and a
cancellation check in its batch loop; the ones that drive their work through
Valkey already have the resumability a task needs.

## Collections

| Collection | Holds |
| --- | --- |
| `task_runs` | One document per run: params, status, actor, progress, lease, error. Also the queue. |
| `task_logs` | Log lines, batched — one document per flush, not per line. The UI tails by asking for `seq` greater than the last it saw. |

`task_logs` is a convenience copy for the UI; the full firehose still reaches
Loki through the normal container-log path. It is capped per run so a task
logging in a loop cannot fill the disk.

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
| `GET /tasks/types` | What this release can run |
| `POST /tasks` | Submit a run |
| `GET /tasks` | Runs, most recent first |
| `GET /tasks/{id}` | One run |
| `GET /tasks/{id}/logs?after_seq=` | Tail its logs |
| `POST /tasks/{id}/cancel` | Request cancellation |

## Not yet built

- **Recurring runs.** The run record already distinguishes `trigger` (`api` vs
  `schedule`) and has a `system` actor, so a schedule needs an enqueue loop and
  a cron expression on `TaskSpec`; `single_flight_key` already stops a schedule
  stacking runs up. Worth noting that the first candidate — trimming old LSST
  cutouts ([#518](https://github.com/boom-astro/boom/issues/518)) — is better
  served by a TTL index on the cutout documents than by a task, with only the
  one-off backfill of existing rows running here.
- **The `data_mutations` ledger.** Runs record what they did, but there is not
  yet a single append-only log of every mutation across tasks, startup
  migrations and the live pipeline.
- **Partitioned execution**, for tasks whose unit of work is a key range rather
  than a chunk.
