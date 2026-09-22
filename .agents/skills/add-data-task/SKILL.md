---
name: add-data-task
description: Use when adding, porting or running a job that mutates BOOM's data outside the live alert pipeline — a migration, backfill, reprocess, catalog ingest or one-off fix. Covers writing the task, the five registration points, testing it locally, and running it against production data from a branch. Also use when tempted to add a binary under src/bin for such a job; that is what this replaces.
---

# Adding a data task

Anything that changes BOOM's data outside the alert pipeline runs as a **task**,
not as a binary someone runs over SSH. `src/bin/` holds services (`api`,
`scheduler`, `kafka_consumer`, `kafka_producer`, `task_worker`) and two tools
that change nothing (`check_config`, `add_filter`). Adding a data-mutating
binary there is the one thing to avoid: it is untracked, unresumable, and cannot
be watched or cancelled by whoever needs to.

Full reference, including why: [`docs/task-system.md`](../../../docs/task-system.md).

## 1. Write the body

Copy the skeleton in the **Adding a task** section of `docs/task-system.md` into
`src/tasks/<your_task>.rs`. Mirror an existing task close to your shape:

| If the job… | Copy |
| --- | --- |
| walks a collection in batches | `src/tasks/repair_photometry.rs` |
| derives a field from what is already stored | `src/tasks/backfill_host_galaxy.rs` |
| fans out over shards | `src/tasks/backfill_hpx.rs` |
| drives work through Valkey | `src/tasks/enrich_reprocess.rs` |
| downloads and inserts | `src/tasks/catalog_ingest.rs` |

Four things differ from a binary, all because the process is no longer yours:

- **`process::exit` becomes an error.** Exiting kills the worker and every other
  run on it.
- **Check `ctx.is_canceled()` at batch boundaries**, and return
  `TaskError::Canceled`. A boundary is the only place where stopping leaves a
  state you can describe in one sentence.
- **Report with `ctx.progress(...)`, `ctx.info(...)`, `ctx.warn(...)`**, not a
  progress bar nobody is watching.
- **Call `ctx.record_mutation(...)`** when you have changed data, so the ledger
  can answer "why does this collection look like this".

Parameters are a `#[derive(Serialize, Deserialize, ToSchema)]` struct with a
`validate_params`. Doc comments on its fields become the help text on the admin
form, and the JSON Schema is derived from the struct, so there is no frontend
work.

## 2. Register it — five places, all in `src/tasks/mod.rs`

Like API routes, missing one fails quietly rather than loudly:

1. `pub mod <your_task>;`
2. An entry in `TASKS` — `idempotent` decides whether a lost lease is retried or
   failed; `destructive` makes the admin page demand confirmation.
3. An arm in `validate_params` (the API rejects bad parameters at submit).
4. An arm in `dispatch`.
5. An arm in `single_flight_key`, if two concurrent runs would collide. Key it
   on the narrowest thing that actually conflicts, usually the survey or the
   catalog, so unrelated runs are not blocked.

`cargo test --lib tasks::tests` catches a registration you forgot.

## 3. Test it locally

```sh
make dev            # API + task worker under cargo-watch, sharing the dev MongoDB
```

The admin page at `/admin` lists every task with a form built from your params.
Or submit by curl:

```sh
TOKEN=$(curl -s -X POST localhost:4000/auth \
  -d "username=$ADMIN_USER&password=$ADMIN_PASSWORD" | jq -r .access_token)
curl -s -X POST localhost:4000/tasks -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"task_type": "your_task", "params": {"survey": "ztf"}}'
```

Write unit tests for the parts that need no database — parameter validation, the
filter or pipeline your task builds, defaults. Tests that touch MongoDB need the
dev stack up (see `AGENTS.md`).

## 4. Run it against production data, from your branch

You do not need to merge first, and you do not need a shell on the box beyond
the deploy host's compose. From your branch's checkout there:

```sh
export BOOM_GIT_SHA=$(git rev-parse HEAD)
docker compose --profile prod build api task-worker
docker compose --profile prod up -d api task-worker
```

Both, because the API validates `task_type` against its own registry at submit:
a worker that knows your task and an API that does not gives a 400, not a run.
Consumers, schedulers and enrichment workers stay on the deployed release.

`BOOM_GIT_SHA` is compiled in, so the ledger entry names the commit that ran
without anyone writing it down. Put both services back with
`docker compose --profile prod up -d --force-recreate api task-worker` from a
clean checkout.

## Checks before you push

```sh
cargo fmt --all
cargo clippy --lib --bins --tests --message-format=short | grep src/tasks/
cargo test --lib tasks::                  # registry + your task's unit tests
cargo build --tests                       # integration targets compile too
```

## Pitfalls

- **A task that is not resumable must not say `idempotent: true`.** That flag is
  what decides whether a lost lease is retried, and a half-applied migration run
  twice is the one failure nobody notices.
- **Parameters that carry a URI carry a password.** Redaction keys on the field
  name (`*_uri`, `uri`); name the field that way and it is masked in API
  responses and the ledger.
- **Do not leave the binary behind after porting one.** A binary that still
  exists is the path people will keep using.
- **Long runs belong on the task worker, not the API.** Anything expected to run
  for minutes or more goes here rather than in a request handler.
