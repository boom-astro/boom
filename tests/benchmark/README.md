# BOOM benchmark

An end-to-end benchmark of BOOM as deployed: several surveys' nights queued in
Kafka and then drained, against a production-sized database, with every
enrichment feature switchable. It produces the numbers for the BOOM II paper and is meant
to be rerun when the system changes.

`tests/throughput/` is a different tool and stays as it is. It is CI's
regression gate: a small, fast run whose wall time is compared from one commit
to the next. This harness answers different questions:

| | `tests/throughput` | `tests/benchmark` |
|---|---|---|
| Input | one public ZTF night, queued in Kafka before BOOM starts | any mix of ZTF, LSST, DECam and WINTER nights, queued in Kafka before BOOM starts |
| Measures | wall time to drain the backlog | throughput per survey and per stage, headroom over the night's busiest minute, completeness, resources |
| Database | the night's ZTF objects plus NED | a slice of production: every object, neighbor and catalog row the replay touches, rolled back to the night's start |
| Features | whatever the CPU image runs; most catalogs empty | each one switched by a profile: catalogs, host association, Babamul, GPU inference and Villar fits, Milvus |
| Filters | 25 copies of one filter | filters exported from a deployment |
| Repeats | one run | plans with interleaved repeats, reported as median and range |

## Requirements

- Docker with Compose v2. Add the NVIDIA Container Toolkit for GPU runs.
- [uv](https://docs.astral.sh/uv/). The harness has its own locked environment.
  Run every command below from the repository root, prefixed with
  `uv run --project tests/benchmark`.
- Disk for `$BENCH_DATA_ROOT` (default `~/boom-bench`): bundles, the seed
  database, and a copy of the seed per run. On btrfs or XFS the copy is a
  reflink and costs almost nothing.
- `--cache cold` (the default) drops the host's page cache from a privileged
  container. Use `--cache asis` where that is not allowed.
- Runs that replay LSST need outbound internet. The LSST alert worker fetches
  packet schemas from Rubin's registry.

Ports are offset from the dev stack's so both can run at once: Mongo 27117,
Valkey 16379, Kafka 19092, API 14000, Milvus 19631, seed Mongo 27217. Override
them with the same-named environment variables (`MONGO_HOST_PORT`, ...).

## Quick start: the old night, new harness

This needs no production access. It reuses the old test's data, so it doubles
as a smoke test of the harness:

```sh
B="uv run --project tests/benchmark"
# 1. A bundle from the archived night. The old test downloads it to
#    data/alerts/ztf/public/20250311 (from the test-data-v1 GitHub release).
$B tests/benchmark/prepare_bundle.py --survey ztf --night 20250311 \
   --source data/alerts/ztf/public/20250311 --out ~/boom-bench/bundles/ztf-20250311-public
# 2. A seed with the old fixture (NED and the night's ZTF objects).
$B tests/benchmark/seed_db.py start
$B tests/benchmark/seed_db.py load-legacy --remove-new-objects
$B tests/benchmark/seed_db.py stop
# 3. The night as Paper I ran it, then with today's features.
$B tests/benchmark/run.py --bundle ~/boom-bench/bundles/ztf-20250311-public \
   --profile paper1-like --n-filters 25
$B tests/benchmark/run.py --bundle ~/boom-bench/bundles/ztf-20250311-public \
   --profile babamul --n-filters 25
```

Each run prints a headline and leaves its results in `logs/benchmark/<run>/`.
The runs warn that most configured catalogs are empty. That warning is the
point: with this fixture, most of the cross-match cost does not exist.

`--remove-new-objects` fixes a gap in the old fixture. The fixture holds a
document for every one of the night's 27,948 objects, including the 4,860
first detected that night. BOOM cross-matches an object and associates its
host only when it creates the object's document. So on the fixture as
published, the replay runs no catalog cross-match at all. Leave the option out
only to reproduce the old numbers (`plans/continuity.yaml` runs both). On any
seed, `run.py` reports how many replayed objects are affected this way.

## The full workflow

### 1. Bundles: the nights to replay

A bundle holds a night's packets byte for byte, plus an index of identifiers,
positions, exposure times and, for captured streams, broker timestamps
(`benchlib/bundle.py`).

- **ZTF, DECam, WINTER** come from archived Avro files. Use `prepare_bundle.py`
  as in the quick start. Partnership ZTF alerts go to their own topic, so give
  `--source` the partnership directory too. The consumer then subscribes to
  both programs.
- **LSST** has no archive in this form. Record a night as it streams with
  `capture.py`, using a consumer group of your own. Start capturing ahead of
  the night you will benchmark. Without a captured night there is no LSST in
  the benchmark.
- Prefer a night when the surveys overlapped, so their alerts find each other
  in cross-survey matching.

Before a long run, check what a plan would send and the night's arrival rates:

```sh
$B tests/benchmark/replay.py --bundle ... --window-seconds 3600 --window-start 0 --plan-only
```

Without a window a run queues the whole night. `--window-seconds 3600
--window-start 0` queues the first hour of each survey's night, which the
comparison plans use to keep runs short. Both start at the night's start, so
one seed serves both.

### 2. The seed database

Every per-alert database operation is keyed by object id or by a few-arcsecond
cone, so a run only needs what its alerts touch, not the whole database.
`slice_fixture.py` copies exactly that out of a production snapshot:

- the replayed objects with their full histories;
- other surveys' objects near each alert, which cross-survey matching reads
  and links;
- catalog rows within each catalog's radius of the alerts. Small catalogs are
  copied whole, as is `MPC_orbits`.

It rolls everything back to the moment the night starts. It removes
photometry from the replayed night, leaves out objects first seen in the night
or later, and drops links to them. Otherwise the replay would push points that
are already there and skip work the night really did. Slice once for the whole
night, with every bundle the plans replay:

```sh
$B tests/benchmark/seed_db.py start
$B tests/benchmark/slice_fixture.py --bundle ztf... --bundle lsst... \
   --source-uri "$PROD_SNAPSHOT_URI" --source-db boom --config config/prod/umn/config.yaml --dry-run
$B tests/benchmark/slice_fixture.py ...same arguments without --dry-run...
$B tests/benchmark/seed_db.py status
$B tests/benchmark/seed_db.py stop        # required: runs copy the seed only when it is stopped
```

`--footprint-radius-deg 0.5` also copies every object within that radius of
the alerts. This keeps index density realistic, at the cost of a larger seed.

Validate the slice once against a full snapshot (`plans/fixture-check.yaml`,
then `compare_runs.py`). Identical cross-survey links, hosts and filter passes
show the slice is complete. The throughput difference shows how much slicing
biases the timing. After that, every sweep can run on the slice.

Memory is controlled directly rather than through database size.
`--cache cold` makes every first read go to disk, `--cache warm` pre-reads the
files, and `--mongo-cache-gb` sizes WiredTiger. Production sits between cold
and warm, so report both (`plans/cache.yaml`).

### 3. A run

`run.py` does the following:

1. Builds the image(s) and writes the run's config: the base `config.yaml`, then
   profiles, then `--set`.
2. Copies the seed and prepares the page cache.
3. Starts Mongo, Valkey, Kafka (and Milvus), inserts the filters, and marks
   `MPC_orbits` fresh.
4. Checks that every configured catalog has rows and that the seed predates
   the replay. With `--strict`, either problem fails the run.
5. Queues the whole window in Kafka.
6. Starts a consumer and a scheduler per replayed survey, and waits until every
   queue is empty and the counts stop moving.
7. Reads the results out of Mongo and Kafka and analyzes them.

`--mode realtime --speed N` instead sends each exposure when it falls due,
with BOOM already running, and adds per-alert latency to the summary. No plan
uses it: a realtime run takes the window's length divided by `--speed`.

Profiles (`profiles/`) switch features. A profile's `benchmark:` block holds
directives for the harness itself:

| Profile | Effect |
|---|---|
| `paper1-like` | NED only, no host association, no Babamul, CPU |
| `ned-only`, `no-host`, `no-babamul`, `babamul` | one feature each |
| `gpu` | CUDA image for the ZTF scheduler: GPU inference, and Villar fits, which only that build computes |
| `cpu` | back to the CPU image after a profile that enabled the GPU |
| `milvus` | embeddings upserted into a local Milvus started with the run; never the shared NRP one |
| `production` | everything on |

Other useful options:

- `--api-load N` runs N API clients during the replay.
- `--milvus-outage 120:180` stops Milvus 120 s after the workers start, for
  180 s.
- `--keep-up` leaves the stack running for the query benchmarks.

### 4. Plans, repeats and the report

`sweep.py plans/<plan>.yaml` runs every step of a plan, interleaving the
repeats. Edit the bundle paths in a plan first. `report.py` then tabulates the
runs with the median and range per step, and can plot one metric against
another.

| Plan | What it shows | Runs | Queued |
|---|---|---|---|
| `night` | the headline: a whole ZTF and LSST night with everything on | 3 | whole night |
| `continuity` | Paper I's night and fixture, so the new numbers connect to the published ones | 9 | whole night |
| `ablation` | what each feature since Paper I costs, step by step | 21 | first hour |
| `scaling` | each worker pool scaled on its own; batch size; GPU count | 60 | first hour |
| `cache` | cold against warm, and the WiredTiger cache size | 10 | first hour |
| `filters` | filter-stage cost against the number of production filters | 10 | first hour |
| `milvus-outage` | that enrichment does not notice Milvus going away | 2 | whole night |
| `fixture-check` | that the sliced seed reproduces a full snapshot | 2 | first hour |

A run takes its setup (copying the seed, starting the stack, loading models)
plus as long as BOOM needs to clear the queues.

```sh
$B tests/benchmark/sweep.py tests/benchmark/plans/night.yaml
$B tests/benchmark/report.py logs/benchmark/*night-* --group-by step
$B tests/benchmark/report.py logs/benchmark/*filters-* --group-by step \
   --plot step per_survey.ZTF.throughput.per_s
```

### 5. Query benchmarks

These run against a stack left up by `run.py --api --keep-up` (add the
`milvus` profile for similarity search):

- `query/skymap.py` times Babamul's skymap search over the replayed night.
  Each map is sent as is, which takes the 3D credible-volume path, and with its
  distance layers removed, which takes the 2D path. Comparing the two gives the
  cost of the volume cut and how many candidates it removes. By default it uses
  the skymaps tracked in `data/`.
- `query/similarity.py` measures similarity-search latency at several
  concurrencies, and the HNSW index's recall@k against an exact search over
  every stored vector.
- `query/api_load.py` simulates concurrent users; `run.py --api-load` runs it
  while BOOM drains the queues.

### 6. Science validation

`science/` checks whether the context BOOM attaches is right: cross-survey
matching rates and chance matches, host redshifts against TNS, and whether
similarity-search neighbors share a class. The GW injection–recovery test is
designed there but not yet scripted. See `science/README.md`.

## What is measured

Each alert is followed through these instants. Every one is joined on survey
and alert id, and none needs tracing:

| Instant | Source |
|---|---|
| alert worker starts on it | the alert's `created_at` |
| its enrichment batch starts | the alert's `updated_at` |
| published | Kafka CreateTime of its Babamul or filter-result message |

`summary.json` reports, overall and per survey:

| Field | Meaning |
|---|---|
| `throughput.per_s` | alerts enriched, divided by the time from the first alert ingested until every stage, filters included, is idle. Model loading comes before and is not counted |
| `throughput.span_s` | that time: how long BOOM took to clear the window |
| `throughput.headroom` | `per_s` over the night's busiest minute of arrivals. Above 1, BOOM clears alerts faster than the survey delivered them at its peak |
| `night` | the whole night's packets, duration, and mean and peak-minute arrival rates, whatever the window |
| `ingest_rate_per_s`, `enrich_rate_per_s` | the alert stage's and the enrichment stage's own rates, to find the stage that limits |

The summary also records:

- completeness: alerts sent, ingested, enriched, classified, given a Villar
  fit, and published, and how many never arrived;
- the longest each queue got;
- each container's CPU and memory;
- API latency under load, and when Milvus was stopped and restarted.

`timeline.png` (`analyze.py --plots`) shows alerts done and queue lengths
against time since the workers started. `run_meta.json` records the commit,
the image ids, the host's CPU, memory, disks and GPUs, and every warning. A
dirty tree is flagged (`git_dirty`).

## Known limits

- **AppleCiDEr has no switch.** It runs in every configuration, `paper1-like`
  included. For Paper I's exact code path, run `tests/throughput` at the
  Paper I commit.
- **Villar fits come only with the GPU image.** In this tree the fit is
  compiled only into the CUDA build. CPU runs therefore have no fits, and the
  `gpu` step of the ablation adds GPU inference and the fits together.
- **Drain throughput is a ceiling.** With a full queue every enrichment batch
  is full. At a live rate batches are smaller and each alert costs more, so
  headroom overstates the margin somewhat. Per-alert latency needs
  `--mode realtime`.
- **BOOM's timestamps are whole seconds.** `created_at` and `updated_at` come
  from `flare::Time`, which drops fractions of a second, so a drain's span is
  uncertain by about a second. Drains shorter than a minute are coarse; queue
  more of the night for those.
- **The first hour is not the busiest.** The comparison plans queue the first
  hour of the night so they share the whole night's seed. Its mix of fields
  and new objects differs from the night's peak; `night` covers the rest.
- **Everything runs on one host.** BOOM, Mongo, Kafka and Valkey share the
  host's cores and disks, which differs from a deployment that spreads them out.
  Read the per-container CPU in `summary.json` with that in mind.
- **Scheduler startup work can overlap the drain.** At startup the scheduler
  propagates `MPC_orbits` to the current epoch. With a full orbit catalog this
  takes a while and competes with the first alerts.
