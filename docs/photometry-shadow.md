# ZTF photometry shadow proposal

This implements an opt-in experiment in the real ZTF alert worker. MongoDB
remains authoritative and retains its full histories. The experiment measures
whether ClickHouse can reproduce those histories and the photometry features
used by enrichment, and whether delivery survives failures at the store/queue
handoffs. It is not a production cutover or a document-size solution yet.

## The implemented path

1. After decoding and sanitizing an alert, the worker upserts an outbox reference
   keyed by candid, before either the existing-alert shortcut or Mongo ingestion.
   The reference contains the object ID, retry state, and timestamps. It does not
   duplicate the photometry payload. Mongo majority write concern is used.
2. The normal worker writes aux, cutouts, and finally the alert. Its existing Redis
   acknowledgement and enrichment queue behavior continues. A ClickHouse outage
   does not block this path. A failed Mongo outbox write does fail processing,
   leaving the packet subject to BOOM's existing temporary-queue recovery.
3. A background drainer claims bounded batches with expiring, uniquely owned
   leases. It checks that the corresponding Mongo alert exists. References whose
   alert never finished stay visible as `waiting_for_mongo` and are not delivered.
4. It reads the current authoritative aux document atomically, including its
   version. This snapshot supplies both the data and the ClickHouse revision.
   No ordering assumption about Kafka offsets, candids, or retry wall time is
   needed. An initial document without a version uses zero.
5. It writes that accepted history to ClickHouse and reads it with `FINAL`.
   Histories are compared field by field, and the same ZTF enrichment
   deserializers and photometry-analysis functions run on both sides. If the
   Mongo version changed during comparison, the entry retries a current snapshot.
6. A per-object status record is persisted before the owned outbox reference is
   deleted. Failed delivery or parity leaves the reference for bounded-backoff
   retry. A crash after the sink write can repeat delivery safely. An interrupted
   worker's lease expires; a replacement worker can recover it while idle.

The drainer reads the accepted *current Mongo aux snapshot*, not the original
packet history. This avoids applying an older packet's metadata over a record
Mongo retained. It matches the current enrichment lookup behavior; it does not
provide a historical snapshot of exactly what existed at an earlier publication
instant. The alert-presence gate applies to the reference, not a separate commit
record for every point already present in an object's aux history.

## Data contract

ClickHouse stores complete normalized records, including upper limits, signed
flux, forced-photometry input fields, zero points, program IDs, and extra fields
needed by current clients and filters. BOOM's existing unit conversions run once.
The JSON parser's `float_roundtrip` feature preserves the exact binary floating
values that otherwise differed by one bit with the real fixtures.

The ZTF key is `(object_id, kind, band, jd, programid)`, and `revision` is the
Mongo aux version. This follows the current epoch-based object history model.
Other surveys and independent exposures at identical keys need an explicit
measurement identity. A higher accepted Mongo version can replace a record.
Deletions or changes to identity fields are not implemented: extra sink rows
produce a visible parity failure instead of being silently accepted.

Raw reads and on-demand bins use `FINAL`. `Store::bins(object_id, cadence_seconds)`
reads normalized columns without parsing payloads. Fixed UTC buckets support
1–31536000 seconds; 86400 seconds gives UTC calendar days. Measurement kinds,
bands and programs stay separate. Inverse-variance flux and uncertainty exclude
upper limits and missing flux, which remain visible in record counts. Negative
flux is valid. No additional quality-mask policy is applied. The native example
exposes `bins OBJECT_ID CADENCE_SECONDS`; this internal method does not implement
public program authorization or a persisted bin cache. There is
no incremental sum materialized view that could count overlapping packets twice.

Each drain claims at most `drain_rows` references and checks their own completed
Mongo alerts in one query. Completed references for the same object share one
source snapshot and verification. Snapshots stream into inserts capped at 4 MiB;
a larger source snapshot is sent alone. Each row retains its source revision.
A shared insert failure acknowledges nothing; a bad source is isolated from
other objects. Successful references are deleted with individual lease-owner
fences only after parity and status persistence.

Full productive batches drain again without an idle sleep. The poll interval
controls idle/error polling and bounds lag-metric refresh frequency. Source
preparation and verification use `concurrent_deliveries` bounded concurrency.
These limits are per alert worker, not a deployment-wide budget.

Full histories are still copied per selected object snapshot. Measure insert
counts, merge work and source overhead before considering incremental extraction. The old seven-column compression
benchmark does not describe the size of these full records.

## Enable in a development stack

Apply `src/photometry/schema.sql` in a dedicated ClickHouse database first.
Then configure the ZTF scheduler, which owns the alert workers:

```sh
BOOM_PHOTOMETRY_SHADOW__URL=http://clickhouse:8123
BOOM_PHOTOMETRY_SHADOW__DATABASE=boom_photometry_shadow
BOOM_PHOTOMETRY_SHADOW__USERNAME=default
BOOM_PHOTOMETRY_SHADOW__PASSWORD=...
BOOM_PHOTOMETRY_SHADOW__POLL_SECONDS=5
BOOM_PHOTOMETRY_SHADOW__DRAIN_ROWS=4
```

An empty URL disables the integration. Credentials come from the environment;
URLs containing credentials are rejected. Production config generation preserves
the empty URL. There is no schema migration or backfill on service startup.
Backfills and administrative repair would need BOOM data tasks before use.

The bounded drain size and poll interval are per alert worker. Inspect the
observed lag before increasing throughput. Mongo collections are
`photometry_shadow_outbox` and `photometry_shadow_status`. Metrics are
`photometry_shadow.pending` and `photometry_shadow.oldest_age_seconds`.
Status records include source/target point counts, exact history differences,
photometry-feature parity, aux revision, and request timings.

A retained orphan is deliberately not auto-deleted: an operator should determine
whether its packet will be recovered before discarding it. The experiment relies
on MongoDB's durability and BOOM's existing recovery of failed ingestion; it does
not add Kafka exactly-once delivery or cross-store transactions.

## Replay and proposal evidence

The `photometry_shadow_replay` example is a development test harness with guards
for loopback services and isolated database names. Its controller lives in the
`boom-clickhouse/examples/boom/tests/proposal.py` checkout. Set `BOOM_SOURCE`
to this checkout when the repositories are not siblings.

It uses the real Avro fixtures in `tests/data/alerts/ztf`, the actual Redis alert
worker, Mongo aux/cutout/alert writes, and the actual enrichment aggregation and
input types. Labeled derivatives of a real packet add late epochs while
retaining its overlapping history. A paced healthy phase overlaps source
ingestion, delivery, and full-history reads, then checks convergence. Fault scenarios cover a reference written
before Mongo completion, ClickHouse HTTP failures after Redis acknowledgement,
idle restart, an expired lease after a successful sink write, the existing-alert shortcut, and a source-accepted metadata correction
followed by an older snapshot replay.

It compares complete histories and photometry properties, including per-band
fits, peaks, detection counts, and episodes. It does not execute ML inference,
host-galaxy or cross-survey feature generation. Feature comparisons for the
hydrated enrichment input use the actual alert epoch as the cutoff.

The report contains bounded field-level parity differences, source/sink counts and versions,
request timings, replay throughput, physical storage statistics from both
stores, pending lag, and a filter dependency inventory. The bundled fixtures are
tiny. These measurements are development evidence, not production capacity or
latency forecasts; Mongo aux also contains non-photometry metadata.

The `photometry_benchmark` example extends this into separate baseline, healthy
shadow, and sink-outage phases using a timestamped, checksum-verified Avro
manifest or a labeled fixture-derived workload. It validates all sink histories
without repairing them and compares a deterministic sample through actual CPU
enrichment properties, model input tensors, predictions, and embeddings. Its
controller, corpus preparation and decision-report documentation are in
`boom-clickhouse/examples/boom/BENCHMARK.md`. Model comparisons run after timed
ingestion; these are ingestion/shadow measurements, not full classification
throughput. Captured content with synthetic pacing is labeled accordingly.

After validation, the example drops each completed disposable Mongo database to
bound disk usage (`keep_phase_data: true` opts out). Failures preserve their phase
data and partial report. `recovery_only: true` runs an independent outage/recovery
experiment without inferring baseline overhead or healthy throughput. Storage
snapshots include per-column compressed/uncompressed bytes, logical payload bytes,
and cumulative dedicated-server insert/merge counters. Counter deltas must be
attributed between phases; file-write counters are not physical device writes.

## The filter boundary remains a decision

The audit uses BOOM's existing field-dependency detector and conservatively flags
whole-document and dynamic field access. A filter reading `prv_candidates`,
`prv_nondetections`, `fp_hists`, or cross-survey equivalents requires review.
Dynamic expressions and collection lookups require review even when a direct
history reference was not found. No filter is automatically approved for a
bounded window, and a stage index is not an inferred window length.

The read-only `photometry_filter_audit` example accepts bare pipelines or BOOM
filter exports, selects `active_fid` from `fv`, and skips inactive documents.
The repository's sample pipelines are only an initial inventory. Run the same
audit on an export of deployed active pipelines before proposing a recent-window
size. Existing Mongo filter execution and program authorization remain in place.
The internal ClickHouse client returns all programs and is not a public API.

Delivery defaults to serial execution. `photometry_shadow.concurrent_deliveries`
(or `BOOM_PHOTOMETRY_SHADOW__CONCURRENT_DELIVERIES`) permits 1–32 parallel
lease-owned deliveries, still capped by `drain_rows` total claims per poll. Each
claim retains its own acknowledgement fence and retry state. Preparation and
verification run concurrently; each chunk is inserted as one synchronous
request. The benchmark records this tuning explicitly; row revision ordering
and source authority are unchanged.

## What would justify the next phase

A production-shaped replay should establish zero unexplained history and
photometry-feature mismatches; no lost completed-source reference across the
recovery scenarios; bounded lag and recovery throughput during sustained
concurrent ingestion; and storage and latency including complete records,
indexes, serialization, and source reads. Test Mongo replica failover and actual
Kafka/Redis restart and recovery separately from the local fault scenarios.

Only then propose Mongo's retained recent window and which filters need a
separate full-history execution path. Retention, deletes/tombstones, a durable
backfill task, schema evolution, and operational ownership must be resolved
before making ClickHouse authoritative for photometry.

## BOOM-owned integration gate

`.github/workflows/photometry-shadow.yaml` runs on PRs and the `clickhouse`/`main`
branches with dedicated Mongo/Redis and a checksum-verified ClickHouse binary.
`python3 tests/photometry_shadow.py` needs no companion repository. It exercises
11 failure/batching scenarios, a disabled baseline with no shadow collections,
all mirrored fixture-derived histories, actual CPU model parity and independent
15-minute/hour/day bin references. It also restores the sink before sending the
second half of the workload and requires measured delivery during new arrivals.
This establishes behavior, not production capacity or permission to trim Mongo.

The benchmark plan's `recovery_overlap: true` uses two manifest-paced segments
separated by a controlled pause. Once the first segment is committed in Mongo,
the sink is restored and the second segment is sent at its relative manifest
spacing. Reports retain actual dispatch times, overlapping source commits and
pending counts. Idle backlog-drain rates are not reported as capacity for this
mode; sampled catch-up during arrivals is a separate observation.

Captured replay is also owned by BOOM. Index unchanged packets with
`python3 tests/prepare_photometry_capture.py CORPUS MANIFEST_JSON --spacing-ms 10`,
then run `python3 tests/photometry_shadow.py --benchmark-only --manifest MANIFEST_JSON
--filters FILTER_EXPORT --deployed-filters --enrichment-samples 32 --timeout-seconds 3600`
against exclusive local test services. The native driver verifies every packet's
SHA-256 and compares every mirrored history after convergence. Archive pacing is
synthetic unless an explicit arrival schedule is supplied; live recovery inserts
a controlled boundary pause. `boom-clickhouse` remains optional research/reporting
material and is not a BOOM build, test, deployment or runtime dependency.

Replay control/upload connections use a 30-second Redis response timeout for
large packets crossing local Docker networking; the alert worker keeps BOOM's
production timeout policy. A failed producer is surfaced before waiting for an
unreachable committed-alert count, and outstanding dispatch is aborted on exit.
Reports of incomplete runs are not capacity evidence. These examples use debug
builds unless explicitly rebuilt otherwise; production acceptance needs the
production build profile and deployment resource limits.
