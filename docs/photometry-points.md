# Photometry points experiment

This is the first migration step toward storing measurements individually.
It adds a Mongo points implementation and a storage-independent history contract.
Aux remains authoritative; enrichment, filters, and API reads use their existing
paths. No ClickHouse server, Valkey cache, backfill, or retention change is needed.

## Enable a comparison

The default is disabled and creates no points collections. For a disposable
replay or a deliberately selected comparison deployment, set:

```yaml
photometry_points:
  mode: compare
```

`BOOM_PHOTOMETRY_POINTS__MODE=compare` also selects this mode. The scheduler's
Compose environment forwards it. Unknown modes fail configuration loading.

After the normal aux, cutout, and alert writes, the ZTF worker reads an accepted
aux snapshot and writes its measurements into `ZTF_photometry_points`. It reads
them back through `PhotometryStore`, reconstructs the three history arrays, and
compares:

- Complete normalized records, including fields enrichment does not currently use.
- The actual `ZtfAlertForEnrichment` input from BOOM's production lookup pipeline.
- Photometry statistics and detection/episode summaries at the current alert epoch.

The comparison replaces only ZTF history arrays; candidate, host, and cross-survey
context remain identical. It does not run ML inference. Results are stored per
candid in `ZTF_photometry_points_comparisons`.
Each completed attempt records `pass`, `mismatch`, or `error`; a failed attempt
replaces any earlier success for that candid.

Comparison completes before the worker acknowledges Redis. A store failure,
inconsistent revision, or mismatch fails processing and leaves the packet for
BOOM's existing temporary-queue recovery. Replaying an already inserted alert
also runs comparison, repairing a partial points write before the `Exists`
shortcut completes. There is no independent background retry service here.

## Measurement contract

`Measurement` contains an object ID, kind, JD, accepted revision, and the complete
normalized record. Kinds are `detection`, `upper_limit`, and `forced`. Records
retain program IDs, bands, upper limits, signed flux, and forced-photometry fields.
JSON serialization preserves binary floating-point round trips and large integer
IDs. A future backend must preserve these values too.

Schema version 1 identifies a measurement by `(object_id, kind, jd)`, following
BOOM's current per-array JD deduplication. Band and program remain record fields;
adding them to the identity now would change existing overlap semantics. The
portable ID is the lowercase SHA-256 of the compact JSON tuple
`[1, object_id, kind, jd]`, using serde_json's finite f64 representation.
The contract is ZTF-specific: surveys with independent measurements at the same
epoch need a revised exposure identity before using this store.

Mongo keeps one current document per measurement. A unique compound index
supports object-history reads and enforces the identity. Batches contain at most
256 point updates. No document contains an object's growing history array.

A higher accepted revision can replace one measurement; older replays cannot
undo it. An identical replay is idempotent. Different records at the same revision
are errors. Partial unordered writes can be retried using the same IDs/revisions.
Deletions and identity changes are not implemented: unexplained extra points make
comparison fail instead of being silently discarded.

During this migration, the accepted revision comes from the aux version, with
legacy unversioned documents treated as revision zero. It is not a candid, arrival
timestamp, or Kafka offset. Reading accepted aux records preserves the current
overlap decisions without inventing a new winner rule. Concurrent aux changes
cause a bounded comparison retry rather than a parity claim against a moving
source. Versions must advance for source corrections.

`PhotometryStore` exposes point writes and sorted, complete object-history reads
using ordinary Rust/JSON values, with no Mongo query expressions in the contract.
The Mongo implementation is separate from the aux migration adapter. Reads are
internal and unrestricted; they are not a public program-authorization boundary.
Individual point writes are atomic, but a whole-history read is not a transaction
or an enrichment freshness guarantee. Comparison checks the source revision
around reads; a future authoritative reader must define its own completion fence.
Aux can already contain updates from other in-flight packets. Comparison mirrors
that current lookup behavior, not a historical publication snapshot for each point.

## What this establishes, and what follows

This PR establishes representation, backend boundaries, idempotent writes, stale
replay protection, and independent reconstruction tests. It does **not** remove
aux contention or its 16 MB limit yet. Comparison reads full accepted histories,
visits their points, and advances per-point revision metadata. It adds work to
ingestion and is not a throughput optimization or production capacity benchmark.

Before a cutover:

1. Specify direct packet-point acceptance, correction ordering, and a durable
   alert-completion fence without depending on an aux version.
2. Benchmark that ingestion path under one-second same-object arrivals with
   concurrent workers, overlapping histories, corrections, and service failures.
3. Implement a resumable backfill through BOOM's task system and establish parity
   on deployed workloads, including filters that read history through Mongo.
4. Switch enrichment to the history reader, then remove arrays from aux only
   after all dependent readers and filters have a supported path.
5. Add Valkey caching if measurements justify it, with explicit freshness and
   rebuild rules. Compare Mongo and ClickHouse implementations using the same
   measurement contract, reads, and binning workload.

ClickHouse remains an optional future implementation. This PR neither requires
it nor makes Mongo's representation part of the backend interface.

## Validation

```sh
cargo test --lib photometry::
cargo test --test test_photometry_points
make check-configs
```

The integration tests use real MongoDB and UUID-named disposable databases, using
the connection from `tests/config.test.yaml` and normal BOOM environment overrides.
They cover twelve actual alert workers sharing one object at one-second epochs,
concurrent same-object points, duplicate and out-of-order replays,
accepted corrections, equal-revision conflicts, actual partial bulk failure,
real-worker enrichment/feature parity, the existing-alert repair path, unexpected
extra points, and the disabled path. They run in the existing Mongo/S3 CI matrix.
