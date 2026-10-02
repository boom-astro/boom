# Distributed ML inference via external model providers

Research spike: let independent instances act as ML model providers instead
of full replicas. A provider registers with BOOM, the enrichment worker
sends it alerts for inference, and scores flow back into the pipeline. This
decouples model development from the core system and makes enrichment a
small distributed system.

Status: proposal, not implemented. Nothing here changes runtime behavior.

## 1. Where we start

- Models run in-process. `src/enrichment/models/mod.rs` defines
  `SharedModels` (one mutex-guarded ONNX `Session` per model) and
  `SharedModelPool` (one set per GPU device, round-robin assignment).
  Models load once at scheduler startup from `data/models/*.onnx`.
- The contract is the Rust `Model` trait
  (`src/enrichment/models/base.rs`): `predict(metadata, triplets)` returns
  scores. Only ONNX fits; a PyTorch or JAX service cannot participate
  without a Rust rewrite plus an ONNX export.
- Batching is local and GPU-sensitive. Each worker pulls one
  `enrichment.batch_size` batch from Valkey per loop
  (`src/enrichment/base.rs`), pads it to a fixed shape, and shares one
  CUDA stream with villar-pso. Per-alert RPCs would destroy that
  efficiency.
- Scores land on the alert document in MongoDB and flow downstream to
  filtering and the `babamul.*` Kafka topics. Provenance today is implicit
  (whatever version of the `.onnx` file was on disk).

Goal: an outside team runs GPUs anywhere, registers an endpoint, and gets
batches to score. Non-goals: replacing local models, active-active alert
writes, or a general plugin system beyond enrichment.

## 2. Proposed design

### 2.1 Provider contract

One inference interface over HTTP or gRPC, framework-agnostic:

```text
POST /v1/predict
{ "model": "btsbot", "version": "2.0.0", "batch": [ { "candid": 123, "features": {...}, "triplet_ref": "..." } ] }
-> { "model": "btsbot", "version": "2.0.1", "scores": [ {"candid": 123, "p": 0.97} ] }
```

- Small features go inline; large cutouts (63x63x3 triplets) go by
  reference (candid plus a short-lived fetch grant against the API or
  MongoDB) so we do not ship images over the WAN twice.
- Responses must echo `model + version`. The worker stores both alongside
  every score, turning today's implicit provenance into explicit per-alert
  fields.
- Providers advertise `max_batch`, `timeout_p99`, and supported surveys.
  Unknown fields are ignored; missing scores are errors, not zeros.

### 2.2 Registration and discovery

- Phase 1 (static): providers are listed in config, e.g.
  `enrichment.providers: [{ name, endpoint, models, timeout_ms }]`.
  Same pattern as other BOOM config: base default in `config.yaml`,
  per-deployment values in `overrides.yaml`, secrets via `BOOM_` env vars.
- Phase 2 (dynamic): providers heartbeat into a MongoDB `ml_providers`
  collection (`endpoint, models, version, capacity, expires_at`) or a
  DNS SRV record. The enrichment worker resolves per batch,
  health-checks, and load-balances. An independent site then joins by
  registering an endpoint, never by joining the replica set.
- Removal is lease-based: a missed heartbeat expires the provider and
  traffic shifts to remaining providers or the local fallback.

### 2.3 Execution modes

- Sync RPC with timeout and fallback (recommended first). The worker sends
  one batch RPC per provider inside `process_alerts`, waits up to the
  deadline, and on failure either uses the local ONNX copy or records
  `model_unavailable` and continues. Simple, preserves pipeline ordering,
  but a slow provider stalls its batch.
- Async per-provider queue (later, when providers have divergent
  latencies). The worker enqueues to a Valkey list or Kafka topic per
  model; providers drain at their own pace; a join step reassembles
  results before filtering. Decoupled and backpressured, but needs reorder
  logic and a straggler policy (emit without the slow model after N
  seconds vs. hold the alert).

### 2.4 Batching, GPU locality, and data flow

```text
Valkey enrichment queue -> worker pulls batch -> builds features once
  -> fan out: local ONNX models run in-process, remote batch sent per provider
  -> merge scores onto alert doc -> MongoDB write -> filter queue
```

- The worker keeps grouping candids with the existing `batch_size` and
  sends one batch RPC per provider, never one RPC per alert.
- Prefer providers that accept preprocessed tensors over raw alerts; the
  worker already pays the preprocessing cost once for local models.
- Providers that need cutouts fetch them by reference rather than
  receiving pixels inline, keeping WAN payloads small.

## 3. Cross-cutting concerns

- Failure: per-provider timeouts, retries with backoff, circuit breaker,
  and explicit fallback. A bad or slow provider degrades to fewer scores,
  never to a stuck pipeline or poisoned MongoDB writes. Validate response
  schemas before merging.
- Security: token or mTLS auth on provider endpoints, per-provider rate
  limits, and short-lived fetch grants for cutout-by-reference. Providers
  get alert features, not database credentials.
- Observability: per-provider latency histograms, error and fallback
  rates, batch-size distributions, and version labels on every score, all
  visible in the existing Grafana and tracing setup.
- Versioning: config pins the minimum accepted model version; responses
  carrying a newer version are recorded, not rejected, so upgrades are
  visible before they are required.

## 4. Rollout

- Phase 0: extract an `InferenceClient` enum (`Local(SharedModels)` vs.
  `Remote(endpoint)`) behind the current `Model::predict` call sites. No
  behavior change; enables testing both paths.
- Phase 1: one remote provider via static config, sync RPC, explicit
  fallback, and a dashboard. Proves the contract and the operational
  load (auth, timeouts, provenance).
- Phase 2: dynamic registry with heartbeats, async queues for slow
  providers, independent provider autoscaling, and versioned score
  queries in the API.

## 5. Open questions

1. Payload shape: inline features vs. provider-fetches-all. Measure WAN
   cost of triplets before committing.
2. Straggler policy for async mode: hold vs. emit-partial, per survey.
3. Score schema evolution: how filters pin model versions without
   redeploys.
4. Cost attribution: per-provider usage accounting if providers are
   externally operated.

## 6. References

- Current code: `src/enrichment/base.rs` (worker loop and queues),
  `src/enrichment/models/mod.rs` (`SharedModels`, `SharedModelPool`),
  `src/enrichment/models/base.rs` (`Model` trait, triplet prep),
  `docs/gpu.md` (ONNX and CUDA setup), `docs/alert-processing.md`
  (pipeline order: alert, enrichment, filter).
- Related spike: `docs/replication.md` (full-site replicas and failover;
  providers are the lightweight alternative to joining as a replica).
