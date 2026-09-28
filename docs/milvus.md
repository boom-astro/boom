# Milvus vector database

BOOM stores the AppleCiDEr fusion model's embeddings in [Milvus](https://milvus.io)
so that objects can be retrieved by similarity. The target deployment is the
[NRP-managed Milvus](https://nrp.ai/documentation/userdocs/ai/vector-database/)
service, which NRP runs for us.

The integration to feed the vector embeddings to Milvus is **off by default**. Deployments that don't use it need no
configuration at all.

## What the vectors are

The embeddings come from `data/models/cider_fusion_plus_embedding.onnx`, whose
`fusion_embedding` output is **384 floats**. The model's final operation divides
by the L2 norm, so the vectors are **unit length** — which is why the default
metric is `COSINE` (for normalized vectors this is equivalent to inner product).

The collection's primary key is `object_id`, so it holds **one vector per
object, not per alert**. Writing an object that already exists replaces its
vector, meaning the collection tracks each object's most recently ingested
embedding. The `candid` and `jd` fields record which alert that was.

| Field | Type | Notes |
|---|---|---|
| `object_id` | `VarChar` (max 64) | Primary key, supplied by BOOM (not auto-generated) |
| `embedding` | `FloatVector` (dim 384) | L2-normalized fusion embedding |
| `candid` | `Int64` | Alert the stored embedding came from |
| `jd` | `Double` | Julian date of that alert |

## Credentials

BOOM connects with a single **administrative/user account** owned by the maintainers.

One database serves the whole project and `config.yaml` carries the name (`umn_babamul_vectordb`).

## Connection details

NRP exposes Milvus over **gRPC only** — there is no REST port reachable from
outside the cluster, so the Milvus RESTful API is not an option here. TLS is
terminated with a standard Let's Encrypt certificate, so the system root store
is sufficient (no custom CA, no client certificates).

| | |
|---|---|
| Host | `milvus.nrp-nautilus.io` |
| Port | `50051` |
| Transport | gRPC over TLS (`milvus.tls`, defaults to `true`) |

## Configuring BOOM

Non-secret settings live in `config.yaml` under `milvus:`. Because
`AppConfig` is built with `Environment::with_prefix("boom").separator("__")`,
every field is also settable from the environment: `milvus.database` is
`BOOM_MILVUS__DATABASE`, `milvus.collection.dim` is
`BOOM_MILVUS__COLLECTION__DIM`, and so on.

### Local development

The database name is already in `config.yaml`, so only the credentials need to
go in **`.env`**, which is gitignored and loaded automatically:

```bash
BOOM_MILVUS__ENABLED=true
BOOM_MILVUS__USERNAME=
BOOM_MILVUS__PASSWORD=
```

### On a deployed BOOM server

BOOM and Milvus are hosted separately: BOOM runs on our own servers, while
Milvus is a service NRP hosts. The connection therefore goes over the public
internet to `milvus.nrp-nautilus.io:50051` — BOOM does not need to run inside
NRP's cluster, and nothing about this setup depends on where BOOM is deployed.

However BOOM is launched, all that matters is that `BOOM_MILVUS__*` ends up in
the process environment — `load_config` in `src/conf.rs` reads it from there.

#### Docker Compose via GitHub Actions (Caltech)

This path does **not** use a `.env` file. `.github/workflows/deploy.yaml`
checks out a clean tree (which has no `.env`, since it is gitignored) and
instead injects configuration as job-level environment variables sourced from
**GitHub repository secrets and variables**. Docker Compose then substitutes
those into the `${...}` placeholders in `docker-compose.yaml`.

```
GitHub secrets/variables  ->  deploy.yaml env:  ->  compose substitution  ->  container
```

So git carries the variable *names*; the values live in GitHub's secret store
and are never committed. To enable Milvus in production, set these under
**Settings -> Secrets and variables -> Actions**:

| Name | Kind | Value |
|---|---|---|
| `BOOM_MILVUS__PASSWORD` | **Secret** | the administrative/user account's password |
| `BOOM_MILVUS__ENABLED` | Variable | `true` |
| `BOOM_MILVUS__USERNAME` | Variable | the administrative/user account |

`BOOM_MILVUS__DATABASE` is not in the table because `config.yaml` already
carries the name.

Only the password is secret; the rest are plain variables. If
`BOOM_MILVUS__ENABLED` is unset, Compose defaults it to `false` and the
integration simply stays off.

Some wiring details worth knowing, because all of them fail *silently* rather
than loudly:

1. `deploy.yaml` must list each variable under `env:`. A variable set in GitHub
   but missing from that block never reaches the runner.
2. `docker-compose.yaml` does **not** blanket-forward `BOOM_*` into containers —
   each service enumerates what it wants under `environment:`. `BOOM_MILVUS__*`
   is wired into **`scheduler-ztf`** (runs the AppleCiDEr fusion model, so it produces
   the embeddings) and **`api`** (for serving similarity queries). A new service
   needing Milvus must declare them too, or it falls back to the `config.yaml`
   defaults and quietly runs with Milvus disabled.
3. Every `BOOM_MILVUS__*` entry in `docker-compose.yaml` is written `${VAR:-}`,
   so an unset GitHub variable reaches the container as an **empty string**
   rather than being absent. `load_raw_config` in `src/conf.rs` sets
   `.ignore_empty(true)` on the environment source for exactly this reason:
   without it, an unset `BOOM_MILVUS__DATABASE` would overwrite the
   `config.yaml` name with `""` and BOOM would try to connect to a nameless
   database. The same applies to the empty placeholders in `.env.example`.
   Leaving a variable unset (or blank) keeps the `config.yaml` value.

If BOOM is instead run under Kubernetes, the equivalent is putting
`BOOM_MILVUS__PASSWORD` in a Secret and referencing it with `secretKeyRef`;
pods do not read `.env` files either.

The username and database name are not secret and can be set directly in
`config.yaml` if you prefer.

## Verifying the connection

`milvus_check` confirms the endpoint, credentials, and database name without
writing any data:

```bash
cargo run --bin milvus_check
```

It prints the server version, the databases the configured credentials can see,
and the collections in the configured database. If the configured database is
not in that list, it says so — usually a sign that `BOOM_MILVUS__DATABASE` is
wrong or that the account lacks access to it.

To create the collection and its index (idempotent; existing collections are
left untouched):

```bash
cargo run --bin milvus_check -- --create-collection
```

## Writing embeddings

Once `milvus.enabled` is true, the ZTF enrichment worker upserts each alert's
`fusion_embedding` into the collection right after ML classification, keyed by
`object_id` so a re-observed object overwrites its previous vector. The write is
an `Upsert` RPC batched per enrichment batch.

The worker **connects only** — it does not create the collection. Provision it
once with `milvus_check --create-collection` before starting the workers, since
several enrichment workers run in parallel and must not race to create it.

### A Milvus outage never stops enrichment

Milvus is an optional add-on and Mongo holds the enriched alerts, so nothing in
the Milvus path is fatal. Every failure — failing to connect at startup, or a
failed upsert — is logged and trips a circuit breaker in `MilvusSink`
(`src/milvus/sink.rs`):

- Uploads pause for a backoff that doubles per consecutive failure, from 30s up
  to a 5 minute ceiling. Without this, each batch would pay a full
  `milvus.timeout_seconds` (30s by default) for as long as the outage lasted.
- The connection is dropped and redialled on the first attempt after the pause,
  so recovery needs no worker restart. A success resets the backoff.
- Embeddings produced while the breaker is open are simply dropped. They are
  recomputed the next time an object is observed.

The scheduler's one-time collection provisioning is logged rather than fatal for
the same reason, and the API degrades to "embedding endpoints disabled" when it
cannot reach Milvus at startup.

### The embedding is never written to Mongo

`milvus.enabled` is the only switch. The 384-float vector is stripped from the
alert's Mongo `classifications` document in every case and there is no dual write:

- **Milvus on** — the embedding is written to Milvus only.
- **Milvus off** — the embedding is not stored anywhere; it is computed as part
  of AppleCiDEr inference and dropped.

Either way the AppleCiDEr class probabilities (`applecider_fusion` and
`applecider_outputs`) stay in the Mongo `classifications` document; only the
vector itself is Milvus-only.

## Reading embeddings

`MilvusClient` also exposes the read/manage side of the collection
(`src/milvus/search.rs`):

- `search_embedding(query, top_k)` — similarity search: returns the `top_k`
  nearest `object_id`s to a query vector with their scores (and stored
  `candid`/`jd`), best-first. The query vector is wrapped in a protobuf
  `PlaceholderGroup` (little-endian `f32` bytes) as Milvus requires.
- `get_embeddings(&[object_id])` — fetch stored rows by id, with the vector
  split back out of Milvus's flat column into per-row `Vec<f32>`.
- `count()` — number of objects in the collection (`count(*)`).
- `delete_embeddings(&[object_id])` — remove rows by id.
- `flush()` — seal recent writes so they are immediately searchable (Milvus
  otherwise flushes on its own schedule); handy for read-after-write in a smoke
  test.

Note that `search_embedding` requires the collection to have been **loaded**
(done by `ensure_embedding_collection`), and only rows that have been flushed are
visible to search.

## HTTP endpoints

The same three operations are exposed over HTTP, on both API surfaces. The
handlers are thin wrappers over shared helpers in
`src/api/routes/embeddings.rs`; the duplication is in the routing only, because
the two surfaces authenticate differently and a token for one is not accepted by
the other.

| Operation | Internal API | Babamul API |
| --- | --- | --- |
| Similarity search | `POST /similarity/objects` | `POST /babamul/similarity/objects` |
| Stored count | `GET /embeddings/count` | `GET /babamul/embeddings/count` |
| Delete one (admin) | `DELETE /embeddings/{object_id}` | `DELETE /babamul/embeddings/{object_id}` |

The search takes `{"object_id": ..., "top_k": ...}`. Callers pass an id rather
than a raw 384-float vector: the seed object's stored embedding is resolved
server-side, and the seed is stripped from its own results. `top_k` defaults to
10 and is clamped to 100. A 404 means the object has no stored embedding, which
is an ordinary outcome — only ZTF alerts that clear the AppleCiDEr gate get one.

On the Babamul side, search and count are **public** (listed in
`BABAMUL_PUBLIC_ROUTES`), matching the stats endpoints behind `/dashboard`:
they are read-only and the page is reachable without signing in. The delete is
not, and stays admin-only.

The browser never talks to Milvus directly: it is gRPC on port 50051 behind
credentials that must not ship to a client, so the path is always
browser → BOOM API → Milvus.

### Admins

Deleting an embedding is admin-only on both surfaces, but "admin" means a
different field on each: `User::is_admin` for the internal API, and
`BabamulUser::is_admin` for Babamul. The latter is `#[serde(default)]`, so
accounts predating it read back as non-admin, and there is **no endpoint that
grants it** — set it on the user document directly:

```js
db.babamul_users.updateOne({ email: "someone@example.org" }, { $set: { is_admin: true } })
```

`BabamulUserPublic` carries the flag so the web frontend can hide controls the
server would only refuse.

## Web UI

The frontend serves similarity search at `/embeddings`
(`frontend/src/pages/Embeddings.tsx`): an object id plus neighbor count, results
linking through to each object's page, and a tile showing the stored-embedding
count. That count doubles as a health signal — enrichment pauses embedding
uploads while Milvus is unreachable, and a count that stops growing is the
visible symptom. The delete form renders only for admins.

The page is public, like `/dashboard` — no sign-in needed to search. The delete
form renders only for a signed-in admin.

## Regenerating the client

The gRPC client is generated at build time from the protos vendored in
`proto/milvus`, pinned to milvus-proto `v2.6.20`. `protoc` is supplied by the
`protoc-bin-vendored` build dependency, so no system package is needed. See
`proto/milvus/README.md` for how to move to a newer version.
