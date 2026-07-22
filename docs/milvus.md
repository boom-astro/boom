# Milvus vector database

BOOM stores the CIDER fusion model's embeddings in [Milvus](https://milvus.io)
so that objects can be retrieved by similarity. The target deployment is the
[NRP-managed Milvus](https://nrp.ai/documentation/userdocs/ai/vector-database/)
service, which NRP runs for you — there is no operator to install and no
instance to create.

The integration is **off by default**. Deployments that don't use it need no
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

## Getting credentials

1. Make sure you are a member of an NRP **group** that has the Vector DB
   capability enabled. If you are a namespace admin you can create one;
   otherwise ask your namespace admin to add you.
2. Go to the NRP portal's `/milvus` page and click **Get milvus password**. A
   link to a secure page containing your credentials is emailed to you.
3. Work out your database name. NRP derives it from your **group** name,
   converting dashes to underscores (Milvus database names allow only
   alphanumerics and underscores).

   Do not assume it matches your Kubernetes namespace — it often doesn't. Ours
   is **`umn_babamul_vectordb`**, not `umn_babamul`, because the group carries a
   `-vectordb` suffix. If in doubt, run `milvus_check` and read the list of
   databases it prints.

## Connection details

NRP exposes Milvus over **gRPC only** — there is no REST port reachable from
outside the cluster, so the Milvus RESTful API is not an option here. TLS is
terminated with a standard Let's Encrypt certificate, so the system root store
is sufficient (no custom CA, no client certificates).

| | |
|---|---|
| Host | `milvus.nrp-nautilus.io` |
| Port | `50051` |
| Transport | gRPC over TLS |

## Configuring BOOM

Non-secret settings live in `config.yaml` under `milvus:`. Because
`AppConfig` is built with `Environment::with_prefix("boom").separator("__")`,
every field is also settable from the environment: `milvus.database` is
`BOOM_MILVUS__DATABASE`, `milvus.collection.dim` is
`BOOM_MILVUS__COLLECTION__DIM`, and so on.

### Local development

Put real values in **`.env`**, which is gitignored and loaded automatically:

```bash
BOOM_MILVUS__ENABLED=true
BOOM_MILVUS__USERNAME=your-nrp-username
BOOM_MILVUS__PASSWORD=the-password-from-the-emailed-page
BOOM_MILVUS__DATABASE=umn_babamul_vectordb
```

> **Never put a real password in `.env.example`.** That file is committed to
> git; `.env` is not. `.env.example` carries the variable *names* with empty
> values so that others know what to set.

### On a deployed BOOM server

BOOM and Milvus are hosted separately: BOOM runs on our own servers, while
Milvus is a service NRP hosts. The connection therefore goes over the public
internet to `milvus.nrp-nautilus.io:50051` — BOOM does not need to run inside
NRP's cluster, and nothing about this setup depends on where BOOM is deployed.

However BOOM is launched, all that matters is that `BOOM_MILVUS__*` ends up in
the process environment — `load_config` in `src/conf.rs` reads it from there.
BOOM is deployed differently at different sites, so pick the section that
matches yours.

#### Apptainer on MSI (UMN)

BOOM runs on Minnesota Supercomputing Institute HPC nodes, launched by
`apptainer.sh` (branch `apptainer`; see the
[boom-umn](https://github.com/boom-astro/boom-umn) repo for the deployment
guide). There are no GitHub secrets involved.

`apptainer.sh` has a `load_env()` that does:

```bash
set -a
source "$BOOM_DIR/.env"
set +a
```

`set -a` auto-exports every variable, and Apptainer inherits the host
environment by default (the script uses no `--cleanenv`), so anything in that
file reaches the BOOM processes. Add the credentials to `$BOOM_DIR/.env` on the
node:

```bash
BOOM_MILVUS__ENABLED=true
BOOM_MILVUS__USERNAME=your-nrp-username
BOOM_MILVUS__PASSWORD=the-password
BOOM_MILVUS__DATABASE=umn_babamul_vectordb
```

`chmod 600` it. No change to `apptainer.sh` is needed. Restart the affected
services for the new values to be picked up.

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
| `BOOM_MILVUS__PASSWORD` | **Secret** | the password from the NRP portal |
| `BOOM_MILVUS__ENABLED` | Variable | `true` |
| `BOOM_MILVUS__USERNAME` | Variable | your NRP username |
| `BOOM_MILVUS__DATABASE` | Variable | e.g. `umn_babamul_vectordb` |

Only the password is secret; the rest are plain variables. If
`BOOM_MILVUS__ENABLED` is unset, Compose defaults it to `false` and the
integration simply stays off.

Two wiring details worth knowing, because both fail *silently* rather than
loudly:

1. `deploy.yaml` must list each variable under `env:`. A variable set in GitHub
   but missing from that block never reaches the runner.
2. `docker-compose.yaml` does **not** blanket-forward `BOOM_*` into containers —
   each service enumerates what it wants under `environment:`. `BOOM_MILVUS__*`
   is wired into **`scheduler-ztf`** (runs the CIDER fusion model, so it produces
   the embeddings) and **`api`** (for serving similarity queries). A new service
   needing Milvus must declare them too, or it falls back to the `config.yaml`
   defaults and quietly runs with Milvus disabled.

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

It prints the server version, the databases your credentials can see, and the
collections in the configured database. If the configured database is not in the
list, it says so — that almost always means the group-name-to-database-name
conversion is wrong.

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
several enrichment workers run in parallel and must not race to create it. If
Milvus is enabled but unreachable at startup the worker fails fast; a failure
during an individual upsert is logged and non-fatal (the alerts are already
enriched and persisted in Mongo).

### Keeping a Mongo copy — the `WRITE_EMBEDDING_TO_MONGO` toggle

When `milvus.enabled`, the embedding always goes to Milvus. The compile-time
constant `WRITE_EMBEDDING_TO_MONGO` in `src/milvus/mod.rs` decides whether to
*also* keep it in the alert's Mongo `classifications` doc:

- `true` **(beta default)** — dual-write to Mongo and Milvus, so a Milvus/NRP
  outage never loses an embedding.
- `false` — strip the 384-float vector from the Mongo doc; it lives only in
  Milvus.

Changing it requires a rebuild.

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

## Regenerating the client

The gRPC client is generated at build time from the protos vendored in
`proto/milvus`, pinned to milvus-proto `v2.6.20`. `protoc` is supplied by the
`protoc-bin-vendored` build dependency, so no system package is needed. See
`proto/milvus/README.md` for how to move to a newer version.
