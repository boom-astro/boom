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

For a Docker Compose deployment, put the same variables in the `.env` file
**on the server**. Compose reads `.env` from the project directory and
substitutes it into the service definitions.

One thing to know: `docker-compose.yaml` does **not** blanket-forward `BOOM_*`
variables into containers — each service enumerates the variables it wants
under `environment:`. `BOOM_MILVUS__*` is wired into:

- **`scheduler-ztf`** — runs the CIDER fusion model, so it produces the embeddings
- **`api`** — for serving similarity queries

If you add another service that needs Milvus, it must declare these variables
too, or it will silently fall back to the `config.yaml` defaults (i.e. disabled)
even with `.env` set correctly.

If BOOM is instead run under Kubernetes, the equivalent is putting
`BOOM_MILVUS__PASSWORD` in a Secret and referencing it with `secretKeyRef`;
pods do not read `.env` files.

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

## Regenerating the client

The gRPC client is generated at build time from the protos vendored in
`proto/milvus`, pinned to milvus-proto `v2.6.20`. `protoc` is supplied by the
`protoc-bin-vendored` build dependency, so no system package is needed. See
`proto/milvus/README.md` for how to move to a newer version.
