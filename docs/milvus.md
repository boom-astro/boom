# Milvus vector database

BOOM stores the CIDER fusion model's embeddings in [Milvus](https://milvus.io)
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

BOOM connects with a single **administrative account** owned by the maintainers.
The account, its password, and the database name are not self-service: they are
provisioned once and injected into the deployment's environment. This document
deliberately does not describe how to obtain access to that instance.

If you are standing up your own Milvus (self-hosted or through your own
provider), point `milvus.host` / `milvus.port` at it and supply your own
`BOOM_MILVUS__USERNAME` / `BOOM_MILVUS__PASSWORD` / `BOOM_MILVUS__DATABASE`. The
rest of this document applies unchanged.

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
BOOM_MILVUS__USERNAME=
BOOM_MILVUS__PASSWORD=
BOOM_MILVUS__DATABASE=
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
file reaches the BOOM processes. The administrative credentials therefore live
in `$BOOM_DIR/.env` on the node — set `BOOM_MILVUS__ENABLED=true` alongside
`BOOM_MILVUS__USERNAME`, `BOOM_MILVUS__PASSWORD`, and `BOOM_MILVUS__DATABASE`.

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
| `BOOM_MILVUS__PASSWORD` | **Secret** | the administrative account's password |
| `BOOM_MILVUS__ENABLED` | Variable | `true` |
| `BOOM_MILVUS__USERNAME` | Variable | the administrative account |
| `BOOM_MILVUS__DATABASE` | Variable | the target database |

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

It prints the server version, the databases the configured credentials can see,
and the collections in the configured database. If the configured database is
not in that list, it says so — usually a sign that `BOOM_MILVUS__DATABASE` is
wrong or that the account lacks access to it.

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
