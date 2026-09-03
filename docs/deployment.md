# Deploying a BOOM system

## Deployment instances

There is more than one BOOM instance running, and they are **not** replicas of
each other. Each has its own MongoDB and its own filters, so data present on
one is not necessarily present on the other.

| | Caltech | UMN |
| --- | --- | --- |
| Role | Primary production instance | Backup / secondary instance |
| Host | Dedicated server (`kaboom`), `*.kaboom.caltech.edu` | HPC cluster at the University of Minnesota |
| Deploy method | Automated: release tag → `Trigger deployment to production` GitHub Actions workflow on a self-hosted runner | Manual, on the cluster |
| Config in this repo | `config/prod/caltech/` | `config/prod/umn/` |

**They diverge in practice.** Filters live in each instance's own database, and
each instance consumes the upstream alert streams with its own Kafka consumer
groups, so ingestion offsets and back-fill history differ too. Treat UMN as an
independent instance that can take over if Caltech is unavailable, not as a
hot standby that is guaranteed to be in sync. If something needs to exist on
both (a filter, a config change), it has to be applied to both.

Users are the exception: the UMN side runs a recurring sync that carries
accounts over, Babamul ones included, so both instances end up holding the same
users without anyone recreating them.

The Caltech instance also serves [Babamul](https://babamul.caltech.edu), the
public-facing alert broker interface for the ZTF and LSST streams.

### Caltech instance specifics

The `kaboom` machine has two persistent storage volumes that the Compose data
paths point at (see [Data volume configuration](#data-volume-configuration)):

- `/scr`: SSD, used for data that benefits from fast I/O (MongoDB, Valkey).
- `/data`: HDD, used for larger, slower-access data (Kafka).

Administrative access to the machine is via SSH using approved credentials. Ask
the BOOM maintainers to grant access by adding your SSH public key (preferred),
or to share credentials through the team's credential manager; avoid sending
passwords over chat or email, and prefer key-based access over password login.

The GitHub Actions secrets and variables for the `production` environment are
the source of truth for deployment configuration (see the
[checklist below](#checklist-of-github-environment-variables-and-secrets)).

Public endpoints, useful for verifying a deployment:

| Endpoint | Purpose |
| --- | --- |
| `https://api.kaboom.caltech.edu/` | API health, returns JSON with no login |
| `https://api.kaboom.caltech.edu/docs` | Interactive API docs (Scalar) |
| `https://babamul.caltech.edu` | Babamul web app |
| `https://grafana.kaboom.caltech.edu` | Grafana dashboards and pipeline statistics |

## How the deployment is set up

BOOM runs as a single-node Docker Compose stack, deployed by a GitHub Actions
workflow running on a self-hosted runner on that same node. This is not a
self-hostable product: the sections below describe how our own deployment hosts
are put together, and are the reference for rebuilding one or standing up
another instance, not a general installation guide.

Everything here past this point is one-time host setup. Routine deploys are
covered in
[Running, verifying, and rolling back a deployment](#running-verifying-and-rolling-back-a-deployment)
and need none of it.

### Host prerequisites

1. A server, with the DNS records for the deployment's domain pointing at its IP.
1. A wildcard subdomain (e.g. `*.kaboom.caltech.edu`) so the individual
   components can each have their own host: `api.kaboom.caltech.edu`,
   `grafana.kaboom.caltech.edu`, and so on.
1. [Docker](https://docs.docker.com/engine/install/) (Docker Engine, not Docker
   Desktop).
1. [Git LFS](https://git-lfs.com/).

### Create a public Traefik reverse proxy

Traefik handles incoming connections and HTTPS certificates. This is done once
per server, before the first BOOM deploy.

Create a directory on the host to store the Traefik Docker Compose file. Put it
on persistent storage: on `kaboom` this is under `/scr` rather than
`/root/code`, so the paths below use it. Adjust them for another host:

```bash
mkdir -p /scr/ztf/dev/traefik-public/
```

Copy the Traefik Docker Compose file to the host with `scp` or `rsync` from a
local checkout:

```bash
rsync -a config/docker-compose.traefik.yml root@your-server.example.com:/scr/ztf/dev/traefik-public/
```

This Traefik instance expects a Docker "public network" named `traefik-public`
to communicate with BOOM's API and Kafka instance. A single public Traefik proxy
handles HTTP and HTTPS with the outside world, and behind it there can be one or
more stacks on different domains on the same server, which is what would allow,
for example, a production and a staging instance to coexist on one machine.

To create the `traefik-public` network, run the following on the host:

```bash
docker network create traefik-public
```

The Traefik Docker Compose file reads several environment variables from the
shell that starts it, so set them on the host first. `USERNAME` and `PASSWORD`
are the HTTP basic auth credentials for the Traefik dashboard; `HASHED_PASSWORD`
is derived from `PASSWORD` with OpenSSL:

```bash
export USERNAME=admin
export PASSWORD=changethis
export HASHED_PASSWORD=$(openssl passwd -apr1 $PASSWORD)
export DOMAIN=kaboom.caltech.edu
export EMAIL=admin@$DOMAIN
```

`DOMAIN` is the deployment's apex domain and `EMAIL` is the address Let's
Encrypt registers the ACME certificates against. Then start Traefik from the
directory holding the Compose file:

```bash
cd /scr/ztf/dev/traefik-public/
docker compose -f docker-compose.traefik.yml up -d
```

When redeploying an existing Traefik, reuse the values it was last deployed
with.

A few notes for maintaining an existing Traefik deployment:

- `config/docker-compose.traefik.yml` is generic and has needed no changes in
  normal operation. It is copied to the host by hand, so if it ever does change
  upstream, copy the new version over and restart Traefik. This is the only
  manual file copy in the deployment.
- Use a monitored mailbox for `EMAIL` (a team alias is fine) so Let's Encrypt
  expiry and recovery notices are actually read, and keep it stable across
  redeploys; changing it triggers a certificate regeneration, during which
  HTTPS is unavailable.
- The Traefik dashboard password is not critical and can be rotated freely;
  `DOMAIN` and `EMAIL` cannot, since the certificates depend on them.

### Configure a GitHub Actions self-hosted runner for continuous deployment (CD)

The runner runs as a dedicated `github` user, which needs Docker access. As
`root` on the host:

```bash
adduser github
usermod -aG docker github
```

Then switch to that user:

```bash
su - github
```

As the `github` user,
[install a GitHub Actions self-hosted runner following the official guide](https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/adding-self-hosted-runners#adding-a-self-hosted-runner-to-a-repository).
When asked about labels, add `production`: the
[deploy workflow](/.github/workflows/deploy.yaml) targets
`runs-on: [self-hosted, production]`. Labels can also be added later.

The guide ends by telling you to start the runner in the foreground. Don't;
install it as a service instead, so it survives logout and reboots. Leave the
`github` shell and run this back as `root`, from the `actions-runner` directory
in the `github` user's home. Use `~github` rather than `/home/github`: that home
is not necessarily under `/home` (on `kaboom` it is under `/scr`).

```bash
exit
cd ~github/actions-runner
./svc.sh install github
./svc.sh start
./svc.sh status
```

You can read more about this in the official guide:
[Configuring the self-hosted runner application as a service](https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/configuring-the-self-hosted-runner-application-as-a-service).

Installing it as a service matters because it is what makes the runner come back
after a host reboot. If a deploy is triggered but the job never starts, check
**Settings → Actions → Runners** in GitHub: a grey/offline runner means the
service isn't running, and `./svc.sh status` in that directory (as `root`, or
with `sudo`) will say why. Note that a down runner only blocks *new deploys*;
the running BOOM stack is unaffected, since Compose restart policies bring the
containers back on reboot on their own.

### Set secrets for the GitHub Actions deployment workflow

All deployment configuration lives in this repository's `production` GitHub
environment, as **variables** (non-sensitive: domains, consumer group IDs,
data paths) and **secrets** (database, API admin, and Kafka passwords, signing
keys). Nothing is read from a `.env` file on the host. Add them under
**Settings → Secrets and variables → Actions**, following the
[official GitHub guide for setting repository secrets](https://docs.github.com/en/actions/security-guides/using-secrets-in-github-actions#creating-secrets-for-a-repository).

[.github/workflows/deploy.yaml](/.github/workflows/deploy.yaml) is the
authoritative list: anything it references must exist. When a change introduces
a new configuration key that the workflow needs to inject, it has to be added
both to the environment settings and to the workflow's `env:` block, or Compose
will fail at interpolation time.

For generated production configs, [sync-configs workflow](/.github/workflows/sync-configs.yaml)
runs `make configs` on every pull request.
For pull requests opened from branches in this repository, it commits any
generated config changes back to the PR branch automatically.
For fork-based pull requests, GitHub does not safely allow that write-back, so
the workflow fails if generated configs are stale.
The same workflow also runs `make check-configs`, which validates every
generated config at `config/prod/*/config.yaml` via the BOOM parser.
Under the hood, it calls `check_config {path}` for each generated config and
fails if any config is invalid.

#### Checklist of GitHub environment variables and secrets

The `production` environment must define everything the
[deploy workflow](/.github/workflows/deploy.yaml) references. The runner checks
out a clean tree on every deploy (no `.env` file), so every value below comes
from a GitHub Actions **variable** (non-sensitive) or **secret** (sensitive);
nothing is read from a file on the host. A required value that is missing makes
`docker compose` fail at interpolation time, before anything starts.

App settings that are not in this list (e.g. `babamul.webapp_url`, the admin
username/email, crossmatch catalogs) are read from
`config/prod/<deployment>/config.yaml` and intentionally do **not** have env vars
here. Only values that compose interpolates (image build args, Traefik labels,
volume paths, and the specific env keys injected into containers) belong here.

**Variables** (`vars.*`):

| Variable | Required? | Notes |
| --- | --- | --- |
| `DOMAIN` | Yes | Apex domain, e.g. `kaboom.caltech.edu`. |
| `BOOM_CONFIG_PATH` | Yes | Generated prod config, e.g. `./config/prod/caltech/config.yaml`. |
| `STACK_NAME` | No | Hard-coded to `boom` in the workflow; not a GitHub var. |
| `BOOM_API__DOMAIN` | No | Defaults to `api.${DOMAIN}`. |
| `WEBAPP_DOMAIN` | No | Host the web app is served on; defaults to `DOMAIN`. |
| `VITE_PRERELEASE_MODE` | No | `true` gates unreleased features; defaults to `false`. |
| `VITE_PUBLIC_POSTHOG_KEY` | No | PostHog project key; blank disables analytics. Also supplies the server-side `BOOM_POSTHOG__PROJECT_API_KEY`, so web and API activity merge onto one person and a single variable turns both off. |
| `VITE_PUBLIC_POSTHOG_HOST` | No | PostHog host, e.g. `https://us.i.posthog.com`. Also supplies the server-side `BOOM_POSTHOG__HOST`. |
| `BOOM_BABAMUL__ENABLED` | No | Defaults to `false`. |
| `BOOM_BABAMUL__REGISTRATION_ENABLED` | No | Whether the API accepts new account creation; defaults to `true`. Pairs with `VITE_PRERELEASE_MODE`: that decides what the web app offers, this decides what the API allows. |
| `BOOM_GPU__ENABLED` | No | Set `true` to run ONNX inference on GPU. Overrides `config.gpu.enabled`, and selects the `docker-compose.cuda.yaml` overlay. |
| `BOOM_GPU__DEVICE_IDS` | No | Comma-separated CUDA device IDs (e.g. `0,1`). Unset, the deployment config's `gpu.device_ids` applies. Only relevant when `BOOM_GPU__ENABLED=true`. |
| `BOOM_DATA_MONGODB_PATH` | No | Host bind mount for MongoDB; falls back to a named volume. |
| `BOOM_DATA_VALKEY_PATH` | No | Host bind mount for Valkey; falls back to a named volume. |
| `BOOM_DATA_KAFKA_PATH` | No | Host bind mount for Kafka; falls back to a named volume. |
| `BOOM_DATA_CUTOUTS_MONGODB_PATH` | No | Host bind mount for the dedicated cutouts MongoDB (the workflow maps it to compose's `BOOM_CUTOUTS_MONGO_VOLUME`); falls back to a named volume. |
| `BOOM_MONGO_MEM_LIMIT` | No | Memory limit for the alerts MongoDB container, which is what caps its WiredTiger cache. Unset means unlimited, and both mongos then size their cache off total host RAM. See [.env.example](/.env.example) for sizing. |
| `BOOM_CUTOUTS_MONGO_MEM_LIMIT` | No | Same, for the cutouts MongoDB container. |
| `BOOM_KAFKA__CONSUMER__ZTF__SERVER` | Yes | ZTF Kafka bootstrap server. Reused for the WINTER consumer, which shares the same (unauthenticated) broker. |
| `BOOM_KAFKA__CONSUMER__ZTF__GROUP_ID` | Yes | ZTF consumer group ID (per-program suffix added by compose). |
| `BOOM_KAFKA__CONSUMER__LSST__GROUP_ID` | Yes | LSST consumer group ID. |
| `BOOM_KAFKA__CONSUMER__LSST__USERNAME` | Yes | LSST SASL username. |
| `BOOM_KAFKA__CONSUMER__WINTER__GROUP_ID` | Yes | WINTER consumer group ID (the broker itself comes from `BOOM_KAFKA__CONSUMER__ZTF__SERVER`). Kept here, not in the committed config, because the repo is public and the group ID is what an attacker would reuse to join our group and disrupt ingestion on the unauthenticated broker. |
| `KAFKA_EXTERNAL_HOST` | No | Public Kafka hostname for the EXTERNAL listener; defaults to `localhost`. |
| `PROMETHEUS_USER` | Yes | Basic-auth user for the Prometheus endpoint. |
| `GRAFANA_ADMIN_USER` | No | Grafana admin user; defaults to `admin`. |
| `SMTP_SERVER` | No | Blank disables outbound email, and setting it is all that is required to enable it. The workflow deliberately does not inject `SMTP_USERNAME`/`SMTP_PASSWORD`, so the API relays unauthenticated on port 25. |
| `SMTP_FROM_ADDRESS` | No | From address for outbound email; defaults to `noreply@boom.example.com`. |
| `BOOM_API_RATE_LIMIT_AVERAGE` | No | Traefik rate limit; defaults to `50`. |
| `BOOM_API_RATE_LIMIT_BURST` | No | Traefik rate limit; defaults to `200`. |
| `BOOM_API_RATE_LIMIT_PERIOD` | No | Traefik rate limit; defaults to `1s`. |

**Secrets** (`secrets.*`):

| Secret | Required? | Notes |
| --- | --- | --- |
| `BOOM_DATABASE__PASSWORD` | Yes | MongoDB root password for the alerts database. |
| `BOOM_CUTOUTS_STORAGE__PASSWORD` | Yes | Root password for the dedicated cutouts MongoDB. Both `up -d` steps layer in `docker-compose.cutouts-mongo.yaml`, which reads it as `${BOOM_CUTOUTS_STORAGE__PASSWORD:?...}`, so a missing value aborts the deploy at interpolation time. |
| `BOOM_API__AUTH__SECRET_KEY` | Yes | JWT signing key (32+ chars). |
| `BOOM_API__AUTH__ADMIN_PASSWORD` | Yes | Bootstrap admin password. |
| `BOOM_BABAMUL__OAUTH__GOOGLE__CLIENT_ID` / `..._CLIENT_SECRET` | No | Google social sign-in. A provider is enabled only when both halves are non-empty, so leaving them unset simply leaves that button off. The redirect URI registered with the provider must be `<redirect_base_url>/babamul/oauth/google/callback`, where `redirect_base_url` comes from `config/prod/<deployment>/config.yaml`. |
| `BOOM_BABAMUL__OAUTH__GITHUB__CLIENT_ID` / `..._CLIENT_SECRET` | No | Same, for GitHub. |
| `BOOM_BABAMUL__OAUTH__ORCID__CLIENT_ID` / `..._CLIENT_SECRET` | No | Same, for ORCID. The `orcid_sandbox` key in the deployment config picks sandbox.orcid.org over orcid.org. |
| `KAFKA_ADMIN_PASSWORD` | Yes | SASL admin password used by the ACL init script. |
| `KAFKA_READONLY_PASSWORD` | Yes | SASL read-only password for external Kafka access. |
| `BOOM_KAFKA__CONSUMER__LSST__PASSWORD` | Yes | LSST SASL password. |
| `PROMETHEUS_HASHED_PASSWORD` | Yes | bcrypt hash for Prometheus basic auth (store the raw hash; do **not** `$$`-escape it as you would in a `.env`). |
| `GRAFANA_ADMIN_PASSWORD` | Yes | Grafana admin password. |
| `SLACK_WEBHOOK_URL` | No | Grafana alerting webhook; blank uses a placeholder (alerts still fire, POSTs 404). |

### Production config layout

The repository keeps the development baseline in [config.yaml](../config.yaml).
Production-specific changes live under deployment-specific directories in
`config/prod`, for example:

```text
config/prod/
   caltech/
      overrides.yaml
      config.yaml
   umn/
      overrides.yaml
      config.yaml
```

Each deployment gets its own directory here: `caltech/` and `umn/`. UMN is
still deployed manually, but reads its generated config from this repo.

- `overrides.yaml` is the only file you edit for a deployment-specific config.
- `config.yaml` in each deployment directory is generated from the base config
   plus that deployment's overrides.
- Generated files are intended to be committed so the final production config
   is reviewable in pull requests.

To regenerate all committed production configs, run:

```bash
make configs
```

This target scans `config/prod/*/overrides.yaml` and writes the merged config
to `config/prod/*/config.yaml`.

For production, set `BOOM_CONFIG_PATH` in the GitHub Actions environment to the
generated config you want to deploy, for example:

```text
./config/prod/caltech/config.yaml
```

If `BOOM_CONFIG_PATH` is not set, Docker Compose falls back to `./config.yaml`.
That fallback is useful for local development, but production environments
should always set `BOOM_CONFIG_PATH` explicitly.

### Data volume configuration

The main Compose file uses parameterized volume sources for stateful services:

- `BOOM_DATA_MONGODB_PATH` controls MongoDB storage.
- `BOOM_DATA_VALKEY_PATH` controls Valkey storage.
- `BOOM_DATA_KAFKA_PATH` controls Kafka storage.

If these variables are unset, Docker Compose falls back to named Docker
volumes:

- `mongodb`
- `valkey`
- `kafka_data`

That default is appropriate for local development because it requires no host
filesystem preparation.

In production, either keep using named volumes or point each variable at a host
path for explicit bind mounts, which makes backup and storage management easier.
Caltech uses bind mounts, splitting them across the machine's two volumes:
MongoDB and Valkey on the SSD (`/scr`), Kafka on the HDD (`/data`). Set them
like:

```text
BOOM_DATA_MONGODB_PATH=/srv/boom/mongodb
BOOM_DATA_VALKEY_PATH=/srv/boom/valkey
BOOM_DATA_KAFKA_PATH=/srv/boom/kafka
```

When using host paths in production:

1. Create the directories on the deployment host before the first deploy.
2. Ensure the Docker daemon can read and write those directories.
3. Keep those paths stable across deploys.

Kafka bind mounts need one extra check. The Kafka container user must be able
to write to `BOOM_DATA_KAFKA_PATH`. If you see permission errors during broker
startup, fix ownership or permissions on the host directory.

Recommended options (in order of preference):

1. **Prefer Docker named volumes** (`kafka_data`) when possible, which avoids
   host filesystem permission management entirely.
2. **Fix ownership for the Kafka container's runtime user.** Kafka typically
   runs as UID 1000 in the container:

   ```bash
   sudo chown -R 1000:1000 /srv/boom/kafka
   sudo chmod 750 /srv/boom/kafka
   ```

3. **Use infrastructure provisioning** (cloud-init, Ansible, Terraform, etc.)
   to pre-provision the target directory with correct ownership and permissions
   at deploy time, ensuring repeatable deploys.

If you are still seeing permission errors after one of the above, confirm the
UID/GID the Kafka image actually runs as (it can differ between image versions)
and `chown` the directory to match. Avoid world-writable (`chmod 777`)
permissions, even temporarily: on a shared host any process could read or
corrupt Kafka data.

## GitHub deploy safety controls

Production deploys are intentionally constrained by both repository settings and
the workflow in [`.github/workflows/deploy.yaml`](/.github/workflows/deploy.yaml):

1. A repository ruleset named `Tag creation` is active for tag refs (`~ALL`).
   It enforces tag creation/update/deletion protections, with bypass actors set
   to repository roles 2 and 5 (maintainers/admins).
1. The `production` environment has a deployment branch/tag rule that only
   allows tags matching `v*`.
1. The workflow enforces the same model at runtime:
   - it checks that the actor has `maintain` or `admin` repository access.
   - it validates that the selected deploy ref is a tag matching `v*`.

In practice, this means only approved release tags can be deployed to
production, reducing the risk of accidental or unauthorized production changes.

## Running, verifying, and rolling back a deployment

### Triggering a deployment

Deployments run through
[`deploy-trigger.yaml`](/.github/workflows/deploy-trigger.yaml), which calls the
reusable [`deploy.yaml`](/.github/workflows/deploy.yaml). There are two ways in:

- **Publish a release** on GitHub with a `v*` tag. This is the normal path.
- **Run `Trigger deployment to production` manually** (Actions tab → Run
  workflow) and give it the version tag to deploy, e.g. `v1.0.4`. This is the
  path used for rollbacks.

No SSH access to the deployment host is needed for either. The job runs on the
self-hosted runner, checks out the tag, and runs three Compose commands. When
`BOOM_GPU__ENABLED` is `true` it also layers in `-f docker-compose.cuda.yaml`,
shown as `$GPU` here:

```bash
docker compose --profile prod -f docker-compose.yaml $GPU build
docker compose --profile prod -f docker-compose.yaml -f docker-compose.cutouts-mongo.yaml $GPU up -d
docker compose --profile prod -f docker-compose.yaml -f docker-compose.cutouts-mongo.yaml $GPU up -d --force-recreate --no-deps grafana docker-metadata-exporter
```

The third command is not redundant: `grafana` and `docker-metadata-exporter` run
off a bind-mounted repo file, so editing it leaves the service definition
unchanged and `up -d` keeps the old container.

Deploys cause brief downtime: Compose stops each service's container and starts
a new one from the freshly built image, so expect a window of roughly half a
minute where services such as the API are restarting.

On a **fresh** server, the Traefik reverse proxy must be up before the first
BOOM deploy: the Compose file references the `traefik-public` network as an
external network and the deploy fails if it doesn't exist. See
[Create a public Traefik reverse proxy](#create-a-public-traefik-reverse-proxy).
That is a one-time step; routine deploys never touch Traefik.

### Verifying a deployment

Once the workflow finishes green:

1. **Ping the API**: the API root should return JSON immediately with no login
   (for Caltech, `https://api.kaboom.caltech.edu/`).
2. **Check the web app**: `https://babamul.caltech.edu` exercises the API, so
   basic functionality working there is a good sign. If the release changed
   front end code, test what changed: object search, object pages, alert search,
   the Kafka docs page, and the statistics dashboard are the high-traffic paths.
3. **Check Grafana**: confirm ingestion and processing rates look normal and no
   alerts are firing.
4. **Optional:** on the host, `docker compose -p boom ps` should show every
   service up, with the ones that have a healthcheck reporting `healthy`. The
   one exception is `kafka-acl-init`, a one-shot that ends at `Exited (0)`.
   Always pass `-p`: it makes Compose query the engine by project label instead
   of loading the compose files, which is what you want since there is no
   `.env` on the host and interpolation would fail. The project name is pinned
   to `boom` by the `name:` key at the top of `docker-compose.yaml`.

### Rolling back

Re-run `Trigger deployment to production` manually with the last known-good
version tag. Because the workflow deploys whatever tag it is given, this rolls
back both the application and its generated config, and it is usually faster
than reverting commits and cutting a new tag.

**Note:** this does not roll back the deploy pipeline itself.
`deploy-trigger.yaml` calls `deploy.yaml@main` (GitHub forbids expressions in
`uses`), so an old tag is always deployed by today's workflow.

Rolling back the application does **not** roll back data: MongoDB, Kafka, and
Valkey state stays on disk across a deploy. If a release migrated data in a way
that an older version can't read, a tag rollback alone is not sufficient.

## Managing users on an instance

Users are per-instance (see [Deployment instances](#deployment-instances)) and
can only be created by an admin. The bootstrap admin account is created from
`BOOM_API__AUTH__ADMIN_PASSWORD` and the admin username/email in that
deployment's `config/prod/<deployment>/config.yaml`.

The easiest route is the interactive API docs (`/docs` on the instance's API,
e.g. `https://api.kaboom.caltech.edu/docs`): authenticate at the top of the
page, then run the `POST /users` endpoint. Equivalently, `POST /auth` to get a
token and then `POST /users` with it. Non-admin callers get a `403`.

Creating a user at Caltech does not itself create it at UMN, but the UMN side
runs a recurring sync that carries accounts over, Babamul ones included, so
both instances end up holding the same users.

