# Deploying a BOOM system

## Option 1: Single node with Docker Compose and a GitHub Actions self-hosted runner

### Preparation

1. Have a remote server ready and available.
1. Configure the DNS records of your domain to point to the IP of the server
   you just created.
1. Configure a wildcard subdomain for your domain, so that you can have
   multiple subdomains for different services, e.g. `*.boom.caltech.edu`.
   This will be useful for accessing different components,
   like `traefik.boom.caltech.edu`, `api.boom.caltech.edu`, etc.
1. Install and configure [Docker](https://docs.docker.com/engine/install/) on
   the remote server (Docker Engine, not Docker Desktop).
1. Install [Git LFS](https://git-lfs.com/).

### Create a public Traefik reverse proxy

We need a Traefik proxy to handle incoming connections and HTTPS certificates.
Note this will only need to be done once per server.

Create a remote directory to store your Traefik Docker Compose file:

```bash
mkdir -p /root/code/traefik-public/
```

Copy the Traefik Docker Compose file to your server.
This can be done by running the command `scp` or `rsync` in your local terminal:

```bash
rsync -a config/docker-compose.traefik.yml root@your-server.example.com:/root/code/traefik-public/
```

This Traefik instance will expect a Docker "public network" named
`traefik-public` to communicate with BOOM's API and Kafka instance.

This way, there will be a single public Traefik proxy that handles the
communication (HTTP and HTTPS) with the outside world, and then behind that,
there can be one or more stacks with different domains,
even if they are on the same single server.
This could enable, for example,
a production and staging instance on the same machine.

To create a Docker public network named `traefik-public` run the following
command in your remote server:

```bash
docker network create traefik-public
```

The Traefik Docker Compose file expects some environment variables to be set in
your terminal before starting it.
You can do it by running the following commands in your remote server.

Create the username for HTTP basic auth, e.g.,:

```bash
export USERNAME=admin
```

Create an environment variable with the password for HTTP basic auth, e.g.:

```bash
export PASSWORD=changethis
```

Use OpenSSL to generate the hashed version of the password for HTTP basic auth
and store it in an environment variable:

```bash
export HASHED_PASSWORD=$(openssl passwd -apr1 $PASSWORD)
```

To verify that the hashed password is correct, you can print it:

```bash
echo $HASHED_PASSWORD
```

Create an environment variable with the domain name for your server, e.g.:

```bash
export DOMAIN=boom.caltech.edu
```

Create an environment variable with the email for Let's Encrypt, e.g.:

```bash
export EMAIL=admin@$DOMAIN
```

Go to the directory where you copied the Traefik Docker Compose file in your
remote server:

```bash
cd /root/code/traefik-public/
```

Now with the environment variables set and the `docker-compose.traefik.yml` in
place,
you can start the Traefik Docker Compose project
by running the following command:

```bash
docker compose -f docker-compose.traefik.yml up -d
```

### Configure a GitHub Actions self-hosted runner for continuous deployment (CD)

On the remote server, while running as the `root` user,
create a user for GitHub Actions:

```bash
adduser github
```

Add Docker permissions to the `github` user:

```bash
usermod -aG docker github
```

Temporarily switch to the `github` user:

```bash
su - github
```

Go to the `github` user's home directory:

```bash
cd
```

Next,
[Install a GitHub Action self-hosted runner following the official guide](https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/adding-self-hosted-runners#adding-a-self-hosted-runner-to-a-repository).

When asked about labels, add a label for the environment, e.g. `production`.
You can also add labels later.

After installing, the guide will tell you to run a command to start the
runner.
However, to make sure it runs on startup and continues running,
we can install it as a service.
To do that, exit the `github` user and go back to the `root` user:

```bash
exit
```

Go to the `actions-runner` directory inside of the `github` user's home
directory:

```bash
cd /home/github/actions-runner
```

Install the self-hosted runner as a service with the user `github`:

```bash
./svc.sh install github
```

Start the service:

```bash
./svc.sh start
```

Check the status of the service:

```bash
./svc.sh status
```

You can read more about this in the official guide:
[Configuring the self-hosted runner application as a service](https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/configuring-the-self-hosted-runner-application-as-a-service).

### Set secrets for the GitHub Actions deployment workflow

In your repository settings,
configure secrets for the environment variables you need,
the same ones described above, including `SECRET_KEY`, etc.
Follow the [official GitHub guide for setting repository secrets](https://docs.github.com/en/actions/security-guides/using-secrets-in-github-actions#creating-secrets-for-a-repository).

See [.github/workflows/deploy.yaml](/.github/workflows/deploy.yaml)
for the secrets and GitHub environment variables that should be set.

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

At minimum, the production GitHub environment should define these variables in
addition to the usual secrets used by the deploy workflow:

- `BOOM_CONFIG_PATH`
- `BOOM_DATA_MONGODB_PATH` if you want a MongoDB bind mount instead of a Docker volume
- `BOOM_DATA_VALKEY_PATH` if you want a Valkey bind mount instead of a Docker volume
- `BOOM_DATA_KAFKA_PATH` if you want a Kafka bind mount instead of a Docker volume
- `DOMAIN`

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

For production, you can keep using Docker named volumes, or point each variable
at a host path if you want explicit bind mounts for backup and storage
management, for example:

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
permissions, even temporarily — on a shared host any process could read or
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

## Migrating from a dedicated deploy repo to this one

If the initial deployment had its own repo with BOOM in it as a submodule,
this section describes how
to migrate to deploying directly from here.
The benefit of this approach is that new services added to the stack
and new configuration changes don't need to be manually migrated
to the separate repo,
which reduces the amount of manual work required to make changes and
deploy.
We seek to make deployments as automated and painless as possible so we
realize a constant stream of safe changes into production.

In this example, we are using a single node as a GitHub Actions self-hosted
runner, which is already running a production instance that we want to
migrate over to deploying from this repo.
We therefore want to retain all of the production data and minimize downtime.
We will move the self-hosted runner to the organization level rather than
the repo level,
which will allow us to continue deploying from the separate repo until it is
archived.

In this case there was also a separate front end repo started manually with
Docker Compose.
Here we are also merging the front end server into the main stack, so both
of the old projects will need to be stopped.

### Phase 0: Inventory the existing deployment

Before touching anything, capture the current state so the cutover is
mechanical and reversible.

1. Record the data locations the old stack is actually using. On the host,
   inspect the running containers and the old repo's environment to resolve the
   absolute paths behind `BOOM_DATA_MONGODB_PATH`, `BOOM_DATA_VALKEY_PATH`, and
   `BOOM_DATA_KAFKA_PATH`:

   ```bash
   docker inspect mongo broker valkey \
     --format '{{.Name}}{{range .Mounts}} {{.Source}} -> {{.Destination}}{{end}}'
   ```

   The old repo required these variables to be host bind mounts (no Docker
   named-volume fallback), so all stateful data — including the Mongo and Kafka
   data we must preserve — lives at host paths, not in Docker volumes. This is
   what makes a fast, copy-free swap possible. **Caution:** if the old paths
   were set relative (e.g. `./data/kafka`), the data physically lives inside the
   old runner's checkout directory. Resolve them to absolute paths now.
1. Note the old runner's labels (`self-hosted`, `production`) and confirm the
   new repo's [deploy workflow](/.github/workflows/deploy.yaml) targets the same
   labels — it does (`runs-on: [self-hosted, production]`). This is why a single
   org-level runner can serve both repos during the transition.
1. Note the old project/stack names so you can find their containers and
   networks later (`docker compose ls`, `docker network ls`). Remember there are
   two old Compose projects to stop: the BOOM stack in `../boom-deploy-kaboom`
   and the separately-started front end project (now merged into this stack).

### Phase 1: Preparation (no downtime, done ahead of time)

1. Ensure all variables and secrets from the deployment repo have been copied
   over to the main repo. Since we want app behavior to remain the same, it's
   important that these are identical.
1. **Stabilize the data paths.** If the old `BOOM_DATA_*_PATH` values were
   relative to the old checkout, move the data to stable, checkout-independent
   absolute locations so neither repo's working directory matters, for example
   `/srv/boom/{mongodb,valkey,kafka}`. Do this while the old stack is still up
   only if you use a live-safe method; otherwise defer the move into the cutover
   window (Phase 3) to avoid copying a hot database. Set the new repo's
   `BOOM_DATA_MONGODB_PATH`, `BOOM_DATA_VALKEY_PATH`, and `BOOM_DATA_KAFKA_PATH`
   production variables to these absolute paths. Because these are bind mounts,
   pointing the new stack at the same paths reuses the exact on-disk Mongo and
   Kafka data with zero copying — no Docker volume migration is required.
1. Pre-stage everything that doesn't require stopping the old stack: push the
   release tag to the new repo, confirm `make check-configs` passes in CI, and
   confirm the Traefik `traefik-public` network already exists on the host (it
   is shared and should not be torn down).

### Phase 2: Move the self-hosted runner to the organization level

The runner is currently registered at the old repo level. We move it to the
org so both the old repo (temporarily) and this repo can deploy to it. This
lets us validate the new deployment and fall back to the old repo if needed,
before archiving it.

1. Remove the repo-level runner service on the host:

   ```bash
   cd /home/github/actions-runner
   sudo ./svc.sh stop
   sudo ./svc.sh uninstall
   ./config.sh remove --token <REPO_REMOVAL_TOKEN>
   ```

1. Re-register the same runner against the organization, keeping the
   `production` label, then reinstall the service:

   ```bash
   ./config.sh --url https://github.com/<org> --token <ORG_TOKEN> \
     --labels production
   sudo ./svc.sh install github
   sudo ./svc.sh start
   sudo ./svc.sh status
   ```

1. In the org runner settings, grant runner-group access to both the old deploy
   repo and this repo so either can dispatch jobs during the transition.
1. Sanity check: trigger a no-op or `workflow_dispatch` deploy from the **old**
   repo and confirm it still lands on the org runner. At this point nothing has
   changed for production except where the runner is registered.

### Phase 3: Cutover (the short downtime window)

The goal is a fast swap with no data loss. Because the stateful data is on host
bind mounts, `docker compose down` (without `-v`) leaves all data on disk
untouched; the only downtime is the stop/start gap.

1. Quiesce the old stack to get a clean Kafka/Mongo shutdown, then stop both old
   projects **without removing volumes or data**. From the old repo checkout:

   ```bash
   # NEVER pass -v here — that would delete data. Bind-mounted data survives
   # `down` regardless, but stay disciplined.
   docker compose -f docker-compose.yaml down
   ```

   Then stop the separate front end project the same way (`docker compose down`
   in its directory).
1. If you deferred the data move from Phase 1, do it now while everything is
   stopped (a cold copy is consistent):

   ```bash
   rsync -aHAX --delete /old/path/mongodb/  /srv/boom/mongodb/
   rsync -aHAX --delete /old/path/kafka/    /srv/boom/kafka/
   rsync -aHAX --delete /old/path/valkey/   /srv/boom/valkey/
   ```

   If instead you keep the original paths, skip the copy and simply point the
   new repo's `BOOM_DATA_*_PATH` variables at those existing directories.
1. Re-apply the Kafka directory ownership the broker image needs (see the
   "Data volume configuration" section above) so the new stack's broker can
   write to `BOOM_DATA_KAFKA_PATH`.
1. Deploy from this repo: publish the release (or run the `Deploy to production`
   workflow via `workflow_dispatch` with the `v*` tag). The job runs on the
   org-level runner, checks out this repo, and brings up the merged stack —
   including the front end — with `docker compose --profile prod up -d`, using
   the same Mongo and Kafka data on disk.

> Note on Docker named volumes: this repo's Compose file falls back to named
> volumes (`mongodb`, `valkey`, `kafka_data`) only when `BOOM_DATA_*_PATH` is
> unset. Since we set those paths to the old host directories, no named-volume
> swap is needed. If a future deployment did rely on named volumes, the
> equivalent "swap" would be to set `COMPOSE_PROJECT_NAME` to the old project
> name (so `<project>_<volume>` resolves to the same physical volume) or to
> declare the volumes `external`, rather than copying volume contents.

### Monitoring data (Prometheus & Grafana): optional, best-effort

Unlike Mongo, Kafka, and Valkey, the monitoring data does **not** carry over
automatically, and it is not mission-critical:

- The old stack stored Prometheus and Grafana data on host bind mounts
  (`BOOM_DATA_PROMETHEUS_PATH`, `BOOM_DATA_GRAFANA_PATH`).
- This stack stores them in **named Docker volumes** (`prometheus_data`,
  `grafana_data`) that are not parameterized to host paths, so pointing a
  variable at the old directory will not work the way it does for Mongo/Kafka.
- Loki, Tempo, and Promtail are new in this stack and start empty.

The recommended default is to **let them start fresh**:

- Grafana datasources and dashboards are provisioned as code from
  `./config/grafana/provisioning` and `./config/grafana/dashboards`, so they are
  recreated on startup. Only ad-hoc dashboards, users, annotations, and alert
  state live in `grafana_data`.
- Prometheus data is just historical metrics; new metrics accumulate
  immediately after cutover.

If you do want to preserve the history, copy the old host-path data into the new
named volumes during the cutover window (Phase 3), after the new stack has
created the volumes (run `docker volume ls` to find their exact
`<project>_<volume>` names):

```bash
docker run --rm \
  -v boom_prometheus_data:/dest -v /old/path/prometheus:/src:ro \
  alpine sh -c 'cp -a /src/. /dest/'
docker run --rm \
  -v boom_grafana_data:/dest -v /old/path/grafana:/src:ro \
  alpine sh -c 'cp -a /src/. /dest/'
```

Then restart the affected services so they pick up the copied data.

### Phase 4: Verify

1. Confirm all services are healthy: `docker compose --profile prod ps` shows
   everything `running`/`healthy`.
1. Verify data continuity: Mongo collection counts and recent documents match
   pre-cutover expectations, and Kafka topics/offsets and consumer group lag are
   intact (the consumers resume from their committed offsets).
1. Confirm the public endpoints (API, front end, Traefik dashboard) respond over
   HTTPS through the shared `traefik-public` proxy.

### Phase 5: Decommission the old repo

1. Once the new deployment is verified stable, remove the old deploy repo's
   access to the org runner group and disable/delete its deploy workflow so it
   can no longer deploy.
1. Archive `../boom-deploy-kaboom` (and the old front end repo).
1. After a safe retention period, clean up any now-unused old data directories
   if you copied to new absolute paths in Phase 3 (keep them as a backup until
   you are confident in the new deployment).

### Rollback

If the new deployment misbehaves before Phase 5, roll back quickly:

1. `docker compose --profile prod down` on the new stack (no `-v`).
1. Re-deploy from the old repo against the org runner (its workflow still
   targets the same labels), pointing at the original — or restored — data
   paths. Because no data was destroyed and the old repo retained runner access
   until Phase 5, this returns you to the prior known-good state.
