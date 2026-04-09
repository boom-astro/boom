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

For quick testing only, a permissive workaround is:

```bash
sudo chmod 777 /srv/boom/kafka
```

That is acceptable for short-term testing, but production should use the exact
UID/GID or a tighter permission model instead of leaving the directory world-writable.

That's it. Now BOOM will deploy to production on each GitHub release.
