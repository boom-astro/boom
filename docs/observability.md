# Observability

This guide covers how to run BOOM locally with telemetry,
view dashboards in Grafana, generate alert traffic,
and monitor deployed environments behind Traefik.

## Components and architecture

BOOM is instrumented to emit OTLP **metrics** and **traces** to the OTel
Collector, which fans them out to:

- **Prometheus** — time-series metrics database. Also scrapes infra exporters
  directly (cAdvisor, Node Exporter, MongoDB exporter, Valkey/Redis exporter,
  Kafka exporter, docker-metadata-exporter, otel-collector self-metrics).
- **Tempo** — distributed traces. Tempo's metrics generator also remote-writes
  RED (rate/errors/duration) span metrics back to Prometheus, which powers the
  service-graph and latency panels in Grafana.

Container/service logs are shipped separately:

- **Promtail** subscribes to the read-only `docker-socket-proxy` to discover
  containers and tail their logs over the Docker API.
- **Loki** stores them with 7d retention.

**Grafana** is the single visualization layer for all three signals
(Prometheus, Loki, Tempo). When a boom binary emits a log event inside a
traced span, the custom `OtelTraceFormatter` prepends the line with
`trace_id=<32-hex> span_id=<16-hex>`. The Loki datasource's derived field
matches that token and turns it into a link straight to the trace in Tempo.
The Tempo datasource is configured with `tracesToLogsV2` keyed on the
`service.name` resource attribute (matched against the Promtail `service`
label) so jumping from a span back to the surrounding logs is one click.

cAdvisor + `docker-metadata-exporter` together resolve cAdvisor's container
IDs to readable service names. Both run with a reduced security surface
(`read_only`, `no-new-privileges`, dropped Linux capabilities). Promtail and
Node Exporter follow the same hardening.

## Local quick start

Start the full local dev stack (Mongo, Valkey, Kafka, cAdvisor, Node
Exporter, OTel Collector, Loki, Promtail, Tempo, Prometheus, Grafana, API,
and dev worker services):

```sh
make dev
```

Then open Grafana:

- URL: <http://grafana.localhost>
- Username: value of `GRAFANA_ADMIN_USER` in your environment / `.env` (defaults to `admin` if unset)
- Password: value of `GRAFANA_ADMIN_PASSWORD` in your environment / `.env` (required)

Pre-provisioned dashboards:

- **BOOM Observability** — pipeline throughput, queues, worker pools.
- **BOOM Host & Infrastructure** — host CPU/RAM/disk/net, container
  throttling/restarts, recent error logs, span RED metrics.

Prometheus is also available locally at <http://prometheus.localhost> or any
local port mapping provided by your dev override.

Prometheus retention can be adjusted with `PROMETHEUS_RETENTION_TIME`.
Loki retention is hard-coded to 7 days in `config/loki/loki-config.yaml`.
Tempo block retention is hard-coded to 7 days in `config/tempo/tempo-config.yaml`.

## Generate traffic for dashboards

To make charts move, produce alerts and run the scheduler/consumer path,
you can produce a deterministic ZTF batch used in dev:

```sh
make delete-produce-ztf
```

Other batches of alerts can be sent to the system using the `kafka_producer`
app, the usage for which is described in the README.

## What to watch in Grafana

### Pipeline throughput and health (BOOM Observability)

- **Throughput by stage (alerts/s)**
  - `sum(irate(kafka_consumer_alert_processed_total[5m]))`
  - `sum by (survey) (irate(alert_worker_alert_processed_total[5m]))`
  - `sum by (survey) (irate(enrichment_worker_alert_processed_total[5m]))`
  - `sum by (survey) (irate(filter_worker_alert_processed_total[5m]))`
- **Scheduler workers** — configured and live traces for alert, enrichment, and filter worker pools.

### Backpressure and failures

- **Valkey queue depth** from `redis_key_size` for packets, enrichment, and filter queues.
- **Worker error ratio** derived from `*_worker_alert_processed_total{status="error"}`.

### Outputs and infra signals

- **Kafka messages produced** — `scheduler_kafka_alert_published_total`.
- **API requests** — `sum by (api) (rate(api_request_total[5m]))`.
- **Collector metric flow** — accepted/sent/failed metric points from OTel Collector.
- **MongoDB storage stats**, plus **objects per collection** from
  `mongodb_collstats_storageStats_count` (needs the exporter's `collstats`
  collector and `--discovering-mode`).
- **Container CPU/memory** joined against Docker metadata for readable names.

### Host and container infra (BOOM Host & Infrastructure)

- **Service up/down** — `docker_container_up` per Compose service as a state
  timeline. This is the same signal `api-down` alerts on, for every service.
- **Healthcheck state (now)** — current `healthy`/`unhealthy`/`starting`/`none`
  per service; `none` means that service declares no healthcheck, so its
  up/down value only reflects whether the container is running.
- **Prometheus scrape target health** — `up` per job. A gap in the two panels
  above means `docker-metadata` was unscrapable, not that a service went down.
- **Host CPU / memory / disk / network** from Node Exporter.
- **Container CPU throttle ratio** — high values mean Docker `cpus:` limits
  are starving the workload.
- **Container restarts** — `changes(container_start_time_seconds[1h])`
  identifies crash-loop behavior.
- **Recent error logs** — Loki LogQL panel filtered to error/warn/panic.
- **Trace RED metrics** — span volume and p95 latency per service, generated
  by Tempo's metrics generator.

## Logs and traces

### Searching logs

In Grafana, open **Explore** → Loki datasource. Useful starting queries:

The `service` label matches the Compose service name, so the schedulers and
consumers are split per-survey: `scheduler-ztf`, `scheduler-lsst`,
`consumer-ztf`, `consumer-lsst`, etc. Promtail also stamps every line with
`host="boom"` so a single label query reaches every container in the stack.

```logql
# All logs from a single service
{service="api"}

# Both ZTF and LSST schedulers
{service=~"scheduler-.*"}

# Errors across the whole stack in the last 15m
{host="boom"} |~ "(?i)error|panic|fatal"

# Logs that carry a trace_id (clickable to jump to Tempo)
{service=~"scheduler-.*"} |= "trace_id="
```

### Searching traces

In **Explore** → Tempo datasource you can:

- Search by service/operation/duration/tag filters.
- Paste a `trace_id` from a log line.
- Click any span and use the "Logs for this span" button to jump back to Loki.

The default sample ratio is 1.0 (all spans exported). For high-throughput
production, lower it via the `OTEL_TRACES_SAMPLER_ARG` environment variable
(e.g. `0.1` keeps 10% of root traces).

## Alerts

Alert rules, contact points, and notification policies are provisioned from
`config/grafana/provisioning/alerting/`. Provisioned rules:

| UID | What it watches |
| --- | --- |
| `disk-fills-in-24h` | `predict_linear` says a filesystem will hit 0 bytes in <24h |
| `container-restart-flapping` | Container restarted >3 times in 1h |
| `cpu-throttling` | >25% of CFS periods throttled for 15m |
| `otel-collector-dropped-metrics` | OTel exporter is failing to send metric points |
| `valkey-queue-backed-up` | Any worker queue >50k entries for 30m *and* flat or growing for 15m (slow drain is fine) |
| `api-down` | The API container is stopped, or running but failing its Docker healthcheck |
| `observability-blind` | Prometheus cannot scrape `docker-metadata-exporter`, so `api-down` cannot fire |

`api-down` reads `docker_container_up` from `scripts/docker_metadata_exporter.py`,
which is 0 when a container is not running **or** is failing its healthcheck.
cAdvisor reports resource usage but knows nothing about healthchecks, so this
exporter is the only thing that sees a container that is up but broken — the
failure mode in [#562](https://github.com/boom-astro/boom/pull/562), where every
request panicked its actix worker while the process stayed alive. The per-state
`docker_container_health_status{status="healthy|unhealthy|starting|none"}` is
exported alongside it for dashboards and ad-hoc queries; both are labelled with
`container_name`, `compose_project`, and `compose_service`, so pointing the same
alert at another service is a one-label change.

That exporter is a single point of failure for `api-down`, and a failed scrape
is not a quiet event: Prometheus writes stale markers for every series of a
failed target immediately, so the series backing the rule disappears within one
scrape interval rather than after the usual 5m staleness window. `api-down`
therefore treats No Data as No Data, and `observability-blind` pages separately
when `up{job="docker-metadata"}` is 0 — the outage is still loud, but under a
name that says which system is broken.

Cold starts are the other thing that used to page: `boom-api` connects to
Mongo, sets up auth, and builds a cutout storage client per survey *before* it
binds its port, so `/` answers nothing until that finishes. The container's
`start_period` covers it; if startup work grows, raise `start_period` in
`docker-compose.yaml` rather than the alert's pending period, which would also
slow down real outage detection.

Grafana reads alerting provisioning **only at startup** (unlike dashboards,
which its file provider re-reads every 30s). Since `config/grafana/provisioning`
is a bind mount, `docker compose up -d` does not notice the change and leaves
the container running, so edits here require an explicit
`docker compose up -d --force-recreate --no-deps grafana`. The production deploy
workflow does this on every run.

All alerts route to a single **Slack** contact point. Set
`SLACK_WEBHOOK_URL` in your environment / `.env` to a Slack incoming-webhook
URL; if unset, alerts still fire and are visible in Grafana but no Slack
message is sent.

## "No-SSH" workflow

The stack is designed so that ops work happens entirely through Grafana:

1. **Saw an alert?** Click through the Slack message into Grafana.
2. **Need stack traces?** Open the alert → linked panel → switch to the Loki
   panel ("Container logs"). Filter to the offending container.
3. **Need request flow?** A logged `trace_id` is clickable; it opens the
   trace in Tempo. From any span, "Logs for this span" jumps back to Loki.
4. **Need infra signals?** Open **BOOM Host & Infrastructure**. Disk, CPU,
   memory, throttling, restarts.

A useful smoke test: simulate a container crash with `docker kill <name>`
and confirm you can see the panic, find the related trace, and identify the
affected service entirely in Grafana.

## Monitoring real deployments behind Traefik

For production/staging, access is via Traefik host routing:

- Grafana: `https://grafana.<your-domain>`
- Prometheus: `https://prometheus.<your-domain>`

Loki and Tempo are intentionally not exposed publicly — Grafana proxies
queries to them on the internal `boom` network.
