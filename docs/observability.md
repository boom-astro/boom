# Observability

This guide covers how to run BOOM locally with telemetry, view dashboards in Grafana, generate alert traffic, and monitor deployed environments behind Traefik.

## Local quick start

Start the full local dev stack (Mongo, Valkey, Kafka, cAdvisor, OTel Collector, Prometheus, Grafana, API, and dev worker services):

```sh
make dev
```

Then open Grafana:

- URL: <http://grafana.localhost>
- Username: value of `GRAFANA_ADMIN_USER` in your environment / `.env` (defaults to `admin` if unset)
- Password: value of `GRAFANA_ADMIN_PASSWORD` in your environment / `.env` (required)

The pre-provisioned dashboard is:

- **BOOM Observability**

Prometheus is also available locally at <http://prometheus.localhost> or any local port mapping provided by your dev override.

## Generate traffic for dashboards

To make charts move, produce alerts and run the scheduler/consumer path.

### Preferred Makefile targets

Produce a deterministic ZTF batch used in dev:

```sh
make produce-ztf
```

### Custom producer invocation

For custom dates, limits, or options, using the binary directly is fine:

```sh
cargo run --bin kafka_producer ztf 20240617 public --limit 500 --server-url localhost:9092 --force
```

You can switch date/program/flags as needed for local load tests.

## What to watch in Grafana

The BOOM dashboard already includes the most operationally useful queries.

### Pipeline throughput and health

- **Throughput by stage (alerts/s)**
  - `sum(irate(kafka_consumer_alert_processed_total[5m]))`
  - `sum by (survey) (irate(alert_worker_alert_processed_total[5m]))`
  - `sum by (survey) (irate(enrichment_worker_alert_processed_total[5m]))`
  - `sum by (survey) (irate(filter_worker_alert_processed_total[5m]))`
- **Scheduler workers**
  - configured and live traces for alert, enrichment, and filter worker pools

### Backpressure and failures

- **Valkey queue depth (messages)**
  - queue sizes from `redis_key_size` for packets, enrichment, and filter queues
- **Worker error ratio by reason**
  - error-rate ratios derived from `*_worker_alert_processed_total{status="error"}`

### Outputs and infra signals

- **Kafka messages produced**
  - `scheduler_kafka_alert_published_total` (with fallback to topic offset growth)
- **API requests**
  - `sum by (api) (rate(api_request_total[5m]))`
- **Collector metric flow**
  - accepted/sent/failed metric points from OTel Collector
- **MongoDB storage** and **MongoDB logical stats**
  - DB growth and object/index counts
- **Container CPU usage by container** and **Container memory usage by container**
  - container-level resource usage joined against Docker metadata for readable names
- **Prometheus / OTEL Collector / Kafka Exporter**
  - `up` checks for observability dependencies

## Monitoring real deployments behind Traefik

For production/staging, access is typically via Traefik host routing.

Current compose labels route Grafana and Prometheus to subdomains:

- Grafana: `https://grafana.<your-domain>`
- Prometheus: `https://prometheus.<your-domain>`

### Subdomain model (recommended)

Use dedicated hosts per service (current default). This is usually simplest for auth, TLS, and link sharing.

### Sub-path model (if you need one domain)

If you expose Grafana as a sub-URL (for example `/grafana`), ensure both Traefik and Grafana are configured for sub-path serving:

- Traefik router rule uses `PathPrefix(`/grafana`)`
- Grafana has `GF_SERVER_ROOT_URL` set to include `/grafana`
- Grafana has `GF_SERVER_SERVE_FROM_SUB_PATH=true`

Without these settings, redirects and static assets can break.

## Operational checklist

1. Verify dependency health first (`Prometheus`, `OTEL Collector`, `Kafka Exporter` panels).
2. Check worker liveness in `Scheduler workers` against configured counts.
3. Compare throughput across consumer/alert/enrichment/filter stages for bottlenecks.
4. Watch queue depth to detect sustained backlog.
5. Use error ratio by reason to identify failure mode shifts.
6. Confirm output publication rate (`Kafka messages produced`) matches expectations.

Prometheus retention can be adjusted with `PROMETHEUS_RETENTION_TIME` in your compose environment.
