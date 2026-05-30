# Observability

This guide covers how to run BOOM locally with telemetry,
view dashboards in Grafana, generate alert traffic,
and monitor deployed environments behind Traefik.

## Components and architecture

BOOM itself is instrumented to send metrics to Otel Collector,
which then exports its data to Prometheus, which serves as the time series
database.
cAdvisor sends container metrics, while a custom `docker-metadata-exporter`
service maps those metrics to service names,
otherwise they would be unintelligible container IDs.
There are exporter services for MongoDB, Valkey, and Kafka,
which also make their metrics available to Prometheus.
Grafana serves as the visualization layer querying Prometheus.

BOOM runs `docker-metadata-exporter` through a restricted
`docker-socket-proxy` (containers list/read endpoints only), and both
`docker-metadata-exporter` and `cadvisor` run with a reduced security surface
(`read_only`, `no-new-privileges`, dropped Linux capabilities).

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

Prometheus retention, i.e., the window for which we save metrics,
can be adjusted with `PROMETHEUS_RETENTION_TIME` in your compose environment.

## Generate traffic for dashboards

To make charts move, produce alerts and run the scheduler/consumer path,
you can produce a deterministic ZTF batch used in dev:

```sh
make delete-produce-ztf
```

Other batches of alerts can be sent to the system using the `kafka_producer`
app, the usage for which is described in the README.

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

## Monitoring real deployments behind Traefik

For production/staging, access is typically via Traefik host routing.

Current compose labels route Grafana and Prometheus to subdomains:

- Grafana: `https://grafana.<your-domain>`
- Prometheus: `https://prometheus.<your-domain>`
