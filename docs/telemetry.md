# Telemetry

## Quick start

Start up boom services as usual,

```sh
docker compose up -d
```

Run the producer and the consumer as desired, e.g.,

```sh
kafka_producer ztf 20250205
kafka_consumer ztf 20250205
```

And in a different terminal, run the scheduler:

```sh
scheduler ztf
```

Now, visit the Prometheus UI at <http://localhost:9090> where you can query the
metrics emitted by boom, as demonstrated in the following examples:

* [Alert workers][alert-worker-queries]
* [ML workers][ml-worker-queries]

[alert-worker-queries]: http://localhost:9090/query?g0.expr=%23+Total+number+of+alerts+processed+by+the+alert+workers%0Aalert_worker_alert_processed_total&g0.show_tree=0&g0.tab=table&g0.range_input=30m&g0.res_type=fixed&g0.res_step=60&g0.display_mode=lines&g0.show_exemplars=0&g1.expr=%23+Instantaneous+throughput+%28alert%2Fs%29%0Asum+by+%28status%29+%28irate%28alert_worker_alert_processed_total%5B5m%5D%29%29&g1.show_tree=0&g1.tab=graph&g1.range_input=30m&g1.res_type=fixed&g1.res_step=60&g1.display_mode=lines&g1.show_exemplars=0&g2.expr=%23+Average+processing+time+per+alert+%28s%29%0A1+%2F+sum+by+%28status%29+%28irate%28alert_worker_alert_processed_total%5B5m%5D%29%29&g2.show_tree=0&g2.tab=graph&g2.range_input=30m&g2.res_type=fixed&g2.res_step=60&g2.display_mode=lines&g2.show_exemplars=0

[ml-worker-queries]: http://localhost:9090/query?g0.expr=%23+Total+number+of+alerts+processed+by+the+ML+workers%0Aml_worker_alert_processed_total&g0.show_tree=0&g0.tab=table&g0.range_input=1h&g0.res_type=auto&g0.res_density=medium&g0.display_mode=lines&g0.show_exemplars=0&g1.expr=%23+Instantaneous+throughput+%28alert%2Fs%29%0Asum+by+%28status%29+%28irate%28ml_worker_alert_processed_total%5B5m%5D%29%29&g1.show_tree=0&g1.tab=graph&g1.range_input=30m&g1.res_type=fixed&g1.res_step=60&g1.display_mode=lines&g1.show_exemplars=0&g2.expr=%23+Average+processing+time+per+alert+%28s%29%0A1+%2F+sum+by+%28status%29+%28irate%28ml_worker_alert_processed_total%5B5m%5D%29%29&g2.show_tree=0&g2.tab=graph&g2.range_input=30m&g2.res_type=fixed&g2.res_step=60&g2.display_mode=lines&g2.show_exemplars=0&g3.expr=%23+Number+of+alerts+per+batch%0Airate%28ml_worker_alert_processed_total%5B5m%5D%29+%2F+irate%28ml_worker_batch_processed_total%5B5m%5D%29&g3.show_tree=0&g3.tab=graph&g3.range_input=30m&g3.res_type=fixed&g3.res_step=60&g3.display_mode=lines&g3.show_exemplars=0

## Overview

Boom is instrumented to emit metrics using OpenTelemetry (OTel). Extensive
documentation for OTel can be found at <https://opentelemetry.io/docs/>.

### Metrics

A **metric** is a characteristic that is recorded over time using an
**instrument**, which may be one of the following:

* **Counter:** A counter is an instrument that records increments and is used to
  track the total count of something over time, e.g., the number of alerts
  processed. **UpDownCounter** is a type of counter whose increment value can be
  positive or negative.
* **Gauge:** A gauge is an instrument that records full values, not increments.
  Gauges are used when the sum over time isn't meaningful. A common example is
  temperature.
* **Histogram:** A histogram is effectively a collection of counters, one
  counter per bin. When a value is recorded by a histogram, the counter of the
  corresponding bin is incremented. Histograms are used when collecting
  statistics is more important or more feasible than tracking precise values,
  e.g., alert processing time.

The OTel spec describes "synchronous" and "asynchronous" versions of each
instrument. This has nothing to do with `async`/`await`. Rather, the
"synchronous" versions record their values directly in the instrumented
application whereas the "asynchronous" versions record their values using a
callback.

Each instrument is associated with one metric. It has a name, an optional
description, and an optional unit. Both
<https://opentelemetry.io/docs/specs/semconv/general/metrics/> and
<https://opentelemetry.io/docs/specs/semconv/general/naming/> provide guidance
naming and such.

Values are usually recorded with **attributes**. These are essentially facets on
the data, each unique combination of attribute values being one facet. For
instance, a counter instrument that tracks the number of alerts processed could
record each increment by status (e.g., "added" "exists", "error") and worker ID.
Each unique combination of status and worker ID has its own total count within
the counter instrument. This means that a single metric will generally manifest
as separate time series within tools like Prometheus.

### Meters

In the Rust code, instruments are created using a **meter**. In general an
application should have one meter. The `utils::o11y::metrics` module defines a
static meter for each binary: `CONSUMER_METER`, `PRODUCER_METER`, and
`SCHEDULER_METER` are used to create instruments in the producer, the consumer,
and the scheduler, respectively. This separation helps prevent their metrics
from getting mixed together during collection. There is no mechanism forcing an
application to use a particular meter, we just need to follow a convention.

*Every metric in the entire codebase can be found by looking for where these
meters are used to create instruments.*

Meters are created using a **meter provider**, which carries **resource**
metadata (information about the "entity" emitting the telemetry, e.g., service
name, instance ID, etc.) and is associated with an **exporter**. Every meter
created by the meter provider has the same resource and exporter. The
`utils::o11y::metrics::init_metrics` function is responsible for creating a
meter provider for the application and setting it as the global meter provider
that can be accessed from anywhere in the codebase at runtime.

Two resource attributes can be set using environment variables:

* BOOM_SCHEDULER_INSTANCE_ID: UUID associated with this instance of the
  scheduler. If not set, one will be generated.
* BOOM_DEPLOYMENT_ENV: Name of the environment where this instance is deployed.
  Defaults to "dev".

The **exporter** is responsible for actually doing something with the data
recorded by the instruments. In boom, we use the OTLP exporter from the
`opentelemetry_otlp` crate, which sends telemetry in OTLP format (OTel's own
protocol for telemetry data) to the OTel Collector.

### Collector

The Otel Collector is a service that receives data from an application and
exports it somewhere else, such as a metrics backend like Prometheus. See
<https://opentelemetry.io/docs/collector/> for details.

This is the piece that allows boom to be fully unaware about where it sends
telemetry, allowing us to instrument *once* and to freely change backends
whenever we need to.

The Collector works by running "pipelines" configured for different kinds of
telemetry. A pipeline consists of one or more **receivers**, **processors**, and
**exporters**. For boom, the metrics pipeline uses the OTLP receiver (to get the
OTLP data exported by the application) and an exporter that is configured to
send metrics to Prometheus.

### Prometheus

[Prometheus](https://prometheus.io/docs/introduction/overview/) is our metrics
backend system. It is responsible for storing all metric data in a database and
making it queryable with PromQL (see
<https://prometheus.io/docs/prometheus/latest/querying/basics/>).

Prometheus typically works by periodically scraping metrics in Prometheus format
from an endpoint exposed by an application. In fact, the OTel Collector can be
configured to be just such an endpoint. However, Prometheus has a built-in
**OTLP receiver** that can be used to get metrics directly from the OTel
Collector, so that's what we use for boom. See
<https://prometheus.io/docs/guides/opentelemetry/> for details.

## OTel Collector

### Docker image

There are different Collector images to choose from. The "core" image is
`otel/opentelemetry-collector`, but there are others, notably
`otel/opentelemetry-collector-contrib`. The contrib image is basically the core
image plus a number of additional components and is therefore larger in size.
The core image is used here because it comes with a Prometheus exporter as well
as the *zpages* and *health_check* extensions, which should be sufficient.
(*health_check* provides a simple health-check endpoint for the Collector, and
*zpages* provides some endpoints for basic info about what's happening inside
the Collector; more on these below).

OpenTelemetry is a rapidly evolving project, so it's a good idea to pin the
image version instead of using `latest`.

To see what components the core image comes with, run the following:

```sh
docker run --rm otel/opentelemetry-collector:0.131.1 components
```

Or, review the manifest at
<https://github.com/open-telemetry/opentelemetry-collector-releases/blob/main/distributions/otelcol/manifest.yaml>.

For comparison, the manifest of the contrib image can be found at
<https://github.com/open-telemetry/opentelemetry-collector-releases/blob/main/distributions/otelcol-contrib/manifest.yaml>.

Be aware that different Collector images may use different config locations;
this can be checked by inspecting the image layers at <https://hub.docker.com>.
For instance, the contrib image looks for `config.yaml` in
`/etc/otelcol-contrib` rather than `/etc/otelcol`.

The image layers at <https://hub.docker.com> show the core image exposes ports
4317, 4318, and 55679. According to the [Collector Quick
start](https://opentelemetry.io/docs/collector/quick-start/), these ports are
used for the following:

* 4317 is for collecting telemetry from OTLP exporters over gRPC
* 4318 is for collecting telemetry from OTLP exporters over HTTP
* 55679 is for ZPages

Ports 4317 and 55679 are therefore published to enable gRPC and ZPages,
respectively.

The other port, 13133, is not explicitly exposed by the image, but it is used by
the *health_check* extension (see
<https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/extension/healthcheckextension/README.md>,
which can be found by searching for "health_check" in the OpenTelemetry Registry
at <https://opentelemetry.io/ecosystem/registry>), therefore it is published
here in the compose file as well.

### Configuration

The Collector configuration system is documented at
<https://opentelemetry.io/docs/collector/configuration/>.

For additional reference, the config file that comes with the core image is
tracked at
<https://github.com/open-telemetry/opentelemetry-collector-releases/blob/main/distributions/otelcol/config.yaml>.

The following config instructs the Collector to take OTLP metrics from an
instrumented application and send them to the debug and OTLP/HTTP exporters. It
also enables the *health_check* and *zpages* extensions:

```yaml
receivers:  # 1
  otlp:
    protocols:
      grpc:
        endpoint: otel-collector:4317

processors:  # 2
  batch:

exporters:  # 3
  otlphttp/prometheus:
    endpoint: "http://prometheus:9090/api/v1/otlp"
  debug:
    verbosity: detailed

extensions:  # 4
  health_check:
    endpoint: otel-collector:13133
  zpages:
    endpoint: otel-collector:55679

service:  # 5
  extensions: [health_check, zpages]
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp/prometheus, debug]
```

1.  This configures a single receiver, one that gets telemetry sent from the
    instrumented application via the OTLP protocol over gRPC at the address
    `otel-collector:4317`.

    * `otlp` because the application uses an OTLP metrics exporter.

    * `protocol: grpc` because the application configures that exporter to
      transport data over gRPC (as opposed to HTTP).

    * `endpoint: otel-collector:4317` because the default host is `localhost`,
      i.e., the loopback interface of the *container*, not that of the Docker
      host. Many resources suggest using `0.0.0.0` instead, allowing the
      Collector to receive packets from all network interfaces, including the
      one corresponding to the Docker network. This isn't good practice, though,
      because it means the service can be reached by literally anyone on any
      connected network. The solution is to use the host name assigned by Docker
      according to `docker-compose.yaml`, which in this case is
      `otel-collector`. See
      <https://opentelemetry.io/docs/security/config-best-practices/#protect-against-denial-of-service-attacks>.

      And of course, `4317` because that's the port the Collector uses for gRPC.

2.  This sets up a `batch` processor, which improves I/O efficiency by
    transmitting telemetry in batches (see
    <https://github.com/open-telemetry/opentelemetry-collector/blob/main/processor/batchprocessor/README.md>).
    `batch` is available as part of the core image.
    
    For context, the Collector uses "processors" to do things with the collected
    data before export (such as batching it up). OpenTelemetry has a short list
    of recommended processors (see
    <https://github.com/open-telemetry/opentelemetry-collector/tree/main/processor#recommended-processors>),
    and one of them is `batch`.

3.  This sets up the exporters:

    * The debug exporter, which basically just tells the Collector to log the
      telemetry instead of sending it somewhere else (e.g., a metrics backend
      like Prometheus).

    * An OTLP/HTTP exporter named "prometheus". The OpenTelemetry guide in the
      Prometheus docs say the OTLP receiver endpoint is
      `/api/v1/otlp/v1/metrics`. It also says, "... the HTTP mode of OTLP should
      be used ...", which suggests using the `otlphttp` exporter. According to
      the [OTLP/HTTP exporter docs](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/otlphttpexporter/README.md),
      '"/v1/metrics" will be appended' and the default encoding is protobuf.

4.  This section configures both the *health_check* extension and the *zpages*
    extension that come with the core image. As for their endpoints, see the
    note about the receiver endpoint above. The ports correspond to the those
    published in the compose file.

5.  None of the components configured above are actually enabled by the
    Collector unless they're used in the `service` section. The `extensions`
    line enables *health_check* and *zpages*. The `pipeline` part tells the
    Collector to take metrics data from the OTLP receiver, send it to the batch
    processor, and then export it using the debug and OTLP/HTTP exporters.

### Test basic functioning of the Collector

If the debug exporter isn't already enabled for the metrics pipeline, then add
it to the list: `exporters: [otlphttp/prometheus, debug]`.

Start boom and the Collector:

```sh
docker compose up -d
```

#### Check the logs

The log for the Collector's container will show the Collector starting up. If
there's an error in the config file, then the Collector will show an error
message and exit. Otherwise, the log will eventually show the message,
"Everything is ready. Begin running and processing data."

#### Check the Collector's health

It's also possible to check the general health of the Collector by visiting
<http://localhost:13133/>.

#### Verify Collector config with ZPages

The *zpages* extension enables ZPages, which can be used to verify the config in
`otel-collector-config.yaml`. Visit <http://localhost:55679/debug/servicez> for
basic build and runtime info, as well as links for pipelines, extensions, and
features. For example, the "Pipelines" section should show a single pipeline
consisting of an OTLP receiver, a batch processor, and a debug exporter.

#### Verify metrics are following into the Collector

Start the instrumented application in another terminal. The Collector log in the
first terminal should eventually start showing `Metrics` lines like,

```
2025-08-08T21:52:55.428Z    info    Metrics {...}
```

These lines confirm that metrics are flowing from the application to the
Collector.

It's also possible to see this in ZPages by visiting
<http://localhost:55679/debug/tracez>. If metrics are being collected, then
trace samples should start showing up under `receiver/otlp/MetricsReceived`.

Another indicator that the application and Collector are properly connected is
whether the meter provider shuts down successfully. If boom can't communicate
with the Collector (e.g., due to a misconfigured endpoint), then it will log a
warning during termination that it wasn't able to properly shut down the meter
provider.

If the application is stopped, then the Collector won't get any more metrics and
it won't emit any more `Metrics` lines. The application can then be started
again and the Collector log will show more metrics being received.

At this point, the basic collection of metrics from the application has been
verified. Both the application and the Collector can be stopped.

## Prometheus

### Getting data into Prometheus

There are two ways to integrate the Collector with Prometheus:

1.  Using the Prometheus OTLP receiver: Use the `otlphttp` exporter in the
    Collector to send metrics directly to Prometheus via OTLP, with HTTP for
    transport and protobuf for payloads.

2.  Using the `prometheus` exporter: Use the `prometheus` exporter in the
    Collector to put Prometheus-formatted metrics on an endpoint (many examples
    show using the Collector's port 8889, though I can't find specific docs for
    this). That endpoint is then scraped by Prometheus.
    <https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/prometheusexporter>

It's not clear which method should be preferred. Is one faster than the other?
The second method is how Prometheus normally ingests data and is known to be
performant. The first method is specialized for OTLP, so maybe there are
optimizations that make it faster than scraping. However, the OTLP receiver is
fairly new, so it's also possible that it's currently slower.

(One difference is that there seems to be more configuration involved with
scraping. There's both a read interval for export to the Collector and a scrape
interval for Prometheus. Also, the prometheus exporter has an expiration period,
and metrics will "expire" (I guess that means they're dropped from the endpoint)
if Prometheus waits too long to scrape. But with the OTLP receiver, there's only
the read interval, which is much simpler.)


### Querying

The Prometheus UI is available at <http://localhost:9090>.

In the query box, look for a menu where you can select "Explore metrics". This
will show a searchable list of all the boom metrics that Prometheus currently
knows about. If boom just started up and the list is empty, then let it run a
little longer. The exporter in the Rust code exports at fixed intervals, so it
could be that it hasn't sent anything to the Collector yet.

Here are some example queries (these examples are mostly for getting familiar
with PromQL and not for documenting which metrics are defined in the code, which
will evolve over time):

* Table: Total number of alerts processed so far by the alert workers:

  If `alert_worker.alert.processed` is an OTel Counter that's incremented
  whenever an alert has been processed, then total number of processed alerts
  is,

  ```promql
  alert_worker_alert_processed_total
  ```

* Graph: Overall throughput from the alert workers:

  ```promql
  sum by (status) (irate(alert_worker_alert_processed_total[5m]))
  ```

  The `[5m]` part creates a 5-minute lookback window at each point in time in
  the time series, and `irate` calculates the rate of change ∆x/∆t from the last
  two data points in that window. Decreasing the window size won't change the
  results *until* it's small enough that sometimes the window doesn't contain
  two data points. Making the window size somewhat generous helps guarantee
  `irate` will get two data points most of the time.

  The exact same result can be obtained from any alert-based histogram, as the
  `_count` time series is just an alert counter regardless of what the bucketed
  quantity is. For example, if `alert_worker.alert.duration` is an OTel
  Histogram that records the time taken to process each alert, then the time
  series `alert_worker_alert_duration_seconds_count` is equivalent to
  `alert_worker_alert_processed_total` and the throughput is,

  ```promql
  sum by (status) (irate(alert_worker_alert_duration_seconds_count[5m]))
  ```

* Graph: Instantaneous average processing time per alert for all alert workers:

  This is just the inverse of the throughput:

  ```promql
  1 / sum by (status) (irate(alert_worker_alert_processed_total[5m]))
  ```

  Or,

  ```promql
  1 / sum by (status) (irate(alert_worker_alert_duration_seconds_count[5m]))
  ```

  These inverses work because the average time per alert is ∆t/∆N = 1 / throughput.

  In general, the average of any histogram is the change in its `_sum` time
  series divided by the change in its `_counts` time series. Therefore, the
  exact same result can be obtained from a duration histogram:

  ```promql
  avg by (status) (
      irate(alert_worker_alert_duration_seconds_sum[5m])
    /
      irate(alert_worker_alert_duration_seconds_count[5m])
  )
  ```

  It's easier to see that using `delta` instead of `irate` would result in an
  average. But `irate` works, too, because the division by time interval in both
  the numerator and the denominator cancels out, so the result is the same as a
  ratio of deltas. Many Prometheus examples show using `rate` to calculate the
  average of a histogram. Using `irate` allows for getting the instantaneous
  average.

* Graph: CDF of processing time for all alert workers ("added" alerts only) over
  a window:

  ```promql
    sum by (le) (increase(alert_worker_alert_duration_seconds_bucket{status="added"}[5m]))
  /
    scalar(increase(alert_worker_alert_duration_seconds_bucket{le="+Inf",status="added"}[5m]))
  ```

  The "le" values are the bin edges.  Without the `increase` range query part,
  the CDF would be for all observations ever recorded, and the effect of each
  new observation becomes decreasingly noticeable.

Note that the http query parameters in your browser's address bar are updated as
you work in the UI. The resulting URL can be bookmarked or shared with others,
serving as a makeshift dashboard.

## Other resources

Instrumenting in Rust:

* <https://docs.rs/opentelemetry/latest/opentelemetry/index.html>
* <https://docs.rs/opentelemetry_sdk/latest/opentelemetry_sdk/index.html>

Exporting to the Collector: <https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/index.html>

The OTLP/HTTP exporter: <https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlphttpexporter>

Prometheus OTLP receiver: <https://prometheus.io/docs/guides/opentelemetry/>
