//! Common metrics utilities.
use std::sync::LazyLock;

use opentelemetry::{KeyValue, metrics::Meter};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    metrics::{SdkMeterProvider, Temporality},
};

// From the `opentelemetry` docs for the `MeterProvider` trait,
//
// > A Meter should be scoped at most to a single application or crate. The name
// > needs to be unique so it does not collide with other names used by an
// > application, nor other applications.
//
// Each binary should get its own, uniquely-named meter so that metrics don't
// merge or collide in the Collector.

/// Global OTel meter used to create instruments throughout the kafka consumer
/// application.
///
/// Only available after calling `init_metrics`.
pub static CONSUMER_METER: LazyLock<Meter> =
    LazyLock::new(|| opentelemetry::global::meter("boom-consumer-meter"));

/// Global OTel meter used to create instruments throughout the kafka producer
/// application.
///
/// Only available after calling `init_metrics`.
pub static PRODUCER_METER: LazyLock<Meter> =
    LazyLock::new(|| opentelemetry::global::meter("boom-producer-meter"));

/// Global OTel meter used to create instruments throughout the scheduler
/// application.
///
/// Only available after calling `init_metrics`.
pub static SCHEDULER_METER: LazyLock<Meter> =
    LazyLock::new(|| opentelemetry::global::meter("boom-scheduler-meter"));

/// The error type returned when initializing metrics.
#[derive(Debug, thiserror::Error)]
pub enum InitMetricsError {
    #[error("failed to build the OTLP exporter")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
}

/// Initialize the OTel metrics system for the application corresponding to the
/// given service and return the resulting meter provider.
///
/// The `instance_id` and `deployment_env` arguments are a UUID and a deployment
/// environment name (e.g., "dev", "prod", etc.), respectively. They distinguish
/// the metrics emitted by this application instance from those emitted by any
/// other instances of the same service.
///
/// This function is responsible for creating an exporter for OTel metrics, a
/// meter provider based on that exporter, and then some global meters used to
/// create instruments for different applications. The meters can be accessed
/// from the static items `CONSUMER_METER`, `PRODUCER_METER`, and
/// `SCHEDULER_METER`, which are only available after this function completes.
///
/// The exporter is an OTLP exporter designed to send metrics every 60 s over
/// gRPC to the OTel Collector at `localhost:4317`. It uses cumulative
/// temporality, which is more natural for Prometheus. (Prometheus support for
/// delta temporality is still experimental. Cumulative temporality is just fine
/// as long as attribute cardinality doesn't explode.)
///
/// The meter provider is returned so it can be cloned if needed (cloning
/// providers is cheap), to have finer control of how/when it's dropped, or so
/// its `shutdown` method can be called later to manually flush metrics.
pub fn init_metrics(
    service_name: String,
    instance_id: uuid::Uuid,
    deployment_env: String,
) -> Result<SdkMeterProvider, InitMetricsError> {
    // From the OTel docs, "A resource represents the entity producing
    // telemetry...". In this case the entity is the app itself.
    let resource = Resource::builder()
        .with_service_name(service_name)
        // https://opentelemetry.io/docs/specs/semconv/resource/
        .with_attributes([
            KeyValue::new("service.instance.id", instance_id.to_string()),
            KeyValue::new("service.namespace", "boom"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment.name", deployment_env),
        ])
        .build();

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_temporality(Temporality::Cumulative)
        .with_tonic()
        .with_endpoint("https://localhost:4317/v1/metrics")
        .build()?;

    // From the OTel docs, "... a Meter Provider is initialized once and its
    // lifecycle matches the application’s lifecycle. Meter Provider initialization
    // also includes Resource and Exporter initialization."
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(exporter)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider.clone());
    Ok(meter_provider)
}
