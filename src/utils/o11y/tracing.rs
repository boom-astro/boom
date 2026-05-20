//! OTLP tracing pipeline.
//!
//! This module mirrors `metrics::init_metrics`: it sets up an OTLP exporter
//! (gRPC, by default to `OTEL_EXPORTER_OTLP_ENDPOINT` or `http://localhost:4317`)
//! that sends spans to the OTel Collector, which in turn forwards them to
//! Grafana Tempo. The returned `SdkTracerProvider` should be kept alive for the
//! lifetime of the program and shut down before exit so spans get flushed.
use opentelemetry::{trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace::{Sampler, SdkTracerProvider},
    Resource,
};

use crate::utils::o11y::is_otel_disabled;

/// The error type returned when initializing tracing.
#[derive(Debug, thiserror::Error)]
pub enum InitTracingError {
    #[error("failed to build the OTLP trace exporter")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
}

/// Build an OTLP `SdkTracerProvider` for the given service.
///
/// Returns `Ok(None)` when `OTEL_SDK_DISABLED=true` is set, in which case the
/// global tracer is left as the SDK's no-op provider and no exporter is
/// created. Callers should pass `None` to `build_subscriber_with_otel` in that
/// case to skip installing the OTel `tracing` layer.
///
/// Reads the OTLP endpoint from `OTEL_EXPORTER_OTLP_ENDPOINT` (default:
/// `http://localhost:4317`) and the trace sample ratio from
/// `OTEL_TRACES_SAMPLER_ARG` (default: 1.0 = sample everything; lower this in
/// high-throughput prod if span volume becomes a problem).
///
/// The provider is also registered globally so libraries that use the
/// `opentelemetry::global` API (e.g. propagators) can pick it up.
pub fn init_tracing(
    service_name: String,
    instance_id: uuid::Uuid,
    deployment_env: String,
) -> Result<Option<SdkTracerProvider>, InitTracingError> {
    if is_otel_disabled() {
        return Ok(None);
    }

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    let sample_ratio = std::env::var("OTEL_TRACES_SAMPLER_ARG")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0);

    let resource = Resource::builder()
        .with_service_name(service_name)
        .with_attributes([
            KeyValue::new("service.instance.id", instance_id.to_string()),
            KeyValue::new("service.namespace", "boom"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment.name", deployment_env),
        ])
        .build();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_sampler(Sampler::TraceIdRatioBased(sample_ratio))
        .with_batch_exporter(exporter)
        .build();

    opentelemetry::global::set_tracer_provider(provider.clone());
    Ok(Some(provider))
}

/// Build a `tracing-opentelemetry` layer attached to the given provider.
///
/// Add this layer to a `tracing_subscriber::Registry` to forward `tracing`
/// spans into the OTLP pipeline. The returned layer is generic over the
/// subscriber it's composed with, so callers should use the standard
/// `Layer::with_filter` / `SubscriberExt::with` patterns.
pub fn otel_layer<S>(
    provider: &SdkTracerProvider,
    service_name: &str,
) -> tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    tracing_opentelemetry::layer().with_tracer(provider.tracer(service_name.to_string()))
}
