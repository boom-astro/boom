use crate::utils::o11y::metrics::API_METER;

use std::sync::LazyLock;

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware::Next,
    Error,
};
use opentelemetry::{metrics::Counter, KeyValue};

static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    API_METER
        .u64_counter("api.request")
        .with_unit("{request}")
        .with_description("Number of HTTP requests handled by the BOOM API service.")
        .build()
});

pub async fn request_metrics_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    let api = if req.path().starts_with("/babamul") {
        "babamul"
    } else {
        "boom"
    };
    let method = req.method().as_str().to_string();

    let response = next.call(req).await;
    let status_code = response
        .as_ref()
        .map(|service_response| service_response.status().as_u16())
        .unwrap_or(500);

    let attrs = [
        KeyValue::new("api", api),
        KeyValue::new("method", method),
        KeyValue::new("status_code", status_code.to_string()),
    ];
    REQUESTS.add(1, &attrs);

    response
}
