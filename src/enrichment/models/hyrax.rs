//! Hyrax models, run on demand against a single object via the Babamul API.
//!
//! Unlike the ACAI and BtsBot models — which the enrichment worker runs over every
//! alert at ingest time and persists into `classifications` — Hyrax models are run
//! interactively and their results are not written back to the object.
//!
//! Every model here runs out of process. boom's Rust API cannot load them itself —
//! TEMPO is a PyTorch transformer, not an ONNX graph — so each one sits behind an
//! HTTP sidecar (see `services/tempo/`) and the API forwards the inputs it has
//! already gathered into a [`HyraxInput`].
//!
//! Nothing about a spec is checked ahead of a request: every registered model is
//! listed and can be selected, and a service that turns out not to be running fails
//! the run it was asked for rather than being filtered out of the list.

use crate::utils::enums::Survey;
use std::collections::HashMap;
use std::sync::LazyLock;
use tracing::instrument;

/// Static description of a model that can be run on demand against one object.
#[derive(Debug)]
pub struct HyraxModelSpec {
    /// Identifier the API accepts in the `model` field of a classify request, and
    /// sends on to the service so it can reject a request meant for another model.
    pub id: &'static str,
    /// Human-readable name, shown in the UI dropdown.
    pub name: &'static str,
    pub description: &'static str,
    /// Environment variable holding the service's base URL, so a deployment can move
    /// the sidecar without a rebuild.
    pub url_env: &'static str,
    /// Base URL used when `url_env` is unset or empty.
    pub default_url: &'static str,
    /// Whether to send forced photometry alongside the alert magnitudes. Forced
    /// photometry re-measures the same epochs, so it changes what the model sees.
    pub include_forced_photometry: bool,
    /// Surveys this model can be run against.
    pub surveys: &'static [Survey],
}

impl HyraxModelSpec {
    /// Base URL of the model's service, with any trailing slash removed so callers
    /// can append `/classify` without doubling the separator.
    pub fn base_url(&self) -> String {
        // Empty counts as unset: compose renders `${VAR:-}` as `VAR=` everywhere it
        // is not configured.
        let url = std::env::var(self.url_env)
            .ok()
            .filter(|url| !url.is_empty())
            .unwrap_or_else(|| self.default_url.to_string());
        url.trim_end_matches('/').to_string()
    }
}

/// The models the API offers, in the order the UI lists them.
pub const HYRAX_MODELS: &[HyraxModelSpec] = &[HyraxModelSpec {
    id: "tempo_evidential",
    name: "TEMPO Evidential",
    description:
        "Photometry-only evidential transformer; reports class probabilities with vacuity.",
    url_env: "TEMPO_SERVICE_URL",
    default_url: "http://tempo-inference:8500",
    include_forced_photometry: false,
    surveys: &[Survey::Ztf],
}];

/// Look up a model spec by the id used on the wire.
pub fn find_model_spec(id: &str) -> Option<&'static HyraxModelSpec> {
    HYRAX_MODELS.iter().find(|spec| spec.id == id)
}

/// What the API sends a model's service about one object.
///
/// The photometry and metadata are `serde_json::Value` rather than typed structs
/// because ZTF and LSST objects carry differently shaped candidates and light
/// curves; a model that wants a specific field reads it by name.
#[derive(Debug)]
pub struct HyraxInput {
    /// Light curve: previous candidates, non-detections and forced photometry.
    pub photometry: serde_json::Value,
    /// The object's brightest alert (candidate block and enrichment), plus
    /// cross-matches.
    pub metadata: serde_json::Value,
}

/// Why a model run failed. Each variant maps to a different HTTP status.
#[derive(Debug, thiserror::Error)]
pub enum HyraxPredictError {
    /// The sidecar could not be reached at all — not running, the URL is wrong, or
    /// it did not answer in time.
    #[error("inference service at {url} is unreachable: {reason}")]
    ServiceUnreachable { url: String, reason: String },
    /// The sidecar is up but its model is not loaded.
    #[error("inference service is not ready: {detail}")]
    ServiceNotReady { detail: String },
    /// The sidecar ran, but the object's data gave the model nothing to score.
    /// `detail` is the sidecar's own message, meant for the caller.
    #[error("object could not be classified: {detail}")]
    Unclassifiable { detail: String },
    /// Any other refusal, which points at the deployment rather than the object: a
    /// model-id mismatch, a wrong URL, or a request the sidecar no longer accepts.
    #[error("inference service returned {status}: {detail}")]
    ServiceStatus { status: u16, detail: String },
    /// The sidecar answered with something that is not a classification.
    #[error("could not read the response from {url}: {reason}")]
    BadResponse { url: String, reason: String },
}

/// Shared client for the inference sidecars.
///
/// One client process-wide so connections are pooled across requests. The total
/// timeout stays under nginx's default 60s `proxy_read_timeout`, so the client gets
/// this API's 503 rather than a gateway error.
static SERVICE_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(50))
        .build()
        .expect("failed to build the Hyrax inference HTTP client")
});

/// What an inference service returns for one object.
///
/// The service labels its own classes, so the API never has to pair a bare score
/// vector with class names it holds separately, and cannot mislabel them.
#[derive(Debug, serde::Deserialize)]
pub struct ServiceClassification {
    pub classes: HashMap<String, f32>,
    pub pred_class: String,
    /// Evidential uncertainty: how little evidence the model had to go on.
    pub vacuity: f32,
    pub predictive_entropy: f32,
    /// How many photometry points survived the model's own preprocessing.
    pub n_events_used: usize,
}

/// FastAPI's error body. Its `detail` is the message the sidecar meant to send.
#[derive(Debug, serde::Deserialize)]
struct ServiceError {
    detail: String,
}

/// Classify one object by POSTing its inputs to `spec`'s inference service.
///
/// The photometry block is forwarded unchanged: the service does the light-curve
/// reshaping, next to the model that defines what shape it wants. The metadata rides
/// along for models that want candidate features; a service that has no use for it
/// ignores it.
#[instrument(skip_all, fields(model = spec.id, object = object_id))]
pub async fn classify_with_service(
    spec: &'static HyraxModelSpec,
    object_id: &str,
    input: &HyraxInput,
) -> Result<ServiceClassification, HyraxPredictError> {
    let url = format!("{}/classify", spec.base_url());
    let response = SERVICE_CLIENT
        .post(&url)
        .json(&serde_json::json!({
            "object_id": object_id,
            "photometry": input.photometry,
            "metadata": input.metadata,
            "model": spec.id,
            "include_forced_photometry": spec.include_forced_photometry,
        }))
        .send()
        .await
        .map_err(|error| HyraxPredictError::ServiceUnreachable {
            url: url.clone(),
            reason: error.to_string(),
        })?;

    let status = response.status();
    if !status.is_success() {
        // Read the body as text first: an unreachable-but-listening proxy answers
        // with HTML, which would fail to parse as FastAPI's error shape.
        let body = response.text().await.unwrap_or_default();
        // FastAPI's own request-validation 422 carries a list, not a string, so only
        // a string `detail` is the sidecar speaking about the object.
        let detail = serde_json::from_str::<ServiceError>(&body).map(|error| error.detail);
        return Err(match (status.as_u16(), detail) {
            (422, Ok(detail)) => HyraxPredictError::Unclassifiable { detail },
            (503, detail) => HyraxPredictError::ServiceNotReady {
                detail: detail.unwrap_or(body),
            },
            (status, detail) => HyraxPredictError::ServiceStatus {
                status,
                detail: detail.unwrap_or(body),
            },
        });
    }

    response
        .json::<ServiceClassification>()
        .await
        .map_err(|error| HyraxPredictError::BadResponse {
            url,
            reason: error.to_string(),
        })
}
