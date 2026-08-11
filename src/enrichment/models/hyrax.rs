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

use crate::enrichment::models::{AcaiModel, Model, ModelError};
use crate::utils::cutouts::AlertCutout;
use crate::utils::enums::Survey;
use ndarray::{Array, Dim};
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
    /// Base URL used when `url_env` is unset.
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
        let url = std::env::var(self.url_env).unwrap_or_else(|_| self.default_url.to_string());
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

/// Everything the API can gather about an object, assembled for every model run
/// whether or not the model asked for all of it.
///
/// Kept whole so that adding a model which wants images or candidate features is a
/// change to what gets forwarded, not a change to what gets collected.
///
/// The photometry and metadata are `serde_json::Value` rather than typed structs
/// because ZTF and LSST objects carry differently shaped candidates and light
/// curves; a model that wants a specific field reads it by name.
#[derive(Debug)]
pub struct HyraxInput {
    /// (1, 63, 63, 3) cutout triplet of the object's brightest alert.
    ///
    /// `None` when the object has no alert to point at or its cutouts were never
    /// stored. Absent rather than fatal: a model that needs an image should say so
    /// itself, which beats refusing to run a light-curve model on an object whose
    /// cutouts happen to be missing.
    pub triplet: Option<Array<f32, Dim<[usize; 4]>>>,
    /// Light curve: previous candidates, non-detections and forced photometry.
    pub photometry: serde_json::Value,
    /// The scored alert's candidate block and enrichment, plus cross-matches.
    pub metadata: serde_json::Value,
}

/// Build the (N, 63, 63, 3) triplet for a set of cutouts.
///
/// [`Model::get_triplet`] is a provided trait method that takes no receiver and
/// never touches `Self`, but Rust still needs an implementor to name it through.
/// Any would do; naming one here keeps that quirk out of the call site.
pub fn build_triplet(cutouts: &[&AlertCutout]) -> Result<Array<f32, Dim<[usize; 4]>>, ModelError> {
    AcaiModel::get_triplet(cutouts)
}

/// Why a model run failed. Each variant maps to a different HTTP status.
#[derive(Debug, thiserror::Error)]
pub enum HyraxPredictError {
    /// The sidecar could not be reached at all — not running, or the URL is wrong.
    #[error("inference service at {url} is unreachable: {reason}")]
    ServiceUnreachable { url: String, reason: String },
    /// The sidecar answered, but refused the request. `detail` is its own message,
    /// forwarded verbatim so the caller sees why rather than a generic failure.
    #[error("inference service returned {status}: {detail}")]
    ServiceStatus { status: u16, detail: String },
    /// The sidecar answered with something that is not a classification.
    #[error("could not read the response from {url}: {reason}")]
    BadResponse { url: String, reason: String },
}

/// Shared client for the inference sidecars.
///
/// One client process-wide so connections are pooled across requests. The timeout is
/// generous because a cold sidecar may still be loading its checkpoint, but bounded
/// so a wedged service cannot pin an actix worker indefinitely.
static SERVICE_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
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
///
/// [`HyraxInput::triplet`] is deliberately not sent. Serializing a 63x63x3 float
/// array into JSON on every call would cost far more than the models that ignore it
/// can justify — an image model wants a binary encoding negotiated with its service,
/// not this.
#[instrument(skip_all, fields(model = spec.id, object = object_id), err)]
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
        let detail = serde_json::from_str::<ServiceError>(&body)
            .map(|error| error.detail)
            .unwrap_or(body);
        return Err(HyraxPredictError::ServiceStatus {
            status: status.as_u16(),
            detail,
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
