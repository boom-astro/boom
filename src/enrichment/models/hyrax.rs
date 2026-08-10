//! Hyrax models, run on demand against a single object via the Babamul API.
//!
//! Unlike the ACAI and BtsBot models — which the enrichment worker runs over every
//! alert at ingest time and persists into `classifications` — Hyrax models are run
//! interactively and their results are not written back to the object.
//!
//! Nothing about a spec is checked ahead of a request: every registered model is
//! listed and can be selected, and a backend that turns out not to be there fails
//! the run it was asked for rather than being filtered out of the list. Whether a
//! model can answer is a property of the moment it is called, not of what happens
//! to sit in `data/models/` at startup.

use crate::enrichment::models::{load_model, Model, ModelError};
use crate::utils::enums::Survey;
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use tracing::{info, instrument};

/// A local ONNX artifact, scored over the object's cutout triplet.
///
/// The tensor names are part of the spec because each exported model names its own
/// inputs and outputs (compare `"triplets"`/`"score"` for ACAI against
/// `"triplet"`/`"fc_out"` for BtsBot).
#[derive(Debug)]
pub struct OnnxBackend {
    /// Path to the ONNX artifact, relative to the process working directory.
    pub path: &'static str,
    /// Name of the ONNX input tensor that takes the (N, 63, 63, 3) cutout triplet.
    pub triplet_input: &'static str,
    /// Name of the ONNX output tensor holding the scores.
    pub output: &'static str,
}

/// An out-of-process inference service, handed the object's photometry over HTTP.
///
/// Exists because not every Hyrax model is an ONNX graph: TEMPO is a PyTorch
/// transformer that boom cannot load in-process, so it runs behind the FastAPI
/// sidecar in `services/tempo/` and the API forwards the light curve it has already
/// gathered.
#[derive(Debug)]
pub struct ServiceBackend {
    /// Environment variable holding the service's base URL, so a deployment can move
    /// the sidecar without a rebuild.
    pub url_env: &'static str,
    /// Base URL used when `url_env` is unset.
    pub default_url: &'static str,
    /// Whether to send forced photometry alongside the alert magnitudes. Forced
    /// photometry re-measures the same epochs, so it changes what the model sees.
    pub include_forced_photometry: bool,
}

impl ServiceBackend {
    /// Base URL of the service, with any trailing slash removed so callers can
    /// append `/classify` without doubling the separator.
    pub fn base_url(&self) -> String {
        let url = std::env::var(self.url_env).unwrap_or_else(|_| self.default_url.to_string());
        url.trim_end_matches('/').to_string()
    }
}

/// How a registered model is actually run.
#[derive(Debug)]
pub enum HyraxBackend {
    Onnx(OnnxBackend),
    Service(ServiceBackend),
}

/// Static description of a model that can be run on demand against one object.
#[derive(Debug)]
pub struct HyraxModelSpec {
    /// Identifier the API accepts in the `model` field of a classify request.
    pub id: &'static str,
    /// Human-readable name, shown in the UI dropdown.
    pub name: &'static str,
    pub description: &'static str,
    /// What runs the model, and what it needs as input.
    pub backend: HyraxBackend,
    /// Class labels, in the model's output order.
    ///
    /// Empty means the labels are not known at compile time: either the model emits
    /// a single score rather than a probability per class, or — as for service
    /// backends — the backend names its own classes in the response.
    pub classes: &'static [&'static str],
    /// Surveys this model can be run against.
    pub surveys: &'static [Survey],
}

impl HyraxModelSpec {
    /// The ONNX backend behind this spec, if it has one.
    pub fn onnx(&self) -> Option<&OnnxBackend> {
        match &self.backend {
            HyraxBackend::Onnx(backend) => Some(backend),
            HyraxBackend::Service(_) => None,
        }
    }

    /// The inference service behind this spec, if it has one.
    pub fn service(&self) -> Option<&ServiceBackend> {
        match &self.backend {
            HyraxBackend::Service(backend) => Some(backend),
            HyraxBackend::Onnx(_) => None,
        }
    }
}

// TODO: the two ONNX entries are placeholders. Replace the paths, tensor names and
// class labels with those of the real Hyrax exports once the artifacts land in
// data/models/.
pub const HYRAX_MODELS: &[HyraxModelSpec] = &[
    HyraxModelSpec {
        id: "hyrax_autoencoder",
        name: "Autoencoder",
        description: "Reconstruction-based anomaly scoring over cutouts.",
        backend: HyraxBackend::Onnx(OnnxBackend {
            path: "data/models/hyrax_autoencoder.onnx",
            triplet_input: "triplet",
            output: "score",
        }),
        classes: &[],
        surveys: &[Survey::Ztf],
    },
    HyraxModelSpec {
        id: "hyrax_cnn",
        name: "CNN Classifier",
        description: "Convolutional classifier over cutouts.",
        backend: HyraxBackend::Onnx(OnnxBackend {
            path: "data/models/hyrax_cnn.onnx",
            triplet_input: "triplet",
            output: "score",
        }),
        classes: &["real", "bogus"],
        surveys: &[Survey::Ztf],
    },
    HyraxModelSpec {
        id: "tempo_evidential",
        name: "TEMPO Evidential",
        description:
            "Photometry-only evidential transformer; reports class probabilities with vacuity.",
        backend: HyraxBackend::Service(ServiceBackend {
            url_env: "TEMPO_SERVICE_URL",
            default_url: "http://tempo-inference:8500",
            include_forced_photometry: false,
        }),
        // Left empty deliberately: the labels come from the taxonomy preset baked
        // into whichever bundle the sidecar has loaded, so the API cannot state them
        // ahead of a run. The classify response carries them.
        classes: &[],
        surveys: &[Survey::Ztf],
    },
];

/// Look up a model spec by the id used on the wire.
pub fn find_model_spec(id: &str) -> Option<&'static HyraxModelSpec> {
    HYRAX_MODELS.iter().find(|spec| spec.id == id)
}

/// Everything the API knows about an object, handed to every Hyrax model run.
///
/// Only [`triplet`](Self::triplet) is bound to an ONNX input today, because the
/// current specs declare a single image tensor. The photometry and metadata travel
/// with every request anyway so that a real Hyrax export wanting light-curve or
/// candidate features can be wired up by naming its tensors in
/// [`HyraxModelSpec`] — no change to the API or to the call site.
///
/// Both extra blocks are `serde_json::Value` rather than typed structs because ZTF
/// and LSST objects carry differently shaped candidates and light curves; a model
/// that wants a specific field reads it by name.
#[derive(Debug)]
pub struct HyraxInput {
    /// (N, 63, 63, 3) cutout triplet.
    pub triplet: Array<f32, Dim<[usize; 4]>>,
    /// Light curve: previous candidates, non-detections and forced photometry.
    pub photometry: serde_json::Value,
    /// Newest alert's candidate and properties, plus classifications and matches.
    pub metadata: serde_json::Value,
}

pub struct HyraxModel {
    model: Session,
    onnx: &'static OnnxBackend,
}

impl HyraxModel {
    /// Load the ONNX artifact behind `onnx`. `spec` is carried for its id, which
    /// names the model in traces.
    #[instrument(skip(spec, onnx), fields(model = spec.id), err)]
    pub fn from_spec(
        spec: &'static HyraxModelSpec,
        onnx: &'static OnnxBackend,
    ) -> Result<Self, ModelError> {
        // Checked here rather than anywhere earlier: a missing artifact is a failure
        // of this run, not a reason to hide the model from the list. `ort` would
        // report it too, but not in a form the API can turn into its own status.
        if !Path::new(onnx.path).is_file() {
            return Err(ModelError::ModelArtifactNotFound(onnx.path.to_string()));
        }
        Ok(Self {
            model: load_model(onnx.path)?,
            onnx,
        })
    }

    /// Run the model over a full object payload.
    ///
    /// Delegates to [`Self::predict_triplet`] for now: the placeholder specs bind
    /// only an image tensor, so `input.photometry` and `input.metadata` are carried
    /// but unread. A spec that grows tensor names for them is the only change
    /// needed to start feeding them to the session.
    #[instrument(skip_all, err)]
    pub fn predict_input(&mut self, input: &HyraxInput) -> Result<Vec<f32>, ModelError> {
        self.predict_triplet(&input.triplet)
    }

    /// Run the model over a batch of cutout triplets.
    ///
    /// Hyrax models score the image data alone, so unlike ACAI and BtsBot there is
    /// no metadata feature vector to assemble.
    #[instrument(skip_all, err)]
    pub fn predict_triplet(
        &mut self,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError> {
        let model_inputs = inputs! {
            self.onnx.triplet_input => TensorRef::from_array_view(image_features)?,
        };

        let outputs = self.model.run(model_inputs)?;

        match outputs[self.onnx.output].try_extract_tensor::<f32>() {
            Ok((_, scores)) => Ok(scores.to_vec()),
            Err(_) => Err(ModelError::ModelOutputToVecError),
        }
    }
}

impl Model for HyraxModel {
    /// Prefer [`HyraxModel::from_spec`]; this exists to satisfy the trait and to
    /// make [`Model::get_triplet`] available for building cutout tensors.
    fn new(path: &str) -> Result<Self, ModelError> {
        let (spec, onnx) = HYRAX_MODELS
            .iter()
            .find_map(|spec| {
                spec.onnx()
                    .filter(|onnx| onnx.path == path)
                    .map(|o| (spec, o))
            })
            .ok_or_else(|| ModelError::UnknownModel(path.to_string()))?;
        Self::from_spec(spec, onnx)
    }

    /// `metadata_features` is ignored — Hyrax models take image data only.
    fn predict(
        &mut self,
        _metadata_features: &Array<f32, Dim<[usize; 2]>>,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, ModelError> {
        self.predict_triplet(image_features)
    }
}

/// Why a model run failed.
///
/// [`ModelError`] cannot cross a thread boundary — `ort::Error<SessionBuilder>` holds
/// raw ONNX Runtime pointers and so is neither `Send` nor `Sync` — but callers run
/// inference on a blocking thread pool and need the reason back. This flattens the
/// failure into an owned, `Send` form while keeping the distinctions that map to
/// different HTTP statuses.
#[derive(Debug, thiserror::Error)]
pub enum HyraxPredictError {
    #[error("unknown model: {0}")]
    UnknownModel(String),
    #[error("model artifact not found on disk: {0}")]
    ArtifactNotFound(String),
    #[error("inference failed: {0}")]
    Inference(String),
    /// The sidecar could not be reached at all — not running, or the URL is wrong.
    #[error("inference service at {url} is unreachable: {reason}")]
    ServiceUnreachable { url: String, reason: String },
    /// The sidecar answered, but refused the request. `detail` is its own message,
    /// forwarded verbatim so the operator sees why rather than a generic failure.
    #[error("inference service returned {status}: {detail}")]
    ServiceStatus { status: u16, detail: String },
}

impl From<ModelError> for HyraxPredictError {
    fn from(error: ModelError) -> Self {
        match error {
            ModelError::UnknownModel(id) => Self::UnknownModel(id),
            ModelError::ModelArtifactNotFound(path) => Self::ArtifactNotFound(path),
            other => Self::Inference(other.to_string()),
        }
    }
}

/// Lazily-loaded, process-wide cache of Hyrax models.
///
/// The API server holds one of these. Models are loaded on first use rather than at
/// startup so that a missing artifact surfaces as a per-request error instead of
/// taking the whole API down, and so the API pays no memory cost for models nobody
/// asks for. As with [`super::SharedModels`], each session sits behind a `Mutex`
/// because `Session::run` needs `&mut self`; concurrent requests for the same model
/// serialize.
#[derive(Default)]
pub struct HyraxModelRegistry {
    loaded: Mutex<HashMap<&'static str, HyraxModel>>,
}

impl std::fmt::Debug for HyraxModelRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyraxModelRegistry").finish_non_exhaustive()
    }
}

impl HyraxModelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Score an object payload with the model registered under `id`, loading the
    /// ONNX artifact on first use.
    ///
    /// This blocks on both the ONNX session mutex and inference itself, so callers
    /// in async contexts should run it on a blocking thread pool.
    #[instrument(skip_all, fields(model = id), err)]
    pub fn predict(&self, id: &str, input: &HyraxInput) -> Result<Vec<f32>, HyraxPredictError> {
        let spec =
            find_model_spec(id).ok_or_else(|| HyraxPredictError::UnknownModel(id.to_string()))?;
        // Service-backed models are run over HTTP by `classify_with_service`, not
        // here: they have no ONNX session to cache and no blocking work to do.
        let onnx = spec
            .onnx()
            .ok_or_else(|| HyraxPredictError::UnknownModel(id.to_string()))?;

        let mut loaded = self
            .loaded
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if !loaded.contains_key(spec.id) {
            info!(
                model = spec.id,
                path = onnx.path,
                "loading Hyrax ONNX model"
            );
            loaded.insert(spec.id, HyraxModel::from_spec(spec, onnx)?);
        }

        Ok(loaded
            .get_mut(spec.id)
            .expect("model just inserted")
            .predict_input(input)?)
    }
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
/// The service labels its own classes, so unlike the ONNX path there is no zip
/// against [`HyraxModelSpec::classes`] and no chance of mislabelling scores.
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

/// Classify one object by POSTing its photometry to `spec`'s inference service.
///
/// The photometry block is forwarded unchanged: the service does the light-curve
/// reshaping, next to the model that defines what shape it wants.
#[instrument(skip_all, fields(model = spec.id, object = object_id), err)]
pub async fn classify_with_service(
    spec: &'static HyraxModelSpec,
    backend: &ServiceBackend,
    object_id: &str,
    photometry: &serde_json::Value,
) -> Result<ServiceClassification, HyraxPredictError> {
    let url = format!("{}/classify", backend.base_url());
    let response = SERVICE_CLIENT
        .post(&url)
        .json(&serde_json::json!({
            "object_id": object_id,
            "photometry": photometry,
            "model": spec.id,
            "include_forced_photometry": backend.include_forced_photometry,
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
        .map_err(|error| {
            HyraxPredictError::Inference(format!(
                "could not read the response from {}: {}",
                url, error
            ))
        })
}
