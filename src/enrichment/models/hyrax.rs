//! Hyrax models, run on demand against a single object via the Babamul API.
//!
//! Unlike the ACAI and BtsBot models — which the enrichment worker runs over every
//! alert at ingest time and persists into `classifications` — Hyrax models are run
//! interactively and their results are not written back to the object.
//!
//! The ONNX artifacts are not in the repo yet, so [`HYRAX_MODELS`] is a placeholder
//! registry: the API plumbing is real and end-to-end, but a model whose `.onnx` file
//! is missing reports itself as unavailable rather than failing at startup. Dropping
//! the artifact into `data/models/` and correcting the tensor names in its spec is
//! all that should be needed to make it live.

use crate::enrichment::models::{load_model, Model, ModelError};
use crate::utils::enums::Survey;
use ndarray::{Array, Dim};
use ort::{inputs, session::Session, value::TensorRef};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use tracing::{info, instrument};

/// Static description of a Hyrax ONNX model.
///
/// The tensor names are part of the spec because each exported model names its own
/// inputs and outputs (compare `"triplets"`/`"score"` for ACAI against
/// `"triplet"`/`"fc_out"` for BtsBot).
#[derive(Debug)]
pub struct HyraxModelSpec {
    /// Identifier the API accepts in the `model` field of a classify request.
    pub id: &'static str,
    /// Human-readable name, shown in the UI dropdown.
    pub name: &'static str,
    pub description: &'static str,
    /// Path to the ONNX artifact, relative to the process working directory.
    pub path: &'static str,
    /// Name of the ONNX input tensor that takes the (N, 63, 63, 3) cutout triplet.
    pub triplet_input: &'static str,
    /// Name of the ONNX output tensor holding the scores.
    pub output: &'static str,
    /// Class labels, in the model's output order. Empty means the model emits a
    /// single score rather than a probability per class.
    pub classes: &'static [&'static str],
    /// Surveys this model can be run against.
    pub surveys: &'static [Survey],
}

impl HyraxModelSpec {
    /// Whether the ONNX artifact is present on disk. False for every entry until
    /// the real Hyrax models are added to `data/models/`.
    pub fn is_available(&self) -> bool {
        Path::new(self.path).is_file()
    }
}

// TODO: placeholder entries. Replace the paths, tensor names and class labels with
// those of the real Hyrax exports once the artifacts land in data/models/.
pub const HYRAX_MODELS: &[HyraxModelSpec] = &[
    HyraxModelSpec {
        id: "hyrax_autoencoder",
        name: "Autoencoder",
        description: "Reconstruction-based anomaly scoring over cutouts.",
        path: "data/models/hyrax_autoencoder.onnx",
        triplet_input: "triplet",
        output: "score",
        classes: &[],
        surveys: &[Survey::Ztf],
    },
    HyraxModelSpec {
        id: "hyrax_cnn",
        name: "CNN Classifier",
        description: "Convolutional classifier over cutouts.",
        path: "data/models/hyrax_cnn.onnx",
        triplet_input: "triplet",
        output: "score",
        classes: &["real", "bogus"],
        surveys: &[Survey::Ztf],
    },
];

/// Look up a model spec by the id used on the wire.
pub fn find_model_spec(id: &str) -> Option<&'static HyraxModelSpec> {
    HYRAX_MODELS.iter().find(|spec| spec.id == id)
}

pub struct HyraxModel {
    model: Session,
    spec: &'static HyraxModelSpec,
}

impl HyraxModel {
    /// Load the ONNX artifact backing `spec`.
    #[instrument(skip(spec), fields(model = spec.id), err)]
    pub fn from_spec(spec: &'static HyraxModelSpec) -> Result<Self, ModelError> {
        if !spec.is_available() {
            return Err(ModelError::ModelArtifactNotFound(spec.path.to_string()));
        }
        Ok(Self {
            model: load_model(spec.path)?,
            spec,
        })
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
            self.spec.triplet_input => TensorRef::from_array_view(image_features)?,
        };

        let outputs = self.model.run(model_inputs)?;

        match outputs[self.spec.output].try_extract_tensor::<f32>() {
            Ok((_, scores)) => Ok(scores.to_vec()),
            Err(_) => Err(ModelError::ModelOutputToVecError),
        }
    }
}

impl Model for HyraxModel {
    /// Prefer [`HyraxModel::from_spec`]; this exists to satisfy the trait and to
    /// make [`Model::get_triplet`] available for building cutout tensors.
    fn new(path: &str) -> Result<Self, ModelError> {
        let spec = HYRAX_MODELS
            .iter()
            .find(|spec| spec.path == path)
            .ok_or_else(|| ModelError::UnknownModel(path.to_string()))?;
        Self::from_spec(spec)
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

    /// Score a batch of cutout triplets with the model registered under `id`,
    /// loading the ONNX artifact on first use.
    ///
    /// This blocks on both the ONNX session mutex and inference itself, so callers
    /// in async contexts should run it on a blocking thread pool.
    #[instrument(skip_all, fields(model = id), err)]
    pub fn predict(
        &self,
        id: &str,
        image_features: &Array<f32, Dim<[usize; 4]>>,
    ) -> Result<Vec<f32>, HyraxPredictError> {
        let spec =
            find_model_spec(id).ok_or_else(|| HyraxPredictError::UnknownModel(id.to_string()))?;

        let mut loaded = self
            .loaded
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if !loaded.contains_key(spec.id) {
            info!(
                model = spec.id,
                path = spec.path,
                "loading Hyrax ONNX model"
            );
            loaded.insert(spec.id, HyraxModel::from_spec(spec)?);
        }

        Ok(loaded
            .get_mut(spec.id)
            .expect("model just inserted")
            .predict_triplet(image_features)?)
    }
}
