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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};

    /// Answer one request on a fresh port with `status` and `body`, and return the
    /// base URL. The request is read in full first, so closing cannot reset it.
    fn serve_once(status: &'static str, body: &'static str) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(&stream);
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = value.trim().parse().unwrap();
                }
            }
            reader.read_exact(&mut vec![0; content_length]).unwrap();
            write!(
                &stream,
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        url
    }

    fn spec_at(url: String) -> &'static HyraxModelSpec {
        Box::leak(Box::new(HyraxModelSpec {
            id: "test_model",
            name: "Test",
            description: "",
            url_env: "BOOM_TEST_HYRAX_URL_NEVER_SET",
            default_url: Box::leak(url.into_boxed_str()),
            include_forced_photometry: false,
            surveys: &[],
        }))
    }

    async fn classify(url: String) -> Result<ServiceClassification, HyraxPredictError> {
        let input = HyraxInput {
            photometry: serde_json::json!({ "prv_candidates": [] }),
            metadata: serde_json::Value::Null,
        };
        classify_with_service(spec_at(url), "ZTF24aaaaaaa", &input).await
    }

    #[tokio::test]
    async fn success_is_parsed() {
        let url = serve_once(
            "200 OK",
            r#"{"classes":{"SN Ia":0.9,"AGN":0.1},"pred_class":"SN Ia","vacuity":0.2,"predictive_entropy":0.3,"n_events_used":12}"#,
        );
        let result = classify(url).await.unwrap();
        assert_eq!(result.pred_class, "SN Ia");
        assert_eq!(result.n_events_used, 12);
    }

    #[tokio::test]
    async fn sidecar_422_with_message_is_the_object() {
        let url = serve_once(
            "422 Unprocessable Entity",
            r#"{"detail":"no usable photometry"}"#,
        );
        match classify(url).await {
            Err(HyraxPredictError::Unclassifiable { detail }) => {
                assert_eq!(detail, "no usable photometry")
            }
            other => panic!("expected Unclassifiable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn validation_422_is_the_deployment() {
        let url = serve_once(
            "422 Unprocessable Entity",
            r#"{"detail":[{"loc":["body","object_id"],"msg":"field required"}]}"#,
        );
        assert!(matches!(
            classify(url).await,
            Err(HyraxPredictError::ServiceStatus { status: 422, .. })
        ));
    }

    #[tokio::test]
    async fn sidecar_503_is_not_ready_even_without_json() {
        let url = serve_once("503 Service Unavailable", "<html>bad gateway</html>");
        assert!(matches!(
            classify(url).await,
            Err(HyraxPredictError::ServiceNotReady { .. })
        ));
    }

    #[tokio::test]
    async fn other_4xx_is_the_deployment() {
        let url = serve_once(
            "400 Bad Request",
            r#"{"detail":"unknown model: test_model"}"#,
        );
        match classify(url).await {
            Err(HyraxPredictError::ServiceStatus {
                status: 400,
                detail,
            }) => {
                assert_eq!(detail, "unknown model: test_model")
            }
            other => panic!("expected ServiceStatus 400, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn closed_port_is_unreachable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        assert!(matches!(
            classify(url).await,
            Err(HyraxPredictError::ServiceUnreachable { .. })
        ));
    }

    #[test]
    fn empty_url_env_falls_back_to_default() {
        let spec = HyraxModelSpec {
            url_env: "BOOM_TEST_HYRAX_URL_EMPTY",
            ..*spec_at("http://default:8500/".to_string())
        };
        std::env::set_var(spec.url_env, "");
        assert_eq!(spec.base_url(), "http://default:8500");
        std::env::set_var(spec.url_env, "http://elsewhere:9000");
        assert_eq!(spec.base_url(), "http://elsewhere:9000");
    }
}
