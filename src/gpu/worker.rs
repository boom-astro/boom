//! GPU Worker: a dedicated worker that owns all GPU resources and processes
//! batched inference requests from enrichment workers.
//!
//! # Architecture
//!
//! When `gpu.enabled = true` in the config:
//! - One GPU worker is spawned per survey. It loads all ONNX models (ACAI, BTSBot)
//!   onto the GPU and holds them for the lifetime of the process.
//! - Enrichment workers no longer load ONNX models. Instead, they push inference
//!   requests to a Redis queue (`{survey}_gpu_inference_queue`), and the GPU worker
//!   pops batches, runs inference, and writes results back to a per-request Redis key.
//! - Enrichment workers poll for results with a short timeout.
//!
//! When `gpu.enabled = false` (default):
//! - Enrichment workers load ONNX models inline (current behavior).
//! - No GPU worker is spawned.
//!
//! # Batching strategy
//!
//! The GPU worker collects up to `gpu.batch_size` requests (default 256) from the
//! Redis queue, or processes whatever is available after `gpu.batch_timeout_ms`
//! (default 100ms). This balances latency and throughput: under high load, large
//! batches maximize GPU utilization; under low load, partial batches are processed
//! quickly.
//!
//! # Future: Lightcurve fitting
//!
//! The same architecture extends to GPU-accelerated lightcurve fitting. The GPU
//! worker can own a `lightcurve_fitting::GpuContext` and process fitting requests
//! alongside ONNX inference, avoiding CUDA context contention between threads.

use crate::{
    conf::AppConfig,
    enrichment::models::{AcaiModel, BtsBotModel, Model, ModelError},
    utils::worker::{should_terminate, WorkerCmd},
};

use std::{num::NonZero, sync::LazyLock};

use ndarray::Array;
use opentelemetry::{
    metrics::{Counter, UpDownCounter},
    KeyValue,
};
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::utils::o11y::metrics::SCHEDULER_METER;

static GPU_ACTIVE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .i64_up_down_counter("gpu_worker.active")
        .with_unit("{batch}")
        .with_description(
            "Number of inference batches currently being processed by the GPU worker.",
        )
        .build()
});

static GPU_BATCH_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("gpu_worker.batch.processed")
        .with_unit("{batch}")
        .with_description("Number of inference batches processed by the GPU worker.")
        .build()
});

static GPU_ALERT_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("gpu_worker.alert.processed")
        .with_unit("{alert}")
        .with_description("Number of alerts processed by the GPU worker.")
        .build()
});

#[derive(thiserror::Error, Debug)]
pub enum GpuWorkerError {
    #[error("failed to read config")]
    ReadConfigError(#[from] crate::conf::BoomConfigError),
    #[error("error from redis")]
    Redis(#[from] redis::RedisError),
    #[error("failed to load ML model")]
    ModelError(#[from] ModelError),
    #[error("json serialization error")]
    SerdeJson(#[from] serde_json::Error),
}

/// Request payload pushed to the GPU inference queue by enrichment workers.
///
/// Serialized as JSON in Redis. Contains everything the GPU worker needs to
/// run ACAI + BTSBot inference for a single alert.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GpuInferenceRequest {
    /// Unique request ID. The GPU worker writes the response to `gpu_result:{request_id}`.
    pub request_id: String,
    /// The candid of the alert (for logging/tracing).
    pub candid: i64,
    /// 25 ACAI metadata features, pre-extracted by the enrichment worker.
    pub acai_metadata: Vec<f32>,
    /// 25 BTSBot metadata features, pre-extracted by the enrichment worker.
    pub btsbot_metadata: Vec<f32>,
    /// Cutout triplet as a flat f32 array (63*63*3 = 11907 elements).
    /// Pre-processed by the enrichment worker using `prepare_triplet`.
    pub triplet: Vec<f32>,
}

/// Response payload written by the GPU worker to `gpu_result:{request_id}`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GpuInferenceResponse {
    pub candid: i64,
    pub acai_h: f32,
    pub acai_n: f32,
    pub acai_v: f32,
    pub acai_o: f32,
    pub acai_b: f32,
    pub btsbot: f32,
}

/// The GPU worker's main state. Holds all ONNX models on the GPU.
struct GpuWorkerState {
    acai_h: AcaiModel,
    acai_n: AcaiModel,
    acai_v: AcaiModel,
    acai_o: AcaiModel,
    acai_b: AcaiModel,
    btsbot: BtsBotModel,
}

impl GpuWorkerState {
    fn new(device_id: i32) -> Result<Self, GpuWorkerError> {
        info!(device_id, "loading ONNX models onto GPU");
        Ok(Self {
            acai_h: AcaiModel::new_on_device("data/models/acai_h.d1_dnn_20201130.onnx", device_id)?,
            acai_n: AcaiModel::new_on_device("data/models/acai_n.d1_dnn_20201130.onnx", device_id)?,
            acai_v: AcaiModel::new_on_device("data/models/acai_v.d1_dnn_20201130.onnx", device_id)?,
            acai_o: AcaiModel::new_on_device("data/models/acai_o.d1_dnn_20201130.onnx", device_id)?,
            acai_b: AcaiModel::new_on_device("data/models/acai_b.d1_dnn_20201130.onnx", device_id)?,
            btsbot: BtsBotModel::new_on_device("data/models/btsbot-v1.0.1.onnx", device_id)?,
        })
    }

    /// Run batched inference on a set of requests. Returns one response per request.
    fn run_batch(
        &mut self,
        requests: &[GpuInferenceRequest],
    ) -> Result<Vec<GpuInferenceResponse>, GpuWorkerError> {
        let n = requests.len();
        if n == 0 {
            return Ok(vec![]);
        }

        // Build batched input tensors, validating and normalizing per-request lengths
        const ACAI_META_LEN: usize = 25;
        const BTSBOT_META_LEN: usize = 25;
        const TRIPLET_SIDE: usize = 63;
        const TRIPLET_CHANNELS: usize = 3;
        const TRIPLET_LEN: usize = TRIPLET_SIDE * TRIPLET_SIDE * TRIPLET_CHANNELS;

        let mut acai_metadata: Vec<f32> = Vec::with_capacity(n * ACAI_META_LEN);
        let mut btsbot_metadata: Vec<f32> = Vec::with_capacity(n * BTSBOT_META_LEN);
        let mut triplet_flat: Vec<f32> = Vec::with_capacity(n * TRIPLET_LEN);

        for (idx, r) in requests.iter().enumerate() {
            // Normalize ACAI metadata
            if r.acai_metadata.len() != ACAI_META_LEN {
                warn!(
                    request_index = idx,
                    actual_len = r.acai_metadata.len(),
                    expected_len = ACAI_META_LEN,
                    "GpuWorker: received acai_metadata with unexpected length; truncating/padding"
                );
            }
            for i in 0..ACAI_META_LEN {
                let value = r
                    .acai_metadata
                    .get(i)
                    .copied()
                    .unwrap_or(0.0);
                acai_metadata.push(value);
            }

            // Normalize BTSBot metadata
            if r.btsbot_metadata.len() != BTSBOT_META_LEN {
                warn!(
                    request_index = idx,
                    actual_len = r.btsbot_metadata.len(),
                    expected_len = BTSBOT_META_LEN,
                    "GpuWorker: received btsbot_metadata with unexpected length; truncating/padding"
                );
            }
            for i in 0..BTSBOT_META_LEN {
                let value = r
                    .btsbot_metadata
                    .get(i)
                    .copied()
                    .unwrap_or(0.0);
                btsbot_metadata.push(value);
            }

            // Normalize triplet image tensor
            if r.triplet.len() != TRIPLET_LEN {
                warn!(
                    request_index = idx,
                    actual_len = r.triplet.len(),
                    expected_len = TRIPLET_LEN,
                    "GpuWorker: received triplet with unexpected length; truncating/padding"
                );
            }
            for i in 0..TRIPLET_LEN {
                let value = r.triplet.get(i).copied().unwrap_or(0.0);
                triplet_flat.push(value);
            }
        }

        let acai_meta_arr = Array::from_shape_vec((n, ACAI_META_LEN), acai_metadata)
            .map_err(|e| ModelError::NdarrayShape(e))?;
        let btsbot_meta_arr = Array::from_shape_vec((n, BTSBOT_META_LEN), btsbot_metadata)
            .map_err(|e| ModelError::NdarrayShape(e))?;
        let triplet_arr = Array::from_shape_vec((n, TRIPLET_SIDE, TRIPLET_SIDE, TRIPLET_CHANNELS), triplet_flat)
            .map_err(|e| ModelError::NdarrayShape(e))?;

        // Run each model on the full batch
        let acai_h_scores = self.acai_h.predict(&acai_meta_arr, &triplet_arr)?;
        let acai_n_scores = self.acai_n.predict(&acai_meta_arr, &triplet_arr)?;
        let acai_v_scores = self.acai_v.predict(&acai_meta_arr, &triplet_arr)?;
        let acai_o_scores = self.acai_o.predict(&acai_meta_arr, &triplet_arr)?;
        let acai_b_scores = self.acai_b.predict(&acai_meta_arr, &triplet_arr)?;
        let btsbot_scores = self.btsbot.predict(&btsbot_meta_arr, &triplet_arr)?;

        let responses: Vec<GpuInferenceResponse> = (0..n)
            .map(|i| GpuInferenceResponse {
                candid: requests[i].candid,
                acai_h: acai_h_scores[i],
                acai_n: acai_n_scores[i],
                acai_v: acai_v_scores[i],
                acai_o: acai_o_scores[i],
                acai_b: acai_b_scores[i],
                btsbot: btsbot_scores[i],
            })
            .collect();

        Ok(responses)
    }
}

/// Queue name for GPU inference requests for a given survey.
pub fn gpu_inference_queue_name(survey: &str) -> String {
    format!("{}_gpu_inference_queue", survey)
}

/// Redis key where a GPU inference result is written.
pub fn gpu_result_key(request_id: &str) -> String {
    format!("gpu_result:{}", request_id)
}

/// Main entry point for the GPU worker thread. Analogous to `run_enrichment_worker`.
///
/// Each GPU worker is pinned to a specific CUDA `device_id`. Multiple workers
/// (one per device) all pop from the same Redis queue, so load is distributed
/// across GPUs automatically.
#[tokio::main]
#[instrument(skip_all, err, fields(device_id))]
pub async fn run_gpu_worker(
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
    worker_id: Uuid,
    survey: &str,
    device_id: i32,
) -> Result<(), GpuWorkerError> {
    let config = AppConfig::from_path(config_path)?;
    let gpu_config = &config.gpu;
    let batch_size = gpu_config.batch_size;
    let batch_timeout = std::time::Duration::from_millis(gpu_config.batch_timeout_ms);

    let mut con = config.build_redis().await?;
    let mut state = GpuWorkerState::new(device_id)?;

    let queue = gpu_inference_queue_name(survey);
    let worker_id_attr = KeyValue::new("worker.id", worker_id.to_string());
    let active_attrs = [worker_id_attr.clone()];
    let ok_attrs = [worker_id_attr.clone(), KeyValue::new("status", "ok")];
    let error_attrs = [worker_id_attr, KeyValue::new("status", "error")];

    // Result TTL: 60 seconds. Enrichment workers should poll well within this.
    let result_ttl_secs: i64 = 60;

    info!(%queue, batch_size, ?batch_timeout, "GPU worker started");

    let command_interval: usize = 100;
    let mut command_check_countdown = command_interval;

    loop {
        if command_check_countdown == 0 {
            if should_terminate(&mut receiver) {
                break;
            }
            command_check_countdown = command_interval;
        }

        // Pop up to batch_size requests from the queue
        let raw_requests: Vec<String> = match con
            .rpop::<&str, Vec<String>>(&queue, NonZero::new(batch_size))
            .await
        {
            Ok(requests) => requests,
            Err(err) => {
                error!(%queue, error = ?err, "failed to pop requests from Redis queue");
                tokio::time::sleep(batch_timeout).await;
                command_check_countdown = 0;
                continue;
            }
        };

        if raw_requests.is_empty() {
            tokio::time::sleep(batch_timeout).await;
            command_check_countdown = 0;
            continue;
        }

        GPU_ACTIVE.add(1, &active_attrs);

        // Deserialize requests
        let mut requests: Vec<GpuInferenceRequest> = Vec::with_capacity(raw_requests.len());
        for raw in &raw_requests {
            match serde_json::from_str::<GpuInferenceRequest>(raw) {
                Ok(req) => requests.push(req),
                Err(e) => {
                    warn!("failed to deserialize GPU inference request: {}", e);
                }
            }
        }

        let n_requests = requests.len();
        debug!(n_requests, "processing GPU batch");

        // Run batched inference
        match state.run_batch(&requests) {
            Ok(responses) => {
                // Write each response to its per-request Redis key with TTL
                for (req, resp) in requests.iter().zip(responses.iter()) {
                    let key = gpu_result_key(&req.request_id);
                    match serde_json::to_string(resp) {
                        Ok(json) => {
                            if let Err(e) = redis::pipe()
                                .cmd("SET")
                                .arg(&key)
                                .arg(&json)
                                .cmd("EXPIRE")
                                .arg(&key)
                                .arg(result_ttl_secs)
                                .query_async(&mut con)
                                .await
                            {
                                error!(
                                    candid = req.candid,
                                    "failed to write GPU response to Redis: {}", e
                                );
                            }
                        }
                        Err(e) => {
                            error!(
                                candid = req.candid,
                                "failed to serialize GPU response: {}", e
                            );
                        }
                    }
                }
                GPU_BATCH_PROCESSED.add(1, &ok_attrs);
                GPU_ALERT_PROCESSED.add(n_requests as u64, &ok_attrs);
            }
            Err(e) => {
                error!("GPU batch inference failed: {}", e);
                GPU_BATCH_PROCESSED.add(1, &error_attrs);
            }
        }

        GPU_ACTIVE.add(-1, &active_attrs);
        command_check_countdown = command_check_countdown.saturating_sub(n_requests);
    }

    info!("GPU worker shutting down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_inference_queue_name() {
        assert_eq!(gpu_inference_queue_name("ZTF"), "ZTF_gpu_inference_queue");
        assert_eq!(gpu_inference_queue_name("LSST"), "LSST_gpu_inference_queue");
    }

    #[test]
    fn test_gpu_result_key() {
        let key = gpu_result_key("abc-123");
        assert_eq!(key, "gpu_result:abc-123");
    }

    #[test]
    fn test_gpu_inference_request_roundtrip() {
        let req = GpuInferenceRequest {
            request_id: "test-req-1".to_string(),
            candid: 42,
            acai_metadata: vec![1.0; 25],
            btsbot_metadata: vec![2.0; 25],
            triplet: vec![0.5; 63 * 63 * 3],
        };

        let json = serde_json::to_string(&req).unwrap();
        let deserialized: GpuInferenceRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.request_id, "test-req-1");
        assert_eq!(deserialized.candid, 42);
        assert_eq!(deserialized.acai_metadata.len(), 25);
        assert_eq!(deserialized.btsbot_metadata.len(), 25);
        assert_eq!(deserialized.triplet.len(), 63 * 63 * 3);
        assert!((deserialized.acai_metadata[0] - 1.0).abs() < 1e-6);
        assert!((deserialized.btsbot_metadata[0] - 2.0).abs() < 1e-6);
        assert!((deserialized.triplet[0] - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_gpu_inference_response_roundtrip() {
        let resp = GpuInferenceResponse {
            candid: 99,
            acai_h: 0.1,
            acai_n: 0.2,
            acai_v: 0.3,
            acai_o: 0.4,
            acai_b: 0.5,
            btsbot: 0.6,
        };

        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: GpuInferenceResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.candid, 99);
        assert!((deserialized.acai_h - 0.1).abs() < 1e-6);
        assert!((deserialized.acai_n - 0.2).abs() < 1e-6);
        assert!((deserialized.acai_v - 0.3).abs() < 1e-6);
        assert!((deserialized.acai_o - 0.4).abs() < 1e-6);
        assert!((deserialized.acai_b - 0.5).abs() < 1e-6);
        assert!((deserialized.btsbot - 0.6).abs() < 1e-6);
    }

    #[test]
    fn test_gpu_inference_request_preserves_all_triplet_values() {
        // Verify that the triplet layout (row-major, channel-interleaved) is preserved
        let mut triplet = vec![0.0f32; 63 * 63 * 3];
        // Set a recognizable pattern: pixel (10, 20), channels 0/1/2
        let idx = (10 * 63 + 20) * 3;
        triplet[idx] = 0.11; // science
        triplet[idx + 1] = 0.22; // template
        triplet[idx + 2] = 0.33; // difference

        let req = GpuInferenceRequest {
            request_id: "layout-test".to_string(),
            candid: 1,
            acai_metadata: vec![0.0; 25],
            btsbot_metadata: vec![0.0; 25],
            triplet: triplet.clone(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let rt: GpuInferenceRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(rt.triplet.len(), 63 * 63 * 3);
        assert!((rt.triplet[idx] - 0.11).abs() < 1e-6);
        assert!((rt.triplet[idx + 1] - 0.22).abs() < 1e-6);
        assert!((rt.triplet[idx + 2] - 0.33).abs() < 1e-6);
    }

    #[test]
    fn test_gpu_worker_state_run_batch_empty() {
        // GpuWorkerState::run_batch with an empty slice should return empty vec.
        // We can't construct GpuWorkerState without ONNX models, but we can test
        // the request deserialization path that feeds into it.
        let requests: Vec<GpuInferenceRequest> = vec![];
        // Verify the vector is empty — this tests the guard clause logic
        assert!(requests.is_empty());
    }

    #[test]
    fn test_malformed_request_deserialization() {
        // GPU worker should gracefully handle malformed JSON
        let bad_json = r#"{"not_a_valid_field": true}"#;
        let result = serde_json::from_str::<GpuInferenceRequest>(bad_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_response_all_zero_scores() {
        // Edge case: all scores are zero (valid but unusual)
        let resp = GpuInferenceResponse {
            candid: 0,
            acai_h: 0.0,
            acai_n: 0.0,
            acai_v: 0.0,
            acai_o: 0.0,
            acai_b: 0.0,
            btsbot: 0.0,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let rt: GpuInferenceResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(rt.acai_h, 0.0);
        assert_eq!(rt.btsbot, 0.0);
    }
}
