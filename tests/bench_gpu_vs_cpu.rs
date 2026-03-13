//! Benchmark: CPU-inline (one-at-a-time) vs GPU-batched inference paths.
//!
//! Measures actual ONNX inference time on CPU for both paths (no Redis involved).
//! The GPU path's `run_batch` is called directly, skipping serialization overhead,
//! so this isolates the effect of batching on ONNX execution.
//!
//! Run with:
//!   cargo test --release --test bench_gpu_vs_cpu -- --ignored --nocapture

use boom::enrichment::models::{AcaiModel, BtsBotModel, Model};
use ndarray::Array;
use std::time::Instant;

fn make_random_metadata(n: usize, n_features: usize) -> Array<f32, ndarray::Dim<[usize; 2]>> {
    // Deterministic pseudo-random features
    let data: Vec<f32> = (0..n * n_features)
        .map(|i| (i as f32 * 0.7123 + 0.3).sin() * 0.5 + 0.5)
        .collect();
    Array::from_shape_vec((n, n_features), data).unwrap()
}

fn make_random_triplets(n: usize) -> Array<f32, ndarray::Dim<[usize; 4]>> {
    let data: Vec<f32> = (0..n * 63 * 63 * 3)
        .map(|i| (i as f32 * 0.3217 + 0.1).sin() * 0.5 + 0.5)
        .collect();
    Array::from_shape_vec((n, 63, 63, 3), data).unwrap()
}

struct AllModels {
    acai_h: AcaiModel,
    acai_n: AcaiModel,
    acai_v: AcaiModel,
    acai_o: AcaiModel,
    acai_b: AcaiModel,
    btsbot: BtsBotModel,
}

impl AllModels {
    fn load() -> Self {
        Self {
            acai_h: AcaiModel::new("data/models/acai_h.d1_dnn_20201130.onnx").unwrap(),
            acai_n: AcaiModel::new("data/models/acai_n.d1_dnn_20201130.onnx").unwrap(),
            acai_v: AcaiModel::new("data/models/acai_v.d1_dnn_20201130.onnx").unwrap(),
            acai_o: AcaiModel::new("data/models/acai_o.d1_dnn_20201130.onnx").unwrap(),
            acai_b: AcaiModel::new("data/models/acai_b.d1_dnn_20201130.onnx").unwrap(),
            btsbot: BtsBotModel::new("data/models/btsbot-v1.0.1.onnx").unwrap(),
        }
    }

    /// CPU-inline path: run 6 models one alert at a time (batch_size=1).
    /// This is what each enrichment worker does today.
    fn run_one_at_a_time(&mut self, n: usize) -> std::time::Duration {
        let acai_meta = make_random_metadata(1, 25);
        let btsbot_meta = make_random_metadata(1, 25);
        let triplet = make_random_triplets(1);

        let start = Instant::now();
        for _ in 0..n {
            self.acai_h.predict(&acai_meta, &triplet).unwrap();
            self.acai_n.predict(&acai_meta, &triplet).unwrap();
            self.acai_v.predict(&acai_meta, &triplet).unwrap();
            self.acai_o.predict(&acai_meta, &triplet).unwrap();
            self.acai_b.predict(&acai_meta, &triplet).unwrap();
            self.btsbot.predict(&btsbot_meta, &triplet).unwrap();
        }
        start.elapsed()
    }

    /// GPU-batched path: run 6 models on the full batch at once.
    /// This is what the GPU worker's run_batch does.
    fn run_batched(&mut self, n: usize) -> std::time::Duration {
        let acai_meta = make_random_metadata(n, 25);
        let btsbot_meta = make_random_metadata(n, 25);
        let triplet = make_random_triplets(n);

        let start = Instant::now();
        self.acai_h.predict(&acai_meta, &triplet).unwrap();
        self.acai_n.predict(&acai_meta, &triplet).unwrap();
        self.acai_v.predict(&acai_meta, &triplet).unwrap();
        self.acai_o.predict(&acai_meta, &triplet).unwrap();
        self.acai_b.predict(&acai_meta, &triplet).unwrap();
        self.btsbot.predict(&btsbot_meta, &triplet).unwrap();
        start.elapsed()
    }
}

/// Also measure the JSON serialization overhead that the GPU path adds.
fn measure_serde_overhead(n: usize) -> (std::time::Duration, usize) {
    use boom::gpu::GpuInferenceRequest;

    let requests: Vec<GpuInferenceRequest> = (0..n)
        .map(|i| GpuInferenceRequest {
            request_id: format!("req-{}", i),
            candid: i as i64,
            acai_metadata: vec![0.5f32; 25],
            btsbot_metadata: vec![0.5f32; 25],
            triplet: vec![0.5f32; 63 * 63 * 3],
        })
        .collect();

    let start = Instant::now();
    let mut total_bytes = 0usize;
    for req in &requests {
        let json = serde_json::to_string(req).unwrap();
        total_bytes += json.len();
        // Simulate the GPU worker deserializing
        let _: GpuInferenceRequest = serde_json::from_str(&json).unwrap();
    }
    (start.elapsed(), total_bytes)
}

#[test]
#[ignore]
fn bench_inference_cpu_vs_batched() {
    // Default to CPU; set USE_GPU=true externally to test GPU path
    let use_gpu = std::env::var("USE_GPU").unwrap_or_else(|_| "false".to_string());
    println!("\nUSE_GPU={}", use_gpu);

    println!("Loading models...");
    let mut models = AllModels::load();

    // Warmup
    println!("Warming up...");
    models.run_one_at_a_time(10);
    models.run_batched(10);

    println!(
        "\n{:>8} {:>14} {:>14} {:>10} {:>14}",
        "batch", "one-at-a-time", "batched", "speedup", "serde overhead"
    );
    println!("{}", "-".repeat(70));

    for &n in &[1, 10, 50, 100, 250, 500, 1000] {
        // Run each path 3 times and take the median
        let mut inline_times = Vec::new();
        let mut batch_times = Vec::new();

        for _ in 0..3 {
            inline_times.push(models.run_one_at_a_time(n));
            batch_times.push(models.run_batched(n));
        }

        inline_times.sort();
        batch_times.sort();

        let inline_ms = inline_times[1].as_secs_f64() * 1000.0;
        let batch_ms = batch_times[1].as_secs_f64() * 1000.0;
        let speedup = inline_ms / batch_ms;

        let (serde_dur, serde_bytes) = measure_serde_overhead(n);
        let serde_ms = serde_dur.as_secs_f64() * 1000.0;

        println!(
            "{:>8} {:>11.1} ms {:>11.1} ms {:>9.2}x {:>9.1} ms ({:.0} KB)",
            n,
            inline_ms,
            batch_ms,
            speedup,
            serde_ms,
            serde_bytes as f64 / 1024.0,
        );
    }

    println!("\nNotes:");
    println!(
        "  - 'one-at-a-time': 6 models × N sequential predict(batch=1) calls (current CPU path)"
    );
    println!("  - 'batched': 6 models × 1 predict(batch=N) call (GPU worker path)");
    println!(
        "  - 'serde overhead': JSON serialize+deserialize N requests (additional GPU path cost)"
    );
    println!("  - USE_GPU={use_gpu}. Set USE_GPU=true to test GPU execution.");
}
