//! Benchmark: sequential (one-at-a-time) vs batched ONNX inference.
//!
//! Measures actual ONNX inference time for both paths.
//! With GPU enabled, batching shows significant speedup due to GPU parallelism.
//!
//! Run with:
//!   cargo test --release --test bench_gpu_vs_cpu -- --ignored --nocapture

use boom::enrichment::models::{AcaiModel, BtsBotModel, Model};
use ndarray::Array;
use std::time::Instant;

fn make_random_metadata(n: usize, n_features: usize) -> Array<f32, ndarray::Dim<[usize; 2]>> {
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

    /// Sequential path: run 6 models one alert at a time (batch_size=1).
    fn run_one_at_a_time(&self, n: usize) -> std::time::Duration {
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

    /// Batched path: run 6 models on the full batch at once.
    fn run_batched(&self, n: usize) -> std::time::Duration {
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

#[test]
#[ignore]
fn bench_inference_cpu_vs_batched() {
    let use_gpu = std::env::var("USE_GPU").unwrap_or_else(|_| "true".to_string());
    println!("\nUSE_GPU={}", use_gpu);

    println!("Loading models...");
    let models = AllModels::load();

    // Warmup
    println!("Warming up...");
    models.run_one_at_a_time(10);
    models.run_batched(10);

    println!(
        "\n{:>8} {:>14} {:>14} {:>10}",
        "batch", "one-at-a-time", "batched", "speedup"
    );
    println!("{}", "-".repeat(50));

    for &n in &[1, 10, 50, 100, 250, 500, 1000] {
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

        println!(
            "{:>8} {:>11.1} ms {:>11.1} ms {:>9.2}x",
            n, inline_ms, batch_ms, speedup,
        );
    }

    println!("\nNotes:");
    println!("  - 'one-at-a-time': 6 models × N sequential predict(batch=1) calls");
    println!("  - 'batched': 6 models × 1 predict(batch=N) call");
    println!("  - USE_GPU={use_gpu}. Set USE_GPU=false to force CPU-only execution.");
}
