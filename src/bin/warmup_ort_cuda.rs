use boom::enrichment::models::{BtsBotModel, Model};
use ndarray::Array;
use std::error::Error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

fn make_metadata_batch() -> Array<f32, ndarray::Dim<[usize; 2]>> {
    Array::from_shape_vec((1, 25), vec![0.5; 25]).expect("valid metadata shape")
}

fn make_triplet_batch() -> Array<f32, ndarray::Dim<[usize; 4]>> {
    Array::from_shape_vec((1, 63, 63, 3), vec![0.5; 63 * 63 * 3]).expect("valid triplet shape")
}

fn main() -> Result<(), Box<dyn Error>> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info,ort=info"))?;

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_filter(env_filter),
        )
        .try_init()?;
    let mut model = BtsBotModel::new("data/models/btsbot-v1.0.1.onnx")?;
    let metadata = make_metadata_batch();
    let triplet = make_triplet_batch();
    let _ = model.predict(&metadata, &triplet)?;
    Ok(())
}
