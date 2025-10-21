use boom::conf::{build_db, build_xmatch_configs, load_config};
use boom::utils::o11y::logging::build_subscriber;
use boom::utils::spatial::xmatch;

#[tokio::main]
async fn main() {
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let config_path = "config.yaml";
    let config = load_config(config_path).unwrap();
    let xmatch_configs = build_xmatch_configs(&config, &boom::utils::enums::Survey::Lsst).unwrap();

    let db = build_db(&config).await.unwrap();

    let (ra, dec) = (8.494546, -43.965188);
    let xmatches = xmatch(ra, dec, &xmatch_configs, &db).await.unwrap();

    println!("Found {} matches:", xmatches.len());
}
