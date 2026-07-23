//! Verifies connectivity to a Milvus vector database and, optionally, creates
//! the embeddings collection.
//!
//! This is the first thing to run after putting credentials in `.env` — it
//! confirms the endpoint, the credentials, and the database name in one shot,
//! without touching any data.
//!
//! ```bash
//! cargo run --bin milvus_check
//! cargo run --bin milvus_check -- --create-collection
//! ```

use boom::{
    conf::{load_dotenv, AppConfig},
    milvus::MilvusClient,
};
use clap::Parser;
use std::error::Error;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

/// Connect to Milvus and report what the credentials can see.
#[derive(Parser)]
struct Cli {
    /// Path to the BOOM config file.
    #[arg(long, default_value = "config.yaml")]
    config: String,

    /// Create the embeddings collection (plus its index) if it does not exist.
    /// Without this flag the run is strictly read-only.
    #[arg(long)]
    create_collection: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    load_dotenv();
    let config = AppConfig::from_path(&cli.config)?;
    let milvus_config = &config.milvus;

    if !milvus_config.enabled {
        return Err(
            "milvus is disabled; set BOOM_MILVUS__ENABLED=true (and the credentials) to use it"
                .into(),
        );
    }

    println!("endpoint:   {}", milvus_config.endpoint());
    println!("database:   {}", milvus_config.database);
    println!("username:   {}", milvus_config.username);
    println!("collection: {}", milvus_config.collection.name);
    println!();

    let mut client = MilvusClient::connect(milvus_config).await?;

    let version = client.version().await?;
    println!("server version: {}", version);

    // Confirms the credentials are valid and shows whether the configured
    // database name actually matches one we can see.
    let databases = client.list_databases().await?;
    println!("databases visible: {}", databases.join(", "));

    if !databases.contains(&milvus_config.database) {
        println!(
            "\nWARNING: configured database {:?} is not in that list. On NRP the \
             database name is your GROUP name with dashes replaced by underscores.",
            milvus_config.database
        );
    }

    if cli.create_collection {
        let created = client.ensure_embedding_collection().await?;
        if created {
            println!(
                "created collection {} (dim {}, metric {})",
                milvus_config.collection.name,
                milvus_config.collection.dim,
                milvus_config.collection.metric_type
            );
        } else {
            println!(
                "collection {} already exists; left untouched",
                milvus_config.collection.name
            );
        }
    }

    let collections = client.list_collections().await?;
    if collections.is_empty() {
        println!("collections in {}: (none)", milvus_config.database);
    } else {
        println!(
            "collections in {}: {}",
            milvus_config.database,
            collections.join(", ")
        );
    }

    Ok(())
}
