use boom::conf::{load_dotenv, AppConfig};
use boom::filter::{Filter, FilterVersion};
use boom::utils::enums::Survey;
use clap::Parser;
use std::collections::HashMap;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
struct Cli {
    #[arg(value_enum, help = "Survey to add a filter for.")]
    survey: Survey,
    #[arg(help = "Name of the filter to be added.")]
    name: String,
    #[arg(help = "Path to the JSON file containing the filter")]
    filter_file: String,
    #[arg(
        long,
        help = "Optional description of the filter.",
        default_value = "Added via CLI"
    )]
    description: String,
}

fn now_jd() -> f64 {
    use chrono::Utc;
    (Utc::now().timestamp() as f64) / 86400.0 + 2440587.5
}

#[tokio::main]
async fn main() {
    // Load environment variables from .env file before anything else
    load_dotenv();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();
    let name = args.name;
    let description = args.description;
    let survey = args.survey;
    let filter_file = args.filter_file;

    // read the JSON as a string
    let filter_pipeline = match std::fs::read_to_string(&filter_file) {
        Ok(filter) => filter,
        Err(e) => {
            eprintln!("Error reading filter file: {}", e);
            std::process::exit(1);
        }
    };

    // Create a bson document with id, active, catalog, permissions
    // group_id, and a fv array with one doc that has a fid field and a pipeline field
    let filter_id: String = uuid::Uuid::new_v4().to_string();

    let permissions = HashMap::from([(Survey::Ztf, vec![1, 2, 3])]);
    let filter = Filter {
        id: filter_id.clone(),
        name: name,
        description: Some(description),
        active: true,
        user_id: "cli".to_string(),
        survey: survey,
        permissions: permissions,
        fv: vec![FilterVersion {
            fid: "v2e0fs".to_string(),
            pipeline: filter_pipeline,
            created_at: now_jd(),
            changelog: Some("Initial version added via CLI".to_string()),
        }],
        active_fid: "v2e0fs".to_string(),
        created_at: now_jd(),
        updated_at: now_jd(),
    };

    // insert the filter into the database
    let config = AppConfig::from_default_path().unwrap();

    let db = match config.build_db().await {
        Ok(db) => db,
        Err(e) => {
            error!("error building db: {}", e);
            std::process::exit(1);
        }
    };

    let collection = db.collection::<Filter>("filters");

    match collection.insert_one(filter).await {
        Ok(_) => {
            println!(
                "Filter with ID {} added successfully from {}",
                filter_id, filter_file
            );
        }
        Err(e) => {
            error!("error inserting filter obj: {}", e);
        }
    }
}
