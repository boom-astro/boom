use clap::Parser;
use mongodb::bson::doc;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;

use boom::conf;
use boom::utils::enums::Survey;

#[derive(Parser)]
struct Cli {
    #[arg(value_enum, help = "Survey to add a filter for.")]
    survey: Survey,
    #[arg(help = "Path to the JSON file containing the filter")]
    filter_file: String,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();
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

    let filter = doc! {
        "id": filter_id.clone(),
        "active": true,
        "catalog": format!("{}_alerts", survey),
        "permissions": [1,2,3],
        "group_id": 41,
        "fv": [
            {
                "fid": "v2e0fs",
                "pipeline": filter_pipeline
            }
        ],
        "active_fid": "v2e0fs",
    };

    // insert the filter into the database
    let config_file = match conf::load_config("config.yaml") {
        Ok(config) => config,
        Err(e) => {
            error!("error loading config file: {}", e);
            std::process::exit(1);
        }
    };

    let db = match conf::build_db(&config_file).await {
        Ok(db) => db,
        Err(e) => {
            error!("error building db: {}", e);
            std::process::exit(1);
        }
    };

    let collection = db.collection::<mongodb::bson::Document>("filters");

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
