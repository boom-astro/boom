use clap::Parser;
use mongodb::bson::doc;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;

use boom::utils::enums::Survey;
use boom::{conf, utils::db::create_index};

#[derive(Parser)]
struct Cli {
    #[arg(value_enum, help = "Survey to add a filter for.")]
    survey: Survey,
    #[arg(help = "Filter ID to add")]
    filter_id: i32,
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
    let filter_id = args.filter_id;
    let filter_file = args.filter_file;

    // read the JSON as a string
    let filter_pipeline = match std::fs::read_to_string(&filter_file) {
        Ok(filter) => filter,
        Err(e) => {
            eprintln!("Error reading filter file: {}", e);
            std::process::exit(1);
        }
    };

    let survey = match survey {
        Survey::Ztf => "ZTF",
        Survey::Lsst => "LSST",
    };

    // create a bson document with filter_id, active, catalog, permissions
    // group_id, and a fv array with one doc that has a fid field and a pipeline field

    let filter = doc! {
        "filter_id": filter_id,
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

    // add a unique index on filter_id
    match create_index(&collection, doc! {"filter_id": 1}, true).await {
        Ok(_) => {}
        Err(e) => {
            error!("error creating index on filter_id: {}", e);
        }
    }

    match collection.insert_one(filter).await {
        Ok(_) => {
            println!("Filter added successfully");
        }
        Err(e) => {
            error!("error inserting filter obj: {}", e);
        }
    }
}
