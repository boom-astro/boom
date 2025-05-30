use clap::Parser;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;

use boom::kafka::{AlertProducer, ZtfAlertProducer};

#[derive(Parser)]
struct Cli {
    #[arg(help = "Survey to produce alerts for. Options are: 'ZTF'")]
    survey: String,
    #[arg(help = "Date of archival alerts to produce, with format YYYYMMDD. Defaults to today.")]
    date: Option<String>,
    #[arg(help = "ID of the program to consume the alerts (ZTF only, defaults to 1=public).")]
    program_id: Option<u8>,
    #[arg(help = "Limit the number of alerts produced")]
    limit: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();

    let date = match args.date {
        Some(date) => chrono::NaiveDate::parse_from_str(&date, "%Y%m%d").unwrap(),
        None => chrono::Utc::now().date_naive().pred_opt().unwrap(),
    };
    let date = date.and_hms_opt(0, 0, 0).unwrap();
    let date_str = date.format("%Y%m%d").to_string();
    let limit = match args.limit {
        Some(l) => l,
        None => 0, // Default to 0 if no limit is provided
    };

    let program_id = match args.program_id {
        Some(id) if id >= 1 && id <= 3 => id,
        None => 1, // Default to program ID 1 (public)
        _ => {
            error!(
                "Invalid program ID: {}, must be 1, 2, or 3",
                args.program_id.unwrap_or(0)
            );
            return Ok(());
        }
    };

    let survey = args.survey;

    match survey.to_lowercase().as_str() {
        "ztf" => {
            let producer = ZtfAlertProducer::new(date_str, limit, Some(program_id));
            producer.produce(None).await?;
        }
        _ => {
            error!("Unknown survey (supported: 'ZTF'): {}", survey);
            return Ok(());
        }
    }

    Ok(())
}
