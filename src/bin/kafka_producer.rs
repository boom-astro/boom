use clap::Parser;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;

use boom::kafka::{AlertProducer, ZtfAlertProducer};
use boom::utils::enums::{ProgramId, Survey};

#[derive(Parser)]
struct Cli {
    #[arg(value_enum, help = "Survey to produce alerts for (from file).")]
    survey: Survey,
    #[arg(
        help = "UTC date of archival alerts to produce, with format YYYYMMDD. Defaults to today."
    )]
    date: Option<String>,
    #[arg(
        default_value_t,
        value_enum,
        help = "ID of the program to produce the alerts (ZTF-only)."
    )]
    program_id: ProgramId,
    #[arg(long, help = "Limit the number of alerts produced")]
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

    let program_id = args.program_id;

    match args.survey {
        Survey::Ztf => {
            let producer = ZtfAlertProducer::new(date_str, limit, Some(program_id));
            producer.produce(None).await?;
        }
        _ => {
            error!("Only ZTF survey is supported for producing alerts from file (for now).");
            return Ok(());
        }
    }

    Ok(())
}
