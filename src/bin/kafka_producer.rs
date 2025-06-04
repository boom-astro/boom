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
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();

    let date = match args.date {
        Some(date) => chrono::NaiveDate::parse_from_str(&date, "%Y%m%d").unwrap(),
        None => chrono::Utc::now().date_naive().pred_opt().unwrap(),
    };
    let limit = args.limit.unwrap_or(0);

    let program_id = args.program_id;

    match args.survey {
        Survey::Ztf => {
            let producer = ZtfAlertProducer::new(date, limit, program_id, true);
            producer.produce(None).await?;
        }
        _ => {
            error!("Only ZTF survey is supported for producing alerts from file (for now).");
            return Ok(());
        }
    }

    Ok(())
}
