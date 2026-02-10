use boom::{
    conf::load_dotenv,
    kafka::{AlertProducer, DecamAlertProducer, ZtfAlertProducer},
    utils::{
        enums::{ProgramId, Survey},
        o11y::logging::build_subscriber,
    },
};

use clap::Parser;
use tracing::error;

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
    #[arg(
        long,
        help = "URL of the Kafka broker to produce to, defaults to localhost:9092"
    )]
    server_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables from .env file before anything else
    load_dotenv();

    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let args = Cli::parse();

    let date = match args.date {
        Some(date) => chrono::NaiveDate::parse_from_str(&date, "%Y%m%d").unwrap(),
        None => chrono::Utc::now().date_naive().pred_opt().unwrap(),
    };
    let limit = args.limit.unwrap_or(0);

    let program_id = args.program_id;

    let server_url = args
        .server_url
        .unwrap_or_else(|| "localhost:9092".to_string());

    let span = tracing::info_span!(
        "kafka::producer::produce",
        survey = ?args.survey, date = %date, limit = limit, program_id = ?program_id,
        server_url = %server_url,
    );
    let _g = span.enter();
    match args.survey {
        Survey::Ztf => {
            let producer = ZtfAlertProducer::new(date, limit, program_id, &server_url, true);
            producer.produce(None).await?;
        }
        Survey::Decam => {
            let producer = DecamAlertProducer::new(date, limit, &server_url, true);
            producer.produce(None).await?;
        }
        _ => {
            error!("Unsupported survey for producing alerts: {}", args.survey);
            return Ok(());
        }
    }

    Ok(())
}
