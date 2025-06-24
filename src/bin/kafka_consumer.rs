use chrono::TimeZone;
use clap::Parser;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use boom::kafka::{AlertConsumer, LsstAlertConsumer, ZtfAlertConsumer};
use boom::utils::enums::{ProgramId, Survey};

#[derive(Parser)]
struct Cli {
    #[arg(value_enum, help = "Survey to consume alerts from.")]
    survey: Survey,
    #[arg(help = "UTC date for which we want to consume alerts, with format YYYYMMDD")]
    date: Option<String>,
    #[arg(
        default_value_t,
        value_enum,
        help = "ID of the program to consume the alerts (ZTF-only)."
    )]
    program_id: ProgramId,
    #[arg(long, value_name = "FILE", help = "Path to the configuration file")]
    config: Option<String>,
    #[arg(
        long,
        help = "Number of processes to use to read the Kafka stream in parallel"
    )]
    processes: Option<usize>,
    #[arg(
        long,
        help = "Clear the in-memory (Valkey) queue of alerts already consumed from Kafka"
    )]
    clear: Option<bool>,
    #[arg(
        long,
        help = "Set a maximum number of alerts to hold in memory (Valkey), default is 15000",
        value_name = "MAX"
    )]
    max_in_queue: Option<usize>,
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
    let date = date.and_hms_opt(0, 0, 0).unwrap();
    let timestamp = chrono::Utc.from_utc_datetime(&date).timestamp();

    let program_id = args.program_id;

    let processes = args.processes.unwrap_or(1);
    let max_in_queue = args.max_in_queue.unwrap_or(15000);
    let clear = args.clear.unwrap_or(false);

    let config_path = match args.config {
        Some(path) => path,
        None => "config.yaml".to_string(),
    };

    let survey = args.survey;

    // TODO: let the user specify if they want to consume real or simulated LSST data
    let simulated = match survey {
        Survey::Lsst => true,
        _ => false,
    };

    match survey {
        Survey::Ztf => {
            let consumer = ZtfAlertConsumer::new(
                processes,
                Some(max_in_queue),
                None,
                None,
                None,
                program_id,
                &config_path,
            );
            if clear {
                let _ = consumer.clear_output_queue();
            }
            consumer.consume(timestamp).await?;
        }
        Survey::Lsst => {
            let consumer = LsstAlertConsumer::new(
                processes,
                Some(max_in_queue),
                None,
                None,
                None,
                simulated,
                &config_path,
            );
            if clear {
                let _ = consumer.clear_output_queue();
            }
            consumer.consume(timestamp).await?;
        }
    }

    Ok(())
}
