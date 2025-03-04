use chrono::TimeZone;
use clap::Parser;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use boom::kafka::{AlertConsumer, LsstAlertConsumer, ZtfAlertConsumer};

#[derive(Parser)]
struct Cli {
    #[arg(help = "Survey to consume alerts from. Options are 'ZTF' or 'LSST'")]
    survey: String,
    #[arg(help = "UTC date for which we want to consume alerts, with format YYYYMMDD")]
    date: Option<String>,
    #[arg(help = "Number of processes to use to read the Kafka stream in parallel")]
    processes: Option<usize>,
    #[arg(help = "Clear the queue of alerts already consumed from Kafka and pushed to Redis")]
    clear: Option<bool>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();
    let date = args.date.unwrap();
    let survey = args.survey;
    let processes = args.processes.unwrap_or(1);
    let clear = args.clear.unwrap_or(false);

    // TODO: based on the location of the telescope, figure out the exact timestamp
    // for the start of the night
    let date = chrono::NaiveDate::parse_from_str(&date, "%Y%m%d").unwrap();
    let date = date.and_hms_opt(0, 0, 0).unwrap();
    let timestamp = chrono::Utc.from_utc_datetime(&date).timestamp();

    match survey.as_str() {
        "ZTF" => {
            let consumer = ZtfAlertConsumer::new(processes, None, None, None, None, None);
            if clear {
                let _ = consumer.clear_output_queue();
            }
            consumer.consume(timestamp).await?;
        }
        "LSST" => {
            let consumer = LsstAlertConsumer::new(processes, Some(20000), None, None, None, None);
            if clear {
                let _ = consumer.clear_output_queue();
            }
            consumer.consume(timestamp).await?;
        }
        _ => {
            panic!("Invalid survey provided. Options are 'ZTF' or 'LSST'");
        }
    }

    Ok(())
}
