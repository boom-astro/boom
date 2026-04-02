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
    #[arg(long, default_value_t = 1, help = "Number of producer runs to execute")]
    repeat: usize,
    #[arg(
        long,
        default_value_t = 2000,
        help = "Delay in milliseconds between repeated runs"
    )]
    interval_ms: u64,
    #[arg(long, help = "Run continuously until interrupted (Ctrl+C)")]
    continuous: bool,
    #[arg(
        long,
        help = "Force production even if the topic already has messages (non-destructive append mode)"
    )]
    force: bool,
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

    let force = args.force || args.continuous || args.repeat > 1;

    if args.continuous {
        loop {
            match args.survey {
                Survey::Ztf => {
                    let producer =
                        ZtfAlertProducer::new(date, limit, program_id.clone(), &server_url, true);
                    producer.produce(None, force).await?;
                }
                Survey::Decam => {
                    let producer = DecamAlertProducer::new(date, limit, &server_url, true);
                    producer.produce(None, force).await?;
                }
                _ => {
                    error!("Unsupported survey for producing alerts: {}", args.survey);
                    return Ok(());
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(args.interval_ms)).await;
        }
    }

    for run_idx in 0..args.repeat {
        match args.survey {
            Survey::Ztf => {
                let producer =
                    ZtfAlertProducer::new(date, limit, program_id.clone(), &server_url, true);
                producer.produce(None, force).await?;
            }
            Survey::Decam => {
                let producer = DecamAlertProducer::new(date, limit, &server_url, true);
                producer.produce(None, force).await?;
            }
            _ => {
                error!("Unsupported survey for producing alerts: {}", args.survey);
                return Ok(());
            }
        }

        if run_idx + 1 < args.repeat {
            tokio::time::sleep(std::time::Duration::from_millis(args.interval_ms)).await;
        }
    }

    Ok(())
}
