use clap::Parser;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;

use boom::kafka::{AlertProducer, DecamAlertProducer, ZtfAlertProducer};

#[derive(Parser)]
struct Cli {
    #[arg(required = true, help = "Name of stream to ingest")]
    stream: Option<String>,
    #[arg(help = "Date of archival alerts to produce, with format YYYYMMDD. Defaults to today.")]
    date: Option<String>,
    #[arg(
        long,
        value_name = "LIMIT",
        help = "Limit the number of alerts produced"
    )]
    limit: Option<i64>,
    #[arg(
        value_name = "PROGRAMID",
        help = "ID of the program producing the alerts (ZTF only, defaults to 1=public)."
    )]
    program_id: Option<u8>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();
    let mut date = chrono::Utc::now().format("%Y%m%d").to_string();
    let mut limit = 0;

    if let Some(d) = args.date {
        if d.len() == 8 {
            date = d;
        } else {
            error!("Invalid date format: {}", d);
            return Ok(());
        }
    }
    if let Some(l) = args.limit {
        limit = l;
    }

    let program_id = args.program_id.unwrap_or(1);
    // program_id might be between 1 and 3 included
    if program_id < 1 || program_id > 3 {
        error!("Invalid program ID: {}, must be 1, 2, or 3", program_id);
        return Ok(());
    }

    let stream = args.stream.unwrap_or_else(|| {
        error!("No stream specified");
        std::process::exit(1);
    });
    let stream = stream.to_lowercase();

    match stream.as_str() {
        "ztf" => {
            let producer = ZtfAlertProducer::new(date, limit, Some(program_id));
            producer.produce(None).await?;
        }
        "decam" => {
            let producer = DecamAlertProducer::new(date, limit, None);
            producer.produce(None).await?;
        }
        _ => {
            error!("Unknown stream (supported: ztf, decam): {}", stream);
            return Ok(());
        }
    }

    Ok(())
}
