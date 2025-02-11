use boom::kafka::produce_from_archive;
use clap::Parser;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
struct Cli {
    #[arg(help = "Date of archival alerts to produce, with format YYYYMMDD. Defaults to today.")]
    date: Option<String>,

    #[arg(
        long,
        value_name = "LIMIT",
        help = "Limit the number of alerts produced"
    )]
    limit: Option<i64>,
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
            info!("Using date from argument: {}", d);
            date = d;
        } else {
            error!("Invalid date format: {}", d);
            return Ok(());
        }
    }
    if let Some(l) = args.limit {
        limit = l;
    }

    let _ = produce_from_archive(&date, limit, None).await.unwrap();

    Ok(())
}
