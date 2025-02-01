use boom::kafka::produce_from_archive;
use std::env;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args: Vec<String> = env::args().collect();
    let mut date = chrono::Utc::now().format("%Y%m%d").to_string();
    let mut limit = 0;
    if args.len() > 1 {
        let arg = &args[1];
        if arg.len() == 8 {
            info!("Using date from argument: {}", arg);
            date = arg.to_string();
        } else {
            error!("Invalid date format: {}", arg);
            return Ok(());
        }
    }
    if args.len() > 2 {
        limit = args[2].parse::<i64>().unwrap_or(0);
    }

    let _ = produce_from_archive(&date, limit).await.unwrap();

    Ok(())
}
