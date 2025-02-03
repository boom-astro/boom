use boom::kafka::consume_alerts;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        error!("Usage: kafka_consumer <topic> <group_id> <exit_on_eof> <max_in_queue>");
        return Ok(());
    }

    let topic = &args[1];
    let group_id = match args.get(2) {
        Some(id) => Some(id.to_string()),
        None => None,
    };

    let mut exit_on_eof = false;
    if args.len() > 3 {
        if args[3] == "true" {
            exit_on_eof = true;
        }
    }
    let max_in_queue: usize = match args.get(4) {
        Some(max_in_queue) => max_in_queue.parse().unwrap(),
        None => 1000,
    };

    let _ = consume_alerts(topic, group_id, exit_on_eof, max_in_queue, None)
        .await
        .unwrap();

    Ok(())
}
