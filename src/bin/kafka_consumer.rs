use boom::{
    kafka::{AlertConsumer, DecamAlertConsumer, LsstAlertConsumer, ZtfAlertConsumer},
    utils::{
        enums::{ProgramId, Survey},
        o11y::build_subscriber,
    },
};

use chrono::{NaiveDate, NaiveDateTime};
use clap::Parser;
use tracing::instrument;

#[derive(Parser)]
struct Cli {
    /// Survey to consume alerts from
    #[arg(value_enum)]
    survey: Survey,

    /// UTC date for which we want to consume alerts, with format YYYYMMDD
    /// [default: yesterday's date]
    #[arg(value_parser = parse_date)]
    date: Option<NaiveDate>, // Easier to deal with the default value after clap

    /// ID of the program to consume the alerts (ZTF-only)
    #[arg(default_value_t, value_enum)]
    program_id: ProgramId,

    /// Path to the configuration file
    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Number of processes to use to read the Kafka stream in parallel
    #[arg(long, default_value_t = 1)]
    processes: usize,

    /// Clear the in-memory (Valkey) queue of alerts already consumed from Kafka
    #[arg(long)]
    clear: bool,

    /// Set a maximum number of alerts to hold in memory (Valkey), default is
    /// 15000
    #[arg(long, value_name = "MAX", default_value_t = 15000)]
    max_in_queue: usize,
}

fn parse_date(s: &str) -> Result<NaiveDate, String> {
    let date =
        NaiveDate::parse_from_str(s, "%Y%m%d").map_err(|_| "expected a date in YYYYMMDD format")?;
    Ok(date)
}

#[instrument(skip_all, fields(survey = %args.survey))]
async fn run(args: Cli) {
    let timestamp = NaiveDateTime::from(args.date.unwrap_or_else(|| {
        chrono::Utc::now()
            .date_naive()
            .pred_opt()
            .expect("previous date is not representable")
    }))
    .and_utc()
    .timestamp();

    // TODO: let the user specify if they want to consume real or simulated LSST data
    let simulated = match args.survey {
        Survey::Lsst => true,
        _ => false,
    };

    match args.survey {
        Survey::Ztf => {
            let consumer = ZtfAlertConsumer::new(
                args.processes,
                Some(args.max_in_queue),
                None,
                None,
                None,
                args.program_id,
                &args.config,
            );
            if args.clear {
                let _ = consumer.clear_output_queue();
            }
            consumer.consume(timestamp).await;
        }
        Survey::Lsst => {
            let consumer = LsstAlertConsumer::new(
                args.processes,
                Some(args.max_in_queue),
                None,
                None,
                None,
                simulated,
                &args.config,
            );
            if args.clear {
                let _ = consumer.clear_output_queue();
            }
            consumer.consume(timestamp).await;
        }
        Survey::Decam => {
            let consumer = DecamAlertConsumer::new(
                args.processes,
                Some(args.max_in_queue),
                None,
                None,
                &args.config,
            );
            if args.clear {
                let _ = consumer.clear_output_queue();
            }
            consumer.consume(timestamp).await;
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");
    run(args).await;
}
