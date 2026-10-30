//! Run with `cargo run --features clickhouse-example --example clickhouse_shadow -- prepare FILE 1`.
//! Uses BOOM's native shadow adapter and ZTF record types.

use anyhow::{bail, Result};
use boom::alert::{ZtfForcedPhot, ZtfPrvCandidate};
use boom::conf::PhotometryShadowConfig;
use boom::photometry::store::{PreparedBatch, Store};
use serde::Deserialize;

// Keep the standalone demo's environment names for CLI compatibility.
fn store_from_env() -> Result<Store> {
    Store::new(&PhotometryShadowConfig {
        url: std::env::var("BOOM_CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8123".into()),
        database: std::env::var("BOOM_CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into()),
        username: std::env::var("BOOM_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into()),
        password: std::env::var("BOOM_CLICKHOUSE_PASSWORD").unwrap_or_default(),
        ..Default::default()
    })
}

#[derive(Deserialize)]
struct Input {
    #[serde(rename = "_id")]
    object_id: String,
    prv_candidates: Vec<ZtfPrvCandidate>,
    #[serde(default)]
    prv_nondetections: Vec<ZtfPrvCandidate>,
    fp_hists: Vec<ZtfForcedPhot>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some(command @ ("prepare" | "write")) if args.len() == 3 => {
            let input: Input = serde_json::from_str(&std::fs::read_to_string(&args[1])?)?;
            // These are the same types available immediately after sanitization
            // in ZtfAlertWorker::process_alert, before check_alert_exists.
            let batch = PreparedBatch::ztf(
                &input.object_id,
                args[2].parse()?,
                &input.prv_candidates,
                &input.prv_nondetections,
                &input.fp_hists,
            )?;
            if command == "prepare" {
                print!("{}", batch.body);
            } else {
                store_from_env()?.insert(&batch).await?;
                eprintln!("Inserted {} normalized BOOM records", batch.rows);
            }
        }
        Some("bins") if args.len()==3 => {
            println!("{}",serde_json::to_string_pretty(&store_from_env()?.bins(&args[1],args[2].parse()?).await?)?);
        }
        Some("read") if args.len() == 2 => {
            let history = store_from_env()?.history(&args[1]).await?;
            // Deserialization restores fields needed by BOOM's enrichment
            // pipelines, including raw forced flux, zero point and programid.
            let detections: Vec<ZtfPrvCandidate> =
                serde_json::from_value(serde_json::to_value(history.prv_candidates)?)?;
            let upper_limits: Vec<ZtfPrvCandidate> =
                serde_json::from_value(serde_json::to_value(history.prv_nondetections)?)?;
            let forced: Vec<ZtfForcedPhot> =
                serde_json::from_value(serde_json::to_value(history.fp_hists)?)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "prv_candidates": detections,
                    "prv_nondetections": upper_limits,
                    "fp_hists": forced,
                }))?
            );
        }
        _ => bail!(
            "usage: clickhouse_shadow prepare FILE REVISION | write FILE REVISION | read OBJECT_ID | bins OBJECT_ID CADENCE_SECONDS"
        ),
    }
    Ok(())
}
