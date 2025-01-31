use redis::AsyncCommands;
use std::env;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

fn download_alerts_from_archive(date: &str) -> Result<i64, Box<dyn std::error::Error>> {
    // given a date in the format YYYYMMDD, download public ZTF alerts from the archive
    // in this method we just validate the date format,
    // then use wget to download the alerts from the archive
    // and finally we extract the alerts to a folder

    // validate the date format
    if date.len() != 8 {
        return Err("Invalid date format".into());
    }

    // create the data folder if it doesn't exist, in data/alerts/ztf/<date>
    let data_folder = format!("data/alerts/ztf/{}", date);

    // if it already exists and has the alerts, we don't need to download them again
    if std::path::Path::new(&data_folder).exists() && std::fs::read_dir(&data_folder)?.count() > 0 {
        info!("Alerts already downloaded to {}", data_folder);
        let count = std::fs::read_dir(&data_folder)?.count();
        return Ok(count as i64);
    }

    std::fs::create_dir_all(&data_folder)?;

    info!("Downloading alerts for date {}", date);
    // download the alerts to data folder
    let url = format!(
        "https://ztf.uw.edu/alerts/public/ztf_public_{}.tar.gz",
        date
    );
    let output = std::process::Command::new("wget")
        .arg(&url)
        .arg("-P")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        return Err("Failed to download alerts".into());
    } else {
        info!("Downloaded alerts to {}", data_folder);
    }

    // extract the alerts
    let output = std::process::Command::new("tar")
        .arg("-xzf")
        .arg(format!("{}/ztf_public_{}.tar.gz", data_folder, date))
        .arg("-C")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        return Err("Failed to extract alerts".into());
    } else {
        info!("Extracted alerts to {}", data_folder);
    }

    // remove the tar.gz file
    std::fs::remove_file(format!("{}/ztf_public_{}.tar.gz", data_folder, date))?;

    // count the number of alerts
    let count = std::fs::read_dir(&data_folder)?.count();

    Ok(count as i64)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        error!("Usage: {} <date>", args[0]);
        return Ok(());
    }
    let date = &args[1];
    let total_nb_alerts = download_alerts_from_archive(date)?;

    let client = redis::Client::open("redis://localhost:6379".to_string())?;
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // empty the queue
    con.del::<&str, usize>("ZTF_alerts_packet_queue")
        .await
        .unwrap();

    let mut total = 0;

    info!("Pushing {} alerts to the queue", total_nb_alerts);
    // start timer
    let start = std::time::Instant::now();
    // poll one message at a time
    for entry in std::fs::read_dir(format!("data/alerts/ztf/{}", date))? {
        let entry = entry?;
        let path = entry.path();
        let payload = std::fs::read(path)?;

        con.rpush::<&str, Vec<u8>, usize>("ZTF_alerts_packet_queue", payload.to_vec())
            .await
            .unwrap();
        total += 1;
        if total % 1000 == 0 {
            info!("Pushed {} items since {:?}", total, start.elapsed());
        }
    }

    Ok(())
}
