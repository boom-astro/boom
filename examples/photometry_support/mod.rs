//! Service guards and worker lifecycle for disposable development experiments.
#![allow(dead_code)]
use anyhow::{ensure, Result};
use boom::{
    alert::{run_alert_worker, ZtfAlertWorker},
    conf::AppConfig,
    utils::worker::WorkerCmd,
};
use mongodb::bson::{doc, Document};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

pub fn validate(config: &AppConfig) -> Result<()> {
    // Fail closed if ambient .env/BOOM variables override the disposable config.
    ensure!(
        config.database.host == "127.0.0.1"
            && config.database.name.starts_with("boom_shadow_replay_"),
        "requires an isolated local replay database"
    );
    ensure!(
        matches!(&config.cutouts_storage, boom::conf::CutoutsStorage::Mongo(m)
        if m.host==config.database.host && m.port==config.database.port && m.name==config.database.name),
        "cutouts must use the same disposable local Mongo database"
    );
    ensure!(
        config.redis.host == "127.0.0.1" && config.redis.port != 6379,
        "requires a dedicated local Redis port"
    );
    let url: reqwest::Url = config.photometry_shadow.url.parse()?;
    ensure!(
        url.host_str() == Some("127.0.0.1"),
        "replay ClickHouse must be loopback-only"
    );
    ensure!(
        config.photometry_shadow.database == config.database.name,
        "ClickHouse must use the same isolated replay database name"
    );
    Ok(())
}

pub async fn wait_for_alerts(db: &mongodb::Database, count: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    while db
        .collection::<Document>("ZTF_alerts")
        .count_documents(doc! {})
        .await?
        < count
    {
        ensure!(
            Instant::now() < deadline,
            "worker did not ingest fixture alerts in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

pub fn spawn_worker(
    path: &str,
) -> (
    mpsc::Sender<WorkerCmd>,
    tokio::task::JoinHandle<Result<(), boom::alert::AlertWorkerError>>,
) {
    let (sender, receiver) = mpsc::channel(1);
    let path = path.to_owned();
    let task = tokio::task::spawn_blocking(move || {
        run_alert_worker::<ZtfAlertWorker>(receiver, &path, Uuid::new_v4())
    });
    (sender, task)
}

pub async fn stop_worker(
    sender: mpsc::Sender<WorkerCmd>,
    task: tokio::task::JoinHandle<Result<(), boom::alert::AlertWorkerError>>,
) -> Result<()> {
    sender.send(WorkerCmd::TERM).await?;
    tokio::time::timeout(Duration::from_secs(10), task).await???;
    Ok(())
}
