use super::models::{now, Actor, Task, TaskStatus, Trigger};
use super::queue::{self, QueueError};
use super::{tns_photometry, tns_sync};
use crate::conf::AppConfig;
use crate::utils::db::is_duplicate_key;
use mongodb::bson::doc;
use mongodb::Database;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const CHECK_INTERVAL: Duration = Duration::from_secs(30);
const TNS_SYNC_OFFSET: Duration = Duration::from_secs(10 * 60);
const TNS_PHOTOMETRY_OFFSET: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone)]
pub struct Schedule {
    pub task_type: &'static str,
    pub every: Duration,
    pub offset: Duration,
    pub params: serde_json::Value,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Fired {
    Queued,
    AlreadyQueued,
    StillActive,
}

impl Schedule {
    pub fn fire_time(&self, now: i64) -> i64 {
        let every = self.every.as_secs().max(1) as i64;
        let offset = self.offset.as_secs() as i64 % every;
        (now - offset).div_euclid(every) * every + offset
    }

    pub fn run_id(&self, fire_time: i64) -> String {
        format!("sched:{}:{fire_time}", self.task_type)
    }
}

fn every_minutes(minutes: u64) -> Duration {
    Duration::from_secs(minutes * 60)
}

pub fn configured(config: &AppConfig) -> Vec<Schedule> {
    let tns = &config.tns;
    if !tns.has_credentials() {
        if tns.has_partial_credentials() {
            warn!("tns.bot_id, tns.bot_name and tns.api_key are not all set; TNS tasks are not scheduled");
        }
        return Vec::new();
    }
    let mut schedules = Vec::new();
    if tns.sync_every_minutes > 0 {
        schedules.push(Schedule {
            task_type: tns_sync::TASK_TYPE,
            every: every_minutes(tns.sync_every_minutes),
            offset: TNS_SYNC_OFFSET,
            params: serde_json::json!({}),
        });
    }
    if tns.photometry.every_minutes > 0 {
        schedules.push(Schedule {
            task_type: tns_photometry::TASK_TYPE,
            every: every_minutes(tns.photometry.every_minutes),
            offset: TNS_PHOTOMETRY_OFFSET,
            params: serde_json::json!({}),
        });
    }
    schedules
}

pub async fn fire(db: &Database, schedule: &Schedule, fire_time: i64) -> Result<Fired, QueueError> {
    if queue::find_active(db, schedule.task_type, doc! {})
        .await?
        .is_some()
    {
        return Ok(Fired::StillActive);
    }
    let task = Task {
        id: schedule.run_id(fire_time),
        task_type: schedule.task_type.to_string(),
        params: schedule.params.clone(),
        status: TaskStatus::Queued,
        actor: Actor::system(),
        trigger: Trigger::Schedule,
        requested_at: now(),
        started_at: None,
        finished_at: None,
        progress: Default::default(),
        worker: None,
        lease_expires_at: None,
        cancel_requested: false,
        error: None,
        attempts: 0,
        checkpoint: None,
        resubmitted_from: None,
    };
    match queue::submit(db, &task).await {
        Ok(()) => Ok(Fired::Queued),
        Err(QueueError::Mongo(error)) if is_duplicate_key(&error) => Ok(Fired::AlreadyQueued),
        Err(error) => Err(error),
    }
}

pub async fn run(db: Database, schedules: Vec<Schedule>, shutting_down: Arc<AtomicBool>) {
    let mut fired: Vec<Option<i64>> = vec![None; schedules.len()];
    while !shutting_down.load(Ordering::Relaxed) {
        let now = chrono::Utc::now().timestamp();
        for (schedule, last) in schedules.iter().zip(fired.iter_mut()) {
            let fire_time = schedule.fire_time(now);
            if *last == Some(fire_time) {
                continue;
            }
            match fire(&db, schedule, fire_time).await {
                Ok(Fired::Queued) => {
                    info!(
                        task_type = schedule.task_type,
                        "queued {}",
                        schedule.run_id(fire_time)
                    );
                    *last = Some(fire_time);
                }
                Ok(Fired::AlreadyQueued) => *last = Some(fire_time),
                Ok(Fired::StillActive) => {}
                Err(error) => warn!(
                    task_type = schedule.task_type,
                    "could not queue a scheduled run: {error}"
                ),
            }
        }
        tokio::time::sleep(CHECK_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_worker_derives_the_same_run_for_a_tick() {
        let hourly = Schedule {
            task_type: tns_sync::TASK_TYPE,
            every: Duration::from_secs(3600),
            offset: Duration::from_secs(600),
            params: serde_json::json!({}),
        };
        let tick = 1_769_328_600;
        assert_eq!(hourly.fire_time(tick), tick);
        assert_eq!(hourly.fire_time(tick + 3599), tick);
        assert_eq!(hourly.fire_time(tick - 1), tick - 3600);
        assert_eq!(hourly.run_id(tick), "sched:tns_sync:1769328600");
    }
}
