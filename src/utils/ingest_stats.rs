use chrono::Utc;
use mongodb::{
    bson::{doc, Document},
    Database,
};
use std::collections::HashMap;
use tracing::warn;

pub const INGEST_STATS_COLLECTION: &str = "ingest_stats";
const ENVELOPE_MAGIC: &[u8] = b"\xffBOOM\x01";
const ENVELOPE_LEN: usize = ENVELOPE_MAGIC.len() + 8;
const MINUTE_MS: i64 = 60_000;

#[derive(Debug, Default)]
pub struct MinuteStat {
    pub received: u64,
    pub processed: u64,
    pub latency_sum: f64,
    pub latency_max: f64,
}

pub type MinuteStats = HashMap<i64, MinuteStat>;

pub fn wrap(received_ms: i64, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ENVELOPE_LEN + payload.len());
    bytes.extend_from_slice(ENVELOPE_MAGIC);
    bytes.extend_from_slice(&received_ms.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

pub fn unwrap(bytes: &[u8]) -> (Option<i64>, &[u8]) {
    if bytes.len() < ENVELOPE_LEN || !bytes.starts_with(ENVELOPE_MAGIC) {
        return (None, bytes);
    }
    let mut millis = [0u8; 8];
    millis.copy_from_slice(&bytes[ENVELOPE_MAGIC.len()..ENVELOPE_LEN]);
    (Some(i64::from_be_bytes(millis)), &bytes[ENVELOPE_LEN..])
}

pub fn minute_of(ms: i64) -> i64 {
    ms.div_euclid(MINUTE_MS) * MINUTE_MS
}

pub fn stat_id(survey: &str, minute_ms: i64) -> String {
    format!("{}_{:013}", survey, minute_ms)
}

fn received_at_key(survey: &str) -> String {
    format!("{}_alerts_received_at", survey)
}

pub async fn mark_received(
    con: &redis::aio::MultiplexedConnection,
    survey: &str,
    candid: i64,
    received_ms: i64,
) {
    let mut con = con.clone();
    if let Err(error) = redis::cmd("HSET")
        .arg(received_at_key(survey))
        .arg(candid)
        .arg(received_ms)
        .query_async::<()>(&mut con)
        .await
    {
        warn!(
            ?error,
            "failed to record when alert {} was received", candid
        );
    }
}

pub async fn flush(db: &Database, survey: &str, stats: &mut MinuteStats) {
    let collection = db.collection::<Document>(INGEST_STATS_COLLECTION);
    for (minute, stat) in stats.drain() {
        let update = doc! {
            "$inc": {
                "received": stat.received as i64,
                "processed": stat.processed as i64,
                "latency_sum": stat.latency_sum,
            },
            "$max": { "latency_max": stat.latency_max },
            "$setOnInsert": { "survey": survey, "minute": minute },
        };
        if let Err(error) = collection
            .update_one(doc! { "_id": stat_id(survey, minute) }, update)
            .upsert(true)
            .await
        {
            warn!(?error, "failed to write ingest stats for {}", survey);
        }
    }
}

pub async fn record_processed(
    con: &redis::aio::MultiplexedConnection,
    db: &Database,
    survey: &str,
    candids: &[i64],
) {
    if candids.is_empty() {
        return;
    }
    let key = received_at_key(survey);
    let mut con = con.clone();
    let received: Vec<Option<i64>> = match redis::cmd("HMGET")
        .arg(&key)
        .arg(candids)
        .query_async::<Vec<Option<i64>>>(&mut con)
        .await
    {
        Ok(received) => received,
        Err(error) => {
            warn!(?error, "failed to read when alerts were received");
            return;
        }
    };
    if let Err(error) = redis::cmd("HDEL")
        .arg(&key)
        .arg(candids)
        .query_async::<i64>(&mut con)
        .await
    {
        warn!(?error, "failed to clear when alerts were received");
    }
    let now = Utc::now().timestamp_millis();
    let mut stats = MinuteStats::new();
    for received_ms in received.into_iter().flatten() {
        let latency = (now - received_ms).max(0) as f64 / 1000.0;
        let stat = stats.entry(minute_of(received_ms)).or_default();
        stat.processed += 1;
        stat.latency_sum += latency;
        stat.latency_max = stat.latency_max.max(latency);
    }
    flush(db, survey, &mut stats).await;
}
