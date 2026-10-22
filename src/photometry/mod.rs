//! Experimental ZTF shadow storage. MongoDB remains authoritative.
pub mod audit;
pub mod parity;
pub mod store;

use anyhow::{ensure, Context, Result};
use futures::{StreamExt, TryStreamExt};
use mongodb::{
    bson::{doc, DateTime, Document},
    options::{CollectionOptions, Hint, ReturnDocument, WriteConcern},
    Collection, Database, IndexModel,
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::LazyLock,
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

use crate::conf::PhotometryShadowConfig;
use store::{PreparedBatch, Store, ZtfHistory};

pub const OUTBOX: &str = "photometry_shadow_outbox";
pub const STATUS: &str = "photometry_shadow_status";
const LEASE_MS: i64 = 180_000;
// A single source document larger than this is delivered alone. Preparation is
// streamed with bounded concurrency rather than retaining every source history.
const INSERT_BYTES: usize = 4 * 1024 * 1024;

struct Delivery {
    object_id: String,
    entries: Vec<Document>,
    revision: u64,
    source: ZtfHistory,
    batch: PreparedBatch,
    mongo_snapshot_ms: f64,
}
static PENDING: LazyLock<opentelemetry::metrics::Gauge<u64>> = LazyLock::new(|| {
    crate::utils::o11y::metrics::SCHEDULER_METER
        .u64_gauge("photometry_shadow.pending")
        .build()
});
static OLDEST: LazyLock<opentelemetry::metrics::Gauge<f64>> = LazyLock::new(|| {
    crate::utils::o11y::metrics::SCHEDULER_METER
        .f64_gauge("photometry_shadow.oldest_age_seconds")
        .build()
});

#[derive(Clone)]
pub struct Shadow {
    pub store: Store,
    db: Database,
    outbox: Collection<Document>,
    concurrent_deliveries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_are_disabled_and_credentials_are_redacted() {
        let mut config = PhotometryShadowConfig::default();
        assert!(config.url.is_empty());
        assert_eq!(config.concurrent_deliveries, 1);
        config.password = "private-value".into();
        assert!(!format!("{config:?}").contains("private-value"));
        config.url = "http://user:private-value@localhost:8123".into();
        assert!(Store::new(&config).is_err());
    }
}

pub struct ShadowWorker {
    shadow: Shadow,
    task: JoinHandle<()>,
}

impl Drop for ShadowWorker {
    fn drop(&mut self) {
        self.task.abort(); // An interrupted lease expires; its entry is not lost.
    }
}

impl ShadowWorker {
    pub async fn enqueue(&self, candid: i64, object_id: &str) -> mongodb::error::Result<()> {
        self.shadow.enqueue(candid, object_id).await
    }
}

#[derive(Debug, Default, Serialize)]
pub struct DrainReport {
    pub claimed: usize,
    pub objects: usize,
    pub insert_requests: usize,
    pub inserted_rows_attempted: usize,
    pub delivered: usize,
    pub waiting_for_mongo: usize,
    pub failed: usize,
}

impl Shadow {
    pub async fn new(config: &PhotometryShadowConfig, db: Database) -> Result<Self> {
        ensure!(!config.url.is_empty(), "shadow URL is not configured");
        ensure!(
            config.poll_seconds > 0 && config.drain_rows > 0,
            "shadow poll_seconds and drain_rows must be positive"
        );
        ensure!(
            (1..=32).contains(&config.concurrent_deliveries),
            "shadow concurrent_deliveries must be between 1 and 32"
        );
        let store = Store::new(config)?;
        let outbox = db.collection_with_options(
            OUTBOX,
            CollectionOptions::builder()
                .write_concern(WriteConcern::majority())
                .build(),
        );
        outbox
            .create_indexes([
                IndexModel::builder()
                    .keys(doc! {"next_attempt":1,"_id":1})
                    .build(),
                IndexModel::builder().keys(doc! {"created_at":1}).build(),
            ])
            .await?;
        Ok(Self {
            store,
            db,
            outbox,
            concurrent_deliveries: config.concurrent_deliveries,
        })
    }

    pub async fn start(
        config: &PhotometryShadowConfig,
        db: Database,
    ) -> Result<Option<ShadowWorker>> {
        if config.url.is_empty() {
            return Ok(None);
        }
        let shadow = Self::new(config, db).await?;
        let drainer = shadow.clone();
        let poll_seconds = config.poll_seconds;
        let limit = config.drain_rows;
        let task = tokio::spawn(async move {
            let mut last_metrics = Instant::now() - Duration::from_secs(poll_seconds);
            loop {
                let busy = match drainer.drain(limit).await {
                    Ok(report) => {
                        if report.delivered + report.failed > 0 {
                            info!(
                                delivered = report.delivered,
                                failed = report.failed,
                                claimed = report.claimed,
                                objects = report.objects,
                                insert_requests = report.insert_requests,
                                "photometry shadow drain"
                            );
                        }
                        // A full, productive batch means backlog may remain.
                        // Poll delay is for idle/error recovery, not throughput.
                        report.claimed == limit && report.delivered > 0
                    }
                    Err(error) => {
                        warn!(error = %error, "photometry shadow drain failed; durable entries retained");
                        false
                    }
                };
                if last_metrics.elapsed() >= Duration::from_secs(poll_seconds) {
                    if let Ok(lag) = drainer.lag().await {
                        PENDING.record(lag["pending"].as_u64().unwrap_or(0), &[]);
                        OLDEST.record(lag["oldest_age_seconds"].as_f64().unwrap_or(0.0), &[]);
                    }
                    last_metrics = Instant::now();
                }
                if busy {
                    tokio::task::yield_now().await;
                } else {
                    tokio::time::sleep(Duration::from_secs(poll_seconds)).await;
                }
            }
        });
        Ok(Some(ShadowWorker { shadow, task }))
    }

    /// Write ahead of the authoritative Mongo alert. Only a completed Mongo
    /// alert makes this reference eligible for delivery. No photometry is
    /// duplicated in the outbox, and an exact retry keeps the original entry.
    pub async fn enqueue(&self, candid: i64, object_id: &str) -> mongodb::error::Result<()> {
        self.outbox.update_one(doc! {"_id": candid}, doc! {"$setOnInsert": {
            "object_id": object_id, "created_at": DateTime::now(),
            "next_attempt": DateTime::from_millis(0), "lease_until": DateTime::from_millis(0), "attempts": 0_i64, "state":"pending"
        }}).upsert(true).await?;
        Ok(())
    }

    pub async fn snapshot(&self, object_id: &str) -> Result<(u64, ZtfHistory)> {
        let source = self
            .db
            .collection::<Document>("ZTF_alerts_aux")
            .find_one(doc! {"_id": object_id})
            .await?
            .context("Mongo aux is missing")?;
        let version = match source.get("version") {
            None | Some(mongodb::bson::Bson::Null) => 0,
            Some(value) => value
                .as_i64()
                .or_else(|| value.as_i32().map(i64::from))
                .context("Mongo aux version must be an integer")?,
        };
        ensure!(version >= 0, "negative Mongo aux version");
        let value = serde_json::to_value(source)?;
        let history = ZtfHistory {
            prv_candidates: serde_json::from_value(
                value
                    .get("prv_candidates")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([])),
            )?,
            prv_nondetections: serde_json::from_value(
                value
                    .get("prv_nondetections")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([])),
            )?,
            fp_hists: serde_json::from_value(
                value
                    .get("fp_hists")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([])),
            )?,
        };
        Ok((version as u64, history))
    }

    /// Replay the currently accepted Mongo snapshot, using its atomic aux
    /// version rather than Kafka offsets, alert IDs, or the retry time.
    pub async fn copy_object(&self, object_id: &str) -> Result<parity::ParityReport> {
        let source_started = Instant::now();
        let (revision, source) = self.snapshot(object_id).await?;
        let mongo_snapshot_ms = source_started.elapsed().as_secs_f64() * 1000.0;
        let batch = PreparedBatch::ztf(
            object_id,
            revision,
            &source.prv_candidates,
            &source.prv_nondetections,
            &source.fp_hists,
        )?;
        let started = Instant::now();
        self.store.insert(&batch).await?;
        let write_ms = started.elapsed().as_secs_f64() * 1000.0;
        self.verify_snapshot(object_id, revision, &source, mongo_snapshot_ms, write_ms)
            .await
    }

    async fn verify_snapshot(
        &self,
        object_id: &str,
        revision: u64,
        source: &ZtfHistory,
        mongo_snapshot_ms: f64,
        write_ms: f64,
    ) -> Result<parity::ParityReport> {
        let started = Instant::now();
        let target = self.store.history(object_id).await?;
        let read_ms = started.elapsed().as_secs_f64() * 1000.0;
        let (current_revision, _) = self.snapshot(object_id).await?;
        ensure!(
            current_revision == revision,
            "Mongo aux changed during comparison; retrying latest snapshot"
        );
        let mut report = parity::compare(source, &target, revision, write_ms, read_ms)?;
        report.mongo_snapshot_ms = mongo_snapshot_ms;
        // Keep mismatches visible even while the outbox is retrying them.
        let report_doc = mongodb::bson::to_document(&report)?;
        self.db
            .collection::<Document>(STATUS)
            .update_one(
                doc! {"_id": object_id},
                doc! {"$set": {"report": report_doc, "checked_at": DateTime::now()}},
            )
            .upsert(true)
            .await?;
        ensure!(
            report.histories_equal && report.features_equal,
            "photometry shadow parity mismatch for object {object_id}"
        );
        Ok(report)
    }

    pub async fn drain(&self, limit: usize) -> Result<DrainReport> {
        let mut report = DrainReport::default();
        let mut entries = Vec::new();
        for _ in 0..limit {
            let now = DateTime::now();
            let owner = Uuid::new_v4().to_string();
            let entry = self.outbox.find_one_and_update(doc! {
                "next_attempt":{"$lte":now}, "lease_until":{"$lte":now}
            },doc! {"$set":{"lease_owner":owner,"lease_until":DateTime::from_millis(now.timestamp_millis()+LEASE_MS),
                "last_attempt":now,"state":"delivering","next_attempt":DateTime::from_millis(now.timestamp_millis()+LEASE_MS)},
                "$inc":{"attempts":1_i64}})
                .sort(doc!{"next_attempt":1,"_id":1}).return_document(ReturnDocument::After).await?;
            let Some(entry) = entry else {
                break;
            };
            entries.push(entry);
        }
        report.claimed = entries.len();
        if entries.is_empty() {
            return Ok(report);
        }
        let candids = entries
            .iter()
            .map(|e| e.get_i64("_id"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        // Gate every reference on its own completed alert before coalescing.
        let accepted = self
            .db
            .collection::<Document>("ZTF_alerts")
            .find(doc! {"_id":{"$in":candids}})
            .projection(doc! {"_id":1,"objectId":1})
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let accepted = accepted
            .iter()
            .map(|d| Ok((d.get_i64("_id")?, d.get_str("objectId")?.to_owned())))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut groups: BTreeMap<String, Vec<Document>> = BTreeMap::new();
        for entry in entries {
            let object_id = entry.get_str("object_id")?;
            if accepted.get(&entry.get_i64("_id")?).map(String::as_str) == Some(object_id) {
                groups.entry(object_id.to_owned()).or_default().push(entry);
            } else {
                self.retry(
                    &entry,
                    "waiting_for_mongo",
                    "authoritative alert not complete",
                )
                .await?;
                report.waiting_for_mongo += 1;
            }
        }
        let preparing = futures::stream::iter(groups)
            .map(|(object_id, entries)| async move {
                let started = Instant::now();
                let prepared = async {
                    let (revision, source) = self.snapshot(&object_id).await?;
                    let mongo_snapshot_ms = started.elapsed().as_secs_f64() * 1000.0;
                    let batch = PreparedBatch::ztf(
                        &object_id,
                        revision,
                        &source.prv_candidates,
                        &source.prv_nondetections,
                        &source.fp_hists,
                    )?;
                    Ok::<_, anyhow::Error>((revision, source, batch, mongo_snapshot_ms))
                }
                .await;
                (object_id, entries, prepared)
            })
            .buffer_unordered(self.concurrent_deliveries);
        futures::pin_mut!(preparing);
        let mut chunk: Vec<Delivery> = Vec::new();
        let mut bytes = 0;
        while let Some((object_id, entries, prepared)) = preparing.next().await {
            match prepared {
                Ok((revision, source, batch, mongo_snapshot_ms)) => {
                    if !chunk.is_empty() && bytes + batch.body.len() > INSERT_BYTES {
                        self.deliver_batch(std::mem::take(&mut chunk), &mut report)
                            .await?;
                        bytes = 0;
                    }
                    report.objects += 1;
                    bytes += batch.body.len();
                    chunk.push(Delivery {
                        object_id,
                        entries,
                        revision,
                        source,
                        batch,
                        mongo_snapshot_ms,
                    });
                }
                Err(error) => {
                    for entry in entries {
                        self.retry(&entry, "retry", &error.to_string()).await?;
                        report.failed += 1;
                    }
                }
            }
        }
        if !chunk.is_empty() {
            self.deliver_batch(chunk, &mut report).await?;
        }
        Ok(report)
    }

    async fn deliver_batch(
        &self,
        mut deliveries: Vec<Delivery>,
        report: &mut DrainReport,
    ) -> Result<()> {
        deliveries.sort_by(|a, b| a.object_id.cmp(&b.object_id));
        let batch =
            PreparedBatch::combine(&deliveries.iter().map(|d| &d.batch).collect::<Vec<_>>());
        let started = Instant::now();
        if batch.rows > 0 {
            report.insert_requests += 1;
        }
        report.inserted_rows_attempted += batch.rows;
        if let Err(error) = self.store.insert(&batch).await {
            // Ambiguous insert outcomes acknowledge nothing. A regrouped retry
            // may have a different token; revisioned FINAL reads still dedupe.
            for delivery in deliveries {
                for entry in delivery.entries {
                    self.retry(&entry, "retry", &error.to_string()).await?;
                    report.failed += 1;
                }
            }
            return Ok(());
        }
        let write_ms = started.elapsed().as_secs_f64() * 1000.0;
        let mut checking = futures::stream::iter(deliveries)
            .map(|delivery| async move {
                let result = self
                    .verify_snapshot(
                        &delivery.object_id,
                        delivery.revision,
                        &delivery.source,
                        delivery.mongo_snapshot_ms,
                        write_ms,
                    )
                    .await;
                let mut delivered = 0;
                let mut failed = 0;
                for entry in delivery.entries {
                    if result.is_ok() {
                        // Fence each acknowledgement independently, even when
                        // several references share one source snapshot and insert.
                        delivered +=
                            self.outbox.delete_one(owned(&entry)?).await?.deleted_count as usize;
                    } else {
                        self.retry(
                            &entry,
                            "retry",
                            &result
                                .as_ref()
                                .err()
                                .context("missing delivery error")?
                                .to_string(),
                        )
                        .await?;
                        failed += 1;
                    }
                }
                Ok::<_, anyhow::Error>((delivered, failed))
            })
            .buffer_unordered(self.concurrent_deliveries);
        while let Some(result) = checking.next().await {
            let (delivered, failed) = result?;
            report.delivered += delivered;
            report.failed += failed;
        }
        Ok(())
    }

    async fn retry(&self, entry: &Document, state: &str, error: &str) -> Result<()> {
        let attempts = entry.get_i64("attempts").unwrap_or(1);
        let delay_ms = (1_i64 << attempts.clamp(1, 8)) * 1000;
        self.outbox.update_one(owned(entry)?,doc!{"$set":{
            "state":state,"last_error":error,"next_attempt":DateTime::from_millis(DateTime::now().timestamp_millis()+delay_ms),
            "lease_until":DateTime::from_millis(0)},"$unset":{"lease_owner":""}}).await?;
        Ok(())
    }

    pub async fn lag(&self) -> Result<serde_json::Value> {
        let pending = self
            .outbox
            .count_documents(doc! {})
            .hint(Hint::Name("_id_".into()))
            .await?;
        let oldest = self
            .outbox
            .find_one(doc! {})
            .sort(doc! {"created_at": 1})
            .await?;
        let oldest_age_seconds = oldest.and_then(|d| {
            d.get_datetime("created_at").ok().map(|t| {
                (DateTime::now().timestamp_millis() - t.timestamp_millis()) as f64 / 1000.0
            })
        });
        Ok(serde_json::json!({"pending": pending, "oldest_age_seconds": oldest_age_seconds}))
    }
}

fn owned(entry: &Document) -> Result<Document> {
    Ok(doc! {"_id":entry.get_i64("_id")?,"lease_owner":entry.get_str("lease_owner")?})
}
