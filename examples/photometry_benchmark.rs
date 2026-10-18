//! Disposable replay benchmark. Never a production ingest or backfill command.
mod photometry_support;
use anyhow::{ensure, Context, Result};
use apache_avro::{types::Value as Avro, Reader, Writer};
use boom::{
    alert::alert_temp_queue_name,
    conf::AppConfig,
    enrichment::{
        create_ztf_alert_pipeline, EnrichmentWorker, ZtfAlertForEnrichment, ZtfEnrichmentWorker,
    },
    photometry::{audit, Shadow, STATUS},
    utils::{db::initialize_survey_indexes, enums::Survey},
};
use futures::{StreamExt, TryStreamExt};
use mongodb::bson::{self, doc, Document};
use photometry_support::{spawn_worker, stop_worker};
use redis::AsyncCommands;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

// Surface dispatch errors before waiting for an unreachable source count.
struct Producer {
    task: tokio::task::JoinHandle<Result<Value>>,
    result: Option<Value>,
}
impl Producer {
    async fn poll(&mut self) -> Result<()> {
        if self.result.is_none() && self.task.is_finished() {
            self.result = Some(
                (&mut self.task)
                    .await
                    .context("joining replay producer")?
                    .context("replay producer failed")?,
            );
        }
        Ok(())
    }
    fn is_finished(&self) -> bool {
        self.result.is_some()
    }
}
impl Drop for Producer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Packet {
    path: PathBuf,
    at_ms: u64,
    sha256: String,
}
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Corpus {
    Captured {
        packets: Vec<Packet>,
        #[serde(default)]
        provenance: Value,
    },
    FixtureDerived {
        templates: Vec<PathBuf>,
        objects: usize,
        epochs: usize,
        interval_ms: u64,
    },
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    corpus: Corpus,
    #[serde(default = "default_timeout")]
    timeout_seconds: u64,
    #[serde(default = "default_samples")]
    enrichment_samples: usize,
    #[serde(default)]
    filters_are_deployed_export: bool,
    #[serde(default)]
    recovery_only: bool,
    #[serde(default)]
    recovery_overlap: bool,
    #[serde(default)]
    keep_phase_data: bool,
    #[serde(default)]
    target_packets_per_second: Option<f64>,
    #[serde(default)]
    max_lag_seconds: Option<f64>,
}
fn default_timeout() -> u64 {
    600
}
fn default_samples() -> usize {
    16
}

#[derive(Clone)]
struct Workload {
    plan: Plan,
    count: usize,
    unique_alerts: usize,
    objects: Vec<String>,
    templates: Vec<Vec<u8>>,
    inventory: Value,
}
fn identity(bytes: &[u8]) -> Result<(i64, String)> {
    let mut reader = Reader::new(bytes)?;
    let Avro::Record(fields) = reader.next().context("empty Avro packet")?? else {
        anyhow::bail!("expected alert record")
    };
    ensure!(
        reader.next().is_none(),
        "one alert per Avro file is required"
    );
    let candid = fields
        .iter()
        .find_map(|(k, v)| match (k.as_str(), v) {
            ("candid", Avro::Long(v)) => Some(*v),
            _ => None,
        })
        .context("missing candid")?;
    let object = fields
        .iter()
        .find_map(|(k, v)| match (k.as_str(), v) {
            ("objectId", Avro::String(v)) => Some(v.clone()),
            _ => None,
        })
        .context("missing objectId")?;
    Ok((candid, object))
}
fn derived(template: &[u8], object: usize, epoch: usize) -> Result<Vec<u8>> {
    let mut reader = Reader::new(template)?;
    let schema = reader.writer_schema().clone();
    let mut value = reader.next().context("empty template")??;
    let Avro::Record(fields) = &mut value else {
        anyhow::bail!("expected record")
    };
    let candid = 4_000_000_000_000_000_000_i64
        .checked_add(
            i64::try_from(object)?
                .checked_mul(1_000_000)
                .context("object overflow")?,
        )
        .and_then(|v| v.checked_add(epoch as i64))
        .context("candid overflow")?;
    for (key, value) in fields.iter_mut() {
        match (key.as_str(), value) {
            ("objectId", value) => *value = Avro::String(format!("ZTFbench{object:09}")),
            ("candid", value) => *value = Avro::Long(candid),
            ("candidate", Avro::Record(candidate)) => {
                for (key, value) in candidate {
                    if key == "candid" {
                        *value = Avro::Long(candid);
                    }
                }
            }
            _ => (),
        }
    }
    // Preserve overlap and introduce a distinct late epoch under every new candid.
    let history = &mut fields
        .iter_mut()
        .find(|(k, _)| k == "prv_candidates")
        .context("missing history")?
        .1;
    let history = if let Avro::Union(_, inner) = history {
        inner.as_mut()
    } else {
        history
    };
    let Avro::Array(points) = history else {
        anyhow::bail!("expected history array")
    };
    let mut late = points.first().context("empty template history")?.clone();
    let Avro::Record(record) = &mut late else {
        anyhow::bail!("expected point")
    };
    for (key, value) in record {
        if let ("jd", Avro::Double(jd)) = (key.as_str(), value) {
            *jd -= 0.00001 * (epoch + 1) as f64;
        }
    }
    points.push(late);
    let mut writer = Writer::new(&schema, Vec::new());
    writer.append(value)?;
    Ok(writer.into_inner()?)
}
impl Workload {
    fn load(plan: Plan) -> Result<Self> {
        ensure!(
            plan.timeout_seconds > 0 && plan.enrichment_samples > 0,
            "positive timeout and parity sample count required"
        );
        ensure!(
            plan.target_packets_per_second
                .is_none_or(|v| v.is_finite() && v > 0.0),
            "invalid throughput target"
        );
        ensure!(
            plan.max_lag_seconds
                .is_none_or(|v| v.is_finite() && v > 0.0),
            "invalid lag target"
        );
        let mut templates = Vec::new();
        let mut ids = BTreeMap::new();
        let mut objects = BTreeSet::new();
        let mut hashes = Vec::new();
        let count = match &plan.corpus {
            Corpus::Captured { packets, .. } => {
                ensure!(!packets.is_empty(), "empty capture");
                ensure!(
                    packets.windows(2).all(|w| w[0].at_ms <= w[1].at_ms),
                    "arrival times must be nondecreasing"
                );
                ensure!(
                    packets.last().unwrap().at_ms < plan.timeout_seconds * 1000,
                    "capture duration exceeds timeout"
                );
                for packet in packets {
                    let bytes = std::fs::read(&packet.path)?;
                    let hash = digest(&bytes);
                    ensure!(
                        hash == packet.sha256,
                        "capture SHA-256 mismatch: {}",
                        packet.path.display()
                    );
                    let (candid, object) = identity(&bytes)?;
                    if let Some(previous) = ids.insert(candid, object.clone()) {
                        ensure!(
                            previous == object,
                            "the same candid belongs to different objects"
                        );
                    }
                    objects.insert(object);
                    hashes.push(json!({"path":packet.path,"sha256":hash,"at_ms":packet.at_ms,"candid":candid}));
                }
                packets.len()
            }
            Corpus::FixtureDerived {
                templates: paths,
                objects: count,
                epochs,
                interval_ms,
            } => {
                ensure!(
                    !paths.is_empty() && *count > 0 && (1..1_000_000).contains(epochs),
                    "invalid fixture dimensions"
                );
                let total = count
                    .checked_mul(*epochs)
                    .context("packet count overflow")?;
                ensure!(
                    (total as u64)
                        .checked_mul(*interval_ms)
                        .is_some_and(|v| v < plan.timeout_seconds * 1000),
                    "schedule exceeds timeout"
                );
                for path in paths {
                    let bytes = std::fs::read(path)?;
                    identity(&bytes)?;
                    hashes.push(json!({"path":path,"sha256":digest(&bytes)}));
                    templates.push(bytes);
                }
                for object in 0..*count {
                    objects.insert(format!("ZTFbench{object:09}"));
                }
                total
            }
        };
        let unique_alerts = if matches!(&plan.corpus, Corpus::Captured { .. }) {
            ids.len()
        } else {
            count
        };
        let inventory = json!({"kind":if templates.is_empty(){"captured"}else{"fixture_derived"},"packets":count,
            "unique_alerts":unique_alerts,"objects":objects.len(),"files":hashes,
            "manifest_sha256":digest(&serde_json::to_vec(&hashes)?),
            "provenance":match &plan.corpus {Corpus::Captured{provenance,..}=>provenance.clone(),_=>json!({"timing_basis":"synthetic_fixed_spacing","content":"derived from repository fixtures"})}});
        Ok(Self {
            plan,
            count,
            unique_alerts,
            objects: objects.into_iter().collect(),
            templates,
            inventory,
        })
    }
    fn at_ms(&self, index: usize) -> u64 {
        match &self.plan.corpus {
            Corpus::Captured { packets, .. } => packets[index].at_ms,
            Corpus::FixtureDerived { interval_ms, .. } => index as u64 * interval_ms,
        }
    }
    fn packet(&self, index: usize) -> Result<Vec<u8>> {
        match &self.plan.corpus {
            Corpus::Captured { packets, .. } => {
                let bytes = std::fs::read(&packets[index].path)?;
                ensure!(
                    digest(&bytes) == packets[index].sha256,
                    "capture changed during replay"
                );
                Ok(bytes)
            }
            Corpus::FixtureDerived { objects, .. } => derived(
                &self.templates[index % objects % self.templates.len()],
                index % objects,
                index / objects,
            ),
        }
    }
}

async fn set_outage(config: &AppConfig, unavailable: bool) -> Result<()> {
    let endpoint =
        reqwest::Url::parse(&config.photometry_shadow.url)?.join("/__boom_replay_fault")?;
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?
        .post(endpoint)
        .body(if unavailable {
            "unavailable"
        } else {
            "healthy"
        })
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    ensure!(
        response == "boom-replay-fault-accepted",
        "benchmark requires its disposable fault proxy"
    );
    Ok(())
}

async fn phase(config_path: &Path, raw: &Value, workload: &Workload, name: &str) -> Result<Value> {
    let mut raw = raw.clone();
    let database = format!(
        "{}_{}",
        raw["database"]["name"]
            .as_str()
            .context("database missing")?,
        name
    );
    for key in ["database", "cutouts_storage", "photometry_shadow"] {
        raw[key]["name"] = json!(database);
    }
    raw["photometry_shadow"]["database"] = json!(database);
    raw["photometry_shadow"]
        .as_object_mut()
        .unwrap()
        .remove("name");
    let baseline = name == "baseline";
    let recovery = name == "recovery";
    if recovery {
        raw["photometry_shadow"]["url"] = raw
            .get("benchmark_fault_proxy_url")
            .context("benchmark controller fault proxy is required")?
            .clone();
    }
    if baseline {
        raw["photometry_shadow"]["url"] = json!("");
    }
    let path = config_path.with_extension(format!("{name}.json"));
    std::fs::write(&path, serde_json::to_vec(&raw)?)?;
    let config = AppConfig::from_path(path.to_str().context("non-UTF8 config path")?)?;
    if baseline {
        let mut guarded = config.clone();
        guarded.photometry_shadow.url = AppConfig::from_path(config_path.to_str().unwrap())?
            .photometry_shadow
            .url;
        photometry_support::validate(&guarded)?;
    } else {
        photometry_support::validate(&config)?;
    }
    let db = config.build_db().await?;
    initialize_survey_indexes(&Survey::Ztf, &db).await?;
    ensure!(
        db.collection::<Document>("ZTF_alerts")
            .count_documents(doc! {})
            .await?
            == 0,
        "phase database is not empty"
    );
    let mut redis = config.build_redis().await?;
    // Test control/upload connections may carry large packets across Docker.
    // Worker connection policy remains BOOM's unchanged production default.
    redis.set_response_timeout(Duration::from_secs(30));
    ensure!(
        redis::cmd("DBSIZE").query_async::<u64>(&mut redis).await? == 0,
        "dedicated Redis is not empty"
    );
    let shadow = if baseline {
        None
    } else {
        let shadow = Shadow::new(&config.photometry_shadow, db.clone()).await?;
        shadow.store.initialize().await?;
        Some(shadow)
    };
    if recovery {
        set_outage(&config, true).await?;
    }
    let (sender, task) = spawn_worker(path.to_str().unwrap());
    let input = Survey::Ztf.alert_input_queue();
    let temp = alert_temp_queue_name(&input);
    let output = "ZTF_alerts_enrichment_queue";
    let started = Instant::now();
    let deadline = started + Duration::from_secs(workload.plan.timeout_seconds);
    let mut producer = redis.clone();
    let producing_workload = workload.clone();
    let producer_input = input.clone();
    let overlap = recovery && workload.plan.recovery_overlap;
    ensure!(
        !overlap || workload.count >= 4,
        "overlap recovery requires at least four packets"
    );
    let split = workload.count / 2;
    let paused = Arc::new(AtomicBool::new(false));
    let resume = Arc::new(tokio::sync::Notify::new());
    let producer_paused = paused.clone();
    let producer_resume = resume.clone();
    let producing_task = tokio::spawn(async move {
        let mut lateness = Vec::new();
        let mut bytes_sent = 0_u64;
        let mut segment_started = started;
        let mut segment_offset = 0;
        let mut dispatch_seconds = Vec::new();
        for i in 0..producing_workload.count {
            if overlap && i == split {
                producer_paused.store(true, Ordering::Release);
                producer_resume.notified().await;
                segment_started = Instant::now();
                segment_offset = producing_workload.at_ms(i);
            }
            let due = segment_started
                + Duration::from_millis(producing_workload.at_ms(i) - segment_offset);
            tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
            let bytes = producing_workload.packet(i)?;
            bytes_sent += bytes.len() as u64;
            lateness.push(Instant::now().saturating_duration_since(due).as_secs_f64() * 1000.0);
            let _: i64 = producer
                .lpush(&producer_input, bytes)
                .await
                .context("dispatching replay packet to Redis")?;
            dispatch_seconds.push(started.elapsed().as_secs_f64());
        }
        Ok::<_, anyhow::Error>(json!({"bytes":bytes_sent,"dispatch_lateness_ms":lateness,
            "last_dispatch_seconds":dispatch_seconds.last(),"dispatch_seconds":dispatch_seconds,
            "timing_policy":if overlap {"two manifest-paced segments with a controlled pause before restoration"}else{"manifest schedule"}}))
    });
    let mut producing = Producer {
        task: producing_task,
        result: None,
    };
    let mut samples = Vec::new();
    let mut reads = Vec::new();
    let mut empty_reads = 0_u64;
    let mut restoration: Option<(Instant, Value, u64)> = None;
    let mut recovery_samples = Vec::new();
    loop {
        producing.poll().await?;
        ensure!(
            Instant::now() < deadline,
            "phase {name} timed out while ingesting"
        );
        ensure!(
            !task.is_finished(),
            "alert worker exited during phase {name}"
        );
        let committed = db
            .collection::<Document>("ZTF_alerts")
            .count_documents(doc! {})
            .await
            .context("counting committed replay alerts")?;
        let queue = redis
            .llen::<_, u64>(&input)
            .await
            .context("reading replay input queue")?;
        let in_flight = redis
            .llen::<_, u64>(&temp)
            .await
            .context("reading replay temporary queue")?;
        let lag = if let Some(shadow) = &shadow {
            shadow.lag().await?
        } else {
            json!({"pending":0,"oldest_age_seconds":0})
        };
        samples.push(json!({"seconds":started.elapsed().as_secs_f64(),"committed":committed,"queue":queue,"in_flight":in_flight,"lag":lag}));
        if overlap
            && restoration.is_none()
            && paused.load(Ordering::Acquire)
            && queue == 0
            && in_flight == 0
        {
            ensure!(
                committed > 0 && lag["pending"].as_u64() == Some(committed),
                "outage lost durable references before restoration"
            );
            let restored = Instant::now();
            set_outage(&config, false).await?;
            restoration = Some((restored, lag.clone(), committed));
            resume.notify_one();
        }
        if let Some((restored, _, _)) = &restoration {
            recovery_samples.push(json!({"seconds":restored.elapsed().as_secs_f64(),
                "since_phase_start_seconds":started.elapsed().as_secs_f64(),"committed":committed,
                "queue":queue,"in_flight":in_flight,"producer_finished":producing.is_finished(),"lag":lag}));
        }
        if !recovery {
            if baseline {
                let object = &workload.objects[samples.len() % workload.objects.len()];
                let t = Instant::now();
                let history = db
                    .collection::<Document>("ZTF_alerts_aux")
                    .find_one(doc! {"_id":object})
                    .await
                    .context("reading baseline Mongo history")?;
                let ms = t.elapsed().as_secs_f64() * 1000.0;
                if history.is_some() {
                    reads.push(ms);
                } else {
                    empty_reads += 1;
                }
            }
            if let Some(shadow) = &shadow {
                let object = &workload.objects[samples.len() % workload.objects.len()];
                let t = Instant::now();
                let history = shadow.store.history(object).await?;
                let ms = t.elapsed().as_secs_f64() * 1000.0;
                if history.prv_candidates.is_empty()
                    && history.prv_nondetections.is_empty()
                    && history.fp_hists.is_empty()
                {
                    empty_reads += 1;
                } else {
                    reads.push(ms);
                }
            }
        }
        if producing.is_finished()
            && committed == workload.unique_alerts as u64
            && queue == 0
            && in_flight == 0
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let produced = producing
        .result
        .take()
        .context("producer did not complete")?;
    let ingest_seconds = started.elapsed().as_secs_f64();
    let mut recovery_result = Value::Null;
    if recovery {
        let shadow = shadow.as_ref().unwrap();
        let (restored, initial, committed_at_restore) = if let Some(restoration) = restoration {
            restoration
        } else {
            let initial = shadow.lag().await?;
            ensure!(
                initial["pending"].as_u64() == Some(workload.unique_alerts as u64),
                "outage lost durable references"
            );
            let restored = Instant::now();
            set_outage(&config, false).await?;
            (restored, initial, workload.unique_alerts as u64)
        };
        loop {
            let lag = shadow.lag().await?;
            recovery_samples.push(json!({"seconds":restored.elapsed().as_secs_f64(),"lag":lag}));
            if lag["pending"].as_u64() == Some(0) {
                break;
            }
            ensure!(
                restored.elapsed().as_secs() < workload.plan.timeout_seconds,
                "recovery timed out"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let seconds = restored.elapsed().as_secs_f64();
        stop_worker(sender, task).await?;
        let restore_seconds = restored.duration_since(started).as_secs_f64();
        let last_dispatch = produced["last_dispatch_seconds"]
            .as_f64()
            .context("missing producer completion")?;
        let arrivals_after_restore = produced["dispatch_seconds"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t.as_f64().is_some_and(|t| t > restore_seconds))
            .count();
        let overlap_seconds = (last_dispatch - restore_seconds).max(0.0);
        let live_samples: Vec<_> = recovery_samples
            .iter()
            .filter(|s| {
                s["since_phase_start_seconds"]
                    .as_f64()
                    .is_some_and(|t| t < last_dispatch)
            })
            .collect();
        let completed_during_arrivals = live_samples
            .iter()
            .filter_map(|s| {
                Some(
                    s["committed"]
                        .as_u64()?
                        .saturating_sub(s["lag"]["pending"].as_u64()?),
                )
            })
            .max()
            .unwrap_or(0);
        let caught_up_during_arrivals = live_samples.iter().any(|s| {
            s["lag"]["pending"] == 0
                && s["queue"] == 0
                && s["committed"]
                    .as_u64()
                    .is_some_and(|n| n > committed_at_restore)
        });
        ensure!(
            !overlap
                || (arrivals_after_restore > 0
                    && overlap_seconds > 0.0
                    && !live_samples.is_empty()),
            "recovery did not overlap observed arrivals"
        );
        recovery_result = json!({"mode":if overlap {"with_arrivals"}else{"idle"},
            "pending_at_restore":initial["pending"],"source_alerts_at_restore":committed_at_restore,"restore_seconds":restore_seconds,
            "seconds":seconds,"initial_backlog_net_drain_per_second":initial["pending"].as_u64().unwrap() as f64/seconds,
            "references_per_second":if overlap {Value::Null}else{json!(initial["pending"].as_u64().unwrap() as f64/seconds)},
            "packets_dispatched_after_restore":arrivals_after_restore,"arrival_overlap_seconds":overlap_seconds,
            "completed_reference_lower_bound_during_arrivals":completed_during_arrivals,
            "caught_up_during_arrivals":caught_up_during_arrivals,"samples":recovery_samples,
            "fault":if overlap {"HTTP 503; same worker; second manifest-paced segment continues after restoring sink"}else{"HTTP 503 via loopback proxy; same worker throughout; no new input during recovery"}});
    } else {
        if let Some(shadow) = &shadow {
            while shadow.lag().await?["pending"].as_u64() != Some(0) {
                ensure!(
                    Instant::now() < deadline,
                    "healthy shadow failed to converge"
                );
                let lag = shadow.lag().await?;
                samples.push(json!({"seconds":started.elapsed().as_secs_f64(),"committed":workload.unique_alerts,"queue":0,"in_flight":0,"lag":lag}));
                let t = Instant::now();
                let history = shadow
                    .store
                    .history(&workload.objects[samples.len() % workload.objects.len()])
                    .await?;
                let ms = t.elapsed().as_secs_f64() * 1000.0;
                if history.prv_candidates.is_empty()
                    && history.prv_nondetections.is_empty()
                    && history.fp_hists.is_empty()
                {
                    empty_reads += 1;
                } else {
                    reads.push(ms);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        stop_worker(sender, task).await?;
    }
    let settled_seconds = started.elapsed().as_secs_f64();
    // All parity and model work below is outside the measured ingestion phase.
    let mut histories = Vec::new();
    let mut enrichment = Vec::new();
    if let Some(shadow) = &shadow {
        histories=futures::stream::iter(workload.objects.iter().cloned()).map(|object| {
            let shadow=shadow.clone();
            tokio::spawn(async move {
            let t = Instant::now();
            let (revision, history) = shadow.snapshot(&object).await?;
            let mongo_ms = t.elapsed().as_secs_f64() * 1000.0;
            let t = Instant::now();
            let mirrored = shadow.store.history(&object).await?;
            let read_ms = t.elapsed().as_secs_f64() * 1000.0;
            let report =
                boom::photometry::parity::compare(&history, &mirrored, revision, 0.0, read_ms)?;
            ensure!(
                report.histories_equal && report.features_equal,
                "history parity failed for {object}"
            );
            let epochs: Vec<f64> = history
                .prv_candidates
                .iter()
                .chain(&history.prv_nondetections)
                .chain(&history.fp_hists)
                .filter_map(|point| point["jd"].as_f64())
                .collect();
            let span = epochs
                .iter()
                .copied()
                .reduce(f64::max)
                .zip(epochs.iter().copied().reduce(f64::min))
                .map(|(last, first)| last - first);
                Ok::<_,anyhow::Error>(json!({"object_id":object,"source_counts":report.source_counts,"revision":report.revision,"history_span_days":span,
                "mongo_ms":mongo_ms,"read_ms":read_ms,"validation":"read_only; no sink repair"}))
            })
        }).buffer_unordered(4).map(|joined|joined.map_err(anyhow::Error::from).and_then(|report|report))
          .try_collect::<Vec<_>>().await?;
        histories.sort_by(|a, b| a["object_id"].as_str().cmp(&b["object_id"].as_str()));
        let worker = ZtfEnrichmentWorker::new(path.to_str().unwrap(), None)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let alerts = db
            .collection::<Document>("ZTF_alerts")
            .find(doc! {})
            .sort(doc! {"_id":1})
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let sample_count = alerts.len().min(workload.plan.enrichment_samples);
        for index in 0..sample_count {
            let alert = &alerts[index * (alerts.len() - 1) / (sample_count - 1).max(1)];
            let candid = alert.get_i64("_id")?;
            let object = alert.get_str("objectId")?;
            let mut pipeline = create_ztf_alert_pipeline(false);
            pipeline[0] = doc! {"$match":{"_id":candid}};
            let source = db
                .collection::<Document>("ZTF_alerts")
                .aggregate(pipeline)
                .await?
                .try_next()
                .await?
                .context("missing enrichment input")?;
            let baseline: ZtfAlertForEnrichment = bson::from_document(source.clone())?;
            let history = shadow.store.history(object).await?;
            let mut hydrated = source;
            hydrated.insert("prv_candidates", bson::to_bson(&history.prv_candidates)?);
            hydrated.insert(
                "prv_nondetections",
                bson::to_bson(&history.prv_nondetections)?,
            );
            hydrated.insert("fp_hists", bson::to_bson(&history.fp_hists)?);
            let hydrated: ZtfAlertForEnrichment = bson::from_document(hydrated)?;
            ensure!(
                serde_json::to_value(&baseline)? == serde_json::to_value(&hydrated)?,
                "typed enrichment hydration mismatch"
            );
            let started = Instant::now();
            let parity = worker
                .compare_shadow_enrichment(baseline, hydrated)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            ensure!(
                parity["equal"] == true,
                "full enrichment parity failed for candid {candid}"
            );
            let pair_ms = started.elapsed().as_secs_f64() * 1000.0;
            let (revision, source_history) = shadow.snapshot(object).await?;
            let mut source_history = serde_json::to_value(source_history)?;
            source_history["_id"] = json!(object);
            source_history["version"] = json!(revision);
            let mut binned = Vec::new();
            for cadence in [900, 3600, 86400] {
                binned.push(json!({"cadence_seconds":cadence,"rows":shadow.store.bins(object,cadence).await?}));
            }
            enrichment.push(
                json!({"candid":candid,"object_id":object,"pair_ms":pair_ms,"parity":parity,
                "binning":{"mongo_history":source_history,"cadences":binned}}),
            );
        }
    }
    let mongo = db.run_command(doc! {"dbStats":1,"scale":1}).await?;
    let aux = db
        .run_command(doc! {"collStats":"ZTF_alerts_aux","scale":1})
        .await?;
    let sink = if let Some(shadow) = &shadow {
        shadow.store.storage().await?
    } else {
        Value::Null
    };
    if !baseline {
        let expected_rows: u64 = histories
            .iter()
            .flat_map(|row| row["source_counts"].as_array().unwrap())
            .map(|value| value.as_u64().unwrap())
            .sum();
        ensure!(
            sink["unique_rows"].as_u64() == Some(expected_rows),
            "sink has missing or extra normalized rows"
        );
    }
    if baseline {
        let collections = db.list_collection_names().await?;
        ensure!(
            !collections
                .iter()
                .any(|name| name == boom::photometry::OUTBOX || name == STATUS),
            "disabled integration created shadow collections"
        );
    }
    let status_count = db
        .collection::<Document>(STATUS)
        .count_documents(doc! {"report.histories_equal":true,"report.features_equal":true})
        .await?;
    if !baseline {
        ensure!(
            status_count == workload.objects.len() as u64,
            "missing matching status records"
        );
    }
    let _: i64 = redis.del(output).await?;
    ensure!(
        redis::cmd("DBSIZE").query_async::<u64>(&mut redis).await? == 0,
        "phase left unexpected Redis keys"
    );
    let report = json!({"phase":name,"packets":workload.count,"unique_alerts":workload.unique_alerts,"objects":workload.objects.len(),
        "ingest_seconds":ingest_seconds,"settled_seconds":settled_seconds,"ingest_packets_per_second":workload.count as f64/ingest_seconds,
        "settled_packets_per_second":workload.count as f64/settled_seconds,"producer":produced,"samples":samples,"concurrent_read_ms":reads,"empty_history_reads":empty_reads,
        "concurrent_read_backend":if baseline {"mongo"} else {"clickhouse"},
        "recovery":recovery_result,"histories":histories,"enrichment":enrichment,"storage":{"mongo_database":mongo,"mongo_aux":aux,"clickhouse":sink}});
    if !workload.plan.keep_phase_data {
        db.drop()
            .await
            .context("dropping disposable completed-phase database")?;
    }
    Ok(report)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        args.len() == 4,
        "usage: photometry_benchmark CONFIG PLAN_JSON FILTER_DIR REPORT_JSON"
    );
    let config = AppConfig::from_path(&args[0])?;
    photometry_support::validate(&config)?;
    ensure!(
        !config.gpu.is_active() && !config.babamul.enabled && !config.milvus.enabled,
        "benchmark requires CPU and disabled external sinks"
    );
    let plan: Plan = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let workload = Workload::load(plan)?;
    let raw: Value = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let mut phases = BTreeMap::new();
    let result: Result<()> = async {
        let selected = if workload.plan.recovery_only {
            vec!["recovery"]
        } else {
            vec!["baseline", "shadow", "recovery"]
        };
        for name in selected {
            println!(
                "Starting {name}: {} packets / {} objects",
                workload.count,
                workload.objects.len()
            );
            let report = phase(Path::new(&args[0]), &raw, &workload, name)
                .await
                .with_context(|| format!("benchmark phase {name}"))?;
            phases.insert(name, report);
            std::fs::write(
                &args[3],
                serde_json::to_vec_pretty(
                    &json!({"status":"RUNNING","workload":workload.inventory,"phases":phases}),
                )?,
            )?;
        }
        Ok(())
    }
    .await;
    let filters = audit::inventory(Path::new(&args[2]))?;
    let report = json!({"status":if result.is_ok(){"PASS"}else{"FAIL"},"error":result.as_ref().err().map(|e|format!("{e:#}")),
        "workload":workload.inventory,"phases":phases,"filters":filters,
        "filters_are_deployed_export":workload.plan.filters_are_deployed_export,
        "delivery":{"concurrent_deliveries":config.photometry_shadow.concurrent_deliveries,"drain_rows":config.photometry_shadow.drain_rows,"poll_seconds":config.photometry_shadow.poll_seconds},
        "targets":{"packets_per_second":workload.plan.target_packets_per_second,"max_lag_seconds":workload.plan.max_lag_seconds},
        "scope":{"history_parity":"all objects after convergence","enrichment_parity":"deterministic sample at actual alert epochs, shared external context, CPU inference",
            "capacity":"single alert worker with concurrent full-history reads; measured offered workload, not maximum capacity",
            "excluded":["Kafka broker/rebalance","Mongo replica failover","GPU inference","production catalog population","retention/deletes/backfill"]}});
    std::fs::write(&args[3], serde_json::to_vec_pretty(&report)?)?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn producer_failure_is_reported_before_waiting_for_all_alerts() {
        let mut producer = Producer {
            task: tokio::spawn(async { anyhow::bail!("injected dispatch failure") }),
            result: None,
        };
        while !producer.task.is_finished() {
            tokio::task::yield_now().await;
        }
        let error = producer.poll().await.unwrap_err();
        assert!(format!("{error:#}").contains("injected dispatch failure"));
        assert!(producer.result.is_none());
    }

    fn plan(corpus: Corpus) -> Plan {
        Plan {
            corpus,
            timeout_seconds: 60,
            enrichment_samples: 4,
            filters_are_deployed_export: false,
            recovery_only: false,
            recovery_overlap: false,
            keep_phase_data: false,
            target_packets_per_second: None,
            max_lag_seconds: None,
        }
    }
    #[test]
    fn generated_workload_keeps_object_identity_and_adds_distinct_late_epochs() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/alerts/ztf/2695378462115010012.avro");
        let workload = Workload::load(plan(Corpus::FixtureDerived {
            templates: vec![path],
            objects: 2,
            epochs: 3,
            interval_ms: 5,
        }))
        .unwrap();
        let mut ids = BTreeSet::new();
        for index in 0..6 {
            let bytes = workload.packet(index).unwrap();
            let (candid, object) = identity(&bytes).unwrap();
            assert!(ids.insert(candid));
            assert_eq!(object, format!("ZTFbench{:09}", index % 2));
            assert_eq!(workload.at_ms(index), index as u64 * 5);
        }
        assert_ne!(workload.packet(0).unwrap(), workload.packet(2).unwrap());
        assert_eq!(workload.inventory["kind"], "fixture_derived");
    }
    #[test]
    fn capture_requires_checksums_and_preserves_arrival_order() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/alerts/ztf/2695378462115010012.avro");
        let packet = Packet {
            sha256: digest(&std::fs::read(&path).unwrap()),
            path,
            at_ms: 10,
        };
        let valid = Workload::load(plan(Corpus::Captured {
            provenance: Value::Null,
            packets: vec![
                packet.clone(),
                Packet {
                    at_ms: 20,
                    ..packet.clone()
                },
            ],
        }))
        .unwrap();
        assert_eq!(valid.unique_alerts, 1);
        assert_eq!(valid.count, 2);
        assert_eq!(valid.packet(0).unwrap(), valid.packet(1).unwrap());
        assert!(Workload::load(plan(Corpus::Captured {
            provenance: Value::Null,
            packets: vec![Packet {
                sha256: "wrong".into(),
                ..packet.clone()
            }]
        }))
        .is_err());
        assert!(Workload::load(plan(Corpus::Captured {
            provenance: Value::Null,
            packets: vec![packet.clone(), Packet { at_ms: 0, ..packet }]
        }))
        .is_err());
    }
}
