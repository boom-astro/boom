//! Development-only replay against isolated services. Never a production backfill.
use anyhow::{ensure, Context, Result};
use boom::{
    alert::alert_temp_queue_name,
    conf::AppConfig,
    enrichment::{create_ztf_alert_pipeline, ZtfAlertForEnrichment},
    photometry::{audit, Shadow, OUTBOX, STATUS},
    utils::enums::Survey,
};
use futures::TryStreamExt;
use mongodb::bson::{self, doc, DateTime, Document};
use redis::AsyncCommands;
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
mod photometry_support;
use photometry_support::{spawn_worker, stop_worker, wait_for_alerts};

fn with_late_history(bytes: &[u8], ordinal: i64) -> Result<Vec<u8>> {
    use apache_avro::{types::Value as Avro, Reader, Writer};
    let mut reader = Reader::new(bytes)?;
    let schema = reader.writer_schema().clone();
    let mut value = reader.next().context("empty real fixture")??;
    let Avro::Record(fields) = &mut value else {
        anyhow::bail!("expected alert record");
    };
    let candid = fields
        .iter()
        .find(|(k, _)| k == "candid")
        .and_then(|(_, v)| {
            if let Avro::Long(v) = v {
                Some(*v)
            } else {
                None
            }
        })
        .context("missing candid")?
        + 1000
        + ordinal;
    fields
        .iter_mut()
        .find(|(k, _)| k == "candid")
        .context("missing candid")?
        .1 = Avro::Long(candid);
    if let Some((_, Avro::Record(candidate))) = fields.iter_mut().find(|(k, _)| k == "candidate") {
        if let Some((_, id)) = candidate.iter_mut().find(|(k, _)| k == "candid") {
            *id = Avro::Long(candid);
        }
    }
    let points = &mut fields
        .iter_mut()
        .find(|(k, _)| k == "prv_candidates")
        .context("missing previous history")?
        .1;
    let points = if let Avro::Union(_, inner) = points {
        inner.as_mut()
    } else {
        points
    };
    let Avro::Array(points) = points else {
        anyhow::bail!("expected previous history array");
    };
    let mut late = points.first().context("empty previous history")?.clone();
    let Avro::Record(late_fields) = &mut late else {
        anyhow::bail!("expected previous record");
    };
    let (_, jd) = late_fields
        .iter_mut()
        .find(|(k, _)| k == "jd")
        .context("missing point epoch")?;
    let Avro::Double(time) = jd else {
        anyhow::bail!("expected double epoch");
    };
    *time -= 0.000001 * (ordinal + 1) as f64;
    points.push(late);
    let mut writer = Writer::new(&schema, Vec::new());
    writer.append(value)?;
    Ok(writer.into_inner()?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        args.len() == 4,
        "usage: photometry_shadow_replay CONFIG FIXTURE_DIR FILTER_DIR REPORT_JSON"
    );
    let config = AppConfig::from_path(&args[0])?;
    photometry_support::validate(&config)?;
    let db = config.build_db().await?;
    ensure!(
        db.collection::<Document>("ZTF_alerts")
            .count_documents(doc! {})
            .await?
            == 0,
        "replay database must be empty"
    );
    let mut redis = config.build_redis().await?;
    ensure!(
        redis::cmd("DBSIZE").query_async::<u64>(&mut redis).await? == 0,
        "replay Redis must be empty"
    );
    let shadow = Shadow::new(&config.photometry_shadow, db.clone()).await?;
    let outcome:Result<()>=async {
    // Match the scheduler's production ZTF index setup before measuring storage.
    boom::utils::db::initialize_survey_indexes(&Survey::Ztf,&db).await?;
    shadow.store.initialize().await?;
    let fixture_dir = PathBuf::from(&args[1]);
    let mut fixtures = std::fs::read_dir(fixture_dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    fixtures.retain(|p| p.extension().is_some_and(|ext| ext == "avro"));
    fixtures.sort();
    ensure!(!fixtures.is_empty(), "no Avro fixtures found");
    let mut expected_alerts = fixtures.len() + 1;
    let input = Survey::Ztf.alert_input_queue();
    let output = "ZTF_alerts_enrichment_queue";
    let temp_queue = alert_temp_queue_name(&input);
    let mut cases = Vec::new();

    // Write-ahead crash: a reference exists, but Mongo has not completed the alert.
    shadow.enqueue(-1, "uncommitted-fixture").await?;
    let waiting = shadow.drain(1).await?;
    ensure!(
        waiting.waiting_for_mongo == 1
            && shadow
                .store
                .history("uncommitted-fixture")
                .await?
                .prv_candidates
                .is_empty(),
        "uncommitted source was mirrored"
    );
    cases.push(
        json!({"scenario":"crash before Mongo completion", "result":"PASS", "waiting":waiting}),
    );

    // ClickHouse outage: a valid local listener returns an immediate HTTP error.
    // Every actual queue worker persists an outbox reference before it acks Redis.
    let mut outage: Value = serde_json::from_str(&std::fs::read_to_string(&args[0])?)?;
    outage["photometry_shadow"]["database"] = json!("intentionally_absent_database");
    let outage_path = format!("{}.outage.json", args[0]);
    std::fs::write(&outage_path, serde_json::to_vec(&outage)?)?;
    let (sender, task) = spawn_worker(&outage_path);
    let started = Instant::now();
    for path in &fixtures {
        let bytes = std::fs::read(path)?;
        let _: i64 = redis.lpush(&input, bytes).await?;
    }
    // A clearly labeled derivative of a real packet preserves the full old
    // history and adds one late epoch, under a distinct alert identity.
    let _: i64 = redis
        .lpush(&input, with_late_history(&std::fs::read(&fixtures[0])?,0)?)
        .await?;
    wait_for_alerts(&db, expected_alerts as u64).await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while redis.llen::<_, usize>(output).await? < expected_alerts
        || redis.llen::<_, usize>(&temp_queue).await? != 0
    {
        ensure!(
            Instant::now() < deadline,
            "Redis job acknowledgement did not complete"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let ingest_seconds = started.elapsed().as_secs_f64();
    // Wait for the failing requests to release their leases before this planned
    // restart. A separate case injects an abandoned lease explicitly.
    let deadline = Instant::now() + Duration::from_secs(15);
    while db
        .collection::<Document>(OUTBOX)
        .count_documents(doc! {"_id":{"$ne":-1_i64},"state":"retry","lease_until":DateTime::from_millis(0)})
        .await?
        != expected_alerts as u64
    {
        ensure!(Instant::now() < deadline, "outage retry was not persisted");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop_worker(sender, task).await?;
    let pending_after_ack = shadow.lag().await?;
    ensure!(
        pending_after_ack["pending"].as_u64().unwrap_or(0) >= expected_alerts as u64,
        "acked source lost its pending reference"
    );
    cases.push(json!({"scenario":"ClickHouse unavailable after Redis acknowledgement", "result":"PASS", "lag":pending_after_ack}));

    // Restart the real worker with the healthy endpoint. Its idle loop must
    // recover durable references without receiving any new packets.
    let (sender, task) = spawn_worker(&args[0]);
    let deadline = Instant::now() + Duration::from_secs(45);
    while db
        .collection::<Document>(OUTBOX)
        .count_documents(doc! {"_id":{"$ne":-1_i64}})
        .await?
        != 0
    {
        ensure!(
            Instant::now() < deadline,
            "restart did not recover pending writes"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    stop_worker(sender, task).await?;
    cases.push(json!({"scenario":"idle worker restart recovers durable writes", "result":"PASS"}));

    // A paced healthy phase exercises source ingestion, draining, and full
    // history reads concurrently. It is a correctness/lag test, not saturation.
    let healthy_packets=16_i64;
    let template=std::fs::read(&fixtures[0])?;
    let read_object={
        let mut reader=apache_avro::Reader::new(&template[..])?;
        let value=reader.next().context("empty template")??;
        let apache_avro::types::Value::Record(fields)=value else {anyhow::bail!("expected template record");};
        fields.into_iter().find_map(|(key,value)|match value {
            apache_avro::types::Value::String(id) if key=="objectId" => Some(id), _=>None
        }).context("missing template object ID")?
    };
    let (_,before_history)=shadow.snapshot(&read_object).await?;
    let before_points=before_history.prv_candidates.len()+before_history.prv_nondetections.len()+before_history.fp_hists.len();
    let concurrent_started=Instant::now();
    let (sender,task)=spawn_worker(&args[0]);
    let mut producer=redis.clone();
    let producer_input=input.clone();
    let producing=tokio::spawn(async move {
        for ordinal in 1..=healthy_packets {
            let _:i64=producer.lpush(&producer_input,with_late_history(&template,ordinal)?).await?;
            tokio::time::sleep(Duration::from_millis(75)).await;
        }
        Ok::<_,anyhow::Error>(())
    });
    let mut concurrent_reads_ms=Vec::new();
    let mut max_pending=0_u64;
    while !producing.is_finished() {
        let started=Instant::now();
        shadow.store.history(&read_object).await?;
        concurrent_reads_ms.push(started.elapsed().as_secs_f64()*1000.0);
        max_pending=max_pending.max(shadow.lag().await?["pending"].as_u64().unwrap_or(0));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    producing.await??;
    expected_alerts+=healthy_packets as usize;
    wait_for_alerts(&db,expected_alerts as u64).await?;
    let deadline=Instant::now()+Duration::from_secs(45);
    while db.collection::<Document>(OUTBOX).count_documents(doc! {"_id":{"$ne":-1_i64}}).await?!=0 {
        ensure!(Instant::now()<deadline,"concurrent delivery failed to converge");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop_worker(sender,task).await?;
    ensure!(!concurrent_reads_ms.is_empty(),"no reads overlapped ingestion");
    let (_,after_history)=shadow.snapshot(&read_object).await?;
    let after_points=after_history.prv_candidates.len()+after_history.prv_nondetections.len()+after_history.fp_hists.len();
    ensure!(after_points==before_points+healthy_packets as usize,"late variants did not append the expected epochs");
    let concurrent_seconds=concurrent_started.elapsed().as_secs_f64();
    cases.push(json!({"scenario":"concurrent ingestion, delivery, and history reads", "result":"PASS", "read_samples":concurrent_reads_ms.len()}));

    let alerts = db
        .collection::<Document>("ZTF_alerts")
        .find(doc! {})
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let mut comparisons = Vec::new();
    for alert in &alerts {
        let candid = alert.get_i64("_id")?;
        let object_id = alert.get_str("objectId")?;
        let report = shadow.copy_object(object_id).await?;
        ensure!(
            report.histories_equal && report.features_equal,
            "history or feature parity failed"
        );
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
        let history = shadow.store.history(object_id).await?;
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
            "actual enrichment input hydration differs"
        );
        let (_, source_history) = shadow.snapshot(object_id).await?;
        let source_features =
            boom::photometry::parity::features(&source_history, baseline.candidate.candidate.jd)?;
        let hydrated_features =
            boom::photometry::parity::features(&history, baseline.candidate.candidate.jd)?;
        ensure!(
            source_features == hydrated_features,
            "photometry properties differ at the alert epoch"
        );
        comparisons.push(json!({"object_id":object_id,"candid":candid,"parity":report,"features_at_alert_epoch":source_features}));
    }
    cases.push(
        json!({"scenario":"overlapping real history with a derived late epoch", "result":"PASS"}),
    );
    cases.push(json!({"scenario":"real enrichment input hydration and photometry properties", "result":"PASS", "alerts":comparisons.len()}));

    // Lost acknowledgement: write already succeeded, but its outbox entry and
    // an expired lease survived. A new owner must replay and acknowledge it.
    let alert = &alerts[0];
    let candid = alert.get_i64("_id")?;
    let object_id = alert.get_str("objectId")?;
    shadow.enqueue(candid, object_id).await?;
    shadow.copy_object(object_id).await?;
    db.collection::<Document>(OUTBOX).update_one(doc! {"_id":candid}, doc! {"$set":{
        "lease_owner":"simulated-dead-worker", "lease_until":DateTime::from_millis(1), "next_attempt":DateTime::from_millis(0)
    }}).await?;
    let recovered = shadow.drain(64).await?;
    ensure!(
        recovered.delivered > 0
            && db
                .collection::<Document>(OUTBOX)
                .count_documents(doc! {"_id":candid})
                .await?
                == 0,
        "expired lease was not recovered"
    );
    cases.push(
        json!({"scenario":"crash after sink write before outbox acknowledgement", "result":"PASS"}),
    );

    // A source-accepted metadata correction increments the existing aux version.
    // Force an older snapshot past token deduplication to test row revisions.
    let (old_revision, old_history) = shadow.snapshot(object_id).await?;
    let mut old_batch = boom::photometry::store::PreparedBatch::ztf(
        object_id,
        old_revision,
        &old_history.prv_candidates,
        &old_history.prv_nondetections,
        &old_history.fp_hists,
    )?;
    db.collection::<Document>("ZTF_alerts_aux")
        .update_one(
            doc! {"_id":object_id},
            doc! {"$set":{"prv_candidates.0.drb":0.125},"$inc":{"version":1}},
        )
        .await?;
    shadow.copy_object(object_id).await?;
    old_batch.token.push_str(":forced-old-snapshot-replay");
    shadow.store.insert(&old_batch).await?;
    let corrected = shadow.copy_object(object_id).await?;
    ensure!(
        corrected.revision > old_revision && corrected.histories_equal,
        "older replay replaced an accepted correction"
    );
    cases.push(json!({"scenario":"accepted Mongo correction survives older snapshot replay", "result":"PASS", "revision":corrected.revision}));

    // An actual packet replay passes through the worker's Exists shortcut.
    let (sender, task) = spawn_worker(&args[0]);
    let _: i64 = redis.lpush(&input, std::fs::read(&fixtures[0])?).await?;
    let deadline = Instant::now() + Duration::from_secs(15);
    while redis.llen::<_, usize>(&input).await? != 0
        || redis.llen::<_, usize>(&temp_queue).await? != 0
        || db.collection::<Document>(OUTBOX).count_documents(doc! {"_id":{"$ne":-1_i64}}).await?!=0
    {
        ensure!(
            Instant::now() < deadline,
            "duplicate packet was not acknowledged"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    stop_worker(sender, task).await?;
    shadow.drain(64).await?;
    ensure!(
        db.collection::<Document>(OUTBOX)
            .count_documents(doc! {"_id":{"$ne":-1_i64}})
            .await?
            == 0,
        "duplicate shortcut left a committed alert undelivered"
    );
    ensure!(
        db.collection::<Document>("ZTF_alerts")
            .count_documents(doc! {})
            .await?
            == expected_alerts as u64,
        "duplicate created another source alert"
    );
    cases.push(
        json!({"scenario":"existing-alert shortcut preserves shadow delivery", "result":"PASS"}),
    );

    // Requeue actual completed alerts together: shared objects must be copied
    // once, but an incomplete reference for an existing object stays pending.
    let accepted = db.collection::<Document>("ZTF_alerts").find(doc!{}).sort(doc!{"_id":1}).await?.try_collect::<Vec<_>>().await?;
    for alert in &accepted { shadow.enqueue(alert.get_i64("_id")?,alert.get_str("objectId")?).await?; }
    let object_a = accepted[0].get_str("objectId")?;
    let other = accepted.iter().find(|a|a.get_str("objectId").ok()!=Some(object_a)).context("batch test needs two real fixture objects")?;
    shadow.enqueue(-2,object_a).await?;
    let batch = shadow.drain(256).await?;
    ensure!(batch.delivered==accepted.len() && batch.objects>=2 && batch.objects<batch.delivered && batch.insert_requests==1,
        "references were not coalesced into one multi-object insert: {batch:?}");
    ensure!(db.collection::<Document>(OUTBOX).find_one(doc!{"_id":-2_i64}).await?.is_some(),"uncommitted reference was acknowledged through a shared object");
    cases.push(json!({"scenario":"batched insert coalesces completed references with individual Mongo gates","result":"PASS","drain":batch}));

    // One invalid source snapshot must not poison another object's delivery.
    let source = db.collection::<Document>("ZTF_alerts_aux").find_one(doc!{"_id":object_a}).await?.context("missing source")?;
    let original = source.get("prv_candidates").context("missing detections")?.clone();
    db.collection::<Document>("ZTF_alerts_aux").update_one(doc!{"_id":object_a},doc!{"$set":{"prv_candidates.0.psfFluxErr":-1.0}}).await?;
    shadow.enqueue(accepted[0].get_i64("_id")?,object_a).await?;
    shadow.enqueue(other.get_i64("_id")?,other.get_str("objectId")?).await?;
    let isolated = shadow.drain(256).await?;
    ensure!(isolated.delivered==1 && isolated.failed==1,"invalid object poisoned unrelated delivery: {isolated:?}");
    db.collection::<Document>("ZTF_alerts_aux").update_one(doc!{"_id":object_a},doc!{"$set":{"prv_candidates":original}}).await?;
    db.collection::<Document>(OUTBOX).update_one(doc!{"_id":accepted[0].get_i64("_id")?},doc!{"$set":{"next_attempt":DateTime::from_millis(0)}}).await?;
    let repaired_source = shadow.drain(256).await?;
    ensure!(repaired_source.delivered==1 && repaired_source.failed==0,"corrected invalid source did not retry");
    cases.push(json!({"scenario":"invalid source is isolated from a healthy batch and retries after source repair","result":"PASS","drain":isolated}));

    let filters=audit::inventory(std::path::Path::new(&args[2]))?;
    let mongo_storage = db
        .run_command(doc! {"collStats":"ZTF_alerts_aux","scale":1})
        .await?;
    let outbox_storage=db.run_command(doc! {"collStats":OUTBOX,"scale":1}).await?;
    let parity_storage=db.run_command(doc! {"collStats":STATUS,"scale":1}).await?;
    let statuses = db
        .collection::<Document>(STATUS)
        .count_documents(doc! {"report.histories_equal":true,"report.features_equal":true})
        .await?;
    let report = json!({"status":"PASS","scope":"real fixture packets through the actual Redis alert worker; photometry properties only, no ML inference",
        "cases":cases,"comparisons":comparisons,"filters":filters,"lag":shadow.lag().await?,"matching_objects":statuses,
        "replay":{"real_packets":fixtures.len(),"derived_late_packets":1+healthy_packets,"outage_ingest_seconds":ingest_seconds,
            "outage_packets_per_second":(fixtures.len()+1) as f64/ingest_seconds,
            "concurrent":{"healthy_packets":healthy_packets,"producer_interval_ms":75,"elapsed_seconds":concurrent_seconds,
            "read_ms":concurrent_reads_ms,"max_pending":max_pending}},
        "storage":{"mongo_aux":mongo_storage,"mongo_outbox":outbox_storage,"mongo_parity":parity_storage,"clickhouse":shadow.store.storage().await?},
        "limitations":["tiny fixture workload; not a production capacity benchmark", "no Kafka broker or rebalance is exercised", "full Mongo histories retained", "filter window sizes require manual review", "no ML, host, or cross-survey feature parity"]});
    std::fs::write(&args[3], serde_json::to_vec_pretty(&report)?)?;
    println!("PASS: real-alert worker replay, durable recovery, history and feature parity, filter inventory");
    Ok(())
    }.await;
    if let Err(error) = &outcome {
        let outbox = db
            .collection::<Document>(OUTBOX)
            .find(doc! {})
            .limit(20)
            .await;
        let outbox = match outbox {
            Ok(cursor) => cursor.try_collect::<Vec<_>>().await.unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        let statuses = db
            .collection::<Document>(STATUS)
            .find(doc! {})
            .limit(20)
            .await;
        let statuses = match statuses {
            Ok(cursor) => cursor.try_collect::<Vec<_>>().await.unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        let failure = json!({"status":"FAIL","error":format!("{error:#}"),"outbox":outbox,"parity_status":statuses});
        let _ = std::fs::write(&args[3], serde_json::to_vec_pretty(&failure)?);
    }
    outcome
}
