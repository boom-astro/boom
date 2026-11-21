//! Real Mongo tests. Every case owns a UUID database and drops only that database.
use anyhow::{ensure, Result};
use boom::{
    alert::{AlertWorker, ProcessAlertStatus, ZtfAlertWorker},
    conf::{AppConfig, PhotometryPointsConfig, PhotometryPointsMode},
    photometry::{
        compare::{accepted_snapshot, mirror_and_compare, STATUS},
        mongo::{MongoPoints, POINTS},
        Kind, Measurement, PhotometryStore,
    },
    utils::{db::initialize_survey_indexes, enums::Survey},
};
use futures::future::try_join_all;
use mongodb::{
    bson::{doc, Document},
    Database,
};
use serde_json::{json, Value};

async fn database() -> Result<(Database, AppConfig)> {
    let config = AppConfig::from_test_config()?;
    ensure!(
        matches!(
            config.database.host.as_str(),
            "localhost" | "127.0.0.1" | "::1"
        ),
        "points tests require a local MongoDB"
    );
    let base = config.build_db().await?;
    let db = base.client().database(&format!(
        "boom_points_test_{}",
        uuid::Uuid::new_v4().simple()
    ));
    Ok((db, config))
}

fn point(epoch: usize, revision: i64) -> Measurement {
    let jd = 2460000.5 + epoch as f64 / 86400.;
    Measurement {
        object_id: "ZTF_same_object".into(),
        kind: Kind::Detection,
        jd,
        revision,
        record: json!({"jd":jd,"band":"g","programid":1,"psfFlux":-12.,"psfFluxErr":1.}),
    }
}

#[test]
fn points_comparison_requires_explicit_opt_in() {
    let config: PhotometryPointsConfig = serde_json::from_value(json!({})).unwrap();
    assert_eq!(config.mode, PhotometryPointsMode::Disabled);
    assert!(serde_json::from_value::<PhotometryPointsConfig>(json!({"mode":"points"})).is_err());
    assert!(serde_json::from_value::<PhotometryPointsConfig>(json!({"enabled":true})).is_err());
}

#[tokio::test]
async fn concurrent_same_object_inserts_and_duplicate_replays() -> Result<()> {
    let (db, _) = database().await?;
    let store = MongoPoints::new(db.clone());
    store.initialize().await?;
    let points: Vec<_> = (0..120).map(|i| point(i, 1)).collect();
    try_join_all((0..12).map(|worker| {
        let store = store.clone();
        let points = points.clone();
        async move {
            // Overlapping packets, one-second cadence, reversed worker order.
            let mut packet: Vec<_> = points.into_iter().skip(worker).step_by(12).collect();
            packet.push(point(0, 1));
            packet.sort_by_key(|p| p.id().unwrap());
            packet.dedup_by_key(|p| p.id().unwrap());
            store.write_points(&packet).await?;
            store.write_points(&packet).await
        }
    }))
    .await?;
    let restored = store.read_history("ZTF_same_object").await?;
    assert_eq!(restored.history.prv_candidates.len(), 120);
    assert!(restored
        .history
        .prv_candidates
        .windows(2)
        .all(|p| p[0]["jd"].as_f64() < p[1]["jd"].as_f64()));
    assert_eq!(
        db.collection::<Document>(POINTS)
            .count_documents(doc! {})
            .await?,
        120
    );
    assert!(store
        .read_history("unrelated")
        .await?
        .history
        .prv_candidates
        .is_empty());
    db.drop().await?;
    Ok(())
}

#[tokio::test]
async fn corrections_are_monotonic_and_equal_revision_conflicts_are_visible() -> Result<()> {
    let (db, _) = database().await?;
    let store = MongoPoints::new(db.clone());
    store.initialize().await?;
    let old = point(0, 0);
    let mut corrected = old.clone();
    corrected.revision = 2;
    corrected.record["psfFlux"] = json!(42.);
    store.write_points(std::slice::from_ref(&old)).await?;
    store.write_points(std::slice::from_ref(&corrected)).await?;
    store.write_points(&[old]).await?;
    assert_eq!(
        store
            .read_history(&corrected.object_id)
            .await?
            .history
            .prv_candidates[0],
        corrected.record
    );
    let mut conflict = corrected.clone();
    conflict.record["programid"] = json!(3);
    assert!(store.write_points(&[conflict]).await.is_err());
    assert_eq!(
        store
            .read_history(&corrected.object_id)
            .await?
            .history
            .prv_candidates[0],
        corrected.record
    );
    let mut forced = corrected.clone();
    forced.kind = Kind::Forced;
    let mut upper = corrected.clone();
    upper.kind = Kind::UpperLimit;
    upper.record["psfFlux"] = Value::Null;
    store.write_points(&[forced, upper]).await?;
    let restored = store.read_history(&corrected.object_id).await?;
    assert_eq!(restored.history.fp_hists.len(), 1);
    assert_eq!(
        restored.history.prv_nondetections[0]["psfFlux"],
        Value::Null
    );
    db.drop().await?;
    Ok(())
}

#[tokio::test]
async fn failed_unordered_bulk_can_be_replayed_without_losing_healthy_points() -> Result<()> {
    let (db, _) = database().await?;
    let store = MongoPoints::new(db.clone());
    store.initialize().await?;
    db.run_command(doc! {"collMod":POINTS,"validator":{"record.psfFluxErr":{"$gt":0}}})
        .await?;
    let mut points: Vec<_> = (0..300).map(|i| point(i, 1)).collect();
    points[100].record["psfFluxErr"] = json!(-1.);
    assert!(store.write_points(&points).await.is_err());
    let count = db
        .collection::<Document>(POINTS)
        .count_documents(doc! {})
        .await?;
    assert!(
        count > 0 && count < 300,
        "fault must leave actual partial writes"
    );
    points[100].record["psfFluxErr"] = json!(1.);
    store.write_points(&points).await?;
    store.write_points(&points).await?;
    assert_eq!(
        store
            .read_history("ZTF_same_object")
            .await?
            .history
            .prv_candidates
            .len(),
        300
    );
    db.drop().await?;
    Ok(())
}

async fn worker_config(
    db: &Database,
    base: &AppConfig,
    mode: &str,
) -> Result<tempfile::NamedTempFile> {
    let m = &base.database;
    let mongo = json!({"host":m.host,"port":m.port,"name":db.name(),"username":m.username,
        "password":m.password,"max_pool_size":20,"replica_set":m.replica_set,"srv":m.srv});
    let mut cutouts = mongo.clone();
    cutouts["type"] = json!("mongo");
    let config = json!({"database":mongo,"cutouts_storage":cutouts,
        "api":{"domain":"localhost","auth":{"secret_key":"points-test-only","token_expiration":3600,
        "admin_username":"points","admin_password":"points-test-only","admin_email":"points@example.invalid"}},
        "kafka":{"consumer":{},"producer":{"server":"unused.invalid:9092"}},
        "crossmatch":{},"photometry_points":{"mode":mode}});
    let file = tempfile::Builder::new().suffix(".json").tempfile()?;
    std::fs::write(file.path(), serde_json::to_vec(&config)?)?;
    let loaded = AppConfig::from_path(file.path().to_str().unwrap())?;
    ensure!(
        loaded.database.name == db.name(),
        "ambient environment overrides isolated test database"
    );
    ensure!(
        matches!(&loaded.cutouts_storage, boom::conf::CutoutsStorage::Mongo(m)
        if m.name == db.name() && m.host == loaded.database.host && m.port == loaded.database.port),
        "cutouts must use the same isolated test database"
    );
    ensure!(
        loaded.photometry_points.mode
            == if mode == "compare" {
                PhotometryPointsMode::Compare
            } else {
                PhotometryPointsMode::Disabled
            },
        "ambient environment overrides test mode"
    );
    Ok(file)
}

fn fixture() -> Vec<u8> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/alerts/ztf");
    let mut files: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|p| p.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "avro"))
        .collect();
    files.sort();
    std::fs::read(&files[0]).unwrap()
}

fn packet_at_epoch(index: usize) -> Result<Vec<u8>> {
    use apache_avro::{types::Value as Avro, Reader, Writer};
    let bytes = fixture();
    let mut reader = Reader::new(bytes.as_slice())?;
    let schema = reader.writer_schema().clone();
    let mut packet = reader.next().unwrap()?;
    let Avro::Record(fields) = &mut packet else {
        anyhow::bail!("expected packet record")
    };
    let candid = 900000000000000000_i64 + index as i64;
    for (name, value) in fields {
        match name.as_str() {
            "candid" => *value = Avro::Long(candid),
            "candidate" => {
                let Avro::Record(candidate) = value else {
                    anyhow::bail!("expected candidate record")
                };
                for (name, value) in candidate {
                    match name.as_str() {
                        "jd" => {
                            let Avro::Double(jd) = value else {
                                anyhow::bail!("expected JD double")
                            };
                            *jd += index as f64 / 86400.;
                        }
                        "candid" => *value = Avro::Long(candid),
                        _ => (),
                    }
                }
            }
            _ => (),
        }
    }
    let mut writer = Writer::new(&schema, Vec::new());
    writer.append(packet)?;
    Ok(writer.into_inner()?)
}

#[tokio::test]
async fn twelve_alert_workers_converge_on_one_object_at_one_second_epochs() -> Result<()> {
    let (db, base) = database().await?;
    initialize_survey_indexes(&Survey::Ztf, &db).await?;
    let file = worker_config(&db, &base, "compare").await?;
    let path = file.path().to_str().unwrap().to_owned();
    let packets = (0..24).map(packet_at_epoch).collect::<Result<Vec<_>>>()?;
    let results = try_join_all((0..12).map(|i| {
        let path = path.clone();
        let packets = packets.clone();
        async move {
            let mut worker = ZtfAlertWorker::new(&path).await?;
            for packet in packets.iter().skip(i).step_by(12) {
                match worker.process_alert(packet).await {
                    Ok(_) | Err(boom::alert::AlertError::PhotometryPoints(_)) => (),
                    Err(error) => return Err(anyhow::Error::from(error)),
                }
            }
            Ok::<_, anyhow::Error>(())
        }
    }))
    .await;
    results?;
    assert_eq!(
        db.collection::<Document>("ZTF_alerts")
            .count_documents(doc! {})
            .await?,
        24
    );
    // Moving-source comparisons may retain packets. Once ingestion is stable,
    // replay every packet through Exists and require every comparison to pass.
    let mut worker = ZtfAlertWorker::new(&path).await?;
    for packet in packets.iter().rev() {
        assert!(matches!(
            worker.process_alert(packet).await?,
            ProcessAlertStatus::Exists(_)
        ));
    }
    let alert = db
        .collection::<Document>("ZTF_alerts")
        .find_one(doc! {})
        .await?
        .unwrap();
    let object = alert.get_str("objectId")?;
    let (_, source) = accepted_snapshot(&db, object).await?;
    let restored = MongoPoints::new(db.clone()).read_history(object).await?;
    assert_eq!(source, restored.history);
    assert!(source.prv_candidates.len() >= 24);
    assert_eq!(db.collection::<Document>(STATUS).count_documents(doc!{
        "report.histories_equal":true,"report.enrichment_equal":true,"report.features_equal":true
    }).await?,24);
    db.drop().await?;
    Ok(())
}

#[tokio::test]
async fn real_worker_compares_production_enrichment_and_repairs_existing_alert_replay() -> Result<()>
{
    let (db, base) = database().await?;
    initialize_survey_indexes(&Survey::Ztf, &db).await?;
    let file = worker_config(&db, &base, "compare").await?;
    let mut worker = ZtfAlertWorker::new(file.path().to_str().unwrap()).await?;
    let packet = fixture();
    let candid = match worker.process_alert(&packet).await? {
        ProcessAlertStatus::Added(c) => c,
        _ => panic!("fresh database"),
    };
    let alert = db
        .collection::<Document>("ZTF_alerts")
        .find_one(doc! {"_id":candid})
        .await?
        .unwrap();
    let object = alert.get_str("objectId")?;
    let store = MongoPoints::new(db.clone());
    let (_, source) = accepted_snapshot(&db, object).await?;
    assert_eq!(source, store.read_history(object).await?.history);
    let status = db
        .collection::<Document>(STATUS)
        .find_one(doc! {"_id":candid})
        .await?
        .unwrap();
    for field in ["histories_equal", "enrichment_equal", "features_equal"] {
        assert!(status.get_document("report")?.get_bool(field)?);
    }
    assert_eq!(status.get_str("state")?, "pass");
    // Simulate interruption after the alert insert and before points complete.
    db.collection::<Document>(POINTS)
        .delete_one(doc! {"object_id":object})
        .await?;
    assert_eq!(
        worker.process_alert(&packet).await?,
        ProcessAlertStatus::Exists(candid)
    );
    assert_eq!(source, store.read_history(object).await?.history);
    // An accepted correction must survive replay of the previous snapshot.
    db.collection::<Document>("ZTF_alerts_aux")
        .update_one(
            doc! {"_id":object},
            doc! {"$set":{"prv_candidates.0.programid":3},"$inc":{"version":1}},
        )
        .await?;
    assert!(mirror_and_compare(&db, &store, candid, object)
        .await?
        .equal());
    store.write_points(&source.measurements(object, 0)?).await?;
    assert_ne!(source, store.read_history(object).await?.history);
    // Unexpected extra points cannot silently pass the migration gate.
    let mut extra = point(0, 0);
    extra.object_id = object.to_owned();
    store.write_points(&[extra]).await?;
    assert!(mirror_and_compare(&db, &store, candid, object)
        .await
        .is_err());
    let failed = db
        .collection::<Document>(STATUS)
        .find_one(doc! {"_id":candid})
        .await?
        .unwrap();
    assert_ne!(failed.get_str("state")?, "pass");
    // JSON must not silently turn an unsupported accepted value into null.
    db.collection::<Document>("ZTF_alerts_aux")
        .update_one(
            doc! {"_id":object},
            doc! {"$set":{"prv_candidates.0.psfFlux":f64::NAN},"$inc":{"version":1}},
        )
        .await?;
    assert!(accepted_snapshot(&db, object).await.is_err());
    db.drop().await?;
    Ok(())
}

#[tokio::test]
async fn disabled_worker_never_creates_points_collections() -> Result<()> {
    let (db, base) = database().await?;
    initialize_survey_indexes(&Survey::Ztf, &db).await?;
    let file = worker_config(&db, &base, "disabled").await?;
    let mut worker = ZtfAlertWorker::new(file.path().to_str().unwrap()).await?;
    worker.process_alert(&fixture()).await?;
    let names = db.list_collection_names().await?;
    assert!(!names.iter().any(|n| n == POINTS || n == STATUS));
    db.drop().await?;
    Ok(())
}
