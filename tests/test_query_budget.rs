//! Guards against the database access patterns that would leave the pipeline
//! unable to work through a night of alerts: extra round trips per alert, a
//! query per alert inside a batch, and queries that stop using an index.
//!
//! Wall time on shared CI runners varies by 30% from run to run, so a timed
//! gate either misses a real regression or fails at random. Command counts
//! and query plans are exact. Whether production keeps up under real load is
//! watched in production, by the `alerts-falling-behind` alert.
//!
//! When a change adds a query on purpose, update the expected counts here in
//! the same PR, so the added round trip is visible in review.
#![recursion_limit = "512"]
use boom::{
    alert::{AlertWorker, LsstAlertWorker, ProcessAlertStatus, ZtfAlertWorker},
    conf::{get_test_db, AppConfig},
    enrichment::{EnrichmentWorker, LsstEnrichmentWorker, ZtfEnrichmentWorker},
    filter::{FilterWorker, LsstFilterWorker, ZtfFilterWorker},
    utils::{
        db::create_index,
        testing::{
            collection_scans, command_counts, insert_test_filter, remove_test_filter,
            AlertRandomizer, MongoCommandLog, RecordedCommand, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::{doc, Document};
use std::collections::BTreeMap;

/// Inside every survey's crossmatch range, so each alert takes the same
/// branches and the counts below do not depend on where it lands.
const DEC: f64 = 10.0;

/// Crossmatch catalogs are loaded out of band by `prepare_catalog`, which
/// gives each one this index. The test database may have empty catalogs
/// without it, and the crossmatch plan is only meaningful once it's there.
async fn ensure_catalog_indexes() {
    let config = AppConfig::from_test_config().unwrap();
    let db = get_test_db().await;
    for catalogs in config.crossmatch.values() {
        for catalog in catalogs {
            create_index(
                &db.collection::<Document>(&catalog.catalog),
                doc! { "coordinates.radec_geojson": "2dsphere" },
                false,
            )
            .await
            .unwrap();
        }
    }
}

async fn assert_no_collection_scans(commands: &[RecordedCommand]) {
    let scans = collection_scans(get_test_db().await.client(), commands).await;
    assert!(
        scans.is_empty(),
        "queries that scan a whole collection:\n{}",
        scans.join("\n")
    );
}

fn counts(expected: &[(&str, usize)]) -> BTreeMap<String, usize> {
    expected.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

/// Processes one alert for a new object, then a second alert for the same
/// object, and returns the commands each one sent.
async fn alert_worker_commands<W: AlertWorker>() -> (Vec<RecordedCommand>, Vec<RecordedCommand>) {
    ensure_catalog_indexes().await;
    let log = MongoCommandLog::new();
    let mut worker = log.observe(W::new(TEST_CONFIG_FILE)).await.unwrap();
    log.take();

    let randomizer = AlertRandomizer::new_randomized(W::survey()).dec(DEC);
    let (candid, _, _, _, bytes) = randomizer.clone().get().await;
    let status = worker.process_alert(&bytes).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));
    let new_object = log.take();

    let (candid, _, _, _, bytes) = randomizer.rand_candid().get().await;
    let status = worker.process_alert(&bytes).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));
    let known_object = log.take();

    assert_no_collection_scans(&new_object).await;
    assert_no_collection_scans(&known_object).await;
    (new_object, known_object)
}

/// Enriches and filters a batch of one alert and then a batch of several,
/// and checks that both cost the same commands: a query per alert inside
/// the batch loop shows up as a count that grows with the batch.
async fn assert_batch_workers_scale<A, E, F>()
where
    A: AlertWorker,
    E: EnrichmentWorker,
    F: FilterWorker,
{
    let survey = A::survey();
    let mut alert_worker = A::new(TEST_CONFIG_FILE).await.unwrap();
    let mut candids = Vec::new();
    for _ in 0..6 {
        let (candid, _, _, _, bytes) = AlertRandomizer::new_randomized(survey.clone())
            .dec(DEC)
            .get()
            .await;
        alert_worker.process_alert(&bytes).await.unwrap();
        candids.push(candid);
    }
    let (one, several) = candids.split_at(1);

    let log = MongoCommandLog::new();
    let mut enrichment_worker = log.observe(E::new(TEST_CONFIG_FILE, None)).await.unwrap();
    log.take();
    let one_enriched = enrichment_worker.process_alerts(one).await.unwrap();
    let one_enrichment = log.take();
    let several_enriched = enrichment_worker.process_alerts(several).await.unwrap();
    let several_enrichment = log.take();
    assert_eq!(several_enriched.len(), several.len());

    let filter_id = insert_test_filter(&survey, true).await.unwrap();
    let filter_worker = log
        .observe(F::new(TEST_CONFIG_FILE, Some(vec![filter_id.clone()])))
        .await;
    let filter_commands = async {
        let mut filter_worker = filter_worker?;
        log.take();
        filter_worker.process_alerts(&one_enriched).await?;
        let one_filter = log.take();
        filter_worker.process_alerts(&several_enriched).await?;
        Ok::<_, boom::filter::FilterWorkerError>((one_filter, log.take()))
    }
    .await;
    remove_test_filter(&filter_id, &survey).await.unwrap();
    let (one_filter, several_filter) = filter_commands.unwrap();

    assert!(!one_enrichment.is_empty() && !one_filter.is_empty());
    assert_eq!(
        command_counts(&several_enrichment),
        command_counts(&one_enrichment),
        "{survey} enrichment commands grow with the batch size"
    );
    assert_eq!(
        command_counts(&several_filter),
        command_counts(&one_filter),
        "{survey} filter commands grow with the batch size"
    );
    assert_no_collection_scans(&several_enrichment).await;
    assert_no_collection_scans(&several_filter).await;
}

#[tokio::test]
async fn test_ztf_alert_worker_query_budget() {
    let (new_object, known_object) = alert_worker_commands::<ZtfAlertWorker>().await;
    let survey_matches = [("find DECAM_alerts_aux", 1), ("find LSST_alerts_aux", 1)];
    let existing_aux = ("find ZTF_alerts_aux", 1);
    let alert = ("insert ZTF_alerts", 1);
    assert_eq!(
        command_counts(&new_object),
        counts(
            &[
                &survey_matches[..],
                &[
                    existing_aux,
                    alert,
                    // Every catalog in one aggregate, `$unionWith`-ed onto the first.
                    ("aggregate PS1_DR1", 1),
                    ("insert ZTF_alerts_aux", 1),
                ],
            ]
            .concat()
        ),
    );
    assert_eq!(
        command_counts(&known_object),
        counts(
            &[
                &survey_matches[..],
                &[existing_aux, alert, ("update ZTF_alerts_aux", 1)],
            ]
            .concat()
        ),
    );
}

#[tokio::test]
async fn test_lsst_alert_worker_query_budget() {
    let (new_object, known_object) = alert_worker_commands::<LsstAlertWorker>().await;
    let survey_matches = [("find DECAM_alerts_aux", 1), ("find ZTF_alerts_aux", 1)];
    let existing_aux = ("find LSST_alerts_aux", 1);
    let alert = ("insert LSST_alerts", 1);
    assert_eq!(
        command_counts(&new_object),
        counts(
            &[
                &survey_matches[..],
                &[
                    existing_aux,
                    alert,
                    ("aggregate LSPSC", 1),
                    ("insert LSST_alerts_aux", 1),
                ],
            ]
            .concat()
        ),
    );
    assert_eq!(
        command_counts(&known_object),
        counts(
            &[
                &survey_matches[..],
                &[existing_aux, alert, ("update LSST_alerts_aux", 1)],
            ]
            .concat()
        ),
    );
}

#[tokio::test]
async fn test_ztf_batch_workers_query_budget() {
    assert_batch_workers_scale::<ZtfAlertWorker, ZtfEnrichmentWorker, ZtfFilterWorker>().await;
}

#[tokio::test]
async fn test_lsst_batch_workers_query_budget() {
    assert_batch_workers_scale::<LsstAlertWorker, LsstEnrichmentWorker, LsstFilterWorker>().await;
}

/// The checks above pass trivially if `collection_scans` stops recognizing
/// a scan, say after a change to Mongo's explain output, so check it can.
#[tokio::test]
async fn test_collection_scans_detects_unindexed_query() {
    let db = get_test_db().await;
    let collection = "query_budget_probe";
    let query = |filter: Document| RecordedCommand {
        name: "find".to_string(),
        database: db.name().to_string(),
        collection: collection.to_string(),
        command: doc! { "find": collection, "filter": filter },
    };
    db.collection::<Document>(collection)
        .insert_one(doc! { "_id": 1, "unindexed": 1 })
        .await
        .unwrap();

    let scans = collection_scans(
        db.client(),
        &[query(doc! { "_id": 1 }), query(doc! { "unindexed": 1 })],
    )
    .await;
    db.collection::<Document>(collection).drop().await.unwrap();

    assert_eq!(scans.len(), 1, "{scans:?}");
    assert!(scans[0].contains("unindexed"));
}
