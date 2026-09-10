#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
//! End-to-end: a sample ZTF alert flows through the whole stream — ingestion,
//! then the enrichment worker (which runs the CIDER fusion model and upserts the
//! resulting embedding to Milvus) — and is then read back out of Milvus with the
//! similarity-search / query / count APIs.
//!
//! The ZTF enrichment worker only writes embeddings when `milvus.enabled`, so
//! this test is **gated on the config**: with Milvus disabled it logs a skip and
//! returns. That is the case in CI, where `.env` is copied from `.env.example`
//! (`BOOM_MILVUS__ENABLED=false`). To run it for real, set `BOOM_MILVUS__*` in
//! `.env` (see `docs/milvus.md`); it then runs against whatever Milvus those
//! credentials point at, provisioning the collection if it does not yet exist.
use boom::{
    alert::{AlertWorker, ProcessAlertStatus},
    conf::{self},
    enrichment::{EnrichmentWorker, ZtfEnrichmentWorker},
    milvus::{EmbeddingRow, MilvusClient},
    utils::{
        enums::Survey,
        testing::{
            drop_alert_from_collections, ztf_alert_worker, AlertRandomizer, TEST_CONFIG_FILE,
        },
    },
};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_ztf_alert_to_milvus_e2e() {
    // Loads `.env` too, so BOOM_MILVUS__* overrides are applied here just as they
    // are inside the enrichment worker.
    let config = conf::load_config(Some(TEST_CONFIG_FILE)).expect("failed to load test config");

    if !config.milvus.enabled {
        eprintln!(
            "skipping test_ztf_alert_to_milvus_e2e: milvus is disabled. Set \
             BOOM_MILVUS__ENABLED=true (and the credentials) in .env to run the \
             full end-to-end flow — see docs/milvus.md."
        );
        return;
    }

    // The enrichment worker connects to Milvus but never provisions the
    // collection, and similarity search needs it loaded — so make sure the
    // collection exists and is loaded up front.
    let mut client = MilvusClient::connect(&config.milvus)
        .await
        .expect("failed to connect to milvus");
    client
        .ensure_embedding_collection()
        .await
        .expect("failed to ensure embeddings collection");
    // ensure_embedding_collection() skips the load when the collection already
    // exists, so load explicitly (idempotent). Search rejects an unloaded
    // collection.
    client
        .load_collection()
        .await
        .expect("failed to load collection");

    // 1. Ingest a sample ZTF alert. Randomizing the candid and object_id keeps
    //    the run self-contained and makes its Milvus row (keyed by object_id)
    //    unique, so the assertions and cleanup below can't collide with other
    //    data in the collection.
    let (candid, object_id, _ra, _dec, bytes) = AlertRandomizer::new(Survey::Ztf)
        .rand_candid()
        .rand_object_id()
        .get()
        .await;

    let mut alert_worker = ztf_alert_worker().await;
    let status = alert_worker.process_alert(&bytes).await.unwrap();
    assert_eq!(status, ProcessAlertStatus::Added(candid));

    // Clear any leftover row from a previously aborted run of this same id.
    client.delete_embeddings(&[&object_id]).await.ok();

    // 2. Enrich: runs ML classification + the CIDER fusion model and upserts the
    //    384-float embedding to Milvus, keyed by object_id.
    let mut enrichment_worker = ZtfEnrichmentWorker::new(TEST_CONFIG_FILE, None)
        .await
        .unwrap();
    let enrichment_output = enrichment_worker
        .process_alerts(&[candid])
        .await
        .expect("enrichment failed");
    // ZTF enrichment returns "programid,candid" strings for the filter worker.
    assert_eq!(enrichment_output, vec![format!("1,{}", candid)]);

    // 3. Seal recent writes so they become visible to reads immediately (Milvus
    //    otherwise flushes on its own schedule).
    client.flush().await.expect("flush failed");

    // 4. Read the embedding back by object_id. Milvus makes upserts queryable
    //    asynchronously, so poll briefly for read-after-write visibility.
    let row = read_embedding_with_retry(&mut client, &object_id).await;
    assert_eq!(row.object_id, object_id);
    assert_eq!(row.candid, candid, "stored candid should match the alert");
    assert_eq!(
        row.embedding.len(),
        config.milvus.collection.dim as usize,
        "stored embedding must have the collection's configured dimensionality"
    );
    assert!(
        row.jd > 0.0,
        "jd should be a real Julian date, got {}",
        row.jd
    );

    // 5. Similarity search with the stored vector must find the object itself, at
    //    a near-perfect score (COSINE self-match on an L2-normalized vector).
    let hits = client
        .search_embedding(&row.embedding, 5)
        .await
        .expect("similarity search failed");
    let self_hit = hits
        .iter()
        .find(|h| h.object_id == object_id)
        .expect("similarity search should return the object we just inserted");
    assert!(
        self_hit.score > 0.99,
        "self-similarity should be ~1.0 under COSINE, got {}",
        self_hit.score
    );
    assert_eq!(
        self_hit.candid,
        Some(candid),
        "search hit should carry the stored candid"
    );

    // The collection now holds at least our row.
    assert!(
        client.count().await.expect("count failed") >= 1,
        "collection should contain at least the row we just wrote"
    );

    // 6. Clean up: remove our Milvus row and the Mongo alert so the test leaves
    //    no trace.
    client
        .delete_embeddings(&[&object_id])
        .await
        .expect("failed to delete embedding");
    drop_alert_from_collections(candid, &Survey::Ztf)
        .await
        .unwrap();
}

/// Poll `get_embeddings` until the row for `object_id` is visible, or give up.
/// A just-upserted vector is not always readable on the first attempt because
/// Milvus applies writes asynchronously.
async fn read_embedding_with_retry(client: &mut MilvusClient, object_id: &str) -> EmbeddingRow {
    const ATTEMPTS: usize = 20;
    for _ in 0..ATTEMPTS {
        let rows = client
            .get_embeddings(&[object_id])
            .await
            .expect("get_embeddings failed");
        if let Some(row) = rows.into_iter().find(|r| r.object_id == object_id) {
            return row;
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("embedding for {object_id} never became visible in Milvus after {ATTEMPTS} attempts");
}
