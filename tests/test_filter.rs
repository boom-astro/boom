#![recursion_limit = "512"] // for large bson docs and CutoutStorage's s3 client
use boom::{
    alert::AlertWorker,
    conf::get_test_db,
    filter::{build_loaded_filter, run_filter, Filter, FilterVersion},
    utils::{
        enums::Survey,
        testing::{
            drop_alert_from_collections, insert_custom_test_filter, insert_test_filter,
            remove_test_filter, ztf_alert_worker, AlertRandomizer,
        },
    },
};
use mongodb::bson::{doc, Document};
use std::collections::HashMap;

#[tokio::test]
async fn test_build_filter() {
    let db = get_test_db().await;
    let filter_collection = db.collection("filters");

    let filter_id = insert_test_filter(&Survey::Ztf, true).await.unwrap();
    let filter_result = build_loaded_filter(
        &filter_id,
        &Survey::Ztf,
        &filter_collection,
        &HashMap::new(),
    )
    .await;
    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();

    let filter = filter_result.unwrap();
    let pipeline: Vec<Document> = vec![
        doc! { "$match": { "_id": { "$in": [] } } },
        doc! { "$project": { "objectId": 1, "candidate": 1, "classifications": 1, "properties": 1, "coordinates": 1 } },
        doc! { "$lookup": { "from": "ZTF_alerts_aux", "localField": "objectId", "foreignField": "_id", "as": "aux" } },
        doc! {
            "$addFields": {
                "aux": mongodb::bson::Bson::Null,
                "prv_candidates": {
                    "$filter": {
                        "input": { "$ifNull": [ {"$arrayElemAt": ["$aux.prv_candidates", 0] }, [] ] },
                        "as": "x",
                        "cond": {
                            "$and": [
                                { "$lt": [{ "$subtract": ["$candidate.jd", "$$x.jd"] }, 365] },
                                { "$lte": ["$$x.jd", "$candidate.jd"]},
                                { "$in": ["$$x.programid", [1_i32]] },
                            ]
                        }
                    }
                }
            }
        },
        doc! { "$match": { "prv_candidates.0": { "$exists": true }, "candidate.drb": { "$gt": 0.5 }, "candidate.ndethist": { "$gt": 1_f64 }, "candidate.magpsf": { "$lte": 18.5 } } },
        doc! { "$project": { "objectId": 1_i64, "annotations.mag_now": { "$round": ["$candidate.magpsf", 2_i64]} } },
    ];
    assert_eq!(pipeline, filter.pipeline);
    let mut expected_permissions = std::collections::HashMap::new();
    expected_permissions.insert(Survey::Ztf, vec![1]);
    assert_eq!(expected_permissions, filter.permissions);
}

#[tokio::test]
async fn test_build_multisurvey_filter() {
    let db = get_test_db().await;
    let filter_collection = db.collection("filters");

    let pipeline_str = r#"
    [
        {
            "$match": {
                "candidate.drb": { "$gte": 0.5 },
                "candidate.magpsf": { "$lte": 18.5 }
            }
        },
        {
            "$match": {
                "prv_candidates.9": { "$exists": true },
                "LSST.prv_candidates.0": { "$exists": true }
            }
        },
        {
            "$project": {
                "objectId": 1,
                "annotations": {
                    "lsst_id": { "$arrayElemAt": [ "$aliases.LSST", 0 ] }
                }
            }
        }
    ]
    "#;

    let filter_id = insert_custom_test_filter(&Survey::Ztf, pipeline_str)
        .await
        .unwrap();
    let filter_result = build_loaded_filter(
        &filter_id,
        &Survey::Ztf,
        &filter_collection,
        &HashMap::new(),
    )
    .await;
    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();

    let filter = filter_result.unwrap();
    let pipeline: Vec<Document> = vec![
        doc! { "$match": { "_id": { "$in": [] } } },
        doc! { "$project": { "objectId": 1, "candidate": 1, "classifications": 1, "properties": 1, "coordinates": 1 } },
        doc! { "$match": {
            "candidate.drb": { "$gte": 0.5 },
            "candidate.magpsf": { "$lte": 18.5 }
        } },
        doc! { "$lookup": { "from": "ZTF_alerts_aux", "localField": "objectId", "foreignField": "_id", "as": "aux" } },
        doc! {
            "$addFields": {
                "aux": mongodb::bson::Bson::Null,
                "prv_candidates": {
                    "$filter": {
                        "input": { "$ifNull": [ {"$arrayElemAt": ["$aux.prv_candidates", 0] }, [] ] },
                        "as": "x",
                        "cond": {
                            "$and": [
                                { "$lt": [{ "$subtract": ["$candidate.jd", "$$x.jd"] }, 365] },
                                { "$lte": ["$$x.jd", "$candidate.jd"]},
                                { "$in": ["$$x.programid", [1_i32]] },
                            ]
                        }
                    }
                },
                "aliases": {
                    "$ifNull": [ {"$arrayElemAt": ["$aux.aliases", 0] }, doc!{}]
                }
            }
        },
        doc! { "$lookup": { "from": "LSST_alerts_aux", "localField": "aliases.LSST.0", "foreignField": "_id", "as": "lsst_aux" } },
        doc! {
            "$addFields": {
                "lsst_aux": mongodb::bson::Bson::Null,
                "LSST.prv_candidates": {
                    "$filter": {
                        "input": { "$ifNull": [ {"$arrayElemAt": ["$lsst_aux.prv_candidates", 0] }, [] ] },
                        "as": "x",
                        "cond": {
                            "$and": [
                                { "$lt": [{ "$subtract": ["$candidate.jd", "$$x.jd"] }, 365] },
                                { "$lte": ["$$x.jd", "$candidate.jd"]},
                            ]
                        }
                    }
                }
            }
        },
        doc! { "$match": {
            "prv_candidates.9": { "$exists": true },
            "LSST.prv_candidates.0": { "$exists": true }
        } },
        doc! { "$project": { "objectId": 1_i64, "annotations": {
            "lsst_id": { "$arrayElemAt": [ "$aliases.LSST", mongodb::bson::Bson::Int64(0)] }
        } } },
    ];
    assert_eq!(pipeline, filter.pipeline);
    let mut expected_permissions = std::collections::HashMap::new();
    expected_permissions.insert(Survey::Ztf, vec![1]);
    assert_eq!(expected_permissions, filter.permissions);
}

#[tokio::test]
async fn test_filter_found() {
    let db = get_test_db().await;
    let filter_id = insert_test_filter(&Survey::Ztf, true).await.unwrap();
    let filter_collection = db.collection("filters");
    let filter_result = build_loaded_filter(
        &filter_id,
        &Survey::Ztf,
        &filter_collection,
        &HashMap::new(),
    )
    .await;
    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();
    assert!(filter_result.is_ok());
}

#[tokio::test]
async fn test_build_filter_with_watchlist() {
    let db = get_test_db().await;
    let filter_collection: mongodb::Collection<Filter> = db.collection("filters");

    let now = flare::Time::now().to_jd();
    let filter_id = uuid::Uuid::new_v4().to_string();
    let pipeline_str = r#"[
        {"$match": {"candidate.drb": {"$gt": 0.5}}},
        {"$project": {"objectId": 1}}
    ]"#;
    let mut permissions = std::collections::HashMap::new();
    permissions.insert(Survey::Ztf, vec![1]);
    let filter = Filter {
        id: filter_id.clone(),
        name: format!("watchlist_filter_{}", &filter_id[..8]),
        description: Some("Watchlist test filter".to_string()),
        survey: Survey::Ztf,
        user_id: "test_user".to_string(),
        watchlist: Some("watchlist_test".to_string()),
        permissions,
        active: true,
        active_fid: "v1".to_string(),
        fv: vec![FilterVersion {
            fid: "v1".to_string(),
            pipeline: pipeline_str.to_string(),
            changelog: None,
            created_at: now,
        }],
        created_at: now,
        updated_at: now,
    };
    filter_collection.insert_one(&filter).await.unwrap();

    // Projection drives which watchlist fields are surfaced under
    // `annotations.watchlist`. `Document` preserves insertion order, so the
    // generated $set fields come out in this order.
    let mut watchlist_projections = HashMap::new();
    watchlist_projections.insert(
        "watchlist_test".to_string(),
        doc! { "classification": 1, "redshift": 1 },
    );

    let loaded = build_loaded_filter(
        &filter_id,
        &Survey::Ztf,
        &filter_collection,
        &watchlist_projections,
    )
    .await
    .unwrap();
    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();

    // The four watchlist stages are appended at the end of the pipeline.
    let n = loaded.pipeline.len();
    assert_eq!(
        loaded.pipeline[n - 4],
        doc! { "$lookup": {
            "from": "watchlist_test",
            "localField": "objectId",
            "foreignField": "matching_ztf_objects",
            "as": "_watchlist_match",
        } }
    );
    assert_eq!(
        loaded.pipeline[n - 3],
        doc! { "$match": { "_watchlist_match.0": { "$exists": true } } }
    );
    // Every matched watchlist entry is surfaced (projected) under
    // annotations.watchlist as an array, then the internal join array is dropped.
    assert_eq!(
        loaded.pipeline[n - 2],
        doc! { "$set": { "annotations.watchlist": {
            "$map": {
                "input": "$_watchlist_match",
                "as": "w",
                "in": {
                    "classification": "$$w.classification",
                    "redshift": "$$w.redshift",
                },
            }
        } } }
    );
    assert_eq!(
        loaded.pipeline[n - 1],
        doc! { "$unset": "_watchlist_match" }
    );
}

// End-to-end regression test for the watchlist feature: a watchlist-bound filter
// whose final $project rebuilds `annotations` must still emit `annotations.watchlist`.
// This is the case the old (early-injection) implementation silently dropped.
#[tokio::test]
async fn test_watchlist_annotations_survive_user_project() {
    let db = get_test_db().await;
    let filter_collection: mongodb::Collection<Filter> = db.collection("filters");
    let watchlist_name = "watchlist_test_e2e";
    let watchlist_collection: mongodb::Collection<Document> = db.collection(watchlist_name);

    // Ingest a real alert and learn its objectId.
    let mut alert_worker = ztf_alert_worker().await;
    let (candid, object_id, _ra, _dec, bytes_content) =
        AlertRandomizer::new_randomized(Survey::Ztf).get().await;
    alert_worker.process_alert(&bytes_content).await.unwrap();

    // Watchlist entry that matches this alert's objectId.
    watchlist_collection
        .insert_one(doc! {
            "_id": format!("wl_{}", &object_id),
            "classification": "AGN",
            "redshift": 0.5_f64,
            "matching_ztf_objects": [&object_id],
        })
        .await
        .unwrap();

    // Filter bound to the watchlist whose final $project REBUILDS `annotations`
    // (annotations.mag_now). Under the old implementation this $project stripped
    // annotations.watchlist; it must now survive because the watchlist stages run last.
    let now = flare::Time::now().to_jd();
    let filter_id = uuid::Uuid::new_v4().to_string();
    let pipeline_str = r#"[
        {"$match": {"candidate.magpsf": {"$lt": 100}}},
        {"$project": {"objectId": 1, "annotations.mag_now": {"$round": ["$candidate.magpsf", 2]}}}
    ]"#;
    let mut permissions = std::collections::HashMap::new();
    permissions.insert(Survey::Ztf, vec![1]);
    let filter = Filter {
        id: filter_id.clone(),
        name: format!("watchlist_e2e_{}", &filter_id[..8]),
        description: Some("Watchlist e2e test filter".to_string()),
        survey: Survey::Ztf,
        user_id: "test_user".to_string(),
        watchlist: Some(watchlist_name.to_string()),
        permissions,
        active: true,
        active_fid: "v1".to_string(),
        fv: vec![FilterVersion {
            fid: "v1".to_string(),
            pipeline: pipeline_str.to_string(),
            changelog: None,
            created_at: now,
        }],
        created_at: now,
        updated_at: now,
    };
    filter_collection.insert_one(&filter).await.unwrap();

    let mut watchlist_projections = HashMap::new();
    watchlist_projections.insert(
        watchlist_name.to_string(),
        doc! { "classification": 1, "redshift": 1 },
    );

    let loaded = build_loaded_filter(
        &filter_id,
        &Survey::Ztf,
        &filter_collection,
        &watchlist_projections,
    )
    .await
    .unwrap();

    let alert_collection: mongodb::Collection<Document> = db.collection("ZTF_alerts");
    let results = run_filter(&[candid], &filter_id, loaded.pipeline, &alert_collection)
        .await
        .unwrap();

    // Cleanup before asserting so a failure doesn't leak state into other tests.
    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();
    drop_alert_from_collections(candid, &Survey::Ztf)
        .await
        .unwrap();
    watchlist_collection.drop().await.unwrap();

    assert_eq!(results.len(), 1, "alert should pass the watchlist filter");
    let annotations = results[0].get_document("annotations").unwrap();

    // User-built annotation survived.
    assert!(
        annotations.get("mag_now").is_some(),
        "user annotation annotations.mag_now must be present"
    );

    // Watchlist annotation was appended and survived the user $project.
    let watchlist = annotations.get_array("watchlist").unwrap();
    assert_eq!(watchlist.len(), 1, "one matching watchlist entry expected");
    let entry = watchlist[0].as_document().unwrap();
    assert_eq!(entry.get_str("classification").unwrap(), "AGN");
    assert_eq!(entry.get_f64("redshift").unwrap(), 0.5);
}

#[tokio::test]
async fn test_no_filter_found() {
    let db = get_test_db().await;
    let filter_collection = db.collection("filters");
    let filter_result = build_loaded_filter(
        "thisdoesnotexist",
        &Survey::Ztf,
        &filter_collection,
        &HashMap::new(),
    )
    .await;
    assert!(filter_result.is_err());
}
