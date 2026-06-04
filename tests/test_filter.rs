use boom::{
    conf::get_test_db,
    filter::{build_loaded_filter, Filter, FilterVersion},
    utils::{
        enums::Survey,
        testing::{insert_custom_test_filter, insert_test_filter, remove_test_filter},
    },
};
use mongodb::bson::{doc, Document};

#[tokio::test]
async fn test_build_filter() {
    let db = get_test_db().await;
    let filter_collection = db.collection("filters");

    let filter_id = insert_test_filter(&Survey::Ztf, true).await.unwrap();
    let filter_result = build_loaded_filter(&filter_id, &Survey::Ztf, &filter_collection).await;
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
    let filter_result = build_loaded_filter(&filter_id, &Survey::Ztf, &filter_collection).await;
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
    let filter_result = build_loaded_filter(&filter_id, &Survey::Ztf, &filter_collection).await;
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

    let loaded = build_loaded_filter(&filter_id, &Survey::Ztf, &filter_collection)
        .await
        .unwrap();
    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();

    // The lookup + match for the watchlist must be injected right after the
    // initial candids $match (idx 0) and static $project (idx 1).
    assert_eq!(
        loaded.pipeline[2],
        doc! { "$lookup": {
            "from": "watchlist_test",
            "localField": "objectId",
            "foreignField": "matching_ztf_objects",
            "as": "_watchlist_match",
        } }
    );
    assert_eq!(
        loaded.pipeline[3],
        doc! { "$match": { "_watchlist_match.0": { "$exists": true } } }
    );
}

#[tokio::test]
async fn test_no_filter_found() {
    let db = get_test_db().await;
    let filter_collection = db.collection("filters");
    let filter_result =
        build_loaded_filter("thisdoesnotexist", &Survey::Ztf, &filter_collection).await;
    assert!(filter_result.is_err());
}
