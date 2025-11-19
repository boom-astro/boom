use boom::{
    conf,
    filter::build_loaded_filter,
    utils::{
        enums::Survey,
        testing::{
            insert_custom_test_filter, insert_test_filter, remove_test_filter, TEST_CONFIG_FILE,
        },
    },
};
use mongodb::bson::{doc, Document};

#[tokio::test]
async fn test_build_filter() {
    let config = conf::load_raw_config(TEST_CONFIG_FILE).unwrap();
    let db = conf::build_db(&config).await.unwrap();
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
                        "input": { "$arrayElemAt": ["$aux.prv_candidates", 0] },
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
    assert_eq!(vec![1], filter.permissions);
}

#[tokio::test]
async fn test_build_multisurvey_filter() {
    let config = conf::load_raw_config(TEST_CONFIG_FILE).unwrap();
    let db = conf::build_db(&config).await.unwrap();
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
                        "input": { "$arrayElemAt": ["$aux.prv_candidates", 0] },
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
                    "$arrayElemAt": ["$aux.aliases", 0]
                }
            }
        },
        doc! { "$lookup": { "from": "LSST_alerts_aux", "localField": "aliases.LSST.0", "foreignField": "_id", "as": "lsst_aux" } },
        doc! {
            "$addFields": {
                "lsst_aux": mongodb::bson::Bson::Null,
                "LSST.prv_candidates": {
                    "$filter": {
                        "input": { "$arrayElemAt": ["$lsst_aux.prv_candidates", 0] },
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
    assert_eq!(vec![1], filter.permissions);
}

#[tokio::test]
async fn test_filter_found() {
    let config = conf::load_raw_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config).await.unwrap();
    let filter_id = insert_test_filter(&Survey::Ztf, true).await.unwrap();
    let filter_collection = db.collection("filters");
    let filter_result = build_loaded_filter(&filter_id, &Survey::Ztf, &filter_collection).await;
    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();
    assert!(filter_result.is_ok());
}

#[tokio::test]
async fn test_no_filter_found() {
    let config = conf::load_raw_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config).await.unwrap();
    let filter_collection = db.collection("filters");
    let filter_result =
        build_loaded_filter("thisdoesnotexist", &Survey::Ztf, &filter_collection).await;
    assert!(filter_result.is_err());
}
