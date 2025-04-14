use boom::{
    conf,
    filter::{Filter, ZtfFilter},
    utils::testing,
};
use mongodb::bson::{doc, Document};

const CONFIG_FILE: &str = "tests/config.test.yaml";

#[tokio::test]
async fn test_build_filter() {
    let config_file = conf::load_config(CONFIG_FILE).unwrap();
    let db = conf::build_db(&config_file).await.unwrap();
    let filter_collection = db.collection("filters");

    let filter_id = testing::insert_test_ztf_filter().await.unwrap();
    let filter_result = ZtfFilter::build(filter_id, &filter_collection).await;
    testing::remove_test_ztf_filter(filter_id).await.unwrap();

    let filter = filter_result.unwrap();
    let pipeline: Vec<Document> = vec![
        doc! { "$match": {} },
        doc! { "$lookup": { "from": "ZTF_alerts_aux", "localField": "objectId", "foreignField": "_id", "as": "aux" } },
        doc! {
            "$project": {
                "objectId": 1, "candidate": 1, "classifications": 1, "coordinates": 1,
                "cross_matches": { "$arrayElemAt": ["$aux.cross_matches", 0] },
                "prv_candidates": {
                    "$filter": {
                        "input": { "$arrayElemAt": ["$aux.prv_candidates", 0] },
                        "as": "x",
                        "cond": {
                            "$and": [
                                { "$in": ["$$x.programid", [1_i32]] },
                                { "$lt": [{ "$subtract": ["$candidate.jd", "$$x.jd"] }, 365] },
                                { "$lte": ["$$x.jd", "$candidate.jd"]}
                            ]
                        }
                    }
                }
            }
        },
        doc! { "$match": { "candidate.drb": { "$gt": 0.5 }, "candidate.ndethist": { "$gt": 1_f64 }, "candidate.magpsf": { "$lte": 18.5 } } },
        doc! { "$project": { "annotations.mag_now": { "$round": ["$candidate.magpsf", 2_i64]} } },
    ];
    assert_eq!(pipeline, filter.pipeline);
    assert_eq!(vec![1], filter.permissions);
}

#[tokio::test]
async fn test_filter_found() {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await.unwrap();
    let filter_id = testing::insert_test_ztf_filter().await.unwrap();
    let filter_collection = db.collection("filters");
    let filter_result = ZtfFilter::build(filter_id, &filter_collection).await;
    testing::remove_test_ztf_filter(filter_id).await.unwrap();
    assert!(filter_result.is_ok());
}

#[tokio::test]
async fn test_no_filter_found() {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await.unwrap();
    let filter_collection = db.collection("filters");
    let filter_result = ZtfFilter::build(-2, &filter_collection).await;
    assert!(filter_result.is_err());
}
