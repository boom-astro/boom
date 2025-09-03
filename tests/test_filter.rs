use boom::{
    conf,
    filter::{uses_field_in_filter, validate_filter_pipeline, Filter, ZtfFilter},
    utils::enums::Survey,
    utils::testing::{insert_test_filter, remove_test_filter, TEST_CONFIG_FILE},
};
use mongodb::bson::{doc, Document};

fn pipeline_to_json(pipeline: &[Document]) -> Vec<serde_json::Value> {
    pipeline
        .iter()
        .map(|doc| serde_json::to_value(doc).unwrap())
        .collect()
}

#[tokio::test]
async fn test_uses_field_in_filter() {
    // this function should detect if a field is used in the filter pipeline
    // so let's write a filter and test it on it.
    let pipeline = [
        doc! { "$match": { "candidate.drb": { "$gt": 0.5 }, "LSST.prv_candidates": { "$exists": true } } },
        doc! { "$project": { "annotations.mag_now": { "$round": ["$candidate.magpsf", 2_i64] } } },
    ];
    // it takes a Vec<serde_json::Value> as input, so we convert the documents to that format
    let pipeline = pipeline_to_json(&pipeline);

    // uses_field_in_filter should return true for "candidate.drb"
    let stage_index = uses_field_in_filter(&pipeline, "candidate.drb");
    assert!(stage_index.is_some());
    assert_eq!(stage_index, Some(0));

    // uses_field_in_filter should also return true for "candidate.magpsf", but it should be in the second stage
    let stage_index = uses_field_in_filter(&pipeline, "candidate.magpsf");
    assert!(stage_index.is_some());
    assert_eq!(stage_index, Some(1));

    // uses_field_in_filter should return true for "LSST.prv_candidates"
    let stage_index = uses_field_in_filter(&pipeline, "LSST.prv_candidates");
    assert!(stage_index.is_some());
    assert_eq!(stage_index, Some(0));

    // however, if we look for "prv_candidates" only, we should not find it (using the prefixes to avoid)
    let stage_index = uses_field_in_filter(&pipeline, "prv_candidates");
    assert!(stage_index.is_none());

    // uses_field_in_filter should return false for "candidate.jd"
    let stage_index = uses_field_in_filter(&pipeline, "candidate.jd");
    assert!(stage_index.is_none());
}

#[tokio::test]
async fn test_validate_filter_pipeline() {
    // this function should validate the filter pipeline
    // we make sure that:
    // - project stages that are an include stages (no "field: 0") specify objectId: 1
    // - project stages that are an exclude stage (with "field: 0") do not mention objectId
    // - project stages do not exclude the _id field or objectId
    // - unset stages do not delete the objectId or _id fields
    // - we don't have any group, unwind, or lookup stages
    // - that the last stage is a project that includes objectId

    // first let's test a valid pipeline, and then we will test some invalid ones
    let valid_pipeline = vec![
        doc! { "$match": {} },
        doc! { "$project": { "objectId": 1, "candidate": 1, "classifications": 1, "coordinates": 1 } },
        doc! { "$project": { "objectId": 1, "annotations.mag_now": { "$round": ["$candidate.magpsf", 2_i64]} } },
    ];
    // it expects a Vec<serde_json::Value> as input, so we convert the documents to that format
    let valid_pipeline = pipeline_to_json(&valid_pipeline);

    // this should return Ok(())
    let result = validate_filter_pipeline(&valid_pipeline);
    assert!(result.is_ok());

    // now let's test a pipeline which is invalid because we exclude the objectId field in a project stage
    let invalid_pipeline = vec![
        doc! { "$match": {} },
        doc! { "$project": { "candidate": 1, "classifications": 1, "coordinates": 1 } }, // objectId is excluded here
        doc! { "$project": { "objectId": 1, "annotations.mag_now": { "$round": ["$candidate.magpsf", 2_i64]} } },
    ];
    let invalid_pipeline = pipeline_to_json(&invalid_pipeline);
    // this should return an error
    let result = validate_filter_pipeline(&invalid_pipeline);
    assert!(result.is_err());

    // now let's test a pipeline which is invalid because we exclude the _id field in a project stage
    let invalid_pipeline = vec![
        doc! { "$match": {} },
        doc! { "$project": { "objectId": 1, "_id": 0, "candidate": 1, "classifications": 1, "coordinates": 1 } }, // _id is excluded here
        doc! { "$project": { "objectId": 1, "annotations.mag_now": { "$round": ["$candidate.magpsf", 2_i64]} } },
    ];
    let invalid_pipeline = pipeline_to_json(&invalid_pipeline);
    // this should return an error
    let result = validate_filter_pipeline(&invalid_pipeline);
    assert!(result.is_err());

    // now let's test a pipeline which is invalid because we unset the objectId field
    let invalid_pipeline = vec![
        doc! { "$match": {} },
        doc! { "$project": { "objectId": 1, "candidate": 1, "classifications": 1, "coordinates": 1 } },
        doc! { "$unset": "objectId" }, // objectId is unset here
        doc! { "$project": { "objectId": 1, "annotations.mag_now": { "$round": ["$candidate.magpsf", 2_i64]} } },
    ];
    let invalid_pipeline = pipeline_to_json(&invalid_pipeline);
    // this should return an error
    let result = validate_filter_pipeline(&invalid_pipeline);
    assert!(result.is_err());

    // now let's test a pipeline which is invalid because the last stage is not a project stage
    let invalid_pipeline = vec![
        doc! { "$match": {} },
        doc! { "$project": { "objectId": 1, "candidate": 1, "classifications": 1, "coordinates": 1 } },
        doc! { "$addFields": { "annotations.mag_now": { "$round": ["$candidate.magpsf", 2_i64]} } }, // last stage is not a project
    ];
    let invalid_pipeline = pipeline_to_json(&invalid_pipeline);
    // this should return an error
    let result = validate_filter_pipeline(&invalid_pipeline);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_build_filter() {
    let config = conf::load_config(TEST_CONFIG_FILE).unwrap();
    let db = conf::build_db(&config).await.unwrap();
    let filter_collection = db.collection("filters");

    let filter_id = insert_test_filter(&Survey::Ztf, true, true).await.unwrap();
    let filter_result = ZtfFilter::build(&filter_id, &filter_collection).await;
    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();

    let filter = filter_result.unwrap();
    let pipeline: Vec<Document> = vec![
        doc! { "$match": { "_id": { "$in": [] } } },
        doc! { "$project": { "objectId": 1, "candidate": 1, "classifications": 1, "properties": 1, "coordinates": 1 } },
        doc! { "$lookup": { "from": "ZTF_alerts_aux", "localField": "objectId", "foreignField": "_id", "as": "aux" } },
        doc! {
            "$addFields": {
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
                "aliases": { "$arrayElemAt": ["$aux.aliases", 0] }
            }
        },
        doc! { "$unset": "aux" },
        doc! { "$lookup": { "from": "LSST_alerts_aux", "localField": "aliases.LSST.0", "foreignField": "_id", "as": "lsst_aux" } },
        doc! {
            "$addFields": {
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
                },
            }
        },
        doc! { "$unset": "lsst_aux" },
        doc! { "$match": { "prv_candidates.0": { "$exists": true }, "LSST.prv_candidates.0": { "$exists": true }, "candidate.drb": { "$gt": 0.5 }, "candidate.ndethist": { "$gt": 1_f64 }, "candidate.magpsf": { "$lte": 18.5 } } },
        doc! { "$project": { "objectId": 1_i64, "annotations.mag_now": { "$round": ["$candidate.magpsf", 2_i64]} } },
    ];
    assert_eq!(pipeline, filter.pipeline);
    assert_eq!(vec![1], filter.permissions);
}

#[tokio::test]
async fn test_filter_found() {
    let config = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config).await.unwrap();
    let filter_id = insert_test_filter(&Survey::Ztf, true, false).await.unwrap();
    let filter_collection = db.collection("filters");
    let filter_result = ZtfFilter::build(&filter_id, &filter_collection).await;
    remove_test_filter(&filter_id, &Survey::Ztf).await.unwrap();
    assert!(filter_result.is_ok());
}

#[tokio::test]
async fn test_no_filter_found() {
    let config = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config).await.unwrap();
    let filter_collection = db.collection("filters");
    let filter_result = ZtfFilter::build("thisdoesntexist", &filter_collection).await;
    assert!(filter_result.is_err());
}
