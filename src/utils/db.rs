use chrono::NaiveDate;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, to_document, Bson, Document},
    options::{Hint, IndexOptions},
    Collection, Database, IndexModel,
};
use serde::Serialize;
use tracing::{error, info, instrument, warn};

use crate::utils::enums::Survey;

#[derive(thiserror::Error, Debug)]
#[error("failed to create index")]
pub struct CreateIndexError(#[from] mongodb::error::Error);

#[instrument(skip(collection, index), fields(collection = collection.name()), err)]
pub async fn create_index(
    collection: &Collection<Document>,
    index: Document,
    unique: bool,
) -> Result<(), CreateIndexError> {
    create_partial_index(collection, index, unique, None).await
}

#[instrument(
    skip(collection, index, partial_filter),
    fields(collection = collection.name()),
    err
)]
pub async fn create_partial_index(
    collection: &Collection<Document>,
    index: Document,
    unique: bool,
    partial_filter: Option<Document>,
) -> Result<(), CreateIndexError> {
    let index_model = IndexModel::builder()
        .keys(index)
        .options(
            IndexOptions::builder()
                .unique(unique)
                .partial_filter_expression(partial_filter)
                .build(),
        )
        .build();
    collection.create_index(index_model).await?;
    Ok(())
}

#[instrument(skip_all)]
pub fn mongify<T: Serialize>(value: &T) -> Document {
    // we removed all the sanitizing logic
    // in favor of using serde's attributes to clean up the data
    // ahead of time.
    // TODO: drop this function entirely and avoid unwrapping
    to_document(value).unwrap()
}

#[instrument(skip_all)]
pub fn mongify_vec<T: Serialize>(value: &Vec<T>) -> Vec<Document> {
    value.iter().map(|v| mongify(v)).collect()
}

#[instrument(skip_all)]
pub fn cutout2bsonbinary(cutout: Vec<u8>) -> mongodb::bson::Binary {
    return mongodb::bson::Binary {
        subtype: mongodb::bson::spec::BinarySubtype::Generic,
        bytes: cutout,
    };
}

/// Count alerts in `<survey>_alerts` for the observing night labelled by `date`
/// (local-noon to local-noon JD window).
///
/// `programids`:
/// - `None` → no permission filter (use for surveys without programid, e.g. LSST,
///   or when caller wants the full unrestricted count).
/// - `Some(pids)` → restrict to `candidate.programid ∈ pids`.
#[instrument(skip(db), err)]
pub async fn count_alerts_for_night(
    db: &Database,
    survey: &Survey,
    date: &NaiveDate,
    programids: Option<&[i32]>,
) -> Result<u64, mongodb::error::Error> {
    let (start_jd, end_jd) = survey.night_jd_window(date);
    let mut filter = doc! {
        "candidate.jd": { "$gte": start_jd, "$lt": end_jd },
    };
    if *survey == Survey::Ztf {
        if let Some(pids) = programids {
            filter.insert("candidate.programid", doc! { "$in": pids });
        }
    }
    let collection: Collection<Document> = db.collection(&format!("{}_alerts", survey));
    collection.count_documents(filter).await
}

// This function, for a given survey name (ZTF, LSST), will create
// the required indexes on the alerts and alerts_aux collections
#[instrument(skip(db), fields(database = db.name()), err)]
pub async fn initialize_survey_indexes(
    survey: &Survey,
    db: &Database,
) -> Result<(), CreateIndexError> {
    let alerts_collection_name = format!("{}_alerts", survey);
    let alerts_aux_collection_name = format!("{}_alerts_aux", survey);

    let alerts_collection: Collection<Document> = db.collection(&alerts_collection_name);
    let alerts_aux_collection: Collection<Document> = db.collection(&alerts_aux_collection_name);

    // create the compound 2dsphere + _id index on the alerts and alerts_aux collections
    let index = doc! {
        "coordinates.radec_geojson": "2dsphere",
        "_id": 1,
    };
    create_index(&alerts_collection, index.clone(), false).await?;
    create_index(&alerts_aux_collection, index, false).await?;

    // create a simple index on the objectId field of the alerts collection
    let index = doc! {
        "objectId": 1,
    };
    create_index(&alerts_collection, index, false).await?;

    // create a compound index on candidate.jd + geojson for temporal+spatial queries (MOC search, etc.)
    let jd_geo_index = doc! {
        "candidate.jd": 1,
        "coordinates.radec_geojson": "2dsphere",
    };
    create_index(&alerts_collection, jd_geo_index, false).await?;

    // ZTF joins a moving object's detections by MPC designation, since objectId is
    // positional. Indexes the raw field, which is present on the whole archive.
    if survey == &Survey::Ztf {
        let index = doc! {
            "candidate.ssnamenr": 1,
            "candidate.jd": -1,
        };
        create_partial_index(
            &alerts_collection,
            index,
            false,
            Some(doc! { "candidate.ssnamenr": { "$exists": true } }),
        )
        .await?;
    }

    // if survey is LSST, create an index on the ssObjectId field of the alerts collection,
    // and on the designation field of the aux collection (used to look up a moving object by
    // its MPC designation, independent of any cross-survey position match)
    if survey == &Survey::Lsst {
        let index = doc! {
            "ssObjectId": 1,
        };
        create_index(&alerts_collection, index, false).await?;

        let index = doc! {
            "designation": 1,
        };
        create_partial_index(
            &alerts_aux_collection,
            index,
            false,
            Some(doc! { "designation": { "$exists": true } }),
        )
        .await?;
    }

    Ok(())
}

/// This function updates a timeseries array by appending new values while deduplicating
/// based on a time field, maintaining sort order, and removing non-finite values.
/// (so we have only one measurement per epoch).
pub fn update_timeseries_op(
    array_field: &str,
    time_field: &str,
    value: &Vec<Document>,
) -> Document {
    let point_field_name = format!("$$point.{}", time_field);
    doc! {
        "$sortArray": {
            "input": {
                "$filter": {
                    "input": {
                        "$reduce": {
                            "input": {
                                "$concatArrays": [
                                    // handle the case where the array_field is not present
                                    { "$ifNull": [format!("${}", array_field), []] },
                                    value
                                ]
                            },
                            "initialValue": [],
                            "in": {
                                "$cond": {
                                    "if": { "$in": [format!("$$this.{}", time_field), format!("$$value.{}", time_field)] },
                                    "then": "$$value",
                                    "else": { "$concatArrays": ["$$value", ["$$this"]] }
                                }
                            }
                        }
                    },
                    "as": "point",
                    "cond": doc! {
                        "$and": [
                            // filter out non-finite values (including NaN and Infinity)
                            { "$isNumber": &point_field_name },
                            { "$eq": [&point_field_name, &point_field_name] },
                            { "$lt": [&point_field_name, f64::INFINITY] },
                            { "$gt": [&point_field_name, f64::NEG_INFINITY] }
                        ]
                    }
                }
            },
            "sortBy": { time_field: 1 }
        }
    }
}

pub fn get_array_element(field: &str) -> Document {
    doc! {
        "$ifNull": [
            {
                "$arrayElemAt": [
                    format!("${}", field),
                    0
                ]
            },
            []
        ]
    }
}

pub fn get_array_dict_element(field: &str) -> Document {
    doc! {
        "$ifNull": [
            {
                "$arrayElemAt": [
                    format!("${}", field),
                    0
                ]
            },
            {}
        ]
    }
}

/// This function generates a MongoDB aggregation operation
/// that filters an array field based on a time window relative to a candidate's jd field.
/// It can also include optional conditions for filtering.
/// The array_field is expected to come from an auxiliary collection
/// (i.e. "ztf_aux.prv_candidates", where "ztf_aux" is an array of documents itself).
pub fn fetch_timeseries_op(
    array_field: &str,
    candidate_jd_field: &str,
    time_window: i32,
    optional_conditions: Option<Vec<Document>>,
) -> Document {
    let mut conditions = vec![
        doc! {
            "$lt": [
                {
                    "$subtract": [
                        format!("${}", candidate_jd_field),
                        "$$x.jd"
                    ]
                },
                time_window
            ]
        },
        doc! { // only datapoints up to (and including) current alert
            "$lte": [
                "$$x.jd",
                format!("${}", candidate_jd_field),
            ]
        },
    ];
    if let Some(mut opts) = optional_conditions {
        conditions.append(&mut opts);
    }
    doc! {
        "$filter": doc! {
            "input": get_array_element(array_field),
            "as": "x",
            "cond": doc! {
                "$and": conditions
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Range sharding: splitting a full-collection pass into contiguous ranges is the
// only way to use more than one thread on it. Ranges are cut on an indexed field
// that tracks insertion order, so that each shard walks a roughly contiguous
// region on disk rather than jumping around it.
// -----------------------------------------------------------------------------

pub const CURSOR_BATCH_SIZE: u32 = 10_000;

/// Without the hint the empty-filter count collection-scans instead of counting the _id index.
pub async fn exact_count(collection: &Collection<Document>) -> Result<u64, mongodb::error::Error> {
    info!("counting the documents in {}", collection.namespace());
    collection
        .count_documents(doc! {})
        .hint(Hint::Keys(doc! { "_id": 1 }))
        .await
}

pub async fn collection_exists(db: &Database, name: &str) -> Result<bool, mongodb::error::Error> {
    Ok(db.list_collection_names().await?.iter().any(|n| n == name))
}

/// `created_at` is exactly insertion order, but it is only indexed if someone created
/// that index; `_id` always is, and both ZTF object ids and LSST diaObject ids happen
/// to be allocated in an order that correlates well with insertion.
pub async fn shard_field(collection: &Collection<Document>) -> &'static str {
    let indexed_on_created_at = match collection.list_indexes().await {
        Ok(cursor) => match cursor.try_collect::<Vec<_>>().await {
            Ok(indexes) => indexes
                .iter()
                .any(|index| index.keys.keys().next().is_some_and(|k| k == "created_at")),
            Err(_) => false,
        },
        Err(_) => false,
    };
    if indexed_on_created_at {
        "created_at"
    } else {
        "_id"
    }
}

pub async fn range_shards(
    collection: &Collection<Document>,
    parts: usize,
    field: &str,
    base_filter: &Document,
) -> Vec<Document> {
    if parts <= 1 {
        return vec![Document::new()];
    }
    // Behind a $match, $sample loses its random-cursor plan and collection-scans.
    let sample_size = if base_filter.is_empty() {
        (parts * 20).min(10_000)
    } else {
        10_000
    };
    info!(
        "sampling {} bounds on '{}' to cut {} shards",
        sample_size, field, parts
    );
    let mut pipeline = vec![doc! { "$sample": { "size": sample_size as i64 } }];
    if !base_filter.is_empty() {
        pipeline.push(doc! { "$match": base_filter.clone() });
    }
    pipeline.push(doc! { "$project": { field: 1 } });
    pipeline.push(doc! { "$sort": { field: 1 } });

    let bounds: Vec<Bson> = match collection.aggregate(pipeline).await {
        Ok(cursor) => match cursor.try_collect::<Vec<Document>>().await {
            Ok(docs) => docs.iter().filter_map(|d| d.get(field).cloned()).collect(),
            Err(e) => {
                warn!(error = %e, "could not sample {} bounds, falling back to a single shard", field);
                Vec::new()
            }
        },
        Err(e) => {
            warn!(error = %e, "could not sample {} bounds, falling back to a single shard", field);
            Vec::new()
        }
    };

    if bounds.len() < parts {
        warn!(
            "only {} sampled bounds on '{}' for {} shards, running as a single shard",
            bounds.len(),
            field,
            parts
        );
        return vec![Document::new()];
    }
    shard_filters(field, &bounds, parts)
}

/// Contiguous filters covering everything; expects `parts >= 2` and `bounds.len() >= parts`.
fn shard_filters(field: &str, bounds: &[Bson], parts: usize) -> Vec<Document> {
    let step = bounds.len() / parts;
    let cuts: Vec<&Bson> = (1..parts).map(|i| &bounds[i * step]).collect();

    let mut shards = Vec::with_capacity(parts);
    shards.push(doc! { "$or": [
        doc! { field: { "$lt": cuts[0].clone() } },
        doc! { field: { "$exists": false } },
    ] });
    for pair in cuts.windows(2) {
        shards.push(doc! { field: { "$gte": pair[0].clone(), "$lt": pair[1].clone() } });
    }
    shards.push(doc! { field: { "$gte": cuts[cuts.len() - 1].clone() } });
    shards
}

/// False: the shards did not cover every document counted before the pass.
pub fn check_shard_coverage(scanned: u64, total: u64, shard_count: usize) -> bool {
    if scanned >= total {
        true
    } else if shard_count > 1 {
        error!(
            "only {} of the {} document(s) counted before the pass were scanned: the shards did \
             not cover them all, re-run with --processes 1",
            scanned, total
        );
        false
    } else {
        warn!(
            "scanned {} of the {} document(s) counted before the pass: documents were modified \
             or deleted while it ran",
            scanned, total
        );
        true
    }
}

pub fn merge_filters(base: &Document, shard: &Document) -> Document {
    if shard.is_empty() {
        base.clone()
    } else if base.is_empty() {
        shard.clone()
    } else {
        doc! { "$and": [base.clone(), shard.clone()] }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TaskError {
    #[error("{0}")]
    Failed(#[from] mongodb::error::Error),
    #[error("task did not run to completion: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// Ignoring a JoinError would report a run that covered only part of the collection as a success.
pub async fn join_tasks<T>(
    handles: Vec<tokio::task::JoinHandle<Result<T, mongodb::error::Error>>>,
    label: &str,
) -> Result<Vec<T>, TaskError> {
    let mut results = Vec::with_capacity(handles.len());
    let mut first_err: Option<TaskError> = None;
    for handle in handles {
        match handle.await {
            Ok(Ok(value)) => results.push(value),
            Ok(Err(e)) => {
                error!("{} failed: {}", label, e);
                first_err.get_or_insert(e.into());
            }
            Err(e) => {
                error!("{} did not run to completion: {}", label, e);
                first_err.get_or_insert(e.into());
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(results),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds(values: &[i32]) -> Vec<Bson> {
        values.iter().map(|v| Bson::Int32(*v)).collect()
    }

    fn lower(shard: &Document, field: &str) -> Option<Bson> {
        shard.get_document(field).ok()?.get("$gte").cloned()
    }

    fn upper(shard: &Document, field: &str) -> Option<Bson> {
        match shard.get_array("$or") {
            Ok(branches) => branches[0]
                .as_document()?
                .get_document(field)
                .ok()?
                .get("$lt")
                .cloned(),
            Err(_) => shard.get_document(field).ok()?.get("$lt").cloned(),
        }
    }

    #[test]
    fn shard_filters_builds_one_filter_per_part() {
        let b = bounds(&(0..100).collect::<Vec<_>>());
        for parts in 2..10 {
            assert_eq!(
                shard_filters("_id", &b, parts).len(),
                parts,
                "parts={}",
                parts
            );
        }
    }

    #[test]
    fn shard_filters_covers_every_value_without_a_gap() {
        let b = bounds(&(0..100).collect::<Vec<_>>());
        for parts in 2..10 {
            let shards = shard_filters("_id", &b, parts);
            assert!(
                lower(&shards[0], "_id").is_none(),
                "the first shard is open-ended"
            );
            assert!(
                upper(shards.last().unwrap(), "_id").is_none(),
                "the last shard is open-ended"
            );
            for pair in shards.windows(2) {
                assert_eq!(
                    upper(&pair[0], "_id"),
                    lower(&pair[1], "_id"),
                    "parts={}, a value falls between two shards",
                    parts
                );
            }
        }
    }

    #[test]
    fn shard_filters_first_shard_also_takes_documents_without_the_field() {
        let shards = shard_filters("created_at", &bounds(&[1, 2, 3, 4]), 2);
        let branches = shards[0].get_array("$or").expect("first shard is an $or");
        assert_eq!(branches.len(), 2);
        assert_eq!(
            branches[1].as_document().unwrap(),
            &doc! { "created_at": { "$exists": false } }
        );
    }

    #[test]
    fn shard_filters_handles_a_sample_as_small_as_the_part_count() {
        let shards = shard_filters("_id", &bounds(&[10, 20, 30]), 3);
        assert_eq!(shards.len(), 3);
        assert_eq!(upper(&shards[0], "_id"), Some(Bson::Int32(20)));
        assert_eq!(lower(&shards[2], "_id"), Some(Bson::Int32(30)));
    }

    #[test]
    fn shard_filters_keeps_covering_everything_when_cuts_repeat() {
        let shards = shard_filters("_id", &bounds(&[7, 7, 7, 7]), 4);
        assert_eq!(shards.len(), 4);
        for pair in shards.windows(2) {
            assert_eq!(upper(&pair[0], "_id"), lower(&pair[1], "_id"));
        }
    }

    #[test]
    fn merge_filters_keeps_whichever_side_is_present() {
        let base = doc! { "coordinates": { "$exists": false } };
        let shard = doc! { "_id": { "$gte": 1 } };
        assert_eq!(merge_filters(&base, &Document::new()), base);
        assert_eq!(merge_filters(&Document::new(), &shard), shard);
        assert_eq!(
            merge_filters(&base, &shard),
            doc! { "$and": [base.clone(), shard.clone()] }
        );
        assert_eq!(
            merge_filters(&Document::new(), &Document::new()),
            Document::new()
        );
    }

    fn io_error(message: &str) -> mongodb::error::Error {
        std::io::Error::other(message.to_string()).into()
    }

    #[tokio::test]
    async fn join_tasks_returns_every_value_when_all_succeed() {
        let handles = vec![
            tokio::spawn(async { Ok::<i32, mongodb::error::Error>(1) }),
            tokio::spawn(async { Ok::<i32, mongodb::error::Error>(2) }),
        ];
        assert_eq!(join_tasks(handles, "task").await.unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn join_tasks_reports_the_first_error() {
        let handles = vec![
            tokio::spawn(async { Err(io_error("first")) }),
            tokio::spawn(async { Err(io_error("second")) }),
        ];
        let error = join_tasks::<i32>(handles, "task").await.unwrap_err();
        assert!(error.to_string().contains("first"), "got {}", error);
    }

    #[tokio::test]
    async fn join_tasks_does_not_swallow_a_panicking_task() {
        let handles = vec![
            tokio::spawn(async { Ok::<i32, mongodb::error::Error>(1) }),
            tokio::spawn(async {
                let ran = false;
                assert!(ran, "panicking on purpose");
                Ok::<i32, mongodb::error::Error>(2)
            }),
        ];
        let error = join_tasks(handles, "task").await.unwrap_err();
        assert!(matches!(error, TaskError::Join(_)), "got {:?}", error);
    }
}
