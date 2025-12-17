use mongodb::{
    bson::{doc, to_document, Document},
    options::IndexOptions,
    Collection, Database, IndexModel,
};
use serde::Serialize;
use tracing::instrument;

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
    let index_model = IndexModel::builder()
        .keys(index)
        .options(IndexOptions::builder().unique(unique).build())
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
pub fn cutout2bsonbinary(cutout: Vec<u8>) -> mongodb::bson::Binary {
    return mongodb::bson::Binary {
        subtype: mongodb::bson::spec::BinarySubtype::Generic,
        bytes: cutout,
    };
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

    Ok(())
}

/// This function updates a timeseries array by appending new values
/// while deduplicating based on a time field
/// (so we have only one measurement per epoch).
pub fn update_timeseries_op(
    array_field: &str,
    time_field: &str,
    value: &Vec<Document>,
) -> Document {
    doc! {
        "$sortArray": {
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
            "sortBy": { time_field: 1 }
        }
    }
}

pub fn get_array_element(field: &str) -> Document {
    doc! {
        "$arrayElemAt": [
            format!("${}", field),
            0
        ]
    }
}

/// This function generates a MongoDB aggregation operation
/// that filters an array field based on a time window relative to a candidate's midpointMjdTai.
/// It can also include optional conditions for filtering.
pub fn fetch_timeseries_op(
    array_field: &str,
    time_window: i32,
    optional_conditions: Option<Vec<Document>>,
) -> Document {
    let mut conditions = vec![
        doc! {
            "$lt": [
                {
                    "$subtract": [
                        "$candidate.midpointMjdTai",
                        "$$x.midpointMjdTai"
                    ]
                },
                time_window
            ]
        },
        doc! { // only datapoints up to (and including) current alert
            "$lte": [
                "$$x.midpointMjdTai",
                "$candidate.midpointMjdTai"
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
