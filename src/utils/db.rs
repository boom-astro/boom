use flare::spatial::radec2lb;
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

#[instrument]
pub fn get_coordinates(ra: f64, dec: f64) -> Document {
    let (l, b) = radec2lb(ra, dec);
    doc! {
        "radec_geojson": {
            "type": "Point",
            "coordinates": [ra - 180.0, dec]
        },
        "l": l,
        "b": b
    }
}

#[instrument]
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
