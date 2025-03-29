use flare::spatial::radec2lb;
use mongodb::bson::to_document;
use serde::Serialize;
use tracing::instrument;

#[derive(thiserror::Error, Debug)]
#[error("failed to create index")]
pub struct CreateIndexError(#[from] mongodb::error::Error);

#[instrument(skip(collection, index), err, fields(collection = collection.name()))]
pub async fn create_index(
    collection: &mongodb::Collection<mongodb::bson::Document>,
    index: mongodb::bson::Document,
    unique: bool,
) -> Result<(), CreateIndexError> {
    let index_model = mongodb::IndexModel::builder()
        .keys(index)
        .options(
            mongodb::options::IndexOptions::builder()
                .unique(unique)
                .build(),
        )
        .build();
    collection.create_index(index_model).await?;
    Ok(())
}

pub fn mongify<T: Serialize>(value: &T) -> mongodb::bson::Document {
    // we removed all the sanitizing logic
    // in favor of using serde's attributes to clean up the data
    // ahead of time.
    // TODO: drop this function entirely and avoid unwrapping
    to_document(value).unwrap()
}

pub fn get_coordinates(ra: f64, dec: f64) -> mongodb::bson::Document {
    let (l, b) = radec2lb(ra, dec);
    mongodb::bson::doc! {
        "radec_geojson": {
            "type": "Point",
            "coordinates": [ra - 180.0, dec]
        },
        "l": l,
        "b": b
    }
}

pub fn cutout2bsonbinary(cutout: Vec<u8>) -> mongodb::bson::Binary {
    return mongodb::bson::Binary {
        subtype: mongodb::bson::spec::BinarySubtype::Generic,
        bytes: cutout,
    };
}
