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
    // sanitize it by removing fields with None values
    let mut cleaned_doc = mongodb::bson::Document::new();
    for (key, value) in to_document(&value).unwrap() {
        if value == mongodb::bson::Bson::Null {
            continue;
        }
        // if the value is f32 or f64 and is NaN, skip it
        if let mongodb::bson::Bson::Double(num) = value {
            if num.is_nan() {
                continue;
            }
        }
        cleaned_doc.insert(key, value);
    }
    cleaned_doc
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
