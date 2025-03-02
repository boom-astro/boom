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
