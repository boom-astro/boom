use tracing::instrument;

#[instrument(skip(collection, index), err, fields(collection = collection.name()))]
pub async fn create_index(
    collection: &mongodb::Collection<mongodb::bson::Document>,
    index: mongodb::bson::Document,
    unique: bool,
) -> mongodb::error::Result<()> {
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
