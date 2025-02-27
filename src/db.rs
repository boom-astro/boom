use tracing::error;

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
    match collection.create_index(index_model).await {
        Err(e) => {
            error!(
                "Error when creating index for collection {}: {}",
                collection.name(),
                e
            );
        }
        Ok(_x) => {}
    }
    Ok(())
}
