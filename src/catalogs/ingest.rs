//! The shared insert path: a pool of workers draining one channel into Mongo.
//!
//! Every format engine parses on the calling task and hands records to this
//! pool, so the parse and the insert overlap and only the engine differs
//! between catalogs.

use crate::utils::spatial::Coordinates;
use mongodb::bson::{to_document, Document};
use mongodb::{Collection, Database};
use serde::Serialize;
use tracing::instrument;

/// Whether a record carries sky coordinates, and so wants a `coordinates`
/// subdocument and a 2dsphere index.
///
/// Implemented per record type rather than sniffed from the serialized
/// document: a catalog that happens to have `ra`/`dec` columns meaning
/// something else must not silently acquire a spatial index.
pub trait HasCoordinates {
    fn has_coordinates() -> bool {
        true
    }
}

#[derive(thiserror::Error, Debug)]
pub enum IngestError {
    #[error("failed to serialize record: {0}")]
    Serialize(#[from] mongodb::bson::ser::Error),
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
    #[error(transparent)]
    Index(#[from] crate::utils::db::CreateIndexError),
    #[error("insert worker panicked: {0}")]
    WorkerPanic(String),
    #[error("{0}")]
    Read(String),
}

/// Render one record as its stored document, adding `coordinates` when the type
/// declares sky positions.
///
/// `ra`/`dec` are read back off the serialized document rather than required on
/// the Rust type, because the catalogs disagree on their width (2MASS stores
/// f32, NED f64) and on whether the fields are renamed on the way out.
fn to_catalog_document<T: Serialize + HasCoordinates>(
    record: &T,
) -> Result<Document, mongodb::bson::ser::Error> {
    let mut doc = to_document(record)?;
    if T::has_coordinates() {
        if let (Ok(ra), Ok(dec)) = (doc.get_f64("ra"), doc.get_f64("dec")) {
            // Coordinates::new also derives galactic l/b, which is what the rest
            // of boom stores alongside radec_geojson.
            doc.insert("coordinates", to_document(&Coordinates::new(ra, dec))?);
        }
    }
    Ok(doc)
}

/// A pool of insert workers fed by a bounded channel.
pub struct Inserter {
    db: Database,
    collection_name: String,
    num_workers: usize,
    batch_size: usize,
    channel_capacity: usize,
}

/// What one file's ingest did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct IngestReport {
    /// Records parsed out of the source and sent to the workers.
    pub read: u64,
    /// Records the workers acknowledged into Mongo.
    pub inserted: u64,
    /// Source records that failed to parse and were skipped.
    pub skipped: u64,
}

impl IngestReport {
    pub fn merge(&mut self, other: IngestReport) {
        self.read += other.read;
        self.inserted += other.inserted;
        self.skipped += other.skipped;
    }
}

impl Inserter {
    pub fn new(
        db: Database,
        collection_name: impl Into<String>,
        num_workers: usize,
        batch_size: usize,
        channel_capacity: usize,
    ) -> Self {
        Self {
            db,
            collection_name: collection_name.into(),
            // A zero here would drop every record on the floor silently.
            num_workers: num_workers.max(1),
            batch_size: batch_size.max(1),
            channel_capacity: channel_capacity.max(1),
        }
    }

    pub fn collection(&self) -> Collection<Document> {
        self.db.collection::<Document>(&self.collection_name)
    }

    /// Start the workers and return the sender to feed them.
    ///
    /// Drop the sender to signal the end of the stream, then call
    /// [`Inserter::finish`] with the handles.
    pub fn start<T>(&self) -> (async_channel::Sender<T>, Vec<InsertWorker>)
    where
        T: Serialize + HasCoordinates + Send + 'static,
    {
        let (sender, receiver) = async_channel::bounded::<T>(self.channel_capacity);
        let mut workers = Vec::with_capacity(self.num_workers);
        for worker_id in 0..self.num_workers {
            let receiver = receiver.clone();
            let collection = self.collection();
            let batch_size = self.batch_size;
            workers.push(tokio::spawn(async move {
                insert_worker(worker_id, receiver, collection, batch_size).await
            }));
        }
        (sender, workers)
    }

    /// Wait for every worker and sum what they inserted.
    ///
    /// A worker that failed is an error rather than a warning: a partially
    /// inserted chunk that reports success would be recorded as done and never
    /// retried.
    pub async fn finish(&self, workers: Vec<InsertWorker>) -> Result<u64, IngestError> {
        let mut inserted = 0;
        let mut first_error = None;
        // Every handle is awaited even after one fails, so no worker is left
        // writing into a collection the caller believes it has finished with.
        for handle in workers {
            match handle.await {
                Ok(Ok(n)) => inserted += n,
                Ok(Err(e)) => first_error = first_error.or(Some(e)),
                Err(e) => {
                    first_error = first_error.or(Some(IngestError::WorkerPanic(e.to_string())));
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(inserted),
        }
    }

    /// Build the 2dsphere index, once the load is done.
    ///
    /// Deliberately not created up front: an index that exists during the load
    /// has to be maintained on every insert, which roughly doubles the time to
    /// ingest a large catalog.
    #[instrument(skip(self), fields(collection = %self.collection_name))]
    pub async fn create_indexes(&self, has_coordinates: bool) -> Result<(), IngestError> {
        if !has_coordinates {
            return Ok(());
        }
        tracing::info!("building 2dsphere index on {}", self.collection_name);
        crate::utils::db::create_index(
            &self.collection(),
            mongodb::bson::doc! { "coordinates.radec_geojson": "2dsphere" },
            false,
        )
        .await?;
        Ok(())
    }
}

pub type InsertWorker = tokio::task::JoinHandle<Result<u64, IngestError>>;

async fn insert_worker<T>(
    worker_id: usize,
    receiver: async_channel::Receiver<T>,
    collection: Collection<Document>,
    batch_size: usize,
) -> Result<u64, IngestError>
where
    T: Serialize + HasCoordinates,
{
    let mut batch: Vec<Document> = Vec::with_capacity(batch_size);
    let mut inserted = 0u64;

    while let Ok(record) = receiver.recv().await {
        batch.push(to_catalog_document(&record)?);
        if batch.len() >= batch_size {
            inserted += write_batch(&collection, std::mem::take(&mut batch), worker_id).await?;
            batch.reserve(batch_size);
        }
    }
    if !batch.is_empty() {
        inserted += write_batch(&collection, batch, worker_id).await?;
    }
    Ok(inserted)
}

/// Insert one batch, tolerating duplicate keys but nothing else.
///
/// Catalogs derive `_id` from a stable source identifier, so re-ingesting a
/// chunk that was interrupted after a partial write is expected to collide.
/// Those collisions mean "already there" and are counted as written; any other
/// bulk-write failure is propagated, because dropping records from a catalog
/// silently produces alerts that look confidently unmatched.
async fn write_batch(
    collection: &Collection<Document>,
    batch: Vec<Document>,
    worker_id: usize,
) -> Result<u64, IngestError> {
    let n = batch.len() as u64;
    let opts = mongodb::options::InsertManyOptions::builder()
        .ordered(false)
        .build();
    match collection.insert_many(batch).with_options(opts).await {
        Ok(result) => Ok(result.inserted_ids.len() as u64),
        Err(e) => match duplicate_key_count(&e) {
            Some(duplicates) => {
                tracing::debug!(
                    worker_id,
                    duplicates,
                    "batch had records already present, treating as written"
                );
                Ok(n)
            }
            None => Err(e.into()),
        },
    }
}

/// `Some(n)` when every write error in the failure was a duplicate key (11000).
fn duplicate_key_count(error: &mongodb::error::Error) -> Option<usize> {
    match *error.kind {
        mongodb::error::ErrorKind::InsertMany(ref failure) => {
            // A write-concern failure means the batch may not be durable, which
            // is not the same thing as "already there".
            if failure.write_concern_error.is_some() {
                return None;
            }
            let errors = failure.write_errors.as_ref()?;
            errors
                .iter()
                .all(|e| e.code == 11000)
                .then_some(errors.len())
        }
        _ => None,
    }
}
