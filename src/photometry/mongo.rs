//! One current document per measurement; no object-sized history document.
use super::{History, Measurement, PhotometryStore, StoredHistory, SCHEMA_VERSION};
use anyhow::{ensure, Result};
use futures::TryStreamExt;
use mongodb::{
    bson::{self, doc, Document},
    options::{IndexOptions, UpdateOneModel, WriteConcern},
    Collection, Database, IndexModel,
};

pub const POINTS: &str = "ZTF_photometry_points";

#[derive(Clone)]
pub struct MongoPoints {
    db: Database,
    points: Collection<Document>,
}

impl MongoPoints {
    pub fn new(db: Database) -> Self {
        let points = db.collection(POINTS);
        Self { db, points }
    }

    pub async fn initialize(&self) -> Result<()> {
        self.points
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"object_id":1,"kind":1,"jd":1})
                    .options(
                        IndexOptions::builder()
                            .unique(true)
                            .name("object_kind_epoch".to_owned())
                            .build(),
                    )
                    .build(),
            )
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl PhotometryStore for MongoPoints {
    async fn write_points(&self, points: &[Measurement]) -> Result<()> {
        // Validate the entire input before any mutation. A failed bulk may still
        // have committed a prefix; retries use the same identities and revisions.
        let mut identities = std::collections::HashSet::new();
        for point in points {
            point.validate()?;
            ensure!(
                identities.insert(point.id()?),
                "duplicate measurement identity in write batch"
            );
        }
        for chunk in points.chunks(256) {
            let mut writes = Vec::with_capacity(chunk.len());
            for point in chunk {
                let mut replacement = bson::to_document(point)?;
                replacement.insert("_id", point.id()?);
                replacement.insert("schema_version", SCHEMA_VERSION);
                let update = vec![doc! {"$replaceWith":{"$cond":[
                    {"$or":[{"$eq":[{"$type":"$revision"},"missing"]},
                        {"$lt":["$revision",point.revision]}]},
                    {"$literal":replacement}, "$$ROOT"
                ]}}];
                writes.push(
                    UpdateOneModel::builder()
                        .namespace(self.points.namespace())
                        .filter(doc! {"_id":point.id()?})
                        .update(update)
                        .upsert(true)
                        .build(),
                );
            }
            self.db
                .client()
                .bulk_write(writes)
                .ordered(false)
                .write_concern(WriteConcern::majority())
                .await?;
            // Equal revisions must denote the same accepted record. Never let
            // a conflicting replay silently replace it or claim success.
            let ids = chunk
                .iter()
                .map(Measurement::id)
                .collect::<Result<Vec<_>>>()?;
            let stored: Vec<Document> = self
                .points
                .find(doc! {"_id":{"$in":ids}})
                .await?
                .try_collect()
                .await?;
            ensure!(
                stored.len() == chunk.len(),
                "missing points after acknowledged write"
            );
            let stored: std::collections::HashMap<_, _> = stored
                .into_iter()
                .map(|d| Ok((d.get_str("_id")?.to_owned(), d)))
                .collect::<Result<_>>()?;
            for point in chunk {
                let row = &stored[&point.id()?];
                let revision = row.get_i64("revision")?;
                ensure!(
                    revision >= point.revision,
                    "stored revision moved backwards"
                );
                if revision == point.revision {
                    let restored: Measurement = bson::from_document(row.clone())?;
                    ensure!(
                        &restored == point,
                        "conflicting records at the same accepted revision"
                    );
                }
            }
        }
        Ok(())
    }

    async fn read_history(&self, object_id: &str) -> Result<StoredHistory> {
        let mut cursor = self
            .points
            .find(doc! {"object_id":object_id})
            .sort(doc! {"kind":1,"jd":1})
            .await?;
        let mut history = History::default();
        let mut max_revision = 0;
        while let Some(row) = cursor.try_next().await? {
            ensure!(
                row.get_i32("schema_version")? == SCHEMA_VERSION,
                "unsupported points schema"
            );
            let point: Measurement = bson::from_document(row.clone())?;
            point.validate()?;
            ensure!(
                point.id()? == row.get_str("_id")?,
                "point identity does not match its record"
            );
            max_revision = max_revision.max(point.revision);
            history.push(point);
        }
        history.sort();
        Ok(StoredHistory {
            history,
            max_revision,
        })
    }
}
