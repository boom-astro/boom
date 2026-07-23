//! Milvus vector database integration.
//!
//! Used to store the CIDER fusion model's embeddings so that objects can be
//! compared by similarity. The target deployment is NRP's managed Milvus, which
//! is reachable over gRPC only — see `docs/milvus.md` for setup.

pub mod client;
pub mod collection;
pub mod insert;
pub mod proto;
pub mod search;

pub use client::{MilvusClient, MilvusError};
pub use collection::{
    embedding_collection_schema, FIELD_CANDID, FIELD_EMBEDDING, FIELD_JD, FIELD_OBJECT_ID,
};
pub use insert::EmbeddingRow;
pub use search::SearchHit;

/// Whether to *also* keep the CIDER fusion embedding in the Mongo
/// `classifications` document, in addition to writing it to Milvus (which,
/// when `milvus.enabled`, always receives it).
///
/// **Beta:** `true` — dual-write so that if the NRP Milvus instance is down the
/// embedding is still safe in Mongo. Once Milvus is trusted, set this to `false`
/// to stop storing a 384-float vector on every alert; the embedding then lives
/// only in Milvus. Changing this requires a rebuild.
pub const WRITE_EMBEDDING_TO_MONGO: bool = true;
