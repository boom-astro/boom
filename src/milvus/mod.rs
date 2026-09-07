//! Milvus vector database integration.
//!
//! Used to store the CIDER fusion model's embeddings so that objects can be
//! compared by similarity. The target deployment is NRP's managed Milvus, which
//! is reachable over gRPC only — see `docs/milvus.md` for setup.

pub mod client;
pub mod collection;
pub mod insert;
pub mod proto;

pub use client::{MilvusClient, MilvusError};
pub use collection::{
    embedding_collection_schema, FIELD_CANDID, FIELD_EMBEDDING, FIELD_JD, FIELD_OBJECT_ID,
};
pub use insert::EmbeddingRow;
