//! Rust bindings generated from the protos vendored in `proto/milvus`.
//!
//! The module nesting here has to mirror the protobuf package structure
//! (`milvus.proto.*`), because the generated code refers to its siblings as
//! `super::common`, `super::schema`, and so on.

#![allow(clippy::all)]

pub mod common {
    include!(concat!(env!("OUT_DIR"), "/milvus.proto.common.rs"));
}

pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/milvus.proto.schema.rs"));
}

pub mod rg {
    include!(concat!(env!("OUT_DIR"), "/milvus.proto.rg.rs"));
}

pub mod feder {
    include!(concat!(env!("OUT_DIR"), "/milvus.proto.feder.rs"));
}

pub mod msg {
    include!(concat!(env!("OUT_DIR"), "/milvus.proto.msg.rs"));
}

pub mod milvus {
    include!(concat!(env!("OUT_DIR"), "/milvus.proto.milvus.rs"));
}
