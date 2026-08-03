//! Generates the Milvus gRPC client from the protos vendored in `proto/milvus`.
//!
//! The protos are pinned to milvus-io/milvus-proto tag `v2.6.20`; see
//! `proto/milvus/README.md` for how to refresh them. They are vendored rather
//! than pulled in as a git submodule so that a plain `cargo build` (and the
//! Dockerfile, which does not fetch submodules) works unchanged.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the bundled protoc so contributors and CI don't need one installed.
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    // We only ever act as a client; skip server codegen to keep build times down.
    //
    // `build_transport(false)` suppresses tonic's generated `connect()`
    // constructor. Milvus declares its own `Connect` RPC, so generating both
    // puts two inherent `connect` methods on the client and fails to compile.
    // We construct the Channel ourselves in src/milvus/client.rs anyway.
    tonic_prost_build::configure()
        .build_server(false)
        .build_transport(false)
        .compile_protos(&["proto/milvus/milvus.proto"], &["proto/milvus"])?;

    println!("cargo:rerun-if-changed=proto/milvus");

    Ok(())
}
