# Vendored Milvus protobuf definitions

These files are copied verbatim from
[milvus-io/milvus-proto](https://github.com/milvus-io/milvus-proto) and are
compiled into a gRPC client by `build.rs` at build time.

| | |
|---|---|
| Upstream tag | `v2.6.20` |
| Commit | `29edb7f431624d4902b98189a4f7fc8138016f36` |

## Why vendored rather than a submodule

Upstream ships these protos to consumers via a git submodule. We copy them in
instead because:

- `cargo build` and the `Dockerfile` work unchanged — neither fetches submodules.
- The exact wire contract we build against is visible in review and in `git log`.

`protoc` itself is supplied by the `protoc-bin-vendored` build dependency, so no
system package is required either.

## Which files, and why these six

`milvus.proto` is the only entry point we compile, but protoc must resolve its
full import graph:

```
milvus.proto
├── common.proto
├── schema.proto
├── rg.proto
├── feder.proto
└── msg.proto
```

`tokenizer.proto` is not reachable from `milvus.proto` and is deliberately not
vendored.

## Refreshing to a new upstream version

```bash
git clone --depth 1 --branch <tag> https://github.com/milvus-io/milvus-proto.git /tmp/milvus-proto
cp /tmp/milvus-proto/proto/{common,schema,milvus,rg,feder,msg}.proto proto/milvus/
```

Then update the tag and commit in the table above. Milvus protos are additive
within a major line, so moving forward within 2.x should not break the client —
but re-run `cargo build` and the `milvus_check` binary against a real server
before relying on it.
