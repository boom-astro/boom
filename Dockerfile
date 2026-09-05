ARG KAFKA_VERSION=4.3.1
ARG SCALA_VERSION=2.13

FROM rust:slim-trixie AS base

ARG KAFKA_VERSION
ARG SCALA_VERSION
# Installed here rather than only in the runtime stage so the dev image has it
# too: the task worker shells out to boompy for catalog sourcing, and it runs
# under cargo-watch in dev exactly as it does from the release binary in prod.
ARG UV_VERSION=0.10.0

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates curl bash tar xz-utils gcc g++ python3 python3-venv libhdf5-dev \
    perl make libsasl2-dev libsasl2-2 default-jre-headless pkg-config clang libclang-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz && \
    tar -xzf /tmp/kafka.tgz -C /opt && \
    ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    rm -f /tmp/kafka.tgz

RUN curl -LsSf https://astral.sh/uv/${UV_VERSION}/install.sh | \
    env UV_INSTALL_DIR=/usr/local/bin UV_UNMANAGED_INSTALL=1 sh

ENV PATH="/opt/kafka/bin:${PATH}"
ENV LIBCLANG_PATH=/usr/lib/llvm-19/lib

WORKDIR /app

FROM base AS builder

ARG ONNXRUNTIME_GPU_VERSION=1.24.4
# Compiled into the binaries and recorded on every data mutation, so the ledger
# can name the commit that produced a change. Absent when unset -- the ledger
# records that honestly rather than inventing a value.
ARG BOOM_GIT_SHA
ENV BOOM_GIT_SHA=${BOOM_GIT_SHA}

RUN python3 -m venv /opt/ort-py && \
    /opt/ort-py/bin/pip install --no-cache-dir "onnxruntime==${ONNXRUNTIME_GPU_VERSION}" && \
    true

RUN ORT_CAPI_DIR="$('/opt/ort-py/bin/python' -c 'import pathlib, onnxruntime as ort; print(pathlib.Path(ort.__file__).resolve().parent / "capi")')" && \
    mkdir -p /opt/ort && \
    cp "${ORT_CAPI_DIR}/libonnxruntime.so.${ONNXRUNTIME_GPU_VERSION}" /opt/ort/ && \
    cp "${ORT_CAPI_DIR}/libonnxruntime_providers_shared.so" /opt/ort/ && \
    rm -rf /opt/ort-py && ln -sf /opt/ort/libonnxruntime.so.${ONNXRUNTIME_GPU_VERSION} /opt/ort/libonnxruntime.so

COPY apache-avro-macros /app/apache-avro-macros
COPY Cargo.toml Cargo.lock /app/
COPY ./src /app/src

# BuildKit cache mounts keep the compiled crates (target/) and the fetched
# dependencies (cargo registry/git) between builds, so only the crates that
# actually changed are recompiled -- unlike a plain image layer, which is
# all-or-nothing and rebuilds the whole workspace on any source change.
#
# A cache mount is NOT part of the image, so the release binaries are copied out
# to /app/bin within the same RUN for the runtime stage to pick up. On CI the
# mounts are persisted across runs by buildkit-cache-dance (see build.yaml);
# locally they persist on the BuildKit builder.
RUN --mount=type=cache,target=/app/target,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release && \
    mkdir -p /app/bin && \
    cp target/release/scheduler \
       target/release/kafka_consumer \
       target/release/kafka_producer \
       target/release/api \
       target/release/migrate_fp_flux \
       target/release/migrate_snr \
       target/release/reprocess_crossmatch \
       target/release/prepare_catalog \
       target/release/copy_cutouts \
       target/release/stream_kowalski_alerts \
       target/release/enrich_reprocess \
       target/release/mpcorb_ingest \
       target/release/task_worker \
       /app/bin/

FROM builder AS dev

RUN cargo install --locked cargo-watch

CMD ["cargo", "watch", "-x", "run --bin api"]

FROM debian:trixie-slim AS app

ARG KAFKA_VERSION=4.3.1
ARG SCALA_VERSION=2.13

ARG UV_VERSION=0.10.0

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates curl bash libsasl2-2 default-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# boompy fetches archival catalogs -- see boompy/README.md. uv manages both the
# interpreter and the dependencies, so there is no system Python to keep in step
# with the lockfile.
RUN curl -LsSf https://astral.sh/uv/${UV_VERSION}/install.sh | \
    env UV_INSTALL_DIR=/usr/local/bin UV_UNMANAGED_INSTALL=1 sh

ENV ORT_DYLIB_PATH=/opt/ort/libonnxruntime.so
ENV LD_LIBRARY_PATH=/opt/ort

COPY --from=builder /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka
ENV PATH="/opt/kafka/bin:${PATH}"

WORKDIR /app

COPY --from=builder /app/bin/scheduler /app/scheduler
COPY --from=builder /app/bin/kafka_consumer /app/kafka_consumer
COPY --from=builder /app/bin/kafka_producer /app/kafka_producer
COPY --from=builder /app/bin/api /app/boom-api
COPY --from=builder /app/bin/migrate_fp_flux /app/migrate_fp_flux
COPY --from=builder /app/bin/migrate_snr /app/migrate_snr
COPY --from=builder /app/bin/reprocess_crossmatch /app/reprocess_crossmatch
COPY --from=builder /app/bin/prepare_catalog /app/prepare_catalog
COPY --from=builder /app/bin/mpcorb_ingest /app/mpcorb_ingest
COPY --from=builder /app/bin/task_worker /app/task_worker
COPY --from=builder /opt/ort /opt/ort

# Resolved at build time from the committed lockfile, so a catalog ingest does
# not depend on PyPI being reachable -- or on resolving to different versions
# than the ones the tests ran against.
COPY boompy /app/boompy
ENV UV_PROJECT_ENVIRONMENT=/app/boompy/.venv
ENV BOOM_BOOMPY_PATH=/app/boompy
RUN uv sync --project /app/boompy --frozen --no-dev
# Temporary
COPY --from=builder /app/bin/copy_cutouts /app/copy_cutouts
COPY --from=builder /app/bin/stream_kowalski_alerts /app/stream_kowalski_alerts
COPY --from=builder /app/bin/enrich_reprocess /app/enrich_reprocess

CMD ["/app/scheduler"]
