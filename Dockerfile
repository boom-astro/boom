ARG KAFKA_VERSION=4.1.1
ARG SCALA_VERSION=2.13

FROM rust:slim-trixie AS base

ARG KAFKA_VERSION
ARG SCALA_VERSION

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates curl bash tar xz-utils gcc g++ python3 python3-venv libhdf5-dev libclang-dev perl make libsasl2-dev libsasl2-2 default-jre-headless pkg-config && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz && \
    tar -xzf /tmp/kafka.tgz -C /opt && \
    ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

WORKDIR /app

FROM base AS builder

ARG ONNXRUNTIME_GPU_VERSION=1.24.4

RUN python3 -m venv /opt/ort-py && \
    /opt/ort-py/bin/pip install --no-cache-dir "onnxruntime==${ONNXRUNTIME_GPU_VERSION}" && \
    true

RUN ORT_CAPI_DIR="$('/opt/ort-py/bin/python' -c 'import pathlib, onnxruntime as ort; print(pathlib.Path(ort.__file__).resolve().parent / "capi")')" && \
    mkdir -p /opt/ort && \
    cp "${ORT_CAPI_DIR}/libonnxruntime.so.1.24.4" /opt/ort/ && \
    cp "${ORT_CAPI_DIR}/libonnxruntime_providers_shared.so" /opt/ort/ && \
    rm -rf /opt/ort-py && ln -sf /opt/ort/libonnxruntime.so.1.24.4 /opt/ort/libonnxruntime.so

COPY apache-avro-macros /app/apache-avro-macros
COPY Cargo.toml Cargo.lock /app/

RUN mkdir -p /app/src && \
    printf '%s\n' 'fn main() {}' > /app/src/main.rs && \
    cargo build --release && rm -rf /app/src

COPY ./src /app/src
RUN cargo build --release

FROM builder AS dev

RUN cargo install --locked cargo-watch

CMD ["cargo", "watch", "-x", "run --bin api"]

FROM base AS app

ENV ORT_DYLIB_PATH=/opt/ort/libonnxruntime.so

COPY --from=builder /app/target/release/scheduler /app/scheduler
COPY --from=builder /app/target/release/kafka_consumer /app/kafka_consumer
COPY --from=builder /app/target/release/kafka_producer /app/kafka_producer
COPY --from=builder /app/target/release/api /app/boom-api
COPY --from=builder /app/target/release/migrate_fp_flux /app/migrate_fp_flux
COPY --from=builder /app/target/release/migrate_snr /app/migrate_snr

RUN ldconfig

CMD ["/app/scheduler"]