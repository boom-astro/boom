# Base image with Rust toolchain and Kafka CLI for building the application
ARG KAFKA_VERSION=4.1.1
ARG SCALA_VERSION=2.13

FROM rust:slim-bookworm AS base

# Install runtime deps + Kafka CLI

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates curl bash tar xz-utils gcc g++ libhdf5-dev libclang-dev perl make libsasl2-dev libsasl2-2 ca-certificates default-jre-headless pkg-config bash && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

WORKDIR /app


# Base image for GPU builds, with NVIDIA CUDA + cuDNN and Rust toolchain
FROM nvidia/cuda:12.8.1-cudnn-devel-ubuntu24.04 AS base-gpu

ARG KAFKA_VERSION
ARG SCALA_VERSION

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates curl bash tar xz-utils gcc g++ libhdf5-dev libclang-dev perl make libsasl2-dev libsasl2-2 default-jre-headless pkg-config python3 python3-pip python3-venv && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal && \
    curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz && \
    tar -xzf /tmp/kafka.tgz -C /opt && \
    ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    rm -f /tmp/kafka.tgz

ENV PATH="/root/.cargo/bin:/opt/kafka/bin:/usr/local/cuda/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/cuda/lib64:/usr/local/cuda/targets/x86_64-linux/lib:/usr/local/cuda/lib"
ENV LIBRARY_PATH="/usr/local/cuda/lib64:/usr/local/cuda/targets/x86_64-linux/lib:/usr/local/cuda/lib"

WORKDIR /app


# Build stage for CPU target
FROM base AS builder

# Build dependencies first to maximize layer cache reuse.
COPY Cargo.toml Cargo.lock /app/
COPY apache-avro-macros /app/apache-avro-macros
RUN mkdir -p /app/src && \
    printf '%s\n' 'fn main() {}' > /app/src/main.rs && \
    cargo build --release && \
    rm -rf /app/src

COPY ./src /app/src
RUN cargo build --release


# Build stage for GPU target
FROM base-gpu AS builder-gpu

ARG ONNXRUNTIME_GPU_VERSION=1.24.4

COPY Cargo.toml Cargo.lock /app/
COPY apache-avro-macros /app/apache-avro-macros
RUN mkdir -p /app/src && \
    printf '%s\n' 'fn main() {}' > /app/src/main.rs && \
    cargo build --release && \
    rm -rf /app/src

COPY ./src /app/src
RUN cargo build --release

# Warmup requires the BTSBot ONNX model at its runtime path.
COPY data/models/btsbot-v1.0.1.onnx /app/data/models/btsbot-v1.0.1.onnx

RUN python3 -m venv /opt/ort-py && \
    /opt/ort-py/bin/pip install --no-cache-dir "onnxruntime-gpu==${ONNXRUNTIME_GPU_VERSION}" && \
    true

RUN ORT_CAPI_DIR="$('/opt/ort-py/bin/python' -c 'import pathlib, onnxruntime as ort; print(pathlib.Path(ort.__file__).resolve().parent / "capi")')" && \
    mkdir -p /opt/ort && \
    cp "${ORT_CAPI_DIR}/libonnxruntime.so.1.24.4" /opt/ort/ && \
    cp "${ORT_CAPI_DIR}/libonnxruntime_providers_cuda.so" /opt/ort/ && \
    cp "${ORT_CAPI_DIR}/libonnxruntime_providers_shared.so" /opt/ort/ && \
    cp "${ORT_CAPI_DIR}/libonnxruntime_providers_tensorrt.so" /opt/ort/ && \
    ln -sf /opt/ort/libonnxruntime.so.1.24.4 /opt/ort/libonnxruntime.so

## Dev target for fast rebuilds in container
FROM builder AS dev

RUN cargo install --locked cargo-watch

CMD ["cargo", "watch", "-x", "run --bin api"]


## Final CPU application image (default target)
FROM base AS app

# Copy the built executables from the builder stage
COPY --from=builder /app/target/release/scheduler /app/scheduler
COPY --from=builder /app/target/release/kafka_consumer /app/kafka_consumer
COPY --from=builder /app/target/release/kafka_producer /app/kafka_producer
COPY --from=builder /app/target/release/api /app/boom-api
# Migration script binaries
COPY --from=builder /app/target/release/migrate_fp_flux /app/migrate_fp_flux
COPY --from=builder /app/target/release/migrate_snr /app/migrate_snr


RUN ldconfig

CMD ["/app/scheduler"]


## Final GPU application image
FROM nvidia/cuda:12.8.1-cudnn-runtime-ubuntu24.04 AS app-gpu

ARG KAFKA_VERSION
ARG SCALA_VERSION

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl bash tar xz-utils libsasl2-2 default-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz && \
    tar -xzf /tmp/kafka.tgz -C /opt && \
    ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    rm -f /tmp/kafka.tgz

WORKDIR /app

ENV PATH="/opt/kafka/bin:/usr/local/cuda/bin:${PATH}"

ENV ORT_DYLIB_PATH=/opt/ort/libonnxruntime.so
ENV LD_LIBRARY_PATH=/opt/ort:/usr/local/cuda/lib64:/usr/local/cuda/targets/x86_64-linux/lib:/usr/local/cuda/lib:${LD_LIBRARY_PATH}

# Copy the built executables from the builder-gpu stage
COPY --from=builder-gpu /app/target/release/scheduler /app/scheduler
COPY --from=builder-gpu /app/target/release/kafka_consumer /app/kafka_consumer
COPY --from=builder-gpu /app/target/release/kafka_producer /app/kafka_producer
COPY --from=builder-gpu /app/target/release/api /app/boom-api
# Migration script binaries
COPY --from=builder-gpu /app/target/release/migrate_fp_flux /app/migrate_fp_flux
COPY --from=builder-gpu /app/target/release/migrate_snr /app/migrate_snr

# Copy the warmed ORT runtime files and the CUDA compute cache
COPY --from=builder-gpu /opt/ort /opt/ort

# Set the entrypoint, though this will be overridden
CMD ["/app/scheduler"]
