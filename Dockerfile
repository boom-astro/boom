FROM debian:trixie AS builder

# CPU builder profile.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates curl gcc g++ libhdf5-dev libclang-dev perl make libsasl2-dev pkg-config bash && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.93.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /app

# Build dependencies first to maximize layer cache reuse.
COPY Cargo.toml Cargo.lock /app/
COPY apache-avro-macros /app/apache-avro-macros
RUN mkdir -p /app/src && \
    printf '%s\n' 'fn main() {}' > /app/src/main.rs && \
    cargo build --release && \
    rm -rf /app/src

COPY ./src /app/src
RUN cargo build --release

FROM nvidia/cuda:12.8.1-cudnn-devel-ubuntu24.04 AS builder-gpu

# GPU builder profile.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates curl gcc g++ libhdf5-dev libclang-dev perl make libsasl2-dev pkg-config bash && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.93.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /app

COPY Cargo.toml Cargo.lock /app/
COPY apache-avro-macros /app/apache-avro-macros
RUN mkdir -p /app/src && \
    printf '%s\n' 'fn main() {}' > /app/src/main.rs && \
    cargo build --release && \
    rm -rf /app/src

COPY ./src /app/src
RUN cargo build --release


## Dev target for fast rebuilds in container
FROM builder AS dev

# Install runtime deps + Kafka CLI
ARG KAFKA_VERSION=4.1.1
ARG SCALA_VERSION=2.13
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates default-jre-headless curl bash tar \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

RUN cargo install --locked cargo-watch

WORKDIR /app

CMD ["cargo", "watch", "-x", "run --bin api"]


## Runtime profile for CPU images
FROM debian:trixie-slim AS runtime

WORKDIR /app

# Install runtime deps + Kafka CLI
ARG KAFKA_VERSION=4.1.1
ARG SCALA_VERSION=2.13
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates default-jre-headless curl bash tar \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

## Runtime profile for GPU images
FROM nvidia/cuda:12.8.1-cudnn-runtime-ubuntu24.04 AS runtime-gpu

WORKDIR /app

ARG KAFKA_VERSION=4.1.1
ARG SCALA_VERSION=2.13
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates default-jre-headless curl bash tar \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

## Final GPU application image
FROM runtime-gpu AS app-gpu

COPY --from=builder-gpu /app/target/release/scheduler /app/scheduler
COPY --from=builder-gpu /app/target/release/kafka_consumer /app/kafka_consumer
COPY --from=builder-gpu /app/target/release/kafka_producer /app/kafka_producer
COPY --from=builder-gpu /app/target/release/api /app/boom-api
COPY --from=builder-gpu /app/target/release/migrate_fp_flux /app/migrate_fp_flux
COPY --from=builder-gpu /app/target/release/migrate_snr /app/migrate_snr

# Copy ONNX Runtime shared libraries (bundled by the ort crate during build)
COPY --from=builder-gpu /app/target/release/libonnxruntime*.so* /usr/lib/x86_64-linux-gnu/
RUN ldconfig

CMD ["/app/scheduler"]

## Final CPU application image (default target)
FROM runtime AS app

# Copy the built executables from the builder stage
COPY --from=builder /app/target/release/scheduler /app/scheduler
COPY --from=builder /app/target/release/kafka_consumer /app/kafka_consumer
COPY --from=builder /app/target/release/kafka_producer /app/kafka_producer
COPY --from=builder /app/target/release/api /app/boom-api
COPY --from=builder /app/target/release/migrate_fp_flux /app/migrate_fp_flux
COPY --from=builder /app/target/release/migrate_snr /app/migrate_snr

# Copy ONNX Runtime shared libraries (bundled by the ort crate during build)
COPY --from=builder /app/target/release/libonnxruntime*.so* /usr/lib/x86_64-linux-gnu/
RUN ldconfig

# Set the entrypoint, though this will be overridden
CMD ["/app/scheduler"]
