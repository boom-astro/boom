ARG ENABLE_GPU=false

FROM rust:1.93.1-slim-trixie AS builder

ARG ENABLE_GPU

RUN apt-get update && \
    apt-get install -y curl gcc g++ libhdf5-dev libclang-dev perl make libsasl2-dev pkg-config && \
    apt-get autoremove && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Conditionally install CUDA toolkit for GPU support
RUN if [ "$ENABLE_GPU" = "true" ]; then \
    apt-get update && \
    apt-get install -y --no-install-recommends wget && \
    wget https://developer.download.nvidia.com/compute/cuda/repos/debian12/x86_64/cuda-keyring_1.1-1_all.deb && \
    dpkg -i cuda-keyring_1.1-1_all.deb && \
    rm cuda-keyring_1.1-1_all.deb && \
    apt-get update && \
    apt-get install -y --no-install-recommends cuda-toolkit-12-8 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*; \
    fi

# First we build an empty rust project to cache dependencies
# this way we skip dependencies build when only the source code changes
RUN cargo init app
COPY Cargo.toml Cargo.lock /app/
COPY apache-avro-macros /app/apache-avro-macros
# Now we build only the dependencies
RUN cd app && cargo build --release && \
    rm -rf app/src

# Now we copy the source code and build the actual application
WORKDIR /app
COPY ./src ./src

# Build the application
RUN cargo build --release


## Dev target for fast rebuilds in container
FROM builder AS dev

# Install runtime deps + Kafka CLI
ARG KAFKA_VERSION=4.1.1
ARG SCALA_VERSION=2.13
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates openjdk-25-jre-headless curl bash tar \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

RUN cargo install --locked cargo-watch

WORKDIR /app

CMD ["cargo", "watch", "-x", "run --bin api"]


## Create a minimal runtime image for binaries
FROM debian:trixie-slim

ARG ENABLE_GPU

WORKDIR /app

# Conditionally install CUDA runtime libraries for GPU support
RUN if [ "$ENABLE_GPU" = "true" ]; then \
    apt-get update && \
    apt-get install -y --no-install-recommends wget && \
    wget https://developer.download.nvidia.com/compute/cuda/repos/debian12/x86_64/cuda-keyring_1.1-1_all.deb && \
    dpkg -i cuda-keyring_1.1-1_all.deb && \
    rm cuda-keyring_1.1-1_all.deb && \
    apt-get update && \
    apt-get install -y --no-install-recommends cuda-libraries-12-8 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*; \
    fi

# Install runtime deps + Kafka CLI
ARG KAFKA_VERSION=4.1.1
ARG SCALA_VERSION=2.13
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates openjdk-25-jre-headless curl bash tar \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

# Copy the built executables from the builder stage
COPY --from=builder /app/target/release/scheduler /app/scheduler
COPY --from=builder /app/target/release/kafka_consumer /app/kafka_consumer
COPY --from=builder /app/target/release/kafka_producer /app/kafka_producer
COPY --from=builder /app/target/release/api /app/boom-api
COPY --from=builder /app/target/release/migrate_fp_flux /app/migrate_fp_flux
COPY --from=builder /app/target/release/migrate_snr /app/migrate_snr

# Set the entrypoint, though this will be overridden
CMD ["/app/scheduler"]
