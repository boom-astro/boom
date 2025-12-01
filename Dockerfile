FROM rust:1.91.1-slim-trixie AS builder

RUN apt-get update && \
    apt-get install -y curl gcc g++ libhdf5-dev perl make libsasl2-dev pkg-config && \
    apt-get autoremove && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

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
ARG KAFKA_VERSION=3.7.1
ARG SCALA_VERSION=2.13
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates openjdk-17-jre-headless curl bash tar \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

RUN cargo install --locked cargo-watch

WORKDIR /app

CMD ["cargo", "watch", "-x", "run --bin api"]


## Create a minimal runtime image for binaries
FROM debian:trixie-slim

WORKDIR /app

# Install runtime deps + Kafka CLI
ARG KAFKA_VERSION=3.7.1
ARG SCALA_VERSION=2.13
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates openjdk-17-jre-headless curl bash tar \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

# Copy the built executables from the builder stage
COPY --from=builder /app/target/release/scheduler /app/scheduler
COPY --from=builder /app/target/release/kafka_consumer /app/kafka_consumer
COPY --from=builder /app/target/release/kafka_producer /app/kafka_producer
COPY --from=builder /app/target/release/api /app/boom-api

# Set the entrypoint, though this will be overridden
CMD ["/app/scheduler"]
