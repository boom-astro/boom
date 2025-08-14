FROM rust:1.87-slim-bookworm AS builder

WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files
COPY ./boom/Cargo.toml ./boom/Cargo.lock ./

RUN apt-get update && \
    apt-get install -y curl gcc g++ libhdf5-dev perl make libsasl2-dev pkg-config && \
    apt-get autoremove && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy the source code
COPY ../../src ./src
COPY ../../api ./api

# Build the application (all of the binaries)
RUN cargo build --release --workspace


## Create a minimal runtime image for binaries
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy in the ML models
COPY boom/data/models /app/data/models

# Copy the built executables from the builder stage
COPY --from=builder /app/target/release/scheduler /app/scheduler
COPY --from=builder /app/target/release/kafka_consumer /app/kafka_consumer
COPY --from=builder /app/target/release/kafka_producer /app/kafka_producer

# Set the entrypoint, though this will be overridden
CMD ["/app/scheduler"]
