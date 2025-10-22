FROM rust:1.87-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y curl gcc g++ libhdf5-dev perl make libsasl2-dev pkg-config && \
    apt-get autoremove && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# First we build an empty rust project to cache dependencies
# this way we skip dependencies build when only the source code changes
RUN cargo init app && cd app && cargo init api
COPY Cargo.toml Cargo.lock /app/
COPY api/Cargo.toml /app/api/
RUN cd app && cargo build --release --workspace && \
    rm -rf app/src && rm -rf app/api/src

# Now we copy the source code and build the actual application
WORKDIR /app
COPY ./src ./src
COPY ./api/src ./api/src

# Build the application (all of the binaries)
RUN cargo build --release --workspace


## Create a minimal runtime image for binaries
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy the built executables from the builder stage
COPY --from=builder /app/target/release/scheduler /app/scheduler
COPY --from=builder /app/target/release/kafka_consumer /app/kafka_consumer
COPY --from=builder /app/target/release/kafka_producer /app/kafka_producer
COPY --from=builder /app/target/release/boom-api /app/boom-api

# Set the entrypoint, though this will be overridden
CMD ["/app/scheduler"]
