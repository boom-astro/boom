FROM rust:1.87-slim-bookworm AS builder

WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

RUN apt-get update && \
    apt-get install -y curl gcc g++ libhdf5-dev perl make libsasl2-dev && \
    apt-get autoremove && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy the source code
COPY src ./src
COPY api ./api

# Build the application (all of the binaries)
RUN cargo build --release --workspace


## Create a minimal runtime image for API
FROM debian:bookworm-slim AS api

WORKDIR /app

# Copy the built executable from the builder stage
COPY --from=builder /app/target/release/boom-api /app/boom-api

# Expose the port your application listens on
EXPOSE 4000

# Set the entrypoint
CMD ["/app/boom-api"]


## Create a minimal runtime image for scheduler
FROM debian:bookworm-slim AS scheduler

WORKDIR /app

# Copy the built executable from the builder stage
COPY --from=builder /app/target/release/scheduler /app/scheduler

# Set the entrypoint
CMD ["/app/scheduler"]
