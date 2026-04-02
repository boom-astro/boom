FROM rust:1.93.1-slim-bookworm AS builder

# Build arg to toggle GPU support (installs CUDA/cuDNN runtime libs)
ARG USE_GPU=true

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl gcc g++ libhdf5-dev libclang-dev perl \
      make libsasl2-dev pkg-config xz-utils && \
    apt-get autoremove && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install CUDA toolkit (nvcc), runtime libs (cudart, cublas, cufft), and cuDNN
ENV CUDA_DIR=/usr/local/cuda
RUN if [ "$USE_GPU" = "true" ]; then \
      CUDA_URL=https://developer.download.nvidia.com/compute/cuda/redist && \
      CUDNN_URL=https://developer.download.nvidia.com/compute/cudnn/redist && \
      mkdir -p $CUDA_DIR && \
      for pkg in \
        ${CUDA_URL}/cuda_nvcc/linux-x86_64/cuda_nvcc-linux-x86_64-12.8.93-archive.tar.xz \
        ${CUDA_URL}/cuda_cudart/linux-x86_64/cuda_cudart-linux-x86_64-12.8.90-archive.tar.xz \
        ${CUDA_URL}/libcublas/linux-x86_64/libcublas-linux-x86_64-12.8.3.14-archive.tar.xz \
        ${CUDA_URL}/libcufft/linux-x86_64/libcufft-linux-x86_64-11.3.3.83-archive.tar.xz \
        ${CUDNN_URL}/cudnn/linux-x86_64/cudnn-linux-x86_64-9.20.0.48_cuda12-archive.tar.xz \
      ; do curl -fsSL "$pkg" | tar -xJf - --strip-components=1 -C $CUDA_DIR; done && \
      ldconfig; \
    fi

ENV PATH="/usr/local/cuda/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/cuda/lib"
ENV LIBRARY_PATH="/usr/local/cuda/lib"

# Copy source and build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY apache-avro-macros ./apache-avro-macros
COPY ./src ./src
RUN if [ "$USE_GPU" = "true" ]; then \
      cargo build --release; \
    else \
      cargo build --release --no-default-features; \
    fi


## Dev target for fast rebuilds in container
FROM builder AS dev

ARG KAFKA_VERSION=4.1.1
ARG SCALA_VERSION=2.13
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates openjdk-17-jre-headless curl bash tar \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

RUN cargo install --locked cargo-watch

WORKDIR /app

CMD ["cargo", "watch", "-x", "run --bin api"]


## Minimal runtime image
FROM debian:bookworm-slim

ARG USE_GPU=true

WORKDIR /app

# Install runtime deps + Kafka CLI
ARG KAFKA_VERSION=4.1.1
ARG SCALA_VERSION=2.13
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 ca-certificates openjdk-17-jre-headless curl bash tar xz-utils \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
    && rm -f /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"

# Install CUDA/cuDNN runtime libraries only (.so files, no toolkit)
ENV CUDA_DIR=/usr/local/cuda/lib
RUN if [ "$USE_GPU" = "true" ]; then \
      CUDA_URL=https://developer.download.nvidia.com/compute/cuda/redist && \
      CUDNN_URL=https://developer.download.nvidia.com/compute/cudnn/redist && \
      mkdir -p $CUDA_DIR && \
      for pkg in \
        ${CUDA_URL}/cuda_cudart/linux-x86_64/cuda_cudart-linux-x86_64-12.8.90-archive.tar.xz \
        ${CUDA_URL}/libcublas/linux-x86_64/libcublas-linux-x86_64-12.8.3.14-archive.tar.xz \
        ${CUDA_URL}/libcufft/linux-x86_64/libcufft-linux-x86_64-11.3.3.83-archive.tar.xz \
        ${CUDNN_URL}/cudnn/linux-x86_64/cudnn-linux-x86_64-9.20.0.48_cuda12-archive.tar.xz \
      ; do curl -fsSL "$pkg" | tar -xJf - --strip-components=2 -C $CUDA_DIR --wildcards '*/lib/*.so*'; done && \
      ldconfig; \
    fi

ENV LD_LIBRARY_PATH="/usr/local/cuda/lib"

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

CMD ["/app/scheduler"]
