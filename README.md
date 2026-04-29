<div id="toc" align="center">
  <ul>
    <summary>
      <h1>
        <img src="docs/boom_logo.png" alt="BOOM logo" width="160">
        <br/>
        BOOM
      </h1>
    </summary>
  </ul>
  <em>Burst & Outburst Observations Monitor</em>
</div>

## Description

BOOM is an alert broker. What sets it apart from other alert brokers is that it is written to be modular, scalable, and performant. Essentially, the pipeline is composed of multiple types of workers, each with a specific task:

1. The `Kafka` consumer(s), reading alerts from astronomical surveys' `Kafka` topics to transfer them to `Redis`/`Valkey` in-memory queues.
2. The Alert Ingestion workers, reading alerts from the `Redis`/`Valkey` queues, responsible of formatting them to BSON documents, and enriching them with crossmatches from archival astronomical catalogs and other surveys before writing the formatted alert packets to a `MongoDB` database.
3. The enrichment workers, running alerts through a series of enrichment classifiers, and writing the results back to the `MongoDB` database.
4. The Filter workers, running user-defined filters on the alerts, and sending the results to Kafka topics for other services to consume.

Workers are managed by a Scheduler that can spawn or kill workers of each type.
Currently, the number of workers is static, but we are working on dynamically scaling the number of workers based on the load of the system.

BOOM also comes with an HTTP API, under development, which will allow users to query the `MongoDB` database, to define their own filters, and to have those filters run on alerts in real-time.

## System Requirements

BOOM runs on macOS and Linux. You'll need:

- `Docker` and `docker compose`: used to run the database, cache/task queue, and `Kafka`;
- `Rust` (a systems programming language) `>= 1.55.0`;
- `tar`: used to extract archived alerts for testing purposes.
- `libssl`, `libsasl2`: required for some Rust crates that depend on native libraries for secure connections and authentication.
- On Linux, you **need** to set `ORT_DYLIB_PATH` to a local ONNX Runtime shared library before running BOOM (for both CPU-only and GPU builds). See the [Linux ONNX Runtime setup](#linux-onnx-runtime-setup) section below for details.

**Note:** On Linux, BOOM will fail to start with a clear error if `ORT_DYLIB_PATH` is not set. This is a hard requirement due to ONNX Runtime's dynamic loading behavior. The process will not run without it.

### Installation steps

#### macOS

- Docker: On macOS we recommend using [Docker Desktop](https://www.docker.com/products/docker-desktop) to install docker. You can download it from the website, and follow the installation instructions. The website will ask you to "choose a plan", but really you just need to create an account and stick with the free tier that offers all of the features you will ever need. Once installed, you can verify the installation by running `docker --version` in your terminal, and `docker compose version` to check that docker compose is installed as well.
- Rust: You can either use [rustup](https://www.rust-lang.org/tools/install) to install Rust, or you can use [Homebrew](https://brew.sh/) to install it. If you choose the latter, you can run `brew install rust` in your terminal. We recommend using rustup, as it allows you to easily switch between different versions of Rust, and to keep your Rust installation up to date. Once installed, you can verify the installation by running `rustc --version` in your terminal. You also want to make sure that cargo is installed, which is the Rust package manager. You can verify this by running `cargo --version` in your terminal.
- System packages are essential for compiling and linking some Rust crates. All those used by BOOM should come with macOS by default, but if you get any errors when compiling it you can try to install them again with Homebrew: `brew install openssl@3 cyrus-sasl gnu-tar`.

#### Linux

- Docker: You can either install Docker Desktop (same instructions as for macOS), or you can just install Docker Engine. The latter is more lightweight. You can follow the [official installation instructions](https://docs.docker.com/engine/install/) for your specific Linux distribution. If you only installed Docker Engine, you'll want to also install [docker compose](https://docs.docker.com/compose/install/). Once installed, you can verify the installation by running `docker --version` in your terminal, and `docker compose version` to check that docker compose is installed as well.
- Rust: You can use [rustup](https://www.rust-lang.org/tools/install) to install Rust. Once installed, you can verify the installation by running `rustc --version` in your terminal. You also want to make sure that cargo is installed, which is the Rust package manager. You can verify this by running `cargo --version` in your terminal.
- `wget` and `tar`: Most Linux distributions come with `wget` and `tar` pre-installed. If not, you can install them with your package manager.
- System packages are essential for compiling and linking some Rust crates. On linux, you can install them with your package manager. For example with `apt` on Ubuntu or Debian-based systems, you can run:

  ```bash
  sudo apt update
  sudo apt install build-essential pkg-config libssl-dev libsasl2-dev -y
  ```

- If you want to use GPU hardware acceleration for enrichment, you need to have the appropriate NVIDIA drivers installed, along with CUDA and cuDNN. See the [GPU inference](#gpu-inference-linux) subsection below for more details.

## Setup

### Environment configuration

BOOM uses environment variables for sensitive configuration like passwords
and API keys.
For local development, you can use the defaults in `.env.example`
by copying it to `.env`:

```sh
cp .env.example .env
```

**Note:** Do not commit `.env` to Git or use the example values
in production.

#### Email configuration (for notifications)

In order to send emails to users, e.g.,
to send Babamul account activation codes,
the email related environmental variables in `.env.example` must be set.

If email is not configured or disabled,
Babamul activation codes will be printed to the console logs instead,
and users will need to contact an administrator to retrieve their activation
code.

### Cutout storage

BOOM supports three backends for storing alert cutout images. Choose one before
starting the stack, then use the corresponding `make` target.

#### Option 1 — Shared MongoDB (simplest, default)

Cutouts are stored as a `{survey}_alerts_cutouts` collection (e.g.
`ztf_alerts_cutouts`) inside the same `boom` database and MongoDB instance as
the alerts. No extra container or credentials needed.

```bash
make dev
```

This is the default and requires no configuration beyond the base `.env`. If
you want to isolate cutouts into a separate database on the same instance, set
`cutouts_storage.name` in `config.yaml` (or `BOOM_CUTOUTS_STORAGE__NAME`) to a
different value (e.g. `boom_cutouts`).

#### Option 2 — Dedicated MongoDB container

Cutouts are stored in a separate `mongo-cutouts` container. This lets you put
the cutout data on a different disk or volume by setting
`BOOM_CUTOUTS_MONGO_VOLUME` in your `.env`:

```env
BOOM_CUTOUTS_MONGO_VOLUME=/mnt/large-disk/boom_cutouts
```

```bash
make dev-mongo
```

`BOOM_CUTOUTS_STORAGE__PASSWORD` must be set in `.env` (it defaults to
`BOOM_DATABASE__PASSWORD` in `.env.example`, which is fine for local dev).
The compose overlay sets `BOOM_CUTOUTS_STORAGE__HOST=mongo-cutouts` automatically —
no `config.yaml` edits are needed for Docker deployments.

In production:

```bash
docker compose -f docker-compose.yaml -f docker-compose.cutouts-mongo.yaml up
```

#### Option 3 — S3-compatible object storage (rustfs)

Cutouts are stored in a rustfs (S3-compatible) bucket. Best for high-throughput
workloads or when you want cutout storage decoupled from MongoDB entirely.

This mode also spins up a **dedicated `valkey-cutouts` container** used as a
read-through cache for cutouts. It is capped at a fixed memory budget
(`BOOM_CUTOUTS_CACHE_MAXMEMORY`, default `1024mb`) and uses the `volatile-ttl`
eviction policy: when full, keys closest to expiry (shortest remaining TTL) are
evicted first, so the cache degrades gracefully without ever blocking writes.

Required env vars (present in `.env.example`):

- `BOOM_CUTOUTS_STORAGE__ACCESS_KEY`
- `BOOM_CUTOUTS_STORAGE__SECRET_KEY`

```bash
make dev-s3
```

The compose overlay injects all infrastructure-determined settings as environment
variables (`BOOM_CUTOUTS_STORAGE__TYPE=s3`, endpoint URL, region, credentials
provider, cache host/port), so **`config.yaml` does not need to be edited for
Docker deployments** — only the two credentials above are required.

For non-Docker deployments (running the binaries directly), set
`type: s3` in the `cutouts_storage` block of `config.yaml` (or
`BOOM_CUTOUTS_STORAGE__TYPE=s3`), then supply credentials and the cache
host. The S3 fields are already present in `config.yaml` with sensible
defaults — only these vars need to be set:

```bash
BOOM_CUTOUTS_STORAGE__TYPE=s3
BOOM_CUTOUTS_STORAGE__ACCESS_KEY=<your-key>
BOOM_CUTOUTS_STORAGE__SECRET_KEY=<your-secret>
BOOM_CUTOUTS_STORAGE__CACHE__HOST=<valkey-host>
```

In production:

```bash
# Optionally pin data and cache sizes via env vars
BOOM_OBJECT_STORAGE_VOLUME=/mnt/fast-disk/cutouts \
BOOM_CUTOUTS_CACHE_MAXMEMORY=2gb \
  docker compose -f docker-compose.yaml -f docker-compose.cutouts-s3.yaml up
```

For local access to the rustfs web UI on port 9000/9001, add a `ports` override
to your local `docker-compose.override.yaml`:

```yaml
services:
  rustfs:
    ports:
      - "9000:9000"
      - "9001:9001"
```

### Linux only

#### ONNX runtime setup {#linux-onnx-runtime-setup}

On Linux, BOOM links to the ONNX Runtime shared library at process start via `ORT_DYLIB_PATH`. This is required regardless of whether you use GPU inference or not. You must set this variable before running any BOOM binary natively.

The easiest way is to install the Python wheel and point to the bundled `.so` file. We recommend [uv](https://docs.astral.sh/uv/getting-started/installation/):

**CPU-only:**

```bash
uv venv --python 3.13 .venv
source .venv/bin/activate
uv pip install "onnxruntime>=1.24,<1.25"
export ORT_DYLIB_PATH="$PWD/.venv/lib/python3.13/site-packages/onnxruntime/capi/libonnxruntime.so.1.24.4"
```

Adjust the version number (`1.24.4`) to match the file actually present in `.venv`:

```bash
ls .venv/lib/python3.13/site-packages/onnxruntime/capi/libonnxruntime.so.*
```

You must export `ORT_DYLIB_PATH` in each shell where you run BOOM natively on Linux, or add it once to your shell's configuration file (e.g., `.bashrc` or `.zshrc`) and source it.

#### GPU inference {#gpu-inference-linux}

For GPU inference on Linux you need, in addition to the above:

1. NVIDIA driver installed and working.
2. A CUDA major version compatible with your driver (we recommend CUDA 12.8).
3. cuDNN 9 for that CUDA major version.
4. At least 10 GiB (10240 MiB) of free VRAM on each configured CUDA device for ZTF enrichment.

BOOM validates this requirement at scheduler startup when running ZTF with GPU enabled, and exits early if a configured device is below the threshold.

And the GPU variant of the ONNX Runtime wheel instead of the CPU one:

```bash
uv venv --python 3.13 .venv
source .venv/bin/activate
uv pip install "onnxruntime-gpu>=1.24,<1.25"
export ORT_DYLIB_PATH="$PWD/.venv/lib/python3.13/site-packages/onnxruntime/capi/libonnxruntime.so.1.24.4"
```

Then enable GPU inference in your BOOM config:

```yaml
# config.yaml
gpu:
  enabled: true
  device_ids: [0]
```

See [docs/gpu.md](docs/gpu.md) for container-vs-native details, troubleshooting, and version notes.

### Start services for local development

Bring up the local dev stack with:

```bash
make dev
```

This starts `api`, `consumer-ztf`, and `scheduler-ztf` with `cargo watch`, plus
the supporting Docker services they need.

## Running BOOM

### Local dev pipeline (recommended)

For local development, use:

```bash
make dev
```

This brings up the hot-reloading API, ZTF consumer, and ZTF scheduler.

To produce alerts for testing, run:

```bash
make delete-produce-ztf
```

If you change the producer date or program, make sure the consumer is reading
the same topic date/program combination.

### Alert Production (not required for production use)

BOOM is meant to be run in production, reading from a real-time Kafka stream of astronomical alerts. **That said, we made it possible to process ZTF alerts from the [ZTF alerts public archive](https://ztf.uw.edu/alerts/public/).**
This is a great way to test BOOM on real data at scale, and not just using the unit tests. To start a Kafka producer, you can run the following command:

```bash
cargo run --release --bin kafka_producer <SURVEY> [DATE] [PROGRAMID]
```

_To see the list of all parameters, documentation, and examples, run the following command:

```bash
cargo run --release --bin kafka_producer -- --help
```

As an example, let's say you want to produce public ZTF alerts that were observed on `20240617` UTC. You can run the following command:

```bash
cargo run --release --bin kafka_producer ztf 20240617 public
```

You can leave that running in the background, and start the rest of the pipeline in another terminal.

*If you'd like to clear the `Kafka` topic before starting the producer, you can run the following command:*

```bash
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 --delete --topic ztf_YYYYMMDD_programid1
```

### Alert Consumption

Next, you can start the `Kafka` consumer with:

```bash
cargo run --release --bin kafka_consumer <SURVEY> [DATE] --programids [PROGRAMIDS]
```

This will start a `Kafka` consumer, which will read the alerts from a given `Kafka` topic and transfer them to `Redis`/`Valkey` in-memory queue that the processing pipeline will read from.

To continue with the previous example, you can run:

```bash
cargo run --release --bin kafka_consumer ztf 20240617 --programids public
```

### Alert Processing

Now that alerts have been queued for processing, let's start the workers that will process them. Instead of starting each worker manually, we provide the `scheduler` binary. You can run it with:

```bash
cargo run --release --bin scheduler <SURVEY> [CONFIG_PATH]
```

Where `<SURVEY>` is the name of the stream you want to process.
For example, to process ZTF alerts, you can run:

```bash
cargo run --release --bin scheduler ztf
```

The scheduler prints a variety of messages to your terminal, e.g.:

- At the start you should see a bunch of `Processed alert with candid: <alert_candid>, queueing for classification` messages, which means that the fake alert worker is picking up on the alerts, processed them, and is queueing them for classification.
- You should then see some `received alerts len: <nb_alerts>` messages, which means that the enrichment worker is processing the alerts successfully.
- You should not see anything related to the filter worker. **This is normal, as we did not define any filters yet!** The next version of the README will include instructions on how to upload a dummy filter to the system for testing purposes.
- What you should definitely see is a lot of `heart beat (MAIN)` messages, which means that the scheduler is running and managing the workers correctly.

Metrics are collected by Prometheus and visible on a Grafana dashboard.
See the [observability docs](docs/observability.md) for more information.

## Stopping BOOM

To stop BOOM, you can simply stop the `Kafka` consumer with `CTRL+C`, and then stop the scheduler with `CTRL+C` as well.
You can also stop the docker containers with:

```bash
docker compose down
```

When you stop the scheduler, it will attempt to gracefully stop all the workers by sending them interrupt signals.
This is still a work in progress, so you might see some error handling taking place in the logs.

**In the next version of the README, we'll provide the user with example scripts to read the output of BOOM (i.e. the alerts that passed the filters) from `Kafka` topics. For now, alerts are send back to `Redis`/`valkey` if they pass any filters.**

## Logging

The logging level is configured using the `RUST_LOG` environment variable, which can be set to one or more directives described in the [`tracing_subscriber` docs](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html).
The simplest directives are "trace", "debug", "info", "warn", "error", and "off", though more advanced directives can be used to set the level for particular crates.
An example of this is boom's default directive---what boom uses when `RUST_LOG` is not set---which is "info,ort=error".
This directive means boom will log at the INFO level, with events from the `ort` crate specifically limited to ERROR.

Setting `RUST_LOG` overwrites the default directive. For instance, `RUST_LOG=debug` will show all DEBUG events from all crates (including `ort`).
If you need to change the general level while keeping `ort` events limited to ERROR, then you'll have to specify that explicitly, e.g., `RUST_LOG=debug,ort=error`.
If you find the filtering on `ort` too restrictive, but you don't want to open it up to INFO, you can set `RUST_LOG=info,ort=warn`.
There's nothing special about `ort` here; directives can be used to control events from any crate.
It's just that `ort` tends to be significantly "noisier" than all of our other dependencies, so it's a useful example.

Span events can be added to the log by setting the `BOOM_SPAN_EVENTS` environment variable to one or more of the following [span lifecycle options](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/struct.Layer.html#method.with_span_events): "new", "enter", "exit", "close", "active", "full", or "none", where multiple values are separated by a comma.
For example, to see events for when spans open and close, set `BOOM_SPAN_EVENTS=new,close`.
"close" is notable because it creates events with execution time information, which may be useful for profiling.

As a more complete example, the following sets the logging level to DEBUG, with `ort` specifically set to WARN, and enables "new" and "close" span events while running the scheduler:

```bash
RUST_LOG=debug,ort=warn BOOM_SPAN_EVENTS=new,close cargo run --bin scheduler -- ztf
```

## Contributing

We welcome contributions! Please read the [CONTRIBUTING.md](CONTRIBUTING.md) file for more information.
We rely on [GitHub issues](https://github.com/boom-astro/boom/issues) to track bugs and feature requests.
