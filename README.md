# BOOM (Burst & Outburst Observations Monitor)

## Description

BOOM is an alert broker. What sets it apart from other alert brokers is that it is written to be modular, scalable, and performant. Essentially, the pipeline is composed of multiple types of workers, each with a specific task:
1. The `Kafka` consumer(s), reading alerts from astronomical surveys' `Kafka` topics to transfer them to `Redis`/`Valkey` in-memory queues.
2. The Alert Ingestion workers, reading alerts from the `Redis`/`Valkey` queues, responsible of formatting them to BSON documents, and enriching them with crossmatches from archival astronomical catalogs and other surveys before writing the formatted alert packets to a `MongoDB` database.
3. The ML workers, running alerts through a series of ML classifiers, and writing the results back to the `MongoDB` database.
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
- If you're on Windows, you must use WSL2 (Windows Subsystem for Linux) and install a Linux distribution like Ubuntu 24.04.

### Installation steps:

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

## Setup

1. Copy the default config file, `config.default.yaml`, to `config.yaml`:
    ```bash
    cp config.default.yaml config.yaml
    ```
2. Same for the `docker-compose.yaml` file:
    ```bash
    cp docker-compose.default.yaml docker-compose.yaml
    ```
3. Launch `Valkey`, `MongoDB`, and `Kafka` using docker, using the provided `docker compose.yaml` file:
    ```bash
    docker compose up -d
    ```
    This may take a couple of minutes the first time you run it, as it needs to download the docker image for each service.
    *To check if the containers are running and healthy, run `docker ps`.*
4. Last but not least, build the Rust binaries. You can do this with or without the `--release` flag, but we recommend using it for better performance:
    ```bash
    cargo build --release
    ```

### API

To run the API server in development mode,
first ensure `cargo-watch` is installed (`cargo install cargo-watch`),
then call:

```sh
make api-dev
```

## Running BOOM:

### Alert Production (not required for production use)

BOOM is meant to be run in production, reading from a real-time Kafka stream of astronomical alerts. **That said, we made it possible to process ZTF alerts from the [ZTF alerts public archive](https://ztf.uw.edu/alerts/public/).**
This is a great way to test BOOM on real data at scale, and not just using the unit tests. To start a Kafka producer, you can run the following command:
```bash
cargo run --release --bin kafka_producer <SURVEY> [DATE] [PROGRAMID]
```

_To see the list of all parameters, documentation, and examples, run the following command:_
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
cargo run --release --bin kafka_consumer <SURVEY> [DATE] [PROGRAMID]
```

This will start a `Kafka` consumer, which will read the alerts from a given `Kafka` topic and transfer them to `Redis`/`Valkey` in-memory queue that the processing pipeline will read from.

To continue with the previous example, you can run:
```bash
cargo run --release --bin kafka_consumer ztf 20240617 public
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
- You should then see some `ML WORKER <worker_id>: received alerts len: <nb_alerts>` messages, which means that the ML worker is processing the alerts successfully.
- You should not see anything related to the filter worker. **This is normal, as we did not define any filters yet!** The next version of the README will include instructions on how to upload a dummy filter to the system for testing purposes.
- What you should definitely see is a lot of `heart beat (MAIN)` messages, which means that the scheduler is running and managing the workers correctly.

## Stopping BOOM:

To stop BOOM, you can simply stop the `Kafka` consumer with `CTRL+C`, and then stop the scheduler with `CTRL+C` as well.
You can also stop the docker containers with:
```bash
docker compose down
```

When you stop the scheduler, it will attempt to gracefully stop all the workers by sending them interrupt signals.
This is still a work in progress, so you might see some error handling taking place in the logs.

**In the next version of the README, we'll provide the user with example scripts to read the output of BOOM (i.e. the alerts that passed the filters) from `Kafka` topics. For now, alerts are send back to `Redis`/`valkey` if they pass any filters.**

## Logging

The logging level is controlled with the `RUST_LOG` environment variable, which can be set to either "trace", "debug", "info", "warn", "error", or "off". For example, to see DEBUG messages, set `RUST_LOG=debug`. The default logging level is INFO.

`RUST_LOG` supports more advanced directives described in the [`tracing_subscriber` docs](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html). A particularly useful feature is setting the level for different crates. This makes it possible to disable events from external crates like `ort` that would otherwise be mixed in with those from boom, e.g., `RUST_LOG=info,ort=off`.

Span events can be added to the log by setting the `BOOM_SPAN_EVENTS` environment variable to one or more of the following [span lifecycle options](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/struct.Layer.html#method.with_span_events): "new", "enter", "exit", "close", "active", "full", or "none", where multiple values are separated by a comma.
For example, to see events for when spans open and close, set `BOOM_SPAN_EVENTS=new,close`.
"close" is notable because it creates events with execution time information, which may be useful for profiling.

As a more complete example, the following sets the logging level to DEBUG, disables logs from the `ort` crate, and enables "new" and "close" span events while running the scheduler:

```bash
RUST_LOG=debug,ort=off BOOM_SPAN_EVENTS=new,close cargo run --bin scheduler -- ztf
```

## Contributing

We welcome contributions! Please read the [CONTRIBUTING.md](CONTRIBUTING.md) file for more information. 
We rely on [GitHub issues](https://github.com/boom-astro/boom/issues) to track bugs and feature requests.
