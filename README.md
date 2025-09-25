# BOOM (Burst & Outburst Observations Monitor)

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
- If you're on Windows, you must use WSL2 (Windows Subsystem for Linux) and install a Linux distribution like Ubuntu 24.04.

*Boom can also be run with `Apptainer` instead of `Docker` for Linux systems.
This is especially useful for running BOOM on HPC systems where Docker is not available.*

### Installation steps:

#### macOS

- Docker: On macOS we recommend using [Docker Desktop](https://www.docker.com/products/docker-desktop) to install docker. You can download it from the website, and follow the installation instructions. The website will ask you to "choose a plan", but really you just need to create an account and stick with the free tier that offers all of the features you will ever need. Once installed, you can verify the installation by running `docker --version` in your terminal, and `docker compose version` to check that docker compose is installed as well.
- Rust: You can either use [rustup](https://www.rust-lang.org/tools/install) to install Rust, or you can use [Homebrew](https://brew.sh/) to install it. If you choose the latter, you can run `brew install rust` in your terminal. We recommend using rustup, as it allows you to easily switch between different versions of Rust, and to keep your Rust installation up to date. Once installed, you can verify the installation by running `rustc --version` in your terminal. You also want to make sure that cargo is installed, which is the Rust package manager. You can verify this by running `cargo --version` in your terminal.
- System packages are essential for compiling and linking some Rust crates. All those used by BOOM should come with macOS by default, but if you get any errors when compiling it you can try to install them again with Homebrew: `brew install openssl@3 cyrus-sasl gnu-tar`.

*Apptainer is not supported on macOS.*
#### Linux

- Docker: You can either install Docker Desktop (same instructions as for macOS), or you can just install Docker Engine. The latter is more lightweight. You can follow the [official installation instructions](https://docs.docker.com/engine/install/) for your specific Linux distribution. If you only installed Docker Engine, you'll want to also install [docker compose](https://docs.docker.com/compose/install/). Once installed, you can verify the installation by running `docker --version` in your terminal, and `docker compose version` to check that docker compose is installed as well.
- Apptainer: You can follow the [installation instructions](https://apptainer.org/docs/admin/main/installation.html#installation-on-linux) for your specific Linux distribution. Once installed, you can verify the installation by running `apptainer --version` in your terminal.
- Rust: You can use [rustup](https://www.rust-lang.org/tools/install) to install Rust. Once installed, you can verify the installation by running `rustc --version` in your terminal. You also want to make sure that cargo is installed, which is the Rust package manager. You can verify this by running `cargo --version` in your terminal.
- `wget` and `tar`: Most Linux distributions come with `wget` and `tar` pre-installed. If not, you can install them with your package manager.
- System packages are essential for compiling and linking some Rust crates. On linux, you can install them with your package manager. For example with `apt` on Ubuntu or Debian-based systems, you can run:
  ```bash
  sudo apt update
  sudo apt install build-essential pkg-config libssl-dev libsasl2-dev -y
  ```

## Setup

1. Install lfs and pull the large files:
    ```bash
    git lfs install
    git lfs pull
    ```
2. Launch `Valkey`, `MongoDB`, and `Kafka` using docker, using the provided `docker-compose.yaml` file:
    ```bash
    docker compose up -d
    ```
    This may take a couple of minutes the first time you run it, as it needs to download the docker image for each service.
    *To check if the containers are running and healthy, run `docker ps`.*

3. Launch `Valkey`, `MongoDB`, `Kafka`, `Otel Collector`, `Prometheus` and `boom` using Apptainer:
    First, build the SIF files. You can do this by running:
    ```bash
    ./apptainer/def/build-sif.sh
    ```
    Then you can launch the services with:
    ```bash
    ./apptainer.sh compose
    ```
    *To check if the instances are running and healthy, run `./apptainer.sh health`.*
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

## Running BOOM for development

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
*or for Apptainer:*
```bash
apptainer exec instance://broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ztf_YYYYMMDD_programid1
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

## Running BOOM in production

### Using Docker
To run the consumer and the scheduler with Docker, you can open a shell in the `boom` container with:
```bash
docker exec -it -w /app boom /bin/bash
```
Then you can run the binaries with:
```bash
./kafka_consumer <SURVEY> [DATE] [PROGRAMID]
./scheduler <SURVEY> [CONFIG_PATH]
```
Or you can run them directly with:
```bash
docker exec -it -w /app boom ./kafka_consumer <SURVEY> [DATE] [PROGRAMID]
docker exec -it -w /app boom ./scheduler <SURVEY> [CONFIG_PATH]
```

### Using Apptainer
To run the consumer and the scheduler with Apptainer, you can open a shell in the `boom` instance with:
```bash
apptainer shell --pwd /app instance://boom
```
Then you can run the binaries with:
```bash
./kafka_consumer <SURVEY> [DATE] [PROGRAMID]
./scheduler <SURVEY> [CONFIG_PATH]
```
Or you can run them directly with:
```bash
apptainer exec instance://boom /app/kafka_consumer <SURVEY> [DATE] [PROGRAMID]
apptainer exec instance://boom /app/scheduler <SURVEY> [CONFIG_PATH]
```

The scheduler prints a variety of messages to your terminal, e.g.:
- At the start you should see a bunch of `Processed alert with candid: <alert_candid>, queueing for classification` messages, which means that the fake alert worker is picking up on the alerts, processed them, and is queueing them for classification.
- You should then see some `received alerts len: <nb_alerts>` messages, which means that the enrichment worker is processing the alerts successfully.
- You should not see anything related to the filter worker. **This is normal, as we did not define any filters yet!** The next version of the README will include instructions on how to upload a dummy filter to the system for testing purposes.
- What you should definitely see is a lot of `heart beat (MAIN)` messages, which means that the scheduler is running and managing the workers correctly.

Metrics are available in the Prometheus instance at <http://localhost:9090>.
Here some links to the Prometheus UI with useful queries already entered:

* [Kakfa consumer][kafka-consumer-queries]
* [Alert workers][alert-worker-queries]
* [Enrichment workers][enrichment-worker-queries]
* [Filter workers][filter-worker-queries]

[kafka-consumer-queries]: http://localhost:9090/query?g0.expr=%23+Total+number+of+alerts+processed+by+the+alert+workers%0Akafka_consumer_alert_processed_total&g0.show_tree=0&g0.tab=table&g0.range_input=30m&g0.res_type=fixed&g0.res_step=60&g0.display_mode=lines&g0.show_exemplars=0&g1.expr=%23+Instantaneous+throughput+%28alert%2Fs%29%0Airate%28kafka_consumer_alert_processed_total%5B5m%5D%29&g1.show_tree=0&g1.tab=graph&g1.range_input=30m&g1.res_type=fixed&g1.res_step=60&g1.display_mode=lines&g1.show_exemplars=0&g2.expr=%23+Average+processing+time+per+alert+%28s%29%0A1+%2F+sum+by+%28status%29+%28irate%28kafka_consumer_alert_processed_total%5B5m%5D%29%29&g2.show_tree=0&g2.tab=graph&g2.range_input=30m&g2.res_type=fixed&g2.res_step=60&g2.display_mode=lines&g2.show_exemplars=0

[alert-worker-queries]: http://localhost:9090/query?g0.expr=%23+Total+number+of+alerts+processed+by+the+alert+workers%0Aalert_worker_alert_processed_total&g0.show_tree=0&g0.tab=table&g0.range_input=30m&g0.res_type=fixed&g0.res_step=60&g0.display_mode=lines&g0.show_exemplars=0&g1.expr=%23+Instantaneous+throughput+%28alert%2Fs%29%0Airate%28alert_worker_alert_processed_total%5B5m%5D%29&g1.show_tree=0&g1.tab=graph&g1.range_input=30m&g1.res_type=fixed&g1.res_step=60&g1.display_mode=lines&g1.show_exemplars=0&g2.expr=%23+Average+processing+time+per+alert+%28s%29%0A1+%2F+sum+by+%28status%29+%28irate%28alert_worker_alert_processed_total%5B5m%5D%29%29&g2.show_tree=0&g2.tab=graph&g2.range_input=30m&g2.res_type=fixed&g2.res_step=60&g2.display_mode=lines&g2.show_exemplars=0

[enrichment-worker-queries]: http://localhost:9090/query?g0.expr=%23+Total+number+of+alerts+processed+by+the+enrichment+workers%0Aenrichment_worker_alert_processed_total&g0.show_tree=0&g0.tab=table&g0.range_input=30m&g0.res_type=fixed&g0.res_step=60&g0.display_mode=lines&g0.show_exemplars=0&g1.expr=%23+Instantaneous+throughput+%28alert%2Fs%29%0Airate%28enrichment_worker_alert_processed_total%5B5m%5D%29&g1.show_tree=0&g1.tab=graph&g1.range_input=30m&g1.res_type=fixed&g1.res_step=60&g1.display_mode=lines&g1.show_exemplars=0&g2.expr=%23+Average+processing+time+per+alert+%28s%29%0A1+%2F+sum+by+%28status%29+%28irate%28enrichment_worker_alert_processed_total%5B5m%5D%29%29&g2.show_tree=0&g2.tab=graph&g2.range_input=30m&g2.res_type=fixed&g2.res_step=60&g2.display_mode=lines&g2.show_exemplars=0&g3.expr=%23+Number+of+alerts+per+batch%2C+averaged+over+the+collection+interval%0Airate%28enrichment_worker_alert_processed_total%5B5m%5D%29+%2F+irate%28enrichment_worker_batch_processed_total%5B5m%5D%29&g3.show_tree=0&g3.tab=graph&g3.range_input=30m&g3.res_type=fixed&g3.res_step=60&g3.display_mode=lines&g3.show_exemplars=0

[filter-worker-queries]: http://localhost:9090/query?g0.expr=%23+Total+number+of+alerts+processed+by+the+filter+workers%0Afilter_worker_alert_processed_total&g0.show_tree=0&g0.tab=table&g0.range_input=30m&g0.res_type=fixed&g0.res_step=60&g0.display_mode=lines&g0.show_exemplars=0&g1.expr=%23+Instantaneous+throughput+%28alert%2Fs%29%0Airate%28filter_worker_alert_processed_total%5B5m%5D%29&g1.show_tree=0&g1.tab=graph&g1.range_input=30m&g1.res_type=fixed&g1.res_step=60&g1.display_mode=lines&g1.show_exemplars=0&g2.expr=%23+Average+processing+time+per+alert+%28s%29%0A1+%2F+irate%28filter_worker_alert_processed_total%5B5m%5D%29&g2.show_tree=0&g2.tab=graph&g2.range_input=30m&g2.res_type=fixed&g2.res_step=60&g2.display_mode=lines&g2.show_exemplars=0&g3.expr=%23+Number+of+alerts+per+batch%2C+averaged+over+the+collection+interval%0Airate%28filter_worker_alert_processed_total%5B5m%5D%29+%2F+ignoring%28reason%29+group_left+irate%28filter_worker_batch_processed_total%5B5m%5D%29%0A%23+irate%28filter_worker_alert_processed_total%5B5m%5D%29%0A%23+irate%28filter_worker_batch_processed_total%5B5m%5D%29&g3.show_tree=0&g3.tab=graph&g3.range_input=30m&g3.res_type=fixed&g3.res_step=60&g3.display_mode=lines&g3.show_exemplars=0

## Stopping BOOM:

To stop BOOM, you can simply stop the `Kafka` consumer with `CTRL+C`, and then stop the scheduler with `CTRL+C` as well.
You can also stop the docker containers with:
```bash
docker compose down
```
Or stop the Apptainer instances with:
```bash
./apptainer.sh stop
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

## Running Benchmark

This repository includes a benchmark to test the system and get an idea of the time it takes to process a certain number of alerts.
This benchmark uses Docker to build the image and run the benchmark, but it can also be run with Apptainer.
The step to run the benchmark are as follows:

### Build Image
For Docker (docker Image):
```bash
  docker buildx create --use
  docker buildx inspect --bootstrap
  docker buildx bake -f tests/throughput/compose.yaml --load
```
For Apptainer (SIF file):
```bash
  ./apptainer/def/build-sif.sh test
```

### Download Data
```bash
  mkdir -p ./data/alerts/ztf/public/20250311
  gdown "https://drive.google.com/uc?id=1BG46oLMbONXhIqiPrepSnhKim1xfiVbB" -O ./data/alerts/kowalski.NED.json.gz
```

### Start Benchmark
Using Docker:
```bash
  uv run tests/throughput/run.py
```

Using Apptainer:
```bash
  ./apptainer.sh benchmark
```

## Contributing

We welcome contributions! Please read the [CONTRIBUTING.md](CONTRIBUTING.md) file for more information. 
We rely on [GitHub issues](https://github.com/boom-astro/boom/issues) to track bugs and feature requests.
