# BOOM (Burst & Outburst Observations Monitor)

## Description

BOOM is an alert broker. What sets it apart from other alert brokers is that it is written to be modular, scalable, and performant. Essentially, the pipeline is composed of multiple types of workers, each with a specific task:
1. The `Kafka` consumer(s), reading alerts from astronomical surveys' `Kafka` topics to transfer them to `Redis`/`Valkey` in-memory queues.
2. The Alert Ingestion workers, reading alerts from the `Redis`/`Valkey` queues, responsible of formatting them to BSON documents, and enriching them with crossmatches from archival astronomical catalogs and other surveys before writing the formatted alert packets to a `MongoDB` database.
3. The ML workers, running alerts through a series of ML classifiers, and writing the results back to the `MongoDB` database.
4. The Filter workers, running user-defined filters on the alerts, and sending the results to Kafka topics for other services to consume.

Workers are managed by a Scheduler that can spawn or kill workers of each type. Currently, the number of workers is static, but we are working on dynamically scaling the number of workers based on the load of the system.

BOOM also comes with an HTTP API, under development, which will allow users to query the `MongoDB` database, to define their own filters, and to have those filters run on alerts in real-time.

## System Requirements

BOOM runs on macOS and Linux. You'll need:

- `Docker` and `docker compose`: used to run the database, cache/task queue, and `Kafka`;
- `Rust` (a systems programming language) `>= 1.55.0`;
- `Python` (a high-level programming language) `>= 3.10`: we recommend using `uv` to create a virtual environment with the required Python dependencies.
- `wget` and `tar`: used to download and extract the ZTF alerts for testing purposes.

### Installation steps:

#### macOS

- Docker: On macOS we recommend using [Docker Desktop](https://www.docker.com/products/docker-desktop) to install docker. You can download it from the website, and follow the installation instructions. The website will ask you to "choose a plan", but really you just need to create an account and stick with the free tier that offers all of the features you will ever need. Once installed, you can verify the installation by running `docker --version` in your terminal, and `docker compose version` to check that docker compose is installed as well.
- Rust: You can either use [rustup](https://www.rust-lang.org/tools/install) to install Rust, or you can use [Homebrew](https://brew.sh/) to install it. If you choose the latter, you can run `brew install rust` in your terminal. We recommend using rustup, as it allows you to easily switch between different versions of Rust, and to keep your Rust installation up to date. Once installed, you can verify the installation by running `rustc --version` in your terminal. You also want to make sure that cargo is installed, which is the Rust package manager. You can verify this by running `cargo --version` in your terminal.
- Python: We strongly recommend using [uv](https://docs.astral.sh/uv/getting-started/installation/) to manage your python installation and virtual environments. You can install it with `brew`, `pip`, or using the [install script](https://docs.astral.sh/uv/getting-started/installation/#install-script). We recommend the later. Once installed, you can verify the installation by running `uv --version` in your terminal.
- `wget` and `tar`: `tar` is already installed on macOS, but you can install `wget` with `brew install wget` or any other package manager you prefer.

#### Linux

- Docker: You can either install Docker Desktop (same instructions as for macOS), or you can just install Docker Engine. The latter is more lightweight. You can follow the [official installation instructions](https://docs.docker.com/engine/install/) for your specific Linux distribution. If you only installed Docker Engine, you'll want to also install [docker compose](https://docs.docker.com/compose/install/). Once installed, you can verify the installation by running `docker --version` in your terminal, and `docker compose version` to check that docker compose is installed as well.
- Rust: You can use [rustup](https://www.rust-lang.org/tools/install) to install Rust. Once installed, you can verify the installation by running `rustc --version` in your terminal. You also want to make sure that cargo is installed, which is the Rust package manager. You can verify this by running `cargo --version` in your terminal.
- Python: We strongly recommend using [uv](https://docs.astral.sh/uv/getting-started/installation/) to manage your python installation and virtual environments. You can install it with `pip`, or using the [install script](https://docs.astral.sh/uv/getting-started/installation/#install-script). We recommend the later. Once installed, you can verify the installation by running `uv --version` in your terminal.
- `wget` and `tar`: Most Linux distributions come with `wget` and `tar` pre-installed. If not, you can install them with your package manager.

## Setup

1. We'll start by creating a python virtual environment to manage our python install and dependencies. Here's how you can do that with `uv`:
    ```bash
    uv venv --python 3.10
    source .venv/bin/activate
    uv pip install -r requirements.txt
    ```
2. Next, copy the default config file, `config.default.yaml`, to `config.yaml`:
    ```bash
    cp config.default.yaml config.yaml
    ```
3. Same for the `docker-compose.yaml` file:
    ```bash
    cp docker-compose.default.yaml docker-compose.yaml
    ```
4. Launch `Valkey`, `MongoDB`, and `Kafka` using docker, using the provided `docker compose.yaml` file:
    ```bash
    docker compose up -d
    ```
    This may take a couple of minutes the first time you run it, as it needs to download the docker image for each service.
    *To check if the containers are running and healthy, run `docker ps`.*
5. Last but not least, build the Rust binaries. You can do this with or without the `--release` flag, but we recommend using it for better performance:
    ```bash
    cargo build --release
    ```

## Running BOOM:

BOOM is meant to be run in production, reading from a real-time stream of astronomical alerts. **That said, we can create our own Kafka stream using the[ZTF alerts public archive](https://ztf.uw.edu/alerts/public/) to test BOOM.** To do so, you can start the `Kafka` producer with:
```bash
cargo run --release --bin kafka_producer <date_in_YYYMMDD_format> <limit>
```
Where `<date_in_YYYMMDD_format>` is the date of the alerts you want to read. We suggest using a night with a very small number of alerts to just get the code running, like `20240617` for example. The script will take care of downloading the alerts from the ZTF IPAC server, writing them on disk for the `Kafka` producer to read, and then will start producing them to the associated `Kafka` topic, `ztf_YYYYMMDD_programid1`. You can leave that running in the background, and start the rest of the pipeline in another terminal. The <limit> argument is optional, and will limit the number of alerts pushed to the `Kafka` topic.

*If you'd like to clear the `Kafka` topic before starting the producer, you can run the following command:*
```bash
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 --delete --topic ztf_YYYYMMDD_programid1
```


Next, you can start the `Kafka` consumer with:
```bash
cargo run --release --bin kafka_consumer <SURVEY> [DATE] [PROCESSES] [CLEAR] [max_in_queue]
```
Where `<SURVEY>` and `[DATE]` determine which topic to read from. In our case, `<SURVEY>` should be `ZTF` and `[DATE]` is the date in YYYMMDD format that was given to `kafka_producer` earlier, telling the consumer to read from the topic named `ztf_YYYYMMDD_programid1`. This naming scheme follows the actual naming scheme used by the real ZTF alert streams.  `<group_id>` is the name of the `Kafka` consumer group (optional), and `<exit_on_eof>` is a boolean that tells the consumer to exit when it reaches the end of the topic. You can set it to `true` for testing purposes, and `false` for production, as you would want the consumer to keep running and reading new alerts as they come in. Last but not least `<max_in_queue>` allows you to set a limit on how many alert packets can be in the redis queue at once. By default, this is set to 1000, and can be set to 0 to be ignored. The script will read the alerts from the `Kafka` topic, and write them to the `Redis`/`Valkey` queue. You can leave that running in the background, and start the rest of the pipeline in another terminal.

Instead of starting each worker manually, we provide the `scheduler`. It reads the number of workers for each type from `config.yaml`. Run the scheduler with:
```bash
cargo run --release --bin scheduler <stream_name> <config_path>
```
Where `<stream_name>` is the name of the stream you want to process. In our case, it would be `ZTF`. `<config_path>` is the path to the config file, which is `config.yaml` by default, and can be omitted.

*Before running the scheduler, make sure that you are in your Python virtual environment. This is required for the ML worker, that will run Python-based ML models. If you created it with `uv` as instructed earlier, you can enter the virtual environment with `source .venv/bin/activate`.*

The scheduler prints a variety of messages to your terminal, e.g.:
- At the start you should see a bunch of `Processed alert with candid: <alert_candid>, queueing for classification` messages, which means that the fake alert worker is picking up on the alerts, processed them, and is queueing them for classification.
- You should then see some `ML WORKER <worker_id>: received alerts len: <nb_alerts>` messages, which means that the ML worker is processing the alerts successfully.
- You should not see anything related to the filter worker. **This is normal, as we did not define any filters yet!** The next version of the README will include instructions on how to upload a dummy filter to the system for testing purposes.
- What you should definitely see is a lot of `heart beat (MAIN)` messages, which means that the scheduler is running and managing the workers correctly.

## Stopping BOOM:

To stop BOOM, you can simply stop the `Kafka` consumer with `CTRL+C`, and then stop the scheduler with `CTRL+C` as well. You can also stop the docker containers with:
```bash
docker compose down
```

When you stop the scheduler, it will attempt to gracefully stop all the workers by sending them interrupt signals. This is still a work in progress, so you might see some error handling taking place in the logs.

**In the next version of the README, we'll provide the user with example scripts to read the output of BOOM (i.e. the alerts that passed the filters) from `Kafka` topics. For now, alerts are send back to `Redis`/`valkey` if they pass any filters.**

## Tests

We are currently working on adding tests to the codebase. You can run the tests with:
```bash
cargo test
```

Tests currently require the kafka, valkey, and mongo Docker containers to be running as described above.

*When running the tests, the config file found in `tests/config.test.yaml` will be used.*

The test suite also runs automagically on every push to the repository, and on every pull request. You can check the status of the tests in the "Actions" tab of the GitHub repository.

## Contributing

We welcome contributions! Please read the [CONTRIBUTING.md](CONTRIBUTING.md) (TBD) file for more information. We rely on [GitHub issues](https://github.com/boom-astro/boom/issues) to track bugs and feature requests.

## Implementation details:

### Dealing with Avro & Rust structs:

It can get pretty painful in Rust to work with Avro schemas, and more specifically to have to write Rust structs that match them. To make this easier, we can use the super useful `rsgen-avro` crate to generate Rust structs from the Avro schemas. Consider this a one-time process that provides a starting point for a given set of schemas. The generated structs can then be modified over time as needed:

1. First, install `rsgen-avro` as a binary with:

    ```bash
    cargo install rsgen-avro --features="build-cli"
    ```

2. Second, download the latest avro schema for the survey you want to add to BOOM, here are some links for references:
    - ZTF: https://github.com/ZwickyTransientFacility/ztf-avro-alert
    - LSST: https://github.com/lsst/alert_packet

3. Then, generate the Rust structs from the Avro schema with:

    ```bash
    rsgen-avro "path/to/the/schema(s)/directory" -
    ```

    This will output the Rust structs to the standard output, which you can then copy-paste in a lib file in the `src` directory, so you can use them in your code.

We already went through this process for the ZTF Avro schema (so no need to do it again), and the corresponding Rust structs are in the `src/types.rs` file. We only slightly modified the `Alert` struct to add methods to create an alert from the bytes of an Avro record (`from_avro_bytes`).

### Dealing with Rust structs and MongoDB BSON documents:

We could in theory just query` MongoDB` in a way that allows us to get Rust structs out of it, and also do the same when writing to the database. However under the hood the `mongodb` crate just serializes back and forth, and since Rust structs can't just "remove" their fields that are null (in a way, they need to enforce a schema), we would end up with a lot of null fields in the database. To avoid this, we use the `bson` crate to serialize the Rust structs to BSON documents, sanitize them (remove the null fields and such), and then write them to the database. When querying the DB, both bson documents or Rust structs can be returned, it depends on the use case.

### Why still using Python for some parts of the pipeline?

For everything ML-related, it's not that easy to just take anyone's model (that was 99% of the time trained with a Python library) and just run it in Rust. We could try (and successfully did for a handful of models) converting them to ONNX, and then running them with `tch-rs` (a Rust wrapper around the `libtorch` C++ library). However, this proved to be a pain on a lot of systems. Thanks to the fact that we use something like `Redis`/`Valkey` as a cache/task queue, we can pretty much send data between whatever language we want, and have the ML models run in Python, and the rest of the pipeline run in Rust. It will also come in handy for the filtering pipeline, as we can leverage any of Python's libraries to do some fancy & complex computation that would be a pain for your average astronomer to write in Rust. So for now, we limit the Rust code to all of the "core" parts of the pipeline where performance is key, and use Python where we can affort to lose a bit of performance for more flexibility.

### Why `Redis`/`Valkey` as a cache/task queue?

There are multiple answers to this:
- Because it is well maintained, well documented, both on the server side and the clients to interact with the software from any programming language.
- Because it is fast, and can handle a lot of data in memory.
- Because it is easy to use, and can be used as a cache, a task queue, a message broker, and more. It's a piece of software we can reuse to solve multiple problems in a large system.
- Because other task queue systems like `Celery`, `Dask`, `RabbitMQ`, `Kafka`, etc. are either too complex, too slow, too hard to maintain, have memory leaks, poor support across multiple programming languages, or all of the above. `Redis`/`Valkey` is simple and fast, and allows us to build the rest of the system on top of it. However the system is designed in a way that we can swap `Redis`/`Valkey` for another task queue system if we ever need to, with minimal changes to the code.

### Why `MongoDB` as a database?

`MongoDB` proved to be a great choice for another broker that `BOOM` is heavily inspired from: `Kowalski`. Mongo has great support across multiple programming languages, is highly flexible, has a rich query language that we can build complex pipelines with (perfect for filtering alerts), and is easy to maintain. It is also fast, and can handle a lot of data. We could have gone with `PostgreSQL`, but we would have lost some flexibility, and we would have had to enforce a schema on the data, which is not ideal for an alert stream that can have a lot of different fields. With `MongoDB`, we do not have to enforce a schema or run database migrations whenever we want to add another astronomical catalog to crossmatch with the alerts.

### Why `Kafka` as a message broker?

`Kafka` has been the standard for astronomical alert brokering for a while now, and offers a lot of features that are useful for our use case. It is highly scalable, fault-tolerant, and can handle a lot of data. It also has a rich ecosystem of tools and libraries that we can leverage to build the rest of the system. We could have gone with `Redis`/`Valkey` as a message broker, but reading from `Kafka` topics is what other downstream services expect, and it would have been a pain to have to maintain a custom solution for that. This way, we can keep the internal cache/task queue and the public facing message broker separate.
