# Architecture

## Implementation details:

### Dealing with Avro & Rust structs:

It can get pretty painful in Rust to work with Avro schemas, and more specifically to have to write Rust structs that match them.
To make this easier, we can use the super useful `rsgen-avro` crate to generate Rust structs from the Avro schemas.
Consider this a one-time process that provides a starting point for a given set of schemas.
The generated structs can then be modified over time as needed:

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

We already went through this process for the ZTF Avro schema (so no need to do it again), and the corresponding Rust structs are in the `src/types.rs` file.
We only slightly modified the `Alert` struct to add methods to create an alert from the bytes of an Avro record (`from_avro_bytes`).

### Dealing with Rust structs and MongoDB BSON documents:

We could in theory just query `MongoDB` in a way that allows us to get Rust structs out of it, and also do the same when writing to the database.
However under the hood the `mongodb` crate just serializes back and forth, and since Rust structs can't just "remove" their fields that are null (in a way, they need to enforce a schema), we would end up with a lot of null fields in the database.
To avoid this, we use the `bson` crate to serialize the Rust structs to BSON documents, sanitize them (remove the null fields and such), and then write them to the database.
When querying the DB, both bson documents or Rust structs can be returned, it depends on the use case.

### Why `Redis`/`Valkey` as a cache/task queue?

There are multiple answers to this:
- Because it is well maintained, well documented, both on the server side and the clients to interact with the software from any programming language.
- Because it is fast, and can handle a lot of data in memory.
- Because it is easy to use, and can be used as a cache, a task queue, a message broker, and more. It's a piece of software we can reuse to solve multiple problems in a large system.
- Because other task queue systems like `Celery`, `Dask`, `RabbitMQ`, `Kafka`, etc. are either too complex, too slow, too hard to maintain, have memory leaks, poor support across multiple programming languages, or all of the above. `Redis`/`Valkey` is simple and fast, and allows us to build the rest of the system on top of it. However the system is designed in a way that we can swap `Redis`/`Valkey` for another task queue system if we ever need to, with minimal changes to the code.

### Why `MongoDB` as a database?

`MongoDB` proved to be a great choice for another broker that `BOOM` is heavily inspired from: `Kowalski`.
Mongo has great support across multiple programming languages, is highly flexible, has a rich query language that we can build complex pipelines with (perfect for filtering alerts), and is easy to maintain.
It is also fast, and can handle a lot of data. We could have gone with `PostgreSQL`, but we would have lost some flexibility, and we would have had to enforce a schema on the data, which is not ideal for an alert stream that can have a lot of different fields.
With `MongoDB`, we do not have to enforce a schema or run database migrations whenever we want to add another astronomical catalog to crossmatch with the alerts.

### Why `Kafka` as a message broker?

`Kafka` has been the standard for astronomical alert brokering for a while now, and offers a lot of features that are useful for our use case.
It is highly scalable, fault-tolerant, and can handle a lot of data.
It also has a rich ecosystem of tools and libraries that we can leverage to build the rest of the system.
We could have gone with `Redis`/`Valkey` as a message broker, but reading from `Kafka` topics is what other downstream services expect, and it would have been a pain to have to maintain a custom solution for that. This way, we can keep the internal cache/task queue and the public facing message broker separate.
