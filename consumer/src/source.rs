/// Adding a new source:
///
/// 1.  Define a public module `foo` under `sources` containing the following:
///
///     a)  A public source type `FooSource` that implements the `Source` trait
///
///     b)  If needed, a public args struct `FooArgs` that implements/derives
///         `clap::Args`, defining the command line arguments used to build
///         `FooSource`
///
///     c)  If needed, a public config struct `FooConfig` that can be created
///         via `config::Config::try_deserialize`, defining the information
///         required to build `FooSource` that isn't already provided by
///         `FooArgs`
///
/// 2.  Add a public field `pub foo: FooConfig` to the `Config` struct
///
/// 3.  Add the new configuration to `config.yaml`
///
/// 4.  Add a variant with a docstring to `SourceCommand` that wraps `FooArgs`,
///     registering the source as a subcommand in the CLI
///
///     The compiler will then require a new match arm in `SourceCommand::build`
///     that creates a `Box<FooSource>` value, where both `FooArgs` (if
///     applicable) and `Config` are in scope and can be used as needed.
///
use color_eyre::eyre::Result;
use rdkafka::consumer::BaseConsumer;

pub trait Source {
    /// Create a new `rdkafka::consumer::BaseConsumer` for this Kafka instance.
    fn make_base_consumer(&self) -> Result<BaseConsumer>;

    /// The Kafka consumer group ID the application should use.
    fn group_id(&self) -> String;

    /// The Kafka topic to consume.
    fn topic(&self) -> String;

    /// The Redis key where messages should be enqueued.
    fn key(&self) -> String;
}
