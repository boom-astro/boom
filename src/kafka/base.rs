use crate::{
    conf::{self, AppConfig, BoomConfigError, KafkaConsumerConfig},
    utils::{
        data::count_files_in_dir,
        o11y::{
            logging::{as_error, log_error, WARN},
            metrics::CONSUMER_METER,
        },
    },
};

use std::sync::{Arc, LazyLock};

use indicatif::ProgressBar;
use opentelemetry::{metrics::Counter, KeyValue};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{BaseConsumer, CommitMode, Consumer},
    error::{KafkaError, KafkaResult},
    message::Message,
    producer::{FutureProducer, FutureRecord, Producer},
    TopicPartitionList,
};
use redis::AsyncCommands;
use tracing::{debug, error, info, instrument, trace, warn};

// NOTE: Global instruments are defined here because reusing instruments is
// considered a best practice. See boom::alert::base.

// Counter for the number of alerts processed by the kafka consumer.
static ALERT_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CONSUMER_METER
        .u64_counter("kafka_consumer.alert.processed")
        .with_unit("{alert}")
        .with_description("Number of alerts processed by the kafka consumer.")
        .build()
});

const MAX_RETRIES_PRODUCER: usize = 6;
const KAFKA_TIMEOUT_SECS: std::time::Duration = std::time::Duration::from_secs(30);

// rdkafka's Metadata type provides *references* to MetadataTopic and
// MetadataPartition values, neither of which implement Clone. We use a custom
// Metadata type to capture the topic and partition information from the rdkafka
// types, which can then can be returned to the caller.
#[instrument(skip_all, err)]
fn get_partition_ids(
    client: &BaseConsumer,
    topic_name: &str,
) -> Result<Option<Vec<i32>>, KafkaError> {
    let cluster_metadata = client.fetch_metadata(Some(topic_name), KAFKA_TIMEOUT_SECS)?;
    let topic = match cluster_metadata
        .topics()
        .iter()
        .find(|metadata_topic| metadata_topic.name() == topic_name)
    {
        Some(topic) => topic,
        None => return Ok(None),
    };
    let partition_ids = topic
        .partitions()
        .iter()
        .map(|metadata_partition| metadata_partition.id())
        .collect::<Vec<_>>();
    Ok(Some(partition_ids))
}

// check that the topic exists and return the number of partitions
#[instrument(skip_all, err)]
pub fn check_kafka_topic_partitions(
    bootstrap_servers: &str,
    topic_name: &str,
    group_id: &str,
    username: Option<String>,
    password: Option<String>,
) -> Result<Option<usize>, KafkaError> {
    let mut client_config = ClientConfig::new();
    client_config
        // Uncomment the following to get logs from kafka (RUST_LOG doesn't work):
        // .set("debug", "consumer,cgrp,topic,fetch")
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id);

    if let (Some(username), Some(password)) = (username, password) {
        client_config
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", "SCRAM-SHA-512")
            .set("sasl.username", username)
            .set("sasl.password", password);
    } else {
        client_config.set("security.protocol", "PLAINTEXT");
    }

    let consumer: BaseConsumer = client_config
        .create()
        .inspect_err(as_error!("failed to create consumer"))?;
    let partition_ids = get_partition_ids(&consumer, topic_name)?;
    Ok(partition_ids.map(|ids| ids.len()))
}

#[instrument(skip_all, err)]
pub async fn initialize_topic(
    bootstrap_servers: &str,
    topic_name: &str,
    expected_nb_partitions: usize,
) -> Result<usize, KafkaError> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let nb_partitions = match check_kafka_topic_partitions(
        bootstrap_servers,
        topic_name,
        "producer-topic-check",
        None,
        None,
    )? {
        Some(nb_partitions) => {
            if nb_partitions != expected_nb_partitions {
                warn!(
                    "Topic {} exists but has {} partitions instead of expected {}",
                    topic_name, nb_partitions, expected_nb_partitions
                );
            }
            nb_partitions
        }
        None => {
            let opts = AdminOptions::new().operation_timeout(Some(KAFKA_TIMEOUT_SECS));
            info!(
                "Creating topic {} with {} partitions...",
                topic_name, expected_nb_partitions
            );
            admin_client
                .create_topics(
                    &[NewTopic::new(
                        topic_name,
                        expected_nb_partitions as i32,
                        TopicReplication::Fixed(1),
                    )],
                    &opts,
                )
                .await?;
            info!(
                "Topic {} created successfully with {} partitions",
                topic_name, expected_nb_partitions
            );
            expected_nb_partitions
        }
    };
    Ok(nb_partitions)
}

#[instrument(skip_all, err)]
pub async fn delete_topic(bootstrap_servers: &str, topic_name: &str) -> Result<(), KafkaError> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let opts = AdminOptions::new().operation_timeout(Some(KAFKA_TIMEOUT_SECS));
    admin_client.delete_topics(&[topic_name], &opts).await?;
    Ok(())
}

#[instrument(skip_all, err)]
pub fn count_messages(
    bootstrap_servers: &str,
    topic_name: &str,
) -> Result<Option<u32>, KafkaError> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;
    match get_partition_ids(&consumer, topic_name) {
        Ok(Some(partition_ids)) => {
            debug!(?topic_name, "topic found");
            let total_messages =
                partition_ids
                    .iter()
                    .try_fold(0u32, |total_messages, &partition_id| {
                        consumer
                            .fetch_watermarks(topic_name, partition_id, KAFKA_TIMEOUT_SECS)
                            .map(|(low, high)| {
                                let count = high - low;
                                debug!(
                                    ?topic_name,
                                    ?partition_id,
                                    ?low,
                                    ?high,
                                    ?count,
                                    "watermarks"
                                );
                                total_messages + count as u32
                            })
                    })?;
            debug!(?topic_name, ?total_messages);
            Ok(Some(total_messages))
        }
        Ok(None) => {
            debug!(?topic_name, "topic not found");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

#[async_trait::async_trait]
pub trait AlertProducer {
    fn topic_name(&self) -> String;
    fn data_directory(&self) -> String;
    fn server_url(&self) -> String;
    fn limit(&self) -> i64;
    fn verbose(&self) -> bool {
        false
    }
    async fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>>;
    fn default_nb_partitions(&self) -> usize;
    async fn produce(
        &self,
        topic: Option<String>,
    ) -> Result<Option<i64>, Box<dyn std::error::Error>> {
        let topic_name = topic.unwrap_or_else(|| self.topic_name());
        if let Some(total_messages) = count_messages(&self.server_url(), &topic_name)? {
            // Topic exists, skip producing if it has the expected number of
            // messages. Count the number of Avro files in the data directory:
            if let Some(avro_count) = count_files_in_dir(&self.data_directory(), Some(&["avro"]))
                .map(|count| Some(count))
                .or_else(|error| match error.kind() {
                    std::io::ErrorKind::NotFound => Ok(None),
                    _ => Err(error),
                })?
            {
                if avro_count == 0 {
                    warn!("data directory {} is empty", self.data_directory());
                }
                debug!(
                    "{} avro files found in {}",
                    avro_count,
                    self.data_directory()
                );
                // If the counts match, then nothing to do, return early.
                if total_messages == avro_count as u32 {
                    info!(
                        "Topic {} already exists with {} messages, no need to produce",
                        topic_name, total_messages
                    );
                    return Ok(None);
                } else {
                    warn!(
                        "Topic {} already exists with {} messages, but {} Avro files found in data directory",
                        topic_name,
                        total_messages,
                        avro_count
                    );
                }
            } else {
                warn!(
                    "Topic {} already exists, but data directory not found",
                    topic_name,
                );
            }
            // The topic and data directory are inconsistent. Delete the topic
            // to start fresh:
            warn!("recreating topic {}", topic_name);
            delete_topic(&self.server_url(), &topic_name).await?;
        }

        match self.download_alerts_from_archive().await {
            Ok(count) => count,
            Err(e) => {
                error!("Error downloading alerts: {}", e);
                return Err(e);
            }
        };

        let limit = self.limit();
        let verbose = self.verbose();

        info!("Initializing kafka alert producer");
        let producer: FutureProducer = ClientConfig::new()
            // Uncomment the following to get logs from kafka (RUST_LOG doesn't work):
            // .set("debug", "broker,topic,msg")
            .set("bootstrap.servers", &self.server_url())
            .set("message.timeout.ms", "5000")
            // it's best to increase batch.size if the cluster
            // is running on another machine. Locally, lower means less
            // latency, since we are not limited by network speed anyways
            .set("batch.size", "16384")
            .set("linger.ms", "5")
            .set("acks", "1")
            .set("max.in.flight.requests.per.connection", "5")
            .set("retries", "3")
            .create()
            .expect("Producer creation error");

        let _ = initialize_topic(
            &self.server_url(),
            &topic_name,
            self.default_nb_partitions(),
        )
        .await?;

        let data_folder = self.data_directory();
        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;

        let total_size = if limit > 0 {
            count.min(limit as usize) as u64
        } else {
            count as u64
        };

        let progress_bar = ProgressBar::new(total_size)
            .with_message(format!("Pushing alerts to {}", topic_name))
            .with_style(indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} [{elapsed_precise}] {human_pos}/{human_len} ({eta})")?);

        let mut total_pushed = 0;
        let start = std::time::Instant::now();
        for entry in std::fs::read_dir(&data_folder)? {
            if entry.is_err() {
                continue;
            }
            let entry = entry.unwrap();
            let path = entry.path();
            if !path.to_str().unwrap().ends_with(".avro") {
                continue;
            }
            let payload = match std::fs::read(&path) {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to read file {:?}: {}", path.to_str(), e);
                    continue;
                }
            };

            // we do not specify a key for the record, to let kafka distribute messages across partitions
            // across partitions evenly with its built-in round-robin strategy
            // use retries in case of transient errors
            let mut n_retries = MAX_RETRIES_PRODUCER;
            while n_retries > 0 {
                let record: FutureRecord<'_, (), Vec<u8>> = FutureRecord::to(&topic_name)
                    .payload(&payload)
                    .timestamp(chrono::Utc::now().timestamp_millis());
                let status = producer.send(record, KAFKA_TIMEOUT_SECS).await;
                match status {
                    Ok(_) => {
                        break;
                    }
                    Err((e, _)) => {
                        error!(
                            "Failed to deliver message: {:?}, retrying ({} left)",
                            e,
                            n_retries - 1
                        );
                    }
                }
                n_retries -= 1;
            }

            total_pushed += 1;
            if verbose {
                progress_bar.inc(1);
            }

            if limit > 0 && total_pushed >= limit {
                info!("Reached limit of {} pushed items", limit);
                break;
            }
        }

        info!(
            "Pushed {} alerts to the queue in {:?}",
            total_pushed,
            start.elapsed()
        );

        // close producer
        producer.flush(KAFKA_TIMEOUT_SECS)?;

        Ok(Some(total_pushed as i64))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error("error from boom::conf ({0})")]
    Config(#[from] conf::BoomConfigError),
    #[error("error from rdkafka")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("error from redis")]
    Redis(#[from] redis::RedisError),
    #[error("error from config")]
    ConfigError(#[from] config::ConfigError),
}

/// UTC dates a date-partitioned survey should currently be subscribed to,
/// oldest first: the day containing `timestamp` plus the preceding
/// `window_days`.
///
/// The default of 1 keeps yesterday alongside today, because a survey night
/// straddles UTC midnight and the tail of it would otherwise be dropped at
/// rollover. Going wider is a deliberate, temporary act: upstream expires a
/// topic's partitions while still advertising its name, so every extra day
/// risks re-adding partitions that fail every poll (survivable — see
/// `is_expired_partition` — but not free). Its intended use is catching up
/// after an upstream outage, bounded by upstream retention.
pub fn subscription_window(timestamp: i64, window_days: u64) -> Vec<chrono::NaiveDate> {
    let today = chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| chrono::Utc::now().date_naive());
    (0..=window_days)
        .rev()
        .filter_map(|days_back| today.checked_sub_days(chrono::Days::new(days_back)))
        .collect()
}

#[async_trait::async_trait]
pub trait AlertConsumer: Sized + Clone + Send + Sync + 'static {
    /// Concrete topic name(s) for a specific date. Used by the producer side and
    /// by the one-shot (`exit_on_eof`) drain path.
    fn topic_names(&self, timestamp: i64) -> Vec<String>;
    /// Concrete topic names the long-running consumer subscribes to at
    /// `timestamp`.
    ///
    /// Date-partitioned surveys return a bounded window (see
    /// [`subscription_window`]); surveys with a single static topic (e.g. LSST)
    /// return that literal name and ignore `timestamp`. The consumer re-invokes
    /// this whenever the UTC date changes and re-subscribes to the result, which
    /// is what makes rollover automatic.
    ///
    /// This deliberately returns literal names rather than a `^…` regex.
    /// librdkafka expands a regex against every topic the cluster advertises,
    /// which for a daily-topic survey is the entire history of past nights — a
    /// set that grows without bound and is almost entirely expired.
    fn subscription_topics(&self, timestamp: i64, window_days: u64) -> Vec<String>;
    fn output_queue(&self) -> String;
    fn survey(&self) -> &'static str;
    #[instrument(skip(self))]
    async fn clear_output_queue(&self, config_path: &str) -> Result<(), ConsumerError> {
        let config = AppConfig::from_path(config_path)?;
        let mut con = config
            .build_redis()
            .await
            .inspect_err(as_error!("failed to connect to redis"))?;
        let _: () = con
            .del(&self.output_queue())
            .await
            .inspect_err(as_error!("failed to delete queue"))?;
        info!(
            "Cleared redis queue {} for Kafka consumer",
            self.output_queue()
        );
        Ok(())
    }
    // No `#[instrument]` here: `consume` runs the consumer loop for the
    // entire process lifetime, and any wrapping span would make every
    // per-message child span a descendant of the same root trace, which
    // grows until Tempo rejects it (TRACE_TOO_LARGE).
    async fn consume(
        &self,
        topics: Option<Vec<String>>,
        start: StartDate,
        kafka_config: Option<KafkaConsumerConfig>,
        n_threads: Option<usize>,
        max_in_queue: Option<usize>,
        exit_on_eof: bool,
        config_path: &str,
    ) -> Result<(), ConsumerError> {
        let config = AppConfig::from_path(config_path)?;
        let survey = self.survey();

        // Resolves the topics to subscribe to for a given instant. The consumer
        // loop re-invokes this on every UTC-date rollover and re-subscribes, so
        // new daily topics are picked up without a restart.
        // exit_on_eof (tests/dev): target the concrete topic for the date.
        let subscribe_to: Arc<dyn Fn(i64, u64) -> Vec<String> + Send + Sync> = match topics {
            Some(overridden) => Arc::new(move |_, _| overridden.clone()),
            None if exit_on_eof => {
                let survey_consumer = self.clone();
                Arc::new(move |at, _| survey_consumer.topic_names(at))
            }
            None => {
                let survey_consumer = self.clone();
                Arc::new(move |at, days| survey_consumer.subscription_topics(at, days))
            }
        };
        let kafka_config = match kafka_config {
            Some(cfg) => cfg,
            None => config
                .kafka
                .consumer
                .iter()
                .find(|(k, _)| k.as_str() == survey)
                .map(|(_, v)| v.clone())
                .ok_or_else(|| {
                    ConsumerError::from(BoomConfigError::MissingKeyError(format!(
                        "kafka.consumer.{}",
                        survey.to_lowercase()
                    )))
                })?,
        };

        let n_threads = n_threads.unwrap_or(1);
        let max_in_queue = max_in_queue.unwrap_or(15000);
        let plan = start.plan(kafka_config.subscription_window_days);

        let mut handles = vec![];
        for i in 0..n_threads {
            let subscribe_to = subscribe_to.clone();
            let output_queue = self.output_queue();
            let config = config.clone();
            let kafka_config = kafka_config.clone();
            let handle = tokio::spawn(async move {
                let result = consumer(
                    &i.to_string(),
                    subscribe_to,
                    &output_queue,
                    max_in_queue,
                    plan,
                    &config,
                    &kafka_config,
                    exit_on_eof,
                    survey,
                )
                .await;
                if let Err(error) = result {
                    log_error!(error, "failed to consume partitions");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            if let Err(error) = handle.await {
                log_error!(error, "failed to join task");
            }
        }

        Ok(())
    }
}

fn seek_to_timestamp(consumer: &BaseConsumer, timestamp_ms: i64) -> KafkaResult<()> {
    // Get current assignment
    let assignment = consumer.assignment()?;

    // Create a new TopicPartitionList with offsets to look up
    let mut tpl_with_timestamps = TopicPartitionList::new();

    for elem in assignment.elements() {
        tpl_with_timestamps.add_partition_offset(
            elem.topic(),
            elem.partition(),
            rdkafka::Offset::Offset(timestamp_ms), // Use timestamp as offset for lookup, in ms
        )?;
    }

    // Query offsets for the given timestamp
    let offsets = consumer.offsets_for_times(tpl_with_timestamps, KAFKA_TIMEOUT_SECS)?;

    // Seek to the resolved offsets
    for elem in offsets.elements() {
        debug!(
            "Seeking partition {} to offset for timestamp {}",
            elem.partition(),
            timestamp_ms
        );
        if let rdkafka::Offset::Offset(offset) = elem.offset() {
            consumer.seek(
                elem.topic(),
                elem.partition(),
                rdkafka::Offset::Offset(offset),
                KAFKA_TIMEOUT_SECS,
            )?;
            debug!(
                "Seeked partition {} to offset {} for timestamp {}",
                elem.partition(),
                offset,
                timestamp_ms
            );
        } else {
            debug!(
                "Seeking partition {} to end as no offset found for timestamp {}",
                elem.partition(),
                timestamp_ms
            );
            // If we didn't find an offset for the timestamp, seek to the end
            consumer.seek(
                elem.topic(),
                elem.partition(),
                rdkafka::Offset::End,
                KAFKA_TIMEOUT_SECS,
            )?;
        }
    }

    Ok(())
}

// Position the given `targets` partitions for the long-running consumer: those
// with a committed offset resume from it; the rest are resolved by `timestamp_ms`
// (an old day's topic has nothing at/after it -> seek to end/skip; today's and
// any newly-created daily topic -> its start).
fn position_partitions(
    consumer: &BaseConsumer,
    targets: &TopicPartitionList,
    timestamp_ms: i64,
) -> KafkaResult<()> {
    if targets.count() == 0 {
        return Ok(());
    }
    let committed = consumer.committed(KAFKA_TIMEOUT_SECS)?;

    let mut to_resolve = TopicPartitionList::new();
    for elem in targets.elements() {
        match committed
            .find_partition(elem.topic(), elem.partition())
            .map(|c| c.offset())
        {
            Some(rdkafka::Offset::Offset(offset)) => {
                consumer.seek(
                    elem.topic(),
                    elem.partition(),
                    rdkafka::Offset::Offset(offset),
                    KAFKA_TIMEOUT_SECS,
                )?;
            }
            _ => {
                to_resolve.add_partition_offset(
                    elem.topic(),
                    elem.partition(),
                    rdkafka::Offset::Offset(timestamp_ms),
                )?;
            }
        }
    }

    if to_resolve.count() == 0 {
        return Ok(());
    }
    let resolved = consumer.offsets_for_times(to_resolve, KAFKA_TIMEOUT_SECS)?;
    // Persist the resolved start/skip position for each uncommitted partition.
    // With the default (eager) assignor, discovering the next day's topic revokes
    // and reassigns everything; committing here means an old skipped topic resumes
    // from its end (stays skipped) rather than resetting to `earliest` and
    // replaying, and today's topic resumes from its start.
    let mut to_commit = TopicPartitionList::new();
    for elem in resolved.elements() {
        let offset = match elem.offset() {
            rdkafka::Offset::Offset(offset) => offset,
            // No message at/after the timestamp (an old or empty topic): resolve
            // the end offset numerically so it can be committed and skipped.
            _ => {
                consumer
                    .fetch_watermarks(elem.topic(), elem.partition(), KAFKA_TIMEOUT_SECS)?
                    .1
            }
        };
        consumer.seek(
            elem.topic(),
            elem.partition(),
            rdkafka::Offset::Offset(offset),
            KAFKA_TIMEOUT_SECS,
        )?;
        to_commit.add_partition_offset(
            elem.topic(),
            elem.partition(),
            rdkafka::Offset::Offset(offset),
        )?;
    }
    if to_commit.count() > 0 {
        consumer.commit(&to_commit, CommitMode::Sync)?;
    }
    Ok(())
}

// Position only partitions that have appeared in the assignment since the last
// call, recording them in `positioned`. Positioning each partition exactly once
// (rather than re-seeking the whole assignment whenever it changes) means a
// still-active partition is never rewound to its lagging committed offset, and a
// same-size membership swap (which a count check would miss) is still handled.
// A genuinely-new daily topic is correctly picked up here; an old retained topic
// re-assigned mid-run is skipped. (A brand-new partition consumed between poll
// and this check would be rewound once here — a bounded, idempotent replay.)
fn reposition_new_partitions(
    consumer: &BaseConsumer,
    timestamp_ms: i64,
    positioned: &mut std::collections::HashSet<(String, i32)>,
) -> KafkaResult<()> {
    let assignment = consumer.assignment()?;
    let mut fresh = TopicPartitionList::new();
    for elem in assignment.elements() {
        if !positioned.contains(&(elem.topic().to_string(), elem.partition())) {
            fresh.add_partition_offset(elem.topic(), elem.partition(), rdkafka::Offset::Invalid)?;
        }
    }
    if fresh.count() == 0 {
        return Ok(());
    }
    position_partitions(consumer, &fresh, timestamp_ms)?;
    for elem in fresh.elements() {
        positioned.insert((elem.topic().to_string(), elem.partition()));
    }
    Ok(())
}

// A rebalance landing mid-pass must not take the consumer down; the caller
// retries on its next pass. Reports whether the pass went through.
fn position_or_warn(
    consumer: &BaseConsumer,
    timestamp_ms: i64,
    positioned: &mut std::collections::HashSet<(String, i32)>,
) -> bool {
    match reposition_new_partitions(consumer, timestamp_ms, positioned) {
        Ok(()) => true,
        Err(error) => {
            log_error!(WARN, error, "failed to position partitions, will retry");
            false
        }
    }
}

/// Where a consumer starts reading, and whether it advances with the clock.
///
/// The caller states which it wants rather than having it inferred from the
/// timestamp. Inferring would get two cases wrong: a pinned run that happens to
/// name today's date is still a pinned run, and a consumer started moments
/// before UTC midnight would race the comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartDate {
    /// Begin at the current UTC day and roll onto each new daily topic as
    /// midnight passes. What the long-running consumers use.
    Current,
    /// Begin at a past instant and catch up every night since, then roll like
    /// `Current`.
    From(i64),
    /// Pinned to one instant — replay, backfill, benchmarks. Never rolls over:
    /// advancing it would unsubscribe the consumer from the very topic it was
    /// started to read.
    Pinned(i64),
}

impl StartDate {
    /// Unix seconds that newly-assigned partitions are positioned to.
    pub fn timestamp(self) -> i64 {
        match self {
            StartDate::Current => current_day_midnight(),
            StartDate::From(timestamp) | StartDate::Pinned(timestamp) => timestamp,
        }
    }

    /// Whether the subscription should roll onto each new UTC day.
    pub fn follows_clock(self) -> bool {
        matches!(self, StartDate::Current | StartDate::From(_))
    }

    /// Resolve into the instants the poll loop runs on, widening the survey's
    /// configured window if `From` reaches further back than it.
    pub fn plan(self, configured_window_days: u64) -> StartPlan {
        let today = current_day_midnight();
        let position_timestamp = match self {
            StartDate::Current => today,
            StartDate::From(timestamp) | StartDate::Pinned(timestamp) => timestamp,
        };
        // `From` reaches *back* to its date; the window still ends at today.
        let subscription_timestamp = match self {
            StartDate::Pinned(timestamp) => timestamp,
            _ => today,
        };
        let window_days = match self {
            StartDate::From(_) => {
                configured_window_days.max(days_between(position_timestamp, subscription_timestamp))
            }
            _ => configured_window_days,
        };
        StartPlan {
            position_timestamp,
            subscription_timestamp,
            window_days,
            follows_clock: self.follows_clock(),
            positions_at_rolled_day: matches!(self, StartDate::Current),
        }
    }
}

/// Resolved once by `consume`, so every worker thread agrees on the day even if
/// they spawn either side of UTC midnight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartPlan {
    /// A partition holding nothing at or after this is skipped to its end.
    pub position_timestamp: i64,
    /// Instant the subscription window ends at.
    pub subscription_timestamp: i64,
    pub window_days: u64,
    pub follows_clock: bool,
    /// False while catching up, whose older nights the rolled day would skip.
    pub positions_at_rolled_day: bool,
}

fn current_day_midnight() -> i64 {
    let now = chrono::Utc::now();
    now.date_naive()
        .and_hms_opt(0, 0, 0)
        .map(|midnight| midnight.and_utc().timestamp())
        .unwrap_or_else(|| now.timestamp())
}

fn days_between(from: i64, to: i64) -> u64 {
    let day = |timestamp| chrono::DateTime::from_timestamp(timestamp, 0).map(|dt| dt.date_naive());
    match (day(from), day(to)) {
        (Some(from), Some(to)) => (to - from).num_days().max(0) as u64,
        _ => 0,
    }
}

/// UTC-midnight timestamp to roll onto, or `None` to stay put. Split out so the
/// decision is testable without a broker.
fn rollover_target(
    follow_current_date: bool,
    subscribed_date: chrono::NaiveDate,
    today: chrono::NaiveDate,
) -> Option<i64> {
    if !follow_current_date || today == subscribed_date {
        return None;
    }
    today
        .and_hms_opt(0, 0, 0)
        .map(|midnight| midnight.and_utc().timestamp())
}

/// Re-subscribe if the UTC date has rolled onto a new daily topic.
///
/// Must be called from both poll loops: a consumer started between nights never
/// receives a first message, so it never leaves the initial-assignment loop.
fn roll_subscription_if_needed(
    consumer: &BaseConsumer,
    subscribe_to: &Arc<dyn Fn(i64, u64) -> Vec<String> + Send + Sync>,
    plan: StartPlan,
    subscribed_date: &mut chrono::NaiveDate,
    position_timestamp: &mut i64,
) {
    let today = chrono::Utc::now().date_naive();
    let Some(new_timestamp) = rollover_target(plan.follows_clock, *subscribed_date, today) else {
        return;
    };
    let rolled = subscribe_to(new_timestamp, plan.window_days);
    let rolled_refs: Vec<&str> = rolled.iter().map(|s| s.as_str()).collect();
    match consumer.subscribe(&rolled_refs) {
        Ok(()) => {
            info!(
                "Rolled subscription from {} to {}, now subscribed to {:?}",
                subscribed_date, today, rolled
            );
            *subscribed_date = today;
            if plan.positions_at_rolled_day {
                *position_timestamp = new_timestamp;
            }
        }
        // Stay on the current subscription; the next check retries.
        Err(error) => log_error!(error, "failed to roll subscription onto new day"),
    }
}

/// Whether a poll error refers to a partition whose topic no longer exists
/// upstream — typically an expired daily topic still present in the group's
/// assignment. Unlike a transient broker error these never recover, so they
/// must not be retried on the same backoff.
fn is_expired_partition(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::MessageConsumption(
            rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition
                | rdkafka::error::RDKafkaErrorCode::UnknownTopic
                | rdkafka::error::RDKafkaErrorCode::UnknownPartition
        )
    )
}

// No `#[instrument]` here: this function is the long-lived Kafka poll loop;
// instrumenting it would funnel every per-message span into one giant trace.
pub async fn consumer(
    id: &str,
    subscribe_to: Arc<dyn Fn(i64, u64) -> Vec<String> + Send + Sync>,
    output_queue: &str,
    max_in_queue: usize,
    plan: StartPlan,
    config: &AppConfig,
    survey_consumer_config: &KafkaConsumerConfig,
    exit_on_eof: bool,
    survey: &'static str,
) -> Result<(), ConsumerError> {
    let server = survey_consumer_config.server.clone();
    // Throwaway group so a one-shot replay never rebalances the long-running
    // consumers; the timestamp suffix keeps one run's threads together.
    let group_id = if exit_on_eof {
        format!(
            "{}-oneshot-{}",
            survey_consumer_config.group_id, plan.position_timestamp
        )
    } else {
        survey_consumer_config.group_id.clone()
    };
    let username = survey_consumer_config.username.clone();
    let password = survey_consumer_config.password.clone();

    let subscription = subscribe_to(plan.subscription_timestamp, plan.window_days);

    let topics: Vec<String> = if exit_on_eof {
        // One-shot drain: keep only concrete topics that have messages, exit if none.
        let mut non_empty_topics = vec![];
        for topic in &subscription {
            let nb_messages = count_messages(&server, topic)?;
            match nb_messages {
                Some(0) => {
                    info!(
                        "No messages available in topic {}, skipping it for consumer {}",
                        topic, id
                    );
                }
                Some(_) => {
                    debug!(
                        "Topic {} has messages, including it for consumer {}",
                        topic, id
                    );
                    non_empty_topics.push(topic.clone());
                }
                None => {
                    info!("Topic {} not found, skipping it for consumer {}", topic, id);
                }
            }
        }
        if non_empty_topics.is_empty() {
            info!(
                "No messages available in any topic, exiting consumer {}",
                id
            );
            return Ok(());
        }
        non_empty_topics
    } else {
        // Long-running: subscribe to the pattern(s); new daily topics roll over automatically.
        debug!("exit_on_eof is false, consuming indefinitely");
        subscription
    };

    let mut client_config = ClientConfig::new();
    client_config
        // Uncomment the following to get logs from kafka (RUST_LOG doesn't work):
        // .set("debug", "consumer,cgrp,topic,fetch")
        .set("bootstrap.servers", &server)
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        // Manual offset storage so the bootstrap seek isn't clobbered; in the
        // prod path we store offsets explicitly after each push (see below).
        .set("enable.auto.offset.store", "false");

    if !exit_on_eof {
        // Long-running consumer: commit stored offsets so restarts and rebalances
        // resume in place instead of replaying. We deliberately keep the default
        // (eager) assignment strategy: switching a live consumer group to
        // cooperative-sticky is an incompatible rebalance protocol, and members
        // already in the group on the eager protocol reject the join with
        // InconsistentGroupProtocol.
        client_config
            .set("enable.auto.commit", "true")
            // How quickly a newly-created daily topic is discovered and joined.
            .set("topic.metadata.refresh.interval.ms", "10000");
    }

    if let (Some(username), Some(password)) = (username, password) {
        client_config
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", "SCRAM-SHA-512")
            .set("sasl.username", username)
            .set("sasl.password", password);
    } else {
        client_config.set("security.protocol", "PLAINTEXT");
    }

    let consumer: BaseConsumer = client_config
        .create()
        .inspect_err(as_error!("failed to create consumer"))?;

    // Subscribe to topic(s)/pattern(s) - broker handles partition assignment.
    let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
    consumer
        .subscribe(&topic_refs)
        .inspect_err(as_error!("failed to subscribe to topics"))?;

    // Wait for initial assignment
    debug!("Waiting for partition assignment...");

    // Partitions already positioned (prod path only); see reposition_new_partitions.
    let mut positioned: std::collections::HashSet<(String, i32)> = std::collections::HashSet::new();

    // Declared here because the initial-assignment loop must roll over too.
    const ROLLOVER_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
    let mut last_rollover_check = std::time::Instant::now();
    // Advance together on rollover (unless catching up), so a new topic starts
    // at its own day.
    let mut subscribed_date = chrono::DateTime::from_timestamp(plan.subscription_timestamp, 0)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| chrono::Utc::now().date_naive());
    let mut position_timestamp = plan.position_timestamp;

    // Idle and wedged look identical at INFO otherwise.
    const WAITING_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
    let mut last_waiting_log = std::time::Instant::now();

    // Poll once to trigger rebalance and get assignment
    loop {
        // This loop can sit across a UTC midnight; roll while waiting.
        if !exit_on_eof && last_rollover_check.elapsed() >= ROLLOVER_CHECK_INTERVAL {
            last_rollover_check = std::time::Instant::now();
            roll_subscription_if_needed(
                &consumer,
                &subscribe_to,
                plan,
                &mut subscribed_date,
                &mut position_timestamp,
            );
            position_or_warn(&consumer, position_timestamp * 1000, &mut positioned);
        }
        if !exit_on_eof && last_waiting_log.elapsed() >= WAITING_LOG_INTERVAL {
            last_waiting_log = std::time::Instant::now();
            info!(
                "Consumer {} waiting for first message; subscribed for {} (idle between nights is normal)",
                id, subscribed_date
            );
        }
        match consumer.poll(KAFKA_TIMEOUT_SECS) {
            Some(Ok(_msg)) => {
                debug!("Got initial assignment, positioning partitions...");
                if exit_on_eof {
                    // One-shot drain: (re)read from the requested timestamp.
                    seek_to_timestamp(&consumer, plan.position_timestamp * 1000)?;
                } else if !position_or_warn(&consumer, position_timestamp * 1000, &mut positioned) {
                    // Consuming unpositioned would replay an old night from
                    // `earliest`; poll again and retry.
                    continue;
                }
                break;
            }
            Some(Err(e)) => {
                if exit_on_eof {
                    if let rdkafka::error::KafkaError::MessageConsumption(
                        rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition,
                    ) = e
                    {
                        info!("Topic or partition unknown, exiting consumer {}", id);
                        return Ok(());
                    }
                }
                error!("Error during initial poll: {:?}", e);
                // sleep and retry
                tokio::time::sleep(core::time::Duration::from_secs(1)).await;
            }
            None => {
                debug!("No message received yet, polling again...");
                // sleep and retry
                tokio::time::sleep(core::time::Duration::from_secs(1)).await;
            }
        }
    }

    // OTel attributes informed by https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
    let consumer_attrs = [
        KeyValue::new("messaging.system", "kafka"),
        KeyValue::new("messaging.destination.name", topics.join(",")),
        KeyValue::new("messaging.consumer.group.name", group_id.to_string()),
        KeyValue::new("messaging.operation.name", "poll"),
        KeyValue::new("messaging.operation.type", "receive"),
        KeyValue::new("messaging.client.id", id.to_string()),
        KeyValue::new("survey", survey),
    ];
    let ok_attrs: Vec<KeyValue> = consumer_attrs
        .iter()
        .cloned()
        .chain([KeyValue::new("status", "ok")])
        .collect();
    let input_error_attrs: Vec<KeyValue> = consumer_attrs
        .iter()
        .cloned()
        .chain([
            KeyValue::new("status", "error"),
            KeyValue::new("reason", "kafka_poll"),
        ])
        .collect();
    let output_error_attrs: Vec<KeyValue> = consumer_attrs
        .iter()
        .cloned()
        .chain([
            KeyValue::new("status", "error"),
            KeyValue::new("reason", "kafka_send"),
        ])
        .collect();

    let mut con = config
        .build_redis()
        .await
        .inspect_err(as_error!("failed to connect to redis"))?;

    let mut total: u64 = 0;
    // start timer
    let start = std::time::Instant::now();

    // Emit a periodic "still alive" line so low-volume surveys (e.g. WINTER,
    // which may push far fewer than the every-1000 progress log) visibly report
    // progress instead of looking stalled. Also confirms the loop is polling
    // even when idle between nights.
    const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
    let mut last_heartbeat = std::time::Instant::now();
    let mut total_at_last_heartbeat: u64 = 0;

    // Expired-partition poll errors repeat indefinitely, so they get a short
    // backoff (enough to keep the loop off a hot spin) and a throttled log.
    const EXPIRED_POLL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);
    const EXPIRED_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);
    let mut last_expired_log = std::time::Instant::now()
        .checked_sub(EXPIRED_LOG_INTERVAL)
        .unwrap_or_else(std::time::Instant::now);

    debug!("Starting Kafka consumer loop...");

    // Process the rest normally
    loop {
        if !exit_on_eof && last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
            let since = total - total_at_last_heartbeat;
            info!(
                "Consumer {} alive: {} messages pushed to '{}' ({} in the last {:?}, {:?} total)",
                id,
                total,
                output_queue,
                since,
                last_heartbeat.elapsed(),
                start.elapsed(),
            );
            last_heartbeat = std::time::Instant::now();
            total_at_last_heartbeat = total;
        }
        // Roll the subscription onto the current UTC day, then pick up any
        // newly-assigned partitions (the new day's topic, or a rebalance).
        if !exit_on_eof && last_rollover_check.elapsed() >= ROLLOVER_CHECK_INTERVAL {
            last_rollover_check = std::time::Instant::now();
            roll_subscription_if_needed(
                &consumer,
                &subscribe_to,
                plan,
                &mut subscribed_date,
                &mut position_timestamp,
            );
            position_or_warn(&consumer, position_timestamp * 1000, &mut positioned);
        }
        if max_in_queue > 0 && total % 1000 == 0 {
            // Pause rather than sleep: a consumer that stops polling for
            // `max.poll.interval.ms` (5 min) is fenced out of the group.
            let mut paused = false;
            loop {
                let nb_in_queue = con
                    .llen::<&str, usize>(&output_queue)
                    .await
                    .inspect_err(as_error!("failed to get queue length"))?;
                if nb_in_queue < max_in_queue {
                    break;
                }
                if !paused {
                    info!(
                        "{} (limit: {}) items in queue, pausing consumption...",
                        nb_in_queue, max_in_queue
                    );
                    consumer.pause(&consumer.assignment()?)?;
                    paused = true;
                }
                // Yields nothing while paused, but forwards anything still in flight.
                if let Some(Ok(message)) = consumer.poll(core::time::Duration::from_millis(500)) {
                    let payload = message.payload().unwrap_or_default();
                    con.rpush::<&str, Vec<u8>, usize>(&output_queue, payload.to_vec())
                        .await
                        .inspect_err(|error| {
                            log_error!(error, "failed to push message to queue");
                            ALERT_PROCESSED.add(1, &output_error_attrs);
                        })?;
                    if !exit_on_eof {
                        if let Err(error) = consumer.store_offset_from_message(&message) {
                            log_error!(error, "failed to store offset");
                        }
                    }
                    ALERT_PROCESSED.add(1, &ok_attrs);
                    total += 1;
                }
            }
            if paused {
                consumer.resume(&consumer.assignment()?)?;
                info!("queue drained, resuming consumption");
            }
        }
        match consumer.poll(KAFKA_TIMEOUT_SECS) {
            Some(Ok(message)) => {
                let payload = message.payload().unwrap_or_default();
                con.rpush::<&str, Vec<u8>, usize>(&output_queue, payload.to_vec())
                    .await
                    .inspect_err(|error| {
                        log_error!(error, "failed to push message to queue");
                        ALERT_PROCESSED.add(1, &output_error_attrs);
                    })?;
                trace!("Pushed message to redis");
                // Mark processed so the periodic auto-commit advances the offset
                // (only the prod path commits; the drain path never stores).
                if !exit_on_eof {
                    if let Err(error) = consumer.store_offset_from_message(&message) {
                        log_error!(error, "failed to store offset");
                    }
                }
                ALERT_PROCESSED.add(1, &ok_attrs);
                total += 1;
                if total % 1000 == 0 {
                    info!(
                        "Consumer {} pushed {} items since {:?}",
                        id,
                        total,
                        start.elapsed()
                    );
                }
                if total == 1 {
                    info!("Consumer received first message, continuing...");
                }
            }
            Some(Err(e)) => {
                ALERT_PROCESSED.add(1, &input_error_attrs);
                // A partition whose topic has expired upstream fails on every
                // poll and never recovers. Serving each one a full second of
                // backoff turns this loop into a treadmill: once enough expired
                // partitions are in the assignment the consumer can no longer
                // drain the live topic, and it never reaches the idle branch
                // below — which is where rollover would otherwise be noticed.
                // Log sparsely and keep polling instead.
                if is_expired_partition(&e) {
                    if last_expired_log.elapsed() >= EXPIRED_LOG_INTERVAL {
                        last_expired_log = std::time::Instant::now();
                        warn!(
                            "Polling a partition whose topic no longer exists upstream \
                             (subscribed date {}): {}",
                            subscribed_date, e
                        );
                    }
                    tokio::time::sleep(EXPIRED_POLL_BACKOFF).await;
                    continue;
                }
                error!("Error while consuming from Kafka, retrying: {}", e);
                tokio::time::sleep(core::time::Duration::from_secs(1)).await;
                continue;
            }
            None => {
                debug!("No message available");
                if exit_on_eof {
                    info!("No more messages, exiting consumer {}", id);
                    break;
                }
                // Idle: catch a topic that has just rolled over.
                position_or_warn(&consumer, position_timestamp * 1000, &mut positioned);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod rollover_tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn test_rolls_onto_a_new_utc_day() {
        let target = rollover_target(true, d(2026, 8, 13), d(2026, 8, 14))
            .expect("a new day must produce a rollover target");
        assert_eq!(target % 86_400, 0, "targets UTC midnight");
        assert_eq!(
            chrono::DateTime::from_timestamp(target, 0)
                .unwrap()
                .date_naive(),
            d(2026, 8, 14)
        );
    }

    #[test]
    fn test_no_roll_within_the_same_day() {
        assert!(rollover_target(true, d(2026, 8, 14), d(2026, 8, 14)).is_none());
    }

    // A pinned run (replay, backfill, benchmark) must never chase the clock.
    #[test]
    fn test_pinned_run_never_rolls() {
        assert!(rollover_target(false, d(2025, 3, 11), d(2026, 8, 14)).is_none());
    }

    // A stale subscription rolls all the way to today, not one day forward.
    #[test]
    fn test_rolls_across_a_multi_day_gap() {
        let target = rollover_target(true, d(2026, 8, 12), d(2026, 8, 14))
            .expect("a stale subscription must roll forward however far behind it is");
        assert_eq!(
            chrono::DateTime::from_timestamp(target, 0)
                .unwrap()
                .date_naive(),
            d(2026, 8, 14),
            "rolls to today, not merely one day forward"
        );
    }
}
