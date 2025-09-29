use crate::{Source, config::TaskConfig};
use boom::utils::o11y::metrics::CONSUMER_METER;

use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use color_eyre::eyre::{Result, eyre};
use opentelemetry::{KeyValue, metrics::Counter};
use rdkafka::{
    Message, Offset, TopicPartitionList,
    consumer::{BaseConsumer, CommitMode, Consumer as _},
};
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};
use tokio::sync::watch;
use tracing::{debug, error, info, instrument, trace, warn};
use uuid::Uuid;

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

/// Specification of a consumer task that can be spawned multiple times.
pub struct ConsumerTask {
    instance_id: Uuid,
    base_consumer_fn: Box<dyn Fn() -> Result<BaseConsumer> + Send + Sync + 'static>,
    group_id: String,
    topic: String,
    offset: Offset,
    key: String,
    config: TaskConfig,
}

impl ConsumerTask {
    /// Create a new instance.
    ///
    /// Calling this method doesn't actually start an asynchronous task; that
    /// only happens when `spawn` is called.
    pub fn new(
        instance_id: Uuid,
        source: Box<dyn Source + Send + Sync + 'static>,
        offset: Offset,
        config: TaskConfig,
    ) -> Self {
        let group_id = source.group_id();
        let topic = source.topic();
        let key = source.key();
        Self {
            instance_id,
            base_consumer_fn: Box::new(move || source.make_base_consumer()),
            group_id,
            topic,
            offset,
            key,
            config,
        }
    }

    fn base_consumer(&self) -> Result<BaseConsumer> {
        (self.base_consumer_fn)()
    }

    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) fn offset(&self) -> &Offset {
        &self.offset
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn fetch_partition_ids(&self) -> Result<Vec<i32>> {
        debug!("fetching partition ids");
        let base_consumer = self.base_consumer()?;
        let metadata = base_consumer.fetch_metadata(
            Some(&self.topic),
            Duration::from_secs(self.config.fetch_metadata_timeout),
        )?;
        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|topic| topic.name() == self.topic)
            .ok_or_else(|| eyre!("topic {} not found", self.topic))?;
        let partition_ids = topic_metadata
            .partitions()
            .into_iter()
            .map(|partition| partition.id())
            .collect();
        Ok(partition_ids)
    }

    pub(crate) fn spawn(
        &self,
        termination_receiver: watch::Receiver<()>,
        id: Uuid,
        partition_ids: Vec<i32>,
        redis_connection: MultiplexedConnection,
        counter: Arc<AtomicUsize>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let consumer = Consumer {
            instance_id: self.instance_id,
            id,
            base_consumer: self.base_consumer()?,
            group_id: self.group_id.clone(),
            topic: self.topic.clone(),
            partition_ids,
            offset: self.offset,
            key: self.key.clone(),
            config: self.config.clone(),
            counter,
        };
        let handle = tokio::spawn(async move {
            consumer.main(termination_receiver, redis_connection).await;
        });
        Ok(handle)
    }
}

pub(crate) struct Consumer {
    instance_id: Uuid,
    id: Uuid,
    base_consumer: BaseConsumer,
    group_id: String,
    topic: String,
    partition_ids: Vec<i32>,
    offset: Offset,
    key: String,
    config: TaskConfig,
    counter: Arc<AtomicUsize>,
}

impl Consumer {
    // Start the consumer and handle any errors.
    #[instrument(
        name = "consumer",
        skip_all,
        fields(instance.id = %self.instance_id, task.id = %self.id)
    )]
    pub(crate) async fn main(
        self,
        termination_receiver: watch::Receiver<()>,
        redis_connection: MultiplexedConnection,
    ) {
        info!(partition_ids = ?self.partition_ids, "starting consumer task");
        self.start(termination_receiver, redis_connection)
            .await
            .unwrap_or_else(
                |error| error!(error.message = %error, error.details = ?error, "consumer error"),
            );
        info!("consumer task finished")
    }

    // Assign the partitions, then wait for the consumer to finish or a
    // termination signal on the receiver.
    async fn start(
        self,
        mut termination_receiver: watch::Receiver<()>,
        mut redis_connection: MultiplexedConnection,
    ) -> Result<()> {
        self.assign_partitions()?;
        tokio::select! {
            result = termination_receiver.changed() => {
                result?;
                info!("received termination signal")
            }
            result = self.consume(&mut redis_connection) => {
                result?;
            }
        }
        Ok(())
    }

    // Assign the partitions and set the starting offset
    fn assign_partitions(&self) -> Result<()> {
        debug!("assigning partitions");
        // NOTE: Partitions are assigned manually as required by librdkafka's
        // assign API, which we're using to be able to set the starting offsets.
        // The other API---subscribe---enables automatic rebalancing and doesn't
        // require manual assignment, but it *only* supports stored offsets.
        let mut tpl = TopicPartitionList::new();
        for id in &self.partition_ids {
            tpl.add_partition_offset(&self.topic, *id, self.offset)?;
        }
        if let Offset::Offset(_) = self.offset {
            tpl = self.base_consumer.offsets_for_times(
                tpl,
                Duration::from_secs(self.config.timestamp_offset_lookup_timeout),
            )?;
        }
        self.base_consumer.assign(&tpl)?;
        let assignment = self.base_consumer.assignment()?; // Sanity check
        debug!(assignment = ?assignment);
        Ok(())
    }

    // Continuously receive messages from kafka via the consumer and enqueue
    // them in redis until the consumer times out waiting for a new message.
    //
    // The consumer offset is committed after each message is successfully
    // enqueued.
    //
    // If enqueuing a message causes the queue size to exceed the given limit,
    // then consuming is paused until the queue is back under the limit.
    //
    // If the consumer produces an error while receiving a message, the error is
    // logged at the WARN level and consuming continues.
    async fn consume(&self, redis_connection: &mut MultiplexedConnection) -> Result<()> {
        info!("consuming");

        // OTel attributes informed by https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
        let consumer_attrs = [
            KeyValue::new("messaging.system", "kafka"),
            KeyValue::new("messaging.destination.name", self.topic.clone()),
            KeyValue::new("messaging.consumer.group.name", self.group_id.clone()),
            KeyValue::new("messaging.operation.name", "poll"),
            KeyValue::new("messaging.operation.type", "receive"),
            KeyValue::new("messaging.client.id", self.instance_id.to_string()),
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

        let queue_soft_limit = self.config.queue_soft_limit;
        let delay = Duration::from_secs(self.config.queue_check_delay);
        loop {
            match self.recv().await {
                Ok(Some(message)) => {
                    trace!(
                        partition_id = %message.partition(),
                        timestamp = ?message.timestamp().to_millis(),
                        "message received"
                    );
                    let Some(payload) = message.payload() else {
                        trace!("payload empty, polling again");
                        continue;
                    };

                    let queue_size =
                        self.enqueue(redis_connection, payload)
                            .await
                            .inspect_err(|_| {
                                ALERT_PROCESSED.add(1, &output_error_attrs);
                            })?;
                    trace!(queue.size = %queue_size, "message enqueued");
                    ALERT_PROCESSED.add(1, &ok_attrs);
                    self.counter.fetch_add(1, Ordering::Relaxed);
                    self.base_consumer
                        .commit_message(&message, CommitMode::Sync)?;

                    // Pause if the queue is full.
                    //
                    // TODO: consider using a bounded tokio channel instead of
                    // pushing to the key directly. This would provide a more
                    // natural backpressure mechanism that doesn't require an
                    // explicity sleep parameter or querying redis.
                    if queue_size >= queue_soft_limit {
                        info!(queue.size = %queue_size, queue.limit = %queue_soft_limit, "queue full, waiting");
                        loop {
                            tokio::time::sleep(delay).await;
                            let queue_size = redis_connection.llen(&self.key).await?;
                            trace!(queue.size = %queue_size, queue.limit = %queue_soft_limit);
                            if queue_size < queue_soft_limit {
                                info!(queue.size = %queue_size, queue.limit = %queue_soft_limit, "queue open, resuming");
                                break;
                            }
                        }
                    }
                }
                Ok(None) => {
                    info!("timed out waiting for new messages, exiting");
                    break;
                }
                Err(error) => {
                    warn!(error.message = %error, "failed to receive message, polling again");
                    ALERT_PROCESSED.add(1, &input_error_attrs);
                }
            }
        }
        Ok(())
    }

    // Asynchronously poll the consumer until a message or an error is obtained, or
    // until the specified timeout period has elapsed.
    //
    // The returned result is either `Ok(Some(_))` containing a message received
    // from the consumer, `Ok(None)` if the timeout was reached before anything was
    // received, or `Err` for an error encountered while polling the consumer.
    //
    // This future is cancel-safe.
    async fn recv<'a>(&'a self) -> Result<Option<rdkafka::message::BorrowedMessage<'a>>> {
        let timer = tokio::time::sleep(Duration::from_secs(self.config.receive_timeout));
        tokio::pin!(timer);

        // Poll forever until something is obtained from the consumer
        let non_blocking_timeout = Duration::from_secs(0);
        let poll_loop = async {
            loop {
                match self.base_consumer.poll(non_blocking_timeout) {
                    Some(result) => {
                        let message = result?;
                        return Ok(message);
                    }
                    None => {
                        // Allow other tasks to make progress
                        tokio::task::yield_now().await;
                    }
                }
            }
        };
        tokio::pin!(poll_loop);

        tokio::select! {
            result = poll_loop => {
                result.map(|message| Some(message))
            }
            _ = timer => {
                Ok(None)
            }
        }
    }

    // Push a message payload to redis.
    //
    // The push operation is retried until it succeeds, with all errors logged at
    // the WARN level. If the timeout expires, then the last error is returned.
    async fn enqueue(
        &self,
        redis_connection: &mut MultiplexedConnection,
        payload: &[u8],
    ) -> Result<usize> {
        let timeout = Duration::from_secs(self.config.enqueue_timeout);
        let delay = Duration::from_secs(self.config.enqueue_retry_delay);
        let start = tokio::time::Instant::now();
        let queue_size = loop {
            match redis_connection.rpush(&self.key, payload).await {
                Ok(queue_size) => break Ok(queue_size),
                Err(error) => {
                    if start.elapsed() >= timeout {
                        warn!("timed out attempting to push to queue");
                        break Err(error);
                    }
                    warn!(error.message = %error, "error pushing to queue, retrying");
                    tokio::time::sleep(delay).await;
                }
            }
        }?;
        Ok(queue_size)
    }
}
