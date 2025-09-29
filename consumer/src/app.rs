use crate::{ConsumerTask, config::AppConfig};
use boom::utils::o11y::{logging::build_subscriber, metrics::init_metrics};

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use color_eyre::eyre::Result;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};
use tokio::{sync::watch, task};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

pub static INIT_SUBSCRIBER: LazyLock<()> = LazyLock::new(|| {
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to set global subscriber");
});

/// Entrypoint type representing an initialized application.
pub struct App {
    clear_first: bool,
    consumer_count: usize,
    consumer_task: ConsumerTask,
    config: AppConfig,
    redis_uri: String,
    meter_provider: SdkMeterProvider,
    counter: Arc<AtomicUsize>,
}

impl App {
    /// Create a new instance and initialize telemetry.
    pub fn new(
        clear_first: bool,
        consumer_count: usize,
        consumer_task: ConsumerTask,
        config: AppConfig,
        redis_uri: String,
    ) -> Self {
        LazyLock::force(&INIT_SUBSCRIBER);

        let meter_provider = init_metrics(
            "consumer".into(),
            config.instance_id.clone(),
            config.deployment_env.clone(),
        )
        .expect("failed to initialize metrics");

        let counter = Arc::new(AtomicUsize::new(0));
        Self {
            clear_first,
            consumer_count,
            consumer_task,
            config,
            redis_uri,
            meter_provider,
            counter,
        }
    }

    /// The application entrypoint: spawn consumer tasks and wait for them to
    /// finish.
    ///
    /// The consumer tasks receive messages from kafka and enqueue them in
    /// redis. Each consumer gets a more-or-less even share of the topic's
    /// partitions, all set to start at the configured offset. A task finishes
    /// on its own when it times out waiting to receive a message. The tasks can
    /// also be told to finish early with ctrl-c.
    #[instrument(
        name = "app",
        skip(self),
        fields(instance.id = %self.config.instance_id),
    )]
    pub async fn run(self) {
        info!(
            deployment.env = %self.config.deployment_env,
            kafka.topic = %self.consumer_task.topic(),
            kafka.offset = ?self.consumer_task.offset(),
            redis.key = %self.consumer_task.key(),
            "starting the application",
        );
        let count = match self.try_run().await {
            Ok(count) => count,
            Err(error) => {
                error!(error.message = %error, error.details = ?error, "application error");
                return;
            }
        };
        info!("done; consumed {count} messages");
    }

    async fn try_run(&self) -> Result<usize> {
        let (handles, termination_sender) = self.start().await?;

        tokio::select! {
            _ = join(handles, termination_sender) => (),
            result = self.report_progress() => result?,
        }

        if let Err(error) = self.meter_provider.shutdown() {
            warn!(error.message = %error, "failed to shut down the meter provider");
        }
        Ok(self.message_count())
    }

    async fn start(&self) -> Result<(HashMap<Uuid, task::JoinHandle<()>>, watch::Sender<()>)> {
        let mut redis_connection = connect_to_redis(&self.redis_uri).await?;

        if self.clear_first {
            let key = self.consumer_task.key();
            debug!("clearing redis queue");
            redis_connection.del(key).await?;
        }

        let all_partition_ids = self.consumer_task.fetch_partition_ids()?;
        let partition_count = all_partition_ids.len();
        if partition_count == 0 {
            warn!("found {} partitions", partition_count);
        } else {
            debug!("found {} partitions", partition_count);
        }

        let mut handles = HashMap::with_capacity(self.consumer_count);
        let (tx, rx) = watch::channel(());
        for partition_ids in self.group_partition_ids(&all_partition_ids) {
            let id = Uuid::new_v4();
            debug!(task.id = %id, "spawning consumer task");
            let handle = self.consumer_task.spawn(
                rx.clone(),
                id,
                partition_ids,
                redis_connection.clone(),
                self.counter.clone(),
            )?;
            handles.insert(id, handle);
        }

        // Drop the receiver so the app can terminate when all its clones are
        // dropped:
        drop(rx);

        debug!("application startup complete");
        Ok((handles, tx))
    }

    fn group_partition_ids(&self, partition_ids: &[i32]) -> Vec<Vec<i32>> {
        let mut groups = vec![vec![]; self.consumer_count];
        for (i, id) in partition_ids.iter().enumerate() {
            groups[i % self.consumer_count].push(*id)
        }
        groups
    }

    async fn report_progress(&self) -> Result<()> {
        let delay = Duration::from_secs(self.config.progress_report_interval);
        loop {
            tokio::time::sleep(delay).await;
            info!("messages consumed: {}", self.message_count());
        }
    }

    fn message_count(&self) -> usize {
        self.counter.load(Ordering::Relaxed)
    }
}

async fn connect_to_redis(url: &str) -> Result<MultiplexedConnection> {
    debug!("connecting to redis");
    let connection = redis::Client::open(url)?
        .get_multiplexed_async_connection()
        .await?;
    Ok(connection)
}

async fn join(
    mut handles: HashMap<Uuid, task::JoinHandle<()>>,
    termination_sender: watch::Sender<()>,
) {
    debug!("joining consumer tasks");
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            match result {
                Ok(_) => debug!("received ctrl-c"),
                Err(error) => warn!(error.message = %error, "failed to listen for ctrl-c, shutting down"),
            }
            debug!("sending termination signal to consumer tasks");
            termination_sender.send(()).unwrap_or_else(|error| {
                warn!(error.message = %error, "failed to send termination signal, consumer tasks likely already completed")
            });
        }

        // The channel sender closes when all of the receivers have been
        // dropped, indicating all of the consumer tasks have completed.
        _ = termination_sender.closed() => {
            debug!("termination sender closed");
        },
    }

    for (id, handle) in handles.drain() {
        debug!(task.id = %id, "joining consumer task");
        handle.await.unwrap_or_else(
            |error| warn!(task.id = %id, error.message = %error, "error while awaiting consumer"),
        );
    }
    debug!("consumer tasks joined");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Source, config::TaskConfig};

    use std::{collections::HashSet, fmt::Debug, hash::Hash, time::Duration};

    use color_eyre::eyre::bail;
    use nix::{
        sys::signal::{Signal, kill},
        unistd::getpid,
    };
    use rdkafka::{
        ClientConfig, Offset,
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        client::DefaultClientContext,
        consumer::BaseConsumer,
        producer::{FutureProducer, FutureRecord},
    };
    use redis::AsyncTypedCommands;

    #[derive(Clone)]
    struct TestSource {
        id: Uuid,
        bootstrap_servers: String,
    }

    impl Source for TestSource {
        fn make_base_consumer(&self) -> Result<BaseConsumer> {
            let base_consumer = ClientConfig::new()
                .set("bootstrap.servers", &self.bootstrap_servers)
                .set("group.id", self.group_id())
                .create()?;
            Ok(base_consumer)
        }

        fn group_id(&self) -> String {
            format!("test_group_{}", self.id)
        }

        fn topic(&self) -> String {
            format!("test_topic_{}", self.id)
        }

        fn key(&self) -> String {
            format!("test_key_{}", self.id)
        }
    }

    struct Fixture {
        source: TestSource,
        consumer_count: usize,
        partition_count: usize,
        records: Vec<i64>,
        redis_uri: String,
        redis_connection: MultiplexedConnection,
    }

    impl Fixture {
        async fn new() -> Result<Self> {
            let id = Uuid::new_v4();
            let source = TestSource {
                id,
                bootstrap_servers: "localhost:9092".into(),
            };
            let records = Self::generate_records(15, 1316926800000, 10);
            let redis_uri = String::from("redis://localhost:6379/");
            let redis_connection = connect_to_redis(&redis_uri).await?;
            let fixture = Self {
                source,
                consumer_count: 2,
                partition_count: 4,
                records,
                redis_uri,
                redis_connection,
            };
            fixture.create_topic().await?;
            Ok(fixture)
        }

        // Generate n records evenly spaced in time starting from the given
        // timestamp (in ms since Unix epoch) and distributed evenly across the
        // given number of partitions.
        fn generate_records(
            record_count: usize,
            start_timestamp: i64,
            timestamp_spacing: i64,
        ) -> Vec<i64> {
            (0..record_count)
                .map(|i| start_timestamp + (i as i64) * timestamp_spacing)
                .collect::<Vec<_>>()
        }

        fn admin_client(&self) -> Result<AdminClient<DefaultClientContext>> {
            let client: AdminClient<_> = rdkafka::config::ClientConfig::new()
                .set("bootstrap.servers", &self.source.bootstrap_servers)
                .create()?;
            Ok(client)
        }

        async fn create_topic(&self) -> Result<()> {
            let topic = self.source.topic();
            let new_topic = NewTopic::new(
                &topic,
                self.partition_count as i32,
                TopicReplication::Fixed(1),
            );
            self.admin_client()?
                .create_topics(&[new_topic], &AdminOptions::new())
                .await?;
            Ok(())
        }

        async fn delete_topic(&self) -> Result<()> {
            let topic = self.source.topic();
            self.admin_client()?
                .delete_topics(&[&topic], &AdminOptions::new())
                .await?;
            Ok(())
        }

        async fn produce_messages<R>(&self, range: R) -> Result<()>
        where
            R: std::slice::SliceIndex<[i64], Output = [i64]>,
        {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &self.source.bootstrap_servers)
                .create()?;

            let topic = self.source.topic();
            for (i, timestamp) in self.records[range].iter().enumerate() {
                let partition_key = (i % self.partition_count).to_string();
                let payload = timestamp.to_string();
                let future_record = FutureRecord::to(&topic)
                    .key(&partition_key)
                    .payload(&payload)
                    .timestamp(*timestamp);
                producer
                    .send(future_record, Duration::from_secs(1))
                    .await
                    .map_err(|(error, _)| error)?;
            }
            tokio::time::sleep(Duration::from_secs(1)).await; // Give Kafka a moment to process
            Ok(())
        }

        fn app(&self, offset: Offset) -> AppBuilder {
            let task_config = TaskConfig {
                fetch_metadata_timeout: 5,
                timestamp_offset_lookup_timeout: 5,
                // The receive timeout needs to be tuned so that it's not so short
                // that the tests fail because the consumers are too "impatient",
                // and not so long that the tests become annoying to run.
                receive_timeout: 2,
                enqueue_timeout: 1,
                enqueue_retry_delay: 1,
                queue_soft_limit: 100,
                queue_check_delay: 1,
            };
            AppBuilder {
                source: self.source.clone(),
                offset,
                task_config,
                consumer_count: self.consumer_count,
                deployment_env: "test".into(),
                redis_uri: self.redis_uri.clone(),
            }
        }

        async fn get_queued(&self) -> Result<Vec<i64>> {
            let mut redis_connection = self.redis_connection.clone();
            let received = redis_connection
                .lrange(&self.source.key(), 0, -1)
                .await?
                .iter()
                .map(|x| x.parse().unwrap())
                .collect();
            Ok(received)
        }

        async fn drain_queue(&self) -> Result<Vec<i64>> {
            let received = self.get_queued().await?;
            self.delete_key().await?;
            Ok(received)
        }

        async fn delete_key(&self) -> Result<()> {
            let mut redis_connection = self.redis_connection.clone();
            redis_connection.del(&self.source.key()).await?;
            Ok(())
        }

        async fn cleanup(&self) -> Result<()> {
            self.delete_topic().await?;
            self.delete_key().await?;
            Ok(())
        }
    }

    struct AppBuilder {
        source: TestSource,
        offset: Offset,
        task_config: TaskConfig,
        consumer_count: usize,
        deployment_env: String,
        redis_uri: String,
    }

    impl AppBuilder {
        fn with_receive_timeout(&mut self, receive_timeout: u64) -> &mut Self {
            self.task_config.receive_timeout = receive_timeout;
            self
        }

        fn with_queue_soft_limit(&mut self, queue_soft_limit: usize) -> &mut Self {
            self.task_config.queue_soft_limit = queue_soft_limit;
            self
        }

        fn build(&self) -> App {
            let instance_id = Uuid::new_v4();
            let source: Box<dyn Source + Send + Sync + 'static> = Box::new(self.source.clone());
            let consumer_task =
                ConsumerTask::new(instance_id, source, self.offset, self.task_config.clone());
            let app_config = AppConfig {
                instance_id,
                deployment_env: self.deployment_env.clone(),
                progress_report_interval: 5,
            };
            App::new(
                false,
                self.consumer_count,
                consumer_task,
                app_config,
                self.redis_uri.clone(),
            )
        }

        // Shortcut for `.build().run()`
        async fn run(&self) {
            self.build().run().await;
        }
    }

    fn assert_set_eq<T: Debug + Eq + Hash>(lhs: &[T], rhs: &[T]) {
        assert_eq!(lhs.iter().collect::<HashSet<_>>(), rhs.iter().collect());
    }

    // This test is best run on its own because it emits ctrl-c, which is picked
    // up by all apps running concurrently.
    #[ignore]
    #[tokio::test]
    async fn shut_down_manually() -> Result<()> {
        let fixture = Fixture::new().await?;

        // With an unlimited receive timeout, the consumer runs forever even if
        // there's nothing to consume:
        let app = fixture
            .app(Offset::End)
            .with_receive_timeout(u64::MAX)
            .build();
        let handle = tokio::spawn(app.run());

        // Let the app run briefly, then send ctrl-c:
        tokio::time::sleep(Duration::from_secs(2)).await;
        kill(getpid(), Signal::SIGINT).unwrap();

        tokio::select! {
            // If the shutdown mechanism works, then this future will finish
            // first:
            _ = handle => (),
            // Otherwise, the timeout will expire and the test will fail:
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                bail!("test timed out")
            }
        }

        fixture.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn queue_has_backpressure() -> Result<()> {
        let mut fixture = Fixture::new().await?;
        fixture.consumer_count = 1; // A single consumer makes this easier to analyze

        // Produce messages and start consuming:
        fixture.produce_messages(..).await?;
        let queue_soft_limit = 2 * fixture.records.len() / 3;
        let app = fixture
            .app(Offset::Beginning)
            .with_queue_soft_limit(queue_soft_limit)
            .build();
        let handle = tokio::spawn(app.run());

        // Wait until the queue apparently stops growing:
        let mut redis_connection = fixture.redis_connection.clone();
        let key = fixture.source.key();
        let mut queue_size = 0;
        let mut was_equal = false;
        loop {
            let new_queue_size = redis_connection.llen(&key).await?;
            if new_queue_size >= queue_soft_limit && new_queue_size == queue_size {
                if was_equal {
                    break; // Can assume the queue has stabilized
                }
                was_equal = true;
            } else {
                queue_size = new_queue_size;
                was_equal = false;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // The queue should contain only the messages up to or maybe slightly
        // above the limit (though won't know precisely which messages):
        let mut all_received = fixture.get_queued().await?;
        assert!(all_received.len() >= queue_soft_limit);
        assert!(all_received.len() < fixture.records.len());

        // Empty the queue, unblocking the consumer. Check that the rest of the
        // messages were consumed:
        fixture.delete_key().await?;
        handle.await?;
        let mut received = fixture.get_queued().await?;
        all_received.append(&mut received);
        assert_set_eq(&all_received, &fixture.records);

        fixture.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn consume_from_beginning() -> Result<()> {
        let fixture = Fixture::new().await?;
        fixture.produce_messages(..).await?;

        // Produce and consume all messages:
        fixture.app(Offset::Beginning).run().await;
        let received = fixture.drain_queue().await?;
        assert_set_eq(&received, &fixture.records);

        // Consume all messages again:
        fixture.app(Offset::Beginning).run().await;
        let received = fixture.drain_queue().await?;
        assert_set_eq(&received, &fixture.records);

        fixture.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn consume_from_end() -> Result<()> {
        let (m, n) = (5, 9);
        let fixture = Fixture::new().await?;

        // Produce and consume first m messages:
        fixture.produce_messages(..m).await?;
        fixture.app(Offset::Beginning).run().await;
        let received = fixture.drain_queue().await?;
        assert_set_eq(&received, &fixture.records[..m]);

        // Produce n - m more (skipped by the consumer):
        fixture.produce_messages(m..n).await?;

        // Start consuming at the end, skipping past the m..n messages that were
        // just produced:
        let app = fixture.app(Offset::End).build();
        let handle = tokio::spawn(app.run());

        // Produce and consume the rest of the messages:
        tokio::time::sleep(Duration::from_secs(1)).await; // ensure the consumers don't miss anything
        fixture.produce_messages(n..).await?;
        handle.await?;
        let received = fixture.drain_queue().await?;
        assert_set_eq(&received, &fixture.records[n..]);

        fixture.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn consume_from_stored() -> Result<()> {
        let n = 8;
        let fixture = Fixture::new().await?;

        // Produce and consume first n messages:
        fixture.produce_messages(..n).await?;
        fixture.app(Offset::Beginning).run().await;
        let received = fixture.drain_queue().await?;
        assert_set_eq(&received, &fixture.records[..n]);

        // Produce the rest of the messages:
        fixture.produce_messages(n..).await?;

        // Consume the rest of the messages:
        fixture.app(Offset::Stored).run().await;
        let received = fixture.drain_queue().await?;
        assert_set_eq(&received, &fixture.records[n..]);

        fixture.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn consume_from_timestamp() -> Result<()> {
        let n = 7;
        let fixture = Fixture::new().await?;

        // Produce and consume all messages:
        fixture.produce_messages(..).await?;
        fixture.app(Offset::Beginning).run().await;
        let received = fixture.drain_queue().await?;
        assert_set_eq(&received, &fixture.records);

        // Consume again all messages on/after timestamp:
        let timestamp = fixture.records[n - 1].midpoint(fixture.records[n]);
        fixture.app(Offset::Offset(timestamp)).run().await;
        let received = fixture.drain_queue().await?;
        assert_set_eq(&received, &fixture.records[n..]);

        fixture.cleanup().await?;
        Ok(())
    }
}
