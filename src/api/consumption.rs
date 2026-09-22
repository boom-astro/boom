//! Attribution of Babamul Kafka stream consumption to Babamul users.
//!
//! # How attribution works
//!
//! Every Kafka credential a user creates gets a generated SCRAM username of
//! `babamul-{credential_id}` (see [`crate::api::routes::babamul`]), and the
//! Python package derives its consumer group from that username
//! (`{username}-{suffix}`). So a consumer group name carries the credential id,
//! and the credential id is stored on the user document — which is what lets us
//! turn an anonymous-looking consumer group back into "this user consumed this
//! much of the stream".
//!
//! Users are free to pick their own group suffix, and the Kafka ACL only
//! constrains groups to the `babamul-` prefix, so a group that matches no known
//! credential is possible. Those are reported under an `unattributed` bucket
//! rather than silently dropped.
//!
//! # What gets emitted
//!
//! Each sampling cycle reads every `babamul.*` topic's watermarks and each
//! consumer group's committed offsets, then emits:
//!
//! - **OTel gauges** (→ Prometheus → Grafana) for committed offsets, lag and
//!   the fraction of the retained stream consumed. This is the ops view: live,
//!   per-topic, high resolution.
//! - **A PostHog event** per user per cycle, but only when that user actually
//!   consumed something, carrying the *delta* since the previous cycle. This is
//!   the product view: which users are really using the stream, and how much.
//!
//! Deltas are computed from in-memory state. A restart re-establishes the
//! baseline and skips one cycle's delta for each group, which under-counts
//! slightly but can never double-count.

use crate::api::analytics::{AnalyticsClient, AnalyticsEvent};

use crate::conf::AppConfig;
use crate::utils::o11y::metrics::API_METER;

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;

use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::Database;
use opentelemetry::metrics::Gauge;
use opentelemetry::KeyValue;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;

const KAFKA_TIMEOUT: Duration = Duration::from_secs(10);
const BABAMUL_TOPIC_PREFIX: &str = "babamul.";
const BABAMUL_GROUP_PREFIX: &str = "babamul-";

/// Bucket used when a consumer group can't be traced back to a credential.
const UNATTRIBUTED: &str = "unattributed";

static COMMITTED_OFFSET: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    API_METER
        .u64_gauge("babamul.kafka.consumer.committed_offset")
        .with_unit("{message}")
        .with_description(
            "Total committed offset across partitions for a Babamul consumer group on a topic.",
        )
        .build()
});

static CONSUMER_LAG: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    API_METER
        .u64_gauge("babamul.kafka.consumer.lag")
        .with_unit("{message}")
        .with_description(
            "Number of retained messages a Babamul consumer group has not yet consumed on a topic.",
        )
        .build()
});

static CONSUMED_FRACTION: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    API_METER
        .f64_gauge("babamul.kafka.consumer.consumed_fraction")
        .with_description(
            "Fraction (0-1) of the currently retained messages on a topic that a Babamul \
             consumer group has consumed.",
        )
        .build()
});

static ACTIVE_CONSUMERS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    API_METER
        .u64_gauge("babamul.kafka.active_consumer_groups")
        .with_unit("{group}")
        .with_description("Number of Babamul consumer groups seen in the last sampling cycle.")
        .build()
});

/// A Kafka credential resolved back to the user that owns it.
///
/// Deliberately does not carry the user-chosen credential *name*: that is free
/// text which could contain a real name, email or hostname, and nothing here
/// should be able to carry it into PostHog.
#[derive(Debug, Clone)]
struct CredentialOwner {
    user_id: String,
    credential_id: String,
}

/// One group's position on one topic at a point in time.
#[derive(Debug, Clone, Copy)]
struct TopicPosition {
    committed: u64,
    available: u64,
    lag: u64,
}

/// Spawn the background sampling loop.
///
/// Returns without spawning when Babamul is disabled, since there is nothing to
/// attribute. The loop itself runs regardless of whether PostHog is configured:
/// the Grafana metrics are useful on their own.
pub fn spawn(config: AppConfig, db: Database, analytics: AnalyticsClient) {
    if !config.babamul.enabled {
        return;
    }

    let interval = Duration::from_secs(config.posthog.consumption_interval_seconds.max(30));
    tokio::spawn(async move {
        let mut previous: HashMap<(String, String), u64> = HashMap::new();
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        tracing::info!(
            interval_secs = interval.as_secs(),
            "Babamul Kafka consumption tracking started"
        );

        loop {
            ticker.tick().await;
            if let Err(error) = run_cycle(&config, &db, &analytics, &mut previous, interval).await {
                // A failed cycle is not fatal — the broker may just be briefly
                // unavailable. Log and try again on the next tick.
                tracing::warn!(%error, "Babamul consumption sampling cycle failed");
            }
        }
    });
}

/// Sample once: read Kafka, emit metrics, emit per-user deltas.
async fn run_cycle(
    config: &AppConfig,
    db: &Database,
    analytics: &AnalyticsClient,
    previous: &mut HashMap<(String, String), u64>,
    interval: Duration,
) -> Result<(), anyhow::Error> {
    let owners = load_credential_owners(db).await?;

    let bootstrap_servers = config.kafka.producer.server.clone();
    // rdkafka's consumer APIs are blocking, so keep them off the async runtime.
    let sample = tokio::task::spawn_blocking(move || sample_kafka(&bootstrap_servers)).await??;

    ACTIVE_CONSUMERS.record(sample.len() as u64, &[]);

    // Per-user accumulation of this cycle's deltas, so a user with several
    // credentials or groups produces one PostHog event rather than several.
    let mut user_deltas: HashMap<String, UserConsumption> = HashMap::new();

    for (group, positions) in &sample {
        let owner = resolve_owner(group, &owners);

        for (topic, position) in positions {
            let attrs = [
                KeyValue::new(
                    "user_id",
                    owner
                        .map(|owner| owner.user_id.clone())
                        .unwrap_or_else(|| UNATTRIBUTED.to_string()),
                ),
                KeyValue::new(
                    "credential_id",
                    owner
                        .map(|owner| owner.credential_id.clone())
                        .unwrap_or_else(|| UNATTRIBUTED.to_string()),
                ),
                KeyValue::new("group", group.clone()),
                KeyValue::new("topic", topic.clone()),
            ];
            COMMITTED_OFFSET.record(position.committed, &attrs);
            CONSUMER_LAG.record(position.lag, &attrs);
            if position.available > 0 {
                let consumed = position.available.saturating_sub(position.lag);
                CONSUMED_FRACTION.record(consumed as f64 / position.available as f64, &attrs);
            }

            // Delta since the previous cycle. `saturating_sub` handles offset
            // resets (a user seeking back, or a topic being recreated) by
            // contributing zero instead of a huge bogus number.
            let key = (group.clone(), topic.clone());
            let delta = match previous.insert(key, position.committed) {
                Some(before) => position.committed.saturating_sub(before),
                // First time we've seen this group/topic: establish a baseline
                // only, so we never attribute pre-existing progress to now.
                None => 0,
            };

            if delta == 0 {
                continue;
            }
            if let Some(owner) = owner {
                let entry = user_deltas.entry(owner.user_id.clone()).or_default();
                entry.messages_consumed += delta;
                entry.lag += position.lag;
                entry.record_topic(topic);
                entry.record_credential(&owner.credential_id);
            }
        }
    }

    // Forget groups that disappeared, so the map can't grow without bound
    // across a long-running process.
    previous.retain(|(group, topic), _| {
        sample
            .get(group)
            .map(|positions| positions.contains_key(topic))
            .unwrap_or(false)
    });

    if analytics.is_enabled() {
        for (user_id, consumption) in user_deltas {
            // Topic names are a fixed, server-controlled vocabulary
            // (`babamul.{survey}.{match}.{class}`), so they are safe to send.
            // Credential *names* are free text the user typed and could easily
            // contain a real name, email or hostname, so only the count and the
            // opaque generated ids leave the service.
            let mut topics: Vec<String> = consumption.topics.into_iter().collect();
            topics.sort();

            analytics.capture(
                AnalyticsEvent::new("babamul_stream_consumed", &user_id)
                    .with("messages_consumed", consumption.messages_consumed)
                    .with("lag", consumption.lag)
                    .with("n_topics", topics.len())
                    .with("topics", &topics)
                    .with("n_credentials", consumption.credentials.len())
                    .with("interval_seconds", interval.as_secs())
                    // Surface the latest activity on the person record so
                    // "who is actively streaming" is answerable without a query
                    // over the event log.
                    .with(
                        "$set",
                        serde_json::json!({
                            "babamul_last_streamed_at": chrono::Utc::now().to_rfc3339(),
                            "babamul_is_stream_consumer": true,
                        }),
                    ),
            );
        }
    }

    Ok(())
}

/// One cycle's consumption for a single user, aggregated across their groups.
#[derive(Debug, Default)]
struct UserConsumption {
    messages_consumed: u64,
    lag: u64,
    topics: std::collections::HashSet<String>,
    /// Generated credential ids, kept only to count how many of a user's
    /// credentials were active. Never sent as values.
    credentials: std::collections::HashSet<String>,
}

impl UserConsumption {
    fn record_topic(&mut self, topic: &str) {
        self.topics.insert(topic.to_string());
    }

    fn record_credential(&mut self, credential_id: &str) {
        self.credentials.insert(credential_id.to_string());
    }
}

/// The only part of a user document this module needs.
///
/// Projected rather than deserializing the full [`BabamulUser`]: this query
/// runs every sampling cycle, and there is no reason to pull password hashes,
/// API tokens and reset tokens over the wire on a timer.
#[derive(Debug, serde::Deserialize)]
struct CredentialOwnerRow {
    #[serde(rename = "_id")]
    id: String,
    #[serde(default)]
    kafka_credentials: Vec<CredentialRow>,
}

#[derive(Debug, serde::Deserialize)]
struct CredentialRow {
    id: String,
    kafka_username: String,
}

/// Build a map of Kafka SCRAM username → owning user, for every credential.
async fn load_credential_owners(
    db: &Database,
) -> Result<HashMap<String, CredentialOwner>, anyhow::Error> {
    let users: mongodb::Collection<CredentialOwnerRow> = db.collection("babamul_users");
    let mut cursor = users
        .find(doc! { "kafka_credentials": { "$exists": true, "$ne": [] } })
        .projection(doc! {
            "_id": 1,
            "kafka_credentials.id": 1,
            "kafka_credentials.kafka_username": 1,
        })
        .await?;

    let mut owners = HashMap::new();
    while let Some(user) = cursor.try_next().await? {
        for credential in &user.kafka_credentials {
            owners.insert(
                credential.kafka_username.clone(),
                CredentialOwner {
                    user_id: user.id.clone(),
                    credential_id: credential.id.clone(),
                },
            );
        }
    }
    Ok(owners)
}

/// Resolve a consumer group to the credential that owns it.
///
/// The package builds group ids as `{kafka_username}-{suffix}`, so match on the
/// username itself or that prefix. Matching against known usernames (rather
/// than parsing a UUID out of the group name) means a user picking an odd
/// suffix can't break attribution.
fn resolve_owner<'a>(
    group: &str,
    owners: &'a HashMap<String, CredentialOwner>,
) -> Option<&'a CredentialOwner> {
    if let Some(owner) = owners.get(group) {
        return Some(owner);
    }
    owners.iter().find_map(|(username, owner)| {
        let suffix = group.strip_prefix(username.as_str())?;
        suffix.starts_with('-').then_some(owner)
    })
}

/// Read every Babamul consumer group's position on every Babamul topic.
///
/// Blocking; call from `spawn_blocking`.
fn sample_kafka(
    bootstrap_servers: &str,
) -> Result<HashMap<String, HashMap<String, TopicPosition>>, anyhow::Error> {
    let admin: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    // Watermarks for all babamul.* partitions, plus the TPL we'll ask each
    // group about.
    let metadata = admin.fetch_metadata(None, KAFKA_TIMEOUT)?;
    let mut watermarks: HashMap<(String, i32), (i64, i64)> = HashMap::new();
    let mut tpl = TopicPartitionList::new();
    for topic in metadata.topics() {
        let name = topic.name();
        if !name.starts_with(BABAMUL_TOPIC_PREFIX) {
            continue;
        }
        for partition in topic.partitions() {
            if let Ok((low, high)) = admin.fetch_watermarks(name, partition.id(), KAFKA_TIMEOUT) {
                watermarks.insert((name.to_string(), partition.id()), (low, high));
                tpl.add_partition(name, partition.id());
            }
        }
    }

    if tpl.count() == 0 {
        return Ok(HashMap::new());
    }

    let groups = admin.fetch_group_list(None, KAFKA_TIMEOUT)?;
    let group_names: Vec<String> = groups
        .groups()
        .iter()
        .map(|group| group.name().to_string())
        .filter(|name| name.starts_with(BABAMUL_GROUP_PREFIX))
        .collect();

    let mut sample: HashMap<String, HashMap<String, TopicPosition>> = HashMap::new();
    for group in group_names {
        // A consumer created with a group id but never subscribed does not join
        // the group, so this is a plain OffsetFetch — it cannot disturb the
        // user's own consumers or trigger a rebalance.
        let probe: BaseConsumer = match ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .create()
        {
            Ok(probe) => probe,
            Err(error) => {
                tracing::warn!(%error, group, "failed to create offset probe for consumer group");
                continue;
            }
        };

        let committed = match probe.committed_offsets(tpl.clone(), KAFKA_TIMEOUT) {
            Ok(committed) => committed,
            Err(error) => {
                tracing::warn!(%error, group, "failed to fetch committed offsets");
                continue;
            }
        };

        let mut per_topic: HashMap<String, TopicPosition> = HashMap::new();
        for element in committed.elements() {
            // `Offset::Offset(n)` is a real commit; `Invalid`/`Stored` mean this
            // group has never committed on that partition, so skip it rather
            // than counting it as position zero.
            let Offset::Offset(offset) = element.offset() else {
                continue;
            };
            if offset < 0 {
                continue;
            }
            let topic = element.topic().to_string();
            let Some(&(low, high)) = watermarks.get(&(topic.clone(), element.partition())) else {
                continue;
            };

            let entry = per_topic.entry(topic).or_insert(TopicPosition {
                committed: 0,
                available: 0,
                lag: 0,
            });
            entry.committed += offset as u64;
            entry.available += high.saturating_sub(low).max(0) as u64;
            entry.lag += high.saturating_sub(offset).max(0) as u64;
        }

        if !per_topic.is_empty() {
            sample.insert(group, per_topic);
        }
    }

    Ok(sample)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owners() -> HashMap<String, CredentialOwner> {
        let mut owners = HashMap::new();
        owners.insert(
            "babamul-abc-123".to_string(),
            CredentialOwner {
                user_id: "user-1".to_string(),
                credential_id: "abc-123".to_string(),
            },
        );
        owners
    }

    #[test]
    fn resolves_a_group_equal_to_the_username() {
        let owners = owners();
        let owner = resolve_owner("babamul-abc-123", &owners).expect("should resolve");
        assert_eq!(owner.user_id, "user-1");
    }

    #[test]
    fn resolves_a_group_with_the_package_suffix() {
        let owners = owners();
        let owner = resolve_owner("babamul-abc-123-client-1", &owners).expect("should resolve");
        assert_eq!(owner.credential_id, "abc-123");
    }

    #[test]
    fn does_not_resolve_a_merely_similar_group() {
        let owners = owners();
        // Same prefix but a different credential — must not be attributed to
        // user-1 just because the string starts the same way.
        assert!(resolve_owner("babamul-abc-1234", &owners).is_none());
        assert!(resolve_owner("babamul-other", &owners).is_none());
    }
}
