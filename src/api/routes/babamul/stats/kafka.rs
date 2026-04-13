use super::STATS_COLLECTION;
use crate::api::models::response;
use crate::conf::AppConfig;
use actix_web::{get, web, HttpResponse};
use chrono::Utc;
use mongodb::{bson::doc, Collection, Database};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const KAFKA_TIMEOUT_SECS: std::time::Duration = std::time::Duration::from_secs(10);
const KAFKA_TOPICS_CACHE_KEY: &str = "kafka_topics";
/// Cache Kafka topic stats for 5 minutes.
const KAFKA_TOPICS_CACHE_SECS: f64 = 5.0 * 60.0;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct KafkaTopicStat {
    pub name: String,
    pub n_alerts: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct KafkaTopicsCacheEntry {
    #[serde(rename = "_id")]
    id: String,
    topics: Vec<KafkaTopicStat>,
    updated_at: f64,
    cache_until: f64,
}

/// List Babamul Kafka topics with their current message counts.
///
/// Returns all `babamul.*` topics with the number of messages currently
/// available in each topic. Results are cached for 5 minutes.
#[utoipa::path(
    get,
    path = "/babamul/stats/kafka",
    responses(
        (status = 200, description = "Kafka topic stats retrieved", body = Vec<KafkaTopicStat>),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Stats"]
)]
#[get("/stats/kafka")]
pub async fn get_kafka_stats(
    config: web::Data<AppConfig>,
    db: web::Data<Database>,
) -> HttpResponse {
    let now_ts = Utc::now().timestamp() as f64;
    let stats_collection: Collection<KafkaTopicsCacheEntry> = db.collection(STATS_COLLECTION);

    // Try cache first
    if let Ok(Some(cached)) = stats_collection
        .find_one(doc! { "_id": KAFKA_TOPICS_CACHE_KEY })
        .await
    {
        if cached.cache_until > now_ts {
            return response::ok(
                &format!("{} topics", cached.topics.len()),
                serde_json::json!(cached.topics),
            );
        }
    }

    // Cache miss — query Kafka
    let bootstrap_servers = config.kafka.producer.server.clone();
    let topics = match web::block(move || list_babamul_topics(&bootstrap_servers)).await {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => {
            return response::internal_error(&format!("Kafka error: {}", e));
        }
        Err(e) => {
            return response::internal_error(&format!("Blocking error: {}", e));
        }
    };

    // Upsert cache
    let cache_entry = KafkaTopicsCacheEntry {
        id: KAFKA_TOPICS_CACHE_KEY.to_string(),
        topics: topics.clone(),
        updated_at: now_ts,
        cache_until: now_ts + KAFKA_TOPICS_CACHE_SECS,
    };
    let _ = stats_collection
        .replace_one(doc! { "_id": KAFKA_TOPICS_CACHE_KEY }, &cache_entry)
        .upsert(true)
        .await;

    response::ok(
        &format!("{} topics", topics.len()),
        serde_json::json!(topics),
    )
}

fn list_babamul_topics(
    bootstrap_servers: &str,
) -> Result<Vec<KafkaTopicStat>, rdkafka::error::KafkaError> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let metadata = consumer.fetch_metadata(None, KAFKA_TIMEOUT_SECS)?;

    let mut topics: Vec<KafkaTopicStat> = Vec::new();
    for topic in metadata.topics() {
        let name = topic.name();
        if !name.starts_with("babamul.") {
            continue;
        }

        let mut n_alerts: u64 = 0;
        for p in topic.partitions() {
            if let Ok((low, high)) = consumer.fetch_watermarks(name, p.id(), KAFKA_TIMEOUT_SECS) {
                n_alerts += if high > low { (high - low) as u64 } else { 0 };
            }
        }

        topics.push(KafkaTopicStat {
            name: name.to_string(),
            n_alerts,
        });
    }

    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(topics)
}
