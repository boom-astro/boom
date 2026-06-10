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
const BABAMUL_KAFKA_TOPICS_CACHE_KEY: &str = "babamul_kafka_topics";
/// Cache Kafka topic stats for 5 minutes.
const BABAMUL_KAFKA_TOPICS_CACHE_SECS: f64 = 5.0 * 60.0;

/// MongoDB cache document storing all Kafka topic stats under a single well-known
/// `_id`, along with the expiration timestamp.
#[derive(Debug, Serialize, Deserialize)]
struct KafkaTopicsCacheEntry {
    #[serde(rename = "_id")]
    id: String,
    topics: Vec<KafkaTopicStat>,
    updated_at: f64,
    cache_until: f64,
}

/// Per-topic Kafka stats entry: topic name, the number of messages currently
/// available in the topic, and the configured retention period in days.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct KafkaTopicStat {
    pub name: String,
    pub n_alerts: u64,
    pub retention_days: u32,
}

/// Realtime alert metrics from OTel: survey name, current alert count, and timestamp when gathered.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RealtimeAlertMetrics {
    pub survey: String,
    pub n_alerts: u32,
    pub gathered_at: i64, // unix timestamp in seconds
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
        .find_one(doc! {
            "_id": BABAMUL_KAFKA_TOPICS_CACHE_KEY,
            "cache_until": { "$gt": now_ts },
        })
        .await
    {
        return response::ok(
            &format!("{} topics", cached.topics.len()),
            serde_json::json!(cached.topics),
        );
    }

    // Cache miss — query Kafka
    let bootstrap_servers = config.kafka.producer.server.clone();
    let retention_days = config.babamul.retention_days;
    let topics =
        match web::block(move || list_babamul_topics(&bootstrap_servers, retention_days)).await {
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
        id: BABAMUL_KAFKA_TOPICS_CACHE_KEY.to_string(),
        topics: topics.clone(),
        updated_at: now_ts,
        cache_until: now_ts + BABAMUL_KAFKA_TOPICS_CACHE_SECS,
    };
    if let Err(e) = stats_collection
        .replace_one(doc! { "_id": BABAMUL_KAFKA_TOPICS_CACHE_KEY }, &cache_entry)
        .upsert(true)
        .await
    {
        tracing::warn!("Failed to upsert Kafka topics cache: {}", e);
    }

    response::ok(
        &format!("{} topics", topics.len()),
        serde_json::json!(topics),
    )
}

/// Fetch realtime alert metrics from the OTel Collector.
///
/// Queries the OTel Collector's Prometheus endpoint and returns current alert
/// counts per survey with the timestamp when they were gathered. The OTel Collector
/// endpoint is configured via the `OTEL_COLLECTOR_METRICS_URL` environment variable,
/// defaulting to `http://localhost:8888/metrics`.
#[utoipa::path(
    get,
    path = "/babamul/stats/kafka",
    responses(
        (status = 200, description = "Realtime alert metrics retrieved", body = Vec<RealtimeAlertMetrics>),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Stats"]
)]
#[get("/stats/kafka")]
pub async fn get_realtime_alerts() -> HttpResponse {
    let collector_url = std::env::var("OTEL_COLLECTOR_METRICS_URL")
        .unwrap_or_else(|_| "http://localhost:8888/metrics".to_string());

    let gathered_at = Utc::now().timestamp();

    match reqwest::get(&collector_url).await {
        Ok(resp) => match resp.text().await {
            Ok(text) => match parse_otel_metrics(&text, gathered_at) {
                Ok(metrics) => {
                    response::ok("realtime alerts", serde_json::json!(metrics))
                }
                Err(e) => {
                    response::internal_error(&format!("Failed to parse metrics: {}", e))
                }
            },
            Err(e) => {
                response::internal_error(&format!("Failed to read OTel response: {}", e))
            }
        },
        Err(e) => {
            response::internal_error(&format!("Failed to query OTel Collector: {}", e))
        }
    }
}

/// Parse Prometheus-format metrics from OTel Collector and extract alert counts.
///
/// Looks for metrics matching the pattern `boom_alerts_total{survey="..."}` and
/// extracts the survey name and alert count from each line. Attaches the gathered_at
/// timestamp to each metric.
fn parse_otel_metrics(text: &str, gathered_at: i64) -> Result<Vec<RealtimeAlertMetrics>, String> {
    let mut metrics = Vec::new();

    for line in text.lines() {
        // Skip comments and empty lines
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        // Look for metrics with "alerts" in the name (e.g., boom_alerts_total)
        if !line.contains("alerts") {
            continue;
        }

        // Parse lines like: boom_alerts_total{survey="ztf",instance="..."} 1234
        if let Some((metric_part, value_part)) = line.split_once(' ') {
            // Extract survey name from labels
            if let Some(survey) = extract_survey_label(metric_part) {
                // Try to parse the numeric value
                match value_part.trim().parse::<u32>() {
                    Ok(n_alerts) => {
                        metrics.push(RealtimeAlertMetrics { survey, n_alerts, gathered_at });
                    }
                    Err(_) => {
                        // Skip lines that don't have valid numeric values
                        continue;
                    }
                }
            }
        }
    }

    if metrics.is_empty() {
        // Return empty list rather than error — OTel may not have metrics yet
        return Ok(Vec::new());
    }

    Ok(metrics)
}

/// Extract the survey label value from a Prometheus metric line.
///
/// Parses labels like `metric_name{survey="ztf",other="value"}` and returns "ztf".
fn extract_survey_label(metric_part: &str) -> Option<String> {
    if let Some(start) = metric_part.find("survey=\"") {
        let after_key = &metric_part[start + 8..];
        if let Some(end) = after_key.find('"') {
            return Some(after_key[..end].to_string());
        }
    }
    None
}

fn list_babamul_topics(
    bootstrap_servers: &str,
    retention_days: u32,
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
            retention_days,
        });
    }

    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(topics)
}
