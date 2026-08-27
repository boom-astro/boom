use actix_web::{get, web, HttpResponse};
use serde::{Deserialize, Serialize};
use tracing::{debug, error};
use utoipa::ToSchema;

/// Realtime alert metrics: survey name, current alert count, and timestamp when gathered.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RealtimeAlertMetrics {
    /// Name of the survey (e.g., "ztf", "lsst")
    pub survey: String,
    /// Total number of alerts currently recorded for this survey
    pub n_alerts: u64,
    /// Unix timestamp in seconds when these metrics were gathered
    pub gathered_at: i64,
}

/// Parse Prometheus-format metrics and extract babamul.kafka.consumer gauge values
/// 
/// Looks for gauge metrics matching the pattern:
/// `babamul_kafka_consumer_committed_offset{survey="...",topic="...",group="..."} <value>`
pub fn parse_otel_metrics(
    text: &str,
    gathered_at: i64,
  ) -> Result<Vec<RealtimeAlertMetrics>, String> {
    let mut survey_totals: std::collections::HashMap<String, u64> =
          
        
        std::collections::HashMap::new();


    for line in text.lines() {
        // Skip comments (lines starting with #) and empty lines
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        // Look for committed offset metrics per survey
        // Pattern: babamul_kafka_consumer_committed_offset{survey="ztf",topic="...",group="..."} 42
        if !line.contains("babamul_kafka_consumer_committed_offset") {
            continue;
        }

        // Extract survey name from labels (e.g., survey="ztf")
        if let Some(metric_part) = line.split_once(' ') {
            if let Some(survey) = extract_survey_label(metric_part.0) {
                // Try to parse the numeric value
                match metric_part.1.trim().parse::<u64>() {
                    Ok(value) => {
                        // Accumulate total per survey
                        *survey_totals.entry(survey).or_insert(0) += value;
                    }
                    Err(_) => continue,
                }
            }
        }
    }

    let mut metrics: Vec<RealtimeAlertMetrics> = survey_totals
        .into_iter()
        .map(|(survey, n_alerts)| RealtimeAlertMetrics {
            survey,
            n_alerts,
            gathered_at,
        })
        .collect();

    // Sort by survey name for consistent output
    metrics.sort_by(|a, b| a.survey.cmp(&b.survey));

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

/// Fetch realtime alert metrics from your Boom API's Prometheus endpoint.
///
/// This endpoint queries your API's native Prometheus metrics (babamul.kafka.consumer.*)
/// to get realtime alert consumption statistics per survey.
///
/// # Examples
/// - GET /babamul/stats/realtime
/// - Returns: [{"survey": "ztf", "n_alerts": 42, "gathered_at": 1234567890}]
#[utoipa::path(
    get,
    path = "/babamul/stats/realtime",
    responses(
        (status = 200, description = "Realtime alert metrics retrieved successfully", body = Vec<RealtimeAlertMetrics>),
        (status = 500, description = "Failed to fetch or parse metrics"),
        (status = 503, description = "Metrics endpoint is not configured or unreachable")
    ),
    tags=["Babamul", "Stats"]
)]
#[get("/stats/realtime")]
pub async fn get_realtime_stats(
    config: web::Data<crate::conf::AppConfig>,
    client: web::Data<reqwest::Client>,
) -> HttpResponse {
    // --- STEP 1: Validate that metrics collection is enabled ---
    if !config.otel.enabled {
        debug!("Metrics collection is disabled; returning 503 Service Unavailable");
        return HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "error": "Metrics collection is not enabled"
        }));
    }

    // --- STEP 2: Build the full URL to the metrics endpoint ---
    // Examples:
    // - http://localhost:9090/metrics (standalone Prometheus)
    // - http://api:9090/metrics (Docker Compose)
    let url = format!(
        "http://{}:{}{}",
        config.otel.host, config.otel.port, config.otel.metrics_endpoint
    );

    debug!("Fetching metrics from: {}", url);

    // --- STEP 3: Make HTTP GET request to metrics endpoint ---
    match client.get(&url).send().await {
        Ok(response) => {
            // HTTP request succeeded; now read the response body
            match response.text().await {
                Ok(text) => {
                    // Successfully read response body as text
                    debug!("Received {} bytes of metrics", text.len());

                    // --- STEP 4: Parse Prometheus-format metrics ---
                    let gathered_at = flare::Time::now().to_utc().timestamp();
                    match parse_otel_metrics(&text, gathered_at) {
                        Ok(metrics) => {
                            // Successfully parsed metrics
                            debug!("Parsed {} alert metrics", metrics.len());
                            // Return the metrics as JSON with 200 OK status
                            HttpResponse::Ok().json(metrics)
                        }
                        Err(e) => {
                            // Parsing failed (malformed Prometheus response)
                            error!("Failed to parse metrics: {}", e);
                            HttpResponse::InternalServerError().json(serde_json::json!({
                                "error": "Failed to parse metrics response",
                                "details": e
                            }))
                        }
                    }
                }
                Err(e) => {
                    // Failed to read response body (network/IO error)
                    error!("Failed to read response body: {}", e);
                    HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": "Failed to read metrics response",
                        "details": e.to_string()
                    }))
                }
            }
        }
        Err(e) => {
            // HTTP request failed (connection refused, DNS error, etc.)
            error!("Failed to connect to metrics endpoint at {}: {}", url, e);
            HttpResponse::ServiceUnavailable().json(serde_json::json!({
                "error": "Metrics endpoint unreachable",
                "details": e.to_string()
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_survey_label() {
        assert_eq!(
            extract_survey_label("babamul_kafka_consumer_committed_offset{survey=\"ztf\",topic=\"babamul.ztf.public\",group=\"babamul-123\"}"),
            Some("ztf".to_string())
        );
        assert_eq!(
            extract_survey_label("babamul_kafka_consumer_committed_offset{survey=\"lsst\",topic=\"babamul.lsst.public\",group=\"babamul-456\"}"),
            Some("lsst".to_string())
        );
    }

    #[test]
    fn test_parse_single_metric() {
        let text = "babamul_kafka_consumer_committed_offset{survey=\"ztf\",topic=\"babamul.ztf.public\",group=\"babamul-123\"} 1000\n";
        let result = parse_otel_metrics(text, 1234567890).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].survey, "ztf");
        assert_eq!(result[0].n_alerts, 1000);
    }

    #[test]
    fn test_parse_multiple_surveys() {
        let text = r#"babamul_kafka_consumer_committed_offset{survey="ztf",topic="babamul.ztf.public",group="babamul-123"} 100
babamul_kafka_consumer_committed_offset{survey="ztf",topic="babamul.ztf.public",group="babamul-456"} 200
babamul_kafka_consumer_committed_offset{survey="lsst",topic="babamul.lsst.public",group="babamul-789"} 300
"#;
        let result = parse_otel_metrics(text, 1234567890).unwrap();
        assert_eq!(result.len(), 2);
        let ztf = result.iter().find(|m| m.survey == "ztf").unwrap();
        let lsst = result.iter().find(|m| m.survey == "lsst").unwrap();
        assert_eq!(ztf.n_alerts, 300); // 100 + 200
        assert_eq!(lsst.n_alerts, 300);
    }
}
