use crate::api::analytics::{AnalyticsClient, AnalyticsEvent};
use crate::api::routes::babamul::BabamulUser;
use crate::utils::o11y::metrics::API_METER;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware::Next,
    web, Error, HttpMessage,
};
use opentelemetry::{metrics::Counter, KeyValue};

static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    API_METER
        .u64_counter("api.request")
        .with_unit("{request}")
        .with_description("Number of HTTP requests handled by the BOOM API service.")
        .build()
});

/// PostHog requires a distinct id even for events that must not create a person.
const ANONYMOUS_DISTINCT_ID: &str = "babamul-anonymous";

pub async fn request_metrics_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    let is_babamul = req.path().starts_with("/babamul");
    let api = if is_babamul { "babamul" } else { "boom" };
    let method = req.method().as_str().to_string();

    let analytics = req
        .app_data::<web::Data<AnalyticsClient>>()
        .map(|client| client.as_ref().clone());
    let client_info = is_babamul.then(|| ClientInfo::from_request(&req));
    // Plain String: an HttpRequest clone alive across the await panics actix's Scope router.
    let path = req.path().to_string();
    let started_at = Instant::now();

    let response = next.call(req).await;
    let status_code = match response.as_ref() {
        Ok(service_response) => service_response.status().as_u16(),
        Err(error) => error.as_response_error().status_code().as_u16(),
    };

    // Bounded by parse_user_agent: an unbounded value here would explode metric cardinality.
    let attrs = [
        KeyValue::new("api", api),
        KeyValue::new("method", method.clone()),
        KeyValue::new("status_code", status_code.to_string()),
        KeyValue::new(
            "client",
            client_info
                .as_ref()
                .and_then(|info| info.client.clone())
                .unwrap_or_else(|| "unknown".to_string()),
        ),
    ];
    REQUESTS.add(1, &attrs);

    let (Some(analytics), Some(client_info)) = (analytics, client_info) else {
        return response;
    };
    if !analytics.is_enabled() {
        return response;
    }

    // Route pattern, not the raw path: object ids would make one property value per object.
    let (endpoint, user) = match response.as_ref() {
        Ok(service_response) => {
            let request = service_response.request();
            (
                request
                    .match_pattern()
                    .unwrap_or_else(|| request.path().to_string()),
                request
                    .extensions()
                    .get::<BabamulUser>()
                    .map(UserIdentity::claim),
            )
        }
        // Still captured: the 401 from a rejected token is the only sign that one lapsed.
        Err(_) => (path, None),
    };

    let enqueued = analytics.capture(build_request_event(
        &endpoint,
        &method,
        status_code,
        started_at.elapsed().as_millis() as u64,
        user.as_ref(),
        &client_info,
    ));

    // A dropped event takes the `$set` with it, so give the hourly slot back.
    if let Some(user) = user.as_ref().filter(|u| !enqueued && u.username.is_some()) {
        release_person_property_refresh(&user.id);
    }

    response
}

struct UserIdentity {
    id: String,
    /// Present only on the request carrying this person's property refresh.
    username: Option<String>,
}

impl UserIdentity {
    /// Also claims the process-wide person-property slot, once per user per TTL.
    fn claim(user: &BabamulUser) -> Self {
        let username = claim_person_property_refresh(&user.id).then(|| user.username.clone());
        Self {
            id: user.id.clone(),
            username,
        }
    }
}

const PERSON_PROPERTY_TTL: Duration = Duration::from_secs(60 * 60);

/// Per-process and deliberately not persisted: a restart costs one extra `$set` per user.
static PERSON_PROPERTIES_SENT: LazyLock<Mutex<HashMap<String, Instant>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A poisoned lock reports `true`: re-sending the properties is harmless.
fn claim_person_property_refresh(user_id: &str) -> bool {
    let now = Instant::now();
    let Ok(mut sent) = PERSON_PROPERTIES_SENT.lock() else {
        return true;
    };

    if sent
        .get(user_id)
        .is_some_and(|last| now.duration_since(*last) < PERSON_PROPERTY_TTL)
    {
        return false;
    }

    // Without the retain the map grows to every user the process has ever served.
    sent.retain(|_, last| now.duration_since(*last) < PERSON_PROPERTY_TTL);
    sent.insert(user_id.to_string(), now);
    true
}

fn release_person_property_refresh(user_id: &str) {
    if let Ok(mut sent) = PERSON_PROPERTIES_SENT.lock() {
        sent.remove(user_id);
    }
}

fn build_request_event(
    endpoint: &str,
    method: &str,
    status_code: u16,
    duration_ms: u64,
    user: Option<&UserIdentity>,
    client_info: &ClientInfo,
) -> AnalyticsEvent {
    let event = AnalyticsEvent::new(
        "babamul_api_request",
        user.map(|user| user.id.as_str())
            .unwrap_or(ANONYMOUS_DISTINCT_ID),
    )
    .with("endpoint", endpoint)
    .with("method", method)
    .with("status_code", status_code)
    .with("success", (200..400).contains(&status_code))
    .with("duration_ms", duration_ms)
    .with("authenticated", user.is_some())
    .with("auth_method", client_info.auth_method)
    .with("client", client_info.client.as_deref().unwrap_or("unknown"))
    .with_opt("client_version", client_info.client_version.as_deref())
    .with_opt("python_version", client_info.python_version.as_deref())
    .with_opt("client_os", client_info.os.as_deref());

    // Unauthenticated traffic must not create person profiles in PostHog.
    let Some(user) = user else {
        return event.anonymous();
    };
    match &user.username {
        Some(username) => event.with("$set", serde_json::json!({ "username": username })),
        None => event,
    }
}

/// Deliberately non-identifying; see the privacy stance in `docs/analytics.md`.
struct ClientInfo {
    client: Option<String>,
    client_version: Option<String>,
    python_version: Option<String>,
    os: Option<String>,
    auth_method: &'static str,
}

impl ClientInfo {
    fn from_request(req: &ServiceRequest) -> Self {
        let user_agent = req
            .headers()
            .get("User-Agent")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");

        // `bbml_` is the personal-access-token prefix; any other Bearer is a client JWT.
        let auth_method = match req
            .headers()
            .get("Authorization")
            .and_then(|value| value.to_str().ok())
        {
            Some(value) if value.starts_with("Bearer bbml_") => "personal_access_token",
            Some(value) if value.starts_with("Bearer ") => "jwt",
            _ => "none",
        };

        let mut info = parse_user_agent(user_agent);
        info.auth_method = auth_method;
        info
    }
}

/// Never returns the raw string: an unusual `User-Agent` must not become a fingerprint.
fn parse_user_agent(user_agent: &str) -> ClientInfo {
    let mut info = ClientInfo {
        client: None,
        client_version: None,
        python_version: None,
        os: None,
        auth_method: "none",
    };

    let user_agent = user_agent.trim();
    if user_agent.is_empty() {
        return info;
    }

    let product = user_agent.split(['(', ' ']).next().unwrap_or("").trim();
    let (name, version) = match product.split_once('/') {
        Some((name, version)) => (name, Some(version.to_string())),
        None => (product, None),
    };

    if name == "babamul-python" {
        info.client = Some(name.to_string());
        info.client_version = version;

        if let Some(comment) = user_agent
            .split_once('(')
            .and_then(|(_, rest)| rest.split_once(')'))
            .map(|(comment, _)| comment)
        {
            for part in comment.split(';') {
                let part = part.trim();
                match part.split_once('/') {
                    Some(("Python", version)) => info.python_version = Some(version.to_string()),
                    _ if !part.is_empty() && info.os.is_none() => info.os = Some(part.to_string()),
                    _ => {}
                }
            }
        }
    } else if user_agent.contains("Mozilla") {
        info.client = Some("browser".to_string());
    } else if !name.is_empty() {
        let bucket = match name.to_ascii_lowercase() {
            n if n.starts_with("python-httpx") => "httpx",
            n if n.starts_with("python-requests") => "requests",
            n if n.starts_with("curl") => "curl",
            _ => "other",
        };
        info.client = Some(bucket.to_string());
    }

    info
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: an HttpRequest clone alive across the await panicked actix's Scope router.
    #[actix_web::test]
    async fn middleware_survives_nested_scope_routing() {
        use actix_web::{middleware::from_fn, test, web, App, HttpResponse};

        let app = test::init_service(
            App::new()
                .wrap(from_fn(request_metrics_middleware))
                .service(web::scope("/nested").route(
                    "/ping",
                    web::get().to(|| async { HttpResponse::Ok().finish() }),
                )),
        )
        .await;

        let req = test::TestRequest::get().uri("/nested/ping").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
    }

    #[test]
    fn parses_the_babamul_package_user_agent() {
        let info = parse_user_agent("babamul-python/0.2.0 (Python/3.12.1; Linux)");
        assert_eq!(info.client.as_deref(), Some("babamul-python"));
        assert_eq!(info.client_version.as_deref(), Some("0.2.0"));
        assert_eq!(info.python_version.as_deref(), Some("3.12.1"));
        assert_eq!(info.os.as_deref(), Some("Linux"));
    }

    #[test]
    fn parses_package_user_agent_without_a_comment() {
        let info = parse_user_agent("babamul-python/0.2.0");
        assert_eq!(info.client.as_deref(), Some("babamul-python"));
        assert_eq!(info.client_version.as_deref(), Some("0.2.0"));
        assert_eq!(info.python_version, None);
        assert_eq!(info.os, None);
    }

    #[test]
    fn buckets_other_clients_without_storing_the_raw_string() {
        assert_eq!(
            parse_user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
                .client
                .as_deref(),
            Some("browser")
        );
        assert_eq!(
            parse_user_agent("python-httpx/0.27.0").client.as_deref(),
            Some("httpx")
        );
        assert_eq!(
            parse_user_agent("curl/8.4.0").client.as_deref(),
            Some("curl")
        );
        assert_eq!(
            parse_user_agent("SomeBespokeClient/1.0").client.as_deref(),
            Some("other")
        );
    }

    #[test]
    fn empty_user_agent_yields_no_client() {
        let info = parse_user_agent("");
        assert!(info.client.is_none());
        assert!(info.client_version.is_none());
    }

    #[test]
    fn rejected_requests_are_captured_as_anonymous() {
        let mut client_info = parse_user_agent("babamul-python/0.2.0 (Python/3.12.1; Linux)");
        client_info.auth_method = "personal_access_token";

        let event = build_request_event("/babamul/profile", "GET", 401, 3, None, &client_info);

        assert_eq!(event.distinct_id, ANONYMOUS_DISTINCT_ID);
        assert_eq!(event.properties.get("status_code").unwrap(), 401);
        assert_eq!(event.properties.get("success").unwrap(), false);
        assert_eq!(event.properties.get("authenticated").unwrap(), false);
        assert_eq!(event.properties.get("client").unwrap(), "babamul-python");
        assert_eq!(
            event.properties.get("auth_method").unwrap(),
            "personal_access_token"
        );
        assert_eq!(
            event.properties.get("$process_person_profile").unwrap(),
            false
        );
        assert!(event.properties.get("$set").is_none());
    }

    #[test]
    fn authenticated_requests_are_keyed_on_the_user_id() {
        let client_info = parse_user_agent("babamul-python/0.2.0 (Python/3.12.1; Linux)");
        let event = build_request_event(
            "/babamul/surveys/{survey}/objects/{object_id}",
            "GET",
            200,
            12,
            Some(&UserIdentity {
                id: "user-42".to_string(),
                username: Some("someone".to_string()),
            }),
            &client_info,
        );

        assert_eq!(event.distinct_id, "user-42");
        assert_eq!(event.properties.get("authenticated").unwrap(), true);
        let set = event.properties.get("$set").unwrap();
        assert_eq!(set.get("username").unwrap(), "someone");
        assert!(set.get("email").is_none(), "the address must not be sent");
        assert_eq!(event.properties.get("success").unwrap(), true);
        // The route pattern, not a path with a real object id baked in.
        assert_eq!(
            event.properties.get("endpoint").unwrap(),
            "/babamul/surveys/{survey}/objects/{object_id}"
        );
        // Identified events must keep person profiles enabled.
        assert!(!event.properties.contains_key("$process_person_profile"));
    }

    #[test]
    fn throttled_requests_keep_their_identity_but_drop_person_properties() {
        let client_info = parse_user_agent("babamul-python/0.2.0 (Python/3.12.1; Linux)");
        let event = build_request_event(
            "/babamul/profile",
            "GET",
            200,
            4,
            Some(&UserIdentity {
                id: "user-42".to_string(),
                username: None,
            }),
            &client_info,
        );

        assert_eq!(event.distinct_id, "user-42");
        assert_eq!(event.properties.get("authenticated").unwrap(), true);
        assert!(event.properties.get("$set").is_none());
        assert!(!event.properties.contains_key("$process_person_profile"));
    }

    #[test]
    fn person_properties_refresh_once_per_user_per_window() {
        // Ids unique to this test: the throttle map is process-wide.
        assert!(claim_person_property_refresh("refresh-window-a"));
        assert!(!claim_person_property_refresh("refresh-window-a"));
        assert!(!claim_person_property_refresh("refresh-window-a"));

        // A different user is unaffected by another's claim.
        assert!(claim_person_property_refresh("refresh-window-b"));
        assert!(!claim_person_property_refresh("refresh-window-b"));
    }

    #[test]
    fn a_released_claim_lets_the_next_request_carry_the_properties() {
        assert!(claim_person_property_refresh("refresh-release"));
        assert!(!claim_person_property_refresh("refresh-release"));

        release_person_property_refresh("refresh-release");

        assert!(claim_person_property_refresh("refresh-release"));
    }
}
