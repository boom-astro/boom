use crate::api::analytics::{AnalyticsClient, AnalyticsEvent};
use crate::api::routes::babamul::BabamulUser;
use crate::utils::o11y::metrics::API_METER;

use std::sync::LazyLock;
use std::time::Instant;

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

/// Distinct id used for unauthenticated Babamul traffic (signup, activation,
/// public stats). These events are flagged so PostHog does not build a person
/// profile from them; the id only exists because PostHog requires one.
const ANONYMOUS_DISTINCT_ID: &str = "babamul-anonymous";

pub async fn request_metrics_middleware(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    let is_babamul = req.path().starts_with("/babamul");
    let api = if is_babamul { "babamul" } else { "boom" };
    let method = req.method().as_str().to_string();

    // Capture request-side context before the request is consumed by `next`.
    let analytics = req
        .app_data::<web::Data<AnalyticsClient>>()
        .map(|client| client.as_ref().clone());
    let client_info = is_babamul.then(|| ClientInfo::from_request(&req));
    // Raw-path fallback for the (rare) `Err` case below, where actix hands
    // back an `Error` with no request attached, so there's no route pattern to
    // read. Must be a plain `String`, not a cloned `HttpRequest`/`ServiceRequest`
    // handle: actix's `Scope` router calls `HttpRequest::match_info_mut()`
    // while routing `req` inside `next.call()`, which does
    // `Rc::get_mut(&mut self.inner).unwrap()` and panics if any other clone of
    // that `HttpRequest` is alive at the time — as a prior version of this
    // middleware did by holding `req.request().clone()` across the `.await`.
    let path = req.path().to_string();
    let started_at = Instant::now();

    let response = next.call(req).await;
    // On the error path actix turns the `Error` into a response later, so read
    // the status the client will actually see rather than assuming 500 — the
    // auth middleware rejects bad tokens with `Err(401)`, which is a status
    // worth getting right.
    let status_code = match response.as_ref() {
        Ok(service_response) => service_response.status().as_u16(),
        Err(error) => error.as_response_error().status_code().as_u16(),
    };

    // `client` is bounded to a handful of buckets by `parse_user_agent`, so it
    // is safe to carry as a metric attribute and lets Grafana separate Python
    // package traffic from the web app without going to PostHog.
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

    // Report Babamul API usage to PostHog. Only Babamul traffic is captured —
    // the main BOOM API is internal, so it has no product-analytics story.
    //
    // Deliberately emitted for both `Ok` and `Err` outcomes: the auth
    // middleware rejects expired or invalid tokens by returning `Err`, and
    // those 401s are exactly the signal that tells us a user's personal access
    // token has lapsed. Capturing only `Ok` would hide them.
    if let (Some(analytics), Some(client_info)) = (analytics, client_info) {
        if analytics.is_enabled() {
            // Only the `Ok` branch has a `ServiceResponse` to read a request
            // back from — by this point dispatch has fully completed, so
            // borrowing its request here (not cloning it earlier) never races
            // the router's own mutable access. Prefer the registered route
            // pattern over the raw path so per-object endpoints don't create
            // one PostHog property value per object id. The auth middleware
            // injects the user on success, so its presence in extensions is
            // exactly "this request was authenticated".
            let (endpoint, user_id) = match response.as_ref() {
                Ok(service_response) => {
                    let request = service_response.request();
                    (
                        request.match_pattern().unwrap_or_else(|| path.clone()),
                        request
                            .extensions()
                            .get::<BabamulUser>()
                            .map(|user| user.id.clone()),
                    )
                }
                Err(_) => (path.clone(), None),
            };

            analytics.capture(build_request_event(
                &endpoint,
                &method,
                status_code,
                started_at.elapsed().as_millis() as u64,
                user_id.as_deref(),
                &client_info,
            ));
        }
    }

    response
}

/// Assemble the `babamul_api_request` event.
///
/// Split out from the middleware so the property shape can be tested without
/// standing up an actix pipeline.
fn build_request_event(
    endpoint: &str,
    method: &str,
    status_code: u16,
    duration_ms: u64,
    user_id: Option<&str>,
    client_info: &ClientInfo,
) -> AnalyticsEvent {
    let event = AnalyticsEvent::new(
        "babamul_api_request",
        user_id.unwrap_or(ANONYMOUS_DISTINCT_ID),
    )
    .with("endpoint", endpoint)
    .with("method", method)
    .with("status_code", status_code)
    .with("success", (200..400).contains(&status_code))
    .with("duration_ms", duration_ms)
    .with("authenticated", user_id.is_some())
    .with("auth_method", client_info.auth_method)
    .with("client", client_info.client.as_deref().unwrap_or("unknown"))
    .with_opt("client_version", client_info.client_version.as_deref())
    .with_opt("python_version", client_info.python_version.as_deref())
    .with_opt("client_os", client_info.os.as_deref());

    // Unauthenticated traffic must not create person profiles in PostHog.
    if user_id.is_some() {
        event
    } else {
        event.anonymous()
    }
}

/// Non-identifying facts about the caller, taken from request headers.
///
/// This is everything we learn about *how* the API is being called. The
/// Babamul Python package sends a `User-Agent` like
/// `babamul-python/0.2.0 (Python/3.12.1; Linux)`; anything else is reported
/// generically so we can still separate package traffic from raw HTTP clients
/// and from the web app.
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

        // Personal access tokens are the package's auth path; JWTs come from
        // the web app's login flow. This distinguishes programmatic from
        // browser usage even for callers that send no useful User-Agent.
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

/// Parse a `User-Agent` into client name/version plus optional environment
/// detail from the parenthesized comment.
///
/// Recognizes the Babamul package's own format and degrades gracefully:
/// browsers are bucketed as `browser`, anything else as `other`. We never
/// store the raw string, so a user cannot be fingerprinted by an unusual one.
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

    // `name/version (comment)` — the product token is everything up to the
    // first space or opening parenthesis.
    let product = user_agent
        .split(['(', ' '])
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    let (name, version) = match product.split_once('/') {
        Some((name, version)) => (name.to_string(), Some(version.to_string())),
        None => (product, None),
    };

    if name == "babamul-python" {
        info.client = Some(name);
        info.client_version = version;

        // Comment is `Python/<version>; <os>`.
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
        // Known non-browser tooling worth telling apart from the package.
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

    /// Regression test for a production incident: every request panicked
    /// because the middleware held `req.request().clone()` (an extra `Rc`
    /// reference) alive across `next.call(req).await`. actix's `Scope` router
    /// calls `HttpRequest::match_info_mut()` — `Rc::get_mut(...).unwrap()` —
    /// while routing inside that `.await`, which panics whenever another
    /// clone of the same `HttpRequest` is alive. Since every route in the API
    /// lives inside a `web::scope(...)`, that made every request — including
    /// the `/` health check — panic the worker handling it, so the container
    /// never passed its healthcheck.
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

    /// A rejected token never reaches a handler — the auth middleware returns
    /// `Err(401)`. That event must still be captured, and captured as an
    /// unauthenticated one, or expired personal access tokens are invisible.
    #[test]
    fn rejected_requests_are_captured_as_anonymous() {
        let mut client_info = parse_user_agent("babamul-python/0.2.0 (Python/3.12.1; Linux)");
        client_info.auth_method = "personal_access_token";

        let event = build_request_event("/babamul/profile", "GET", 401, 3, None, &client_info);

        assert_eq!(event.distinct_id, ANONYMOUS_DISTINCT_ID);
        assert_eq!(event.properties.get("status_code").unwrap(), 401);
        assert_eq!(event.properties.get("success").unwrap(), false);
        assert_eq!(event.properties.get("authenticated").unwrap(), false);
        // Still attributable to the package, which is what makes the 401
        // actionable.
        assert_eq!(event.properties.get("client").unwrap(), "babamul-python");
        assert_eq!(
            event.properties.get("auth_method").unwrap(),
            "personal_access_token"
        );
        // Must not create a person profile for the anonymous bucket.
        assert_eq!(
            event.properties.get("$process_person_profile").unwrap(),
            false
        );
    }

    #[test]
    fn authenticated_requests_are_keyed_on_the_user_id() {
        let client_info = parse_user_agent("babamul-python/0.2.0 (Python/3.12.1; Linux)");
        let event = build_request_event(
            "/babamul/surveys/{survey}/objects/{object_id}",
            "GET",
            200,
            12,
            Some("user-42"),
            &client_info,
        );

        assert_eq!(event.distinct_id, "user-42");
        assert_eq!(event.properties.get("authenticated").unwrap(), true);
        assert_eq!(event.properties.get("success").unwrap(), true);
        // The route pattern, not a path with a real object id baked in.
        assert_eq!(
            event.properties.get("endpoint").unwrap(),
            "/babamul/surveys/{survey}/objects/{object_id}"
        );
        // Identified events must keep person profiles enabled.
        assert!(!event.properties.contains_key("$process_person_profile"));
    }
}
