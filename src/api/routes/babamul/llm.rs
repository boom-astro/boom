//! Natural-language → filter-tree generation backed by Groq.
//!
//! The browser used to call the Groq API directly with a key baked into the
//! frontend bundle. That exposed the key to every visitor, so the call now goes
//! through this authenticated endpoint: the system prompt is built server-side,
//! and the Groq key is resolved from the user's saved key (if any) or the
//! server's default key (`BOOM_BABAMUL__GROQ_API_KEY`).

use super::{decrypt_password, encrypt_password, BabamulSurvey, BabamulUser};
use crate::api::models::response;
use crate::conf::AppConfig;
use actix_web::{post, put, web, HttpResponse};
use mongodb::{bson::doc, Collection, Database};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const GROQ_API_URL: &str = "https://api.groq.com/openai/v1/chat/completions";

/// Maximum length of a natural-language query, in characters.
const MAX_QUERY_LEN: usize = 2000;

/// Build the system prompt that instructs Groq to emit a block/condition
/// filter tree. Ported from the frontend `llmFilterAgent.ts` so the prompt
/// (and the model) are controlled server-side.
fn build_system_prompt(survey: BabamulSurvey) -> String {
    // Shared output-format contract, identical across surveys.
    let format = r#"Given a natural language description of what alerts a user wants to find, generate a filter in the following JSON format.

## Output Format

Return ONLY a JSON array (no markdown, no explanation). The array contains filter blocks:

[
  {
    "id": "root-block",
    "category": "block",
    "operator": "and",
    "children": [...]
  }
]

Each child is either a **condition**:
{
  "id": "cond-1",
  "category": "condition",
  "field": "candidate.drb",
  "operator": "$gt",
  "value": 0.5
}

Or a **nested block** (for OR logic):
{
  "id": "block-2",
  "category": "block",
  "operator": "or",
  "children": [...]
}

## Available Operators
- "$eq" (equals)
- "$ne" (not equal)
- "$gt" (greater than)
- "$gte" (greater than or equal)
- "$lt" (less than)
- "$lte" (less than or equal)
- "$in" (in list)
- "$exists" (field exists, value should be true/false)"#;

    let rules = r#"## Rules
- Always generate unique IDs for each node (use cond-1, cond-2, block-2 etc.)
- Always include at least one condition
- Use "and" as the default operator for the root block
- Do NOT add any date/time/observation-window filters (e.g. candidate.jd). The time range is set separately by the user via dedicated Start JD / End JD controls.
- Return ONLY the JSON array, nothing else"#;

    match survey {
        BabamulSurvey::Ztf => format!(
            r#"You are an astronomical alert filter builder for the Zwicky Transient Facility (ZTF).

{format}

## Available Fields

### Candidate
- candidate.drb (number)
- candidate.magpsf (number)
- candidate.sgscore1 (number)
- candidate.ndethist (number)
- candidate.isdiffpos (boolean)

### Classifications
- classifications.acai_h (number)
- classifications.acai_n (number)
- classifications.acai_v (number)
- classifications.acai_o (number)
- classifications.acai_b (number)
- classifications.btsbot (number)

### Properties
- properties.rock (boolean)
- properties.star (boolean)
- properties.near_brightstar (boolean)
- properties.stationary (boolean)

### Coordinates
- coordinates.l (number)
- coordinates.b (number)

## Common ZTF Conventions
- candidate.drb: deep-learning real-bogus score (0-1). Higher = more likely real. Use > 0.5 for real alerts.
- candidate.magpsf: PSF magnitude. Brighter objects have LOWER magnitude. "brighter than 18" means < 18.
- candidate.sgscore1: star-galaxy score. 0 = galaxy, 1 = star.
- candidate.ndethist: number of spatially coincident detections. Low values = new/recent.
- candidate.isdiffpos: positive difference image detection (true for real transients).
- classifications.acai_h: ACAI "human-interesting" transient score (0-1). Higher = more likely a real transient interesting to humans. Best indicator for supernovae.
- classifications.acai_n: ACAI "nuclear" transient score (0-1). Higher = near galaxy nucleus (AGN-like).
- classifications.acai_v: ACAI "variable star" score (0-1). Higher = likely variable star.
- classifications.acai_o: ACAI "orphan" transient score (0-1). Higher = hostless/orphan transient.
- classifications.acai_b: ACAI "bogus" score (0-1). Higher = likely bogus/artifact.
- classifications.btsbot: BTS Bot real transient score (0-1). Higher = more likely a real transient.
- properties.rock: boolean. true = likely a solar system object (asteroid).
- properties.star: boolean. true = likely a star, not a transient.
- properties.near_brightstar: boolean. true = near a bright star (higher artifact risk).
- properties.stationary: boolean. true = not moving (non-asteroid).
- coordinates.l: galactic longitude in degrees.
- coordinates.b: galactic latitude in degrees. |b| < 15 = galactic plane (higher stellar contamination).

{rules}
- For "real transients" or "supernovae", include classifications.acai_h > 0.5 AND properties.rock = false AND properties.star = false
- For "bright transients", combine classifications.acai_h > 0.5 with candidate.magpsf < [threshold]
- For filtering out bogus, use classifications.acai_b < 0.5
- For avoiding the galactic plane, use coordinates.b > 15 OR coordinates.b < -15"#
        ),
        BabamulSurvey::Lsst => format!(
            r#"You are an astronomical alert filter builder for the Vera C. Rubin Observatory LSST survey.

{format}

## Available Fields

### Candidate
- candidate.magpsf (number)
- candidate.reliability (number)

### Properties
- properties.rock (boolean)
- properties.star (boolean)
- properties.stationary (boolean)

### Coordinates
- coordinates.l (number)
- coordinates.b (number)

## Common LSST Conventions
- candidate.magpsf: PSF magnitude. Brighter objects have LOWER magnitude. "brighter than 22" means < 22.
- candidate.reliability: real-bogus score (0-1). Higher = more likely real. Use > 0.5 for real alerts.
- properties.rock: boolean. true = likely a solar system object (asteroid).
- properties.star: boolean. true = likely a star, not a transient.
- properties.stationary: boolean. true = not moving (non-asteroid).
- coordinates.b: galactic latitude in degrees. |b| < 15 = galactic plane (higher stellar contamination).

{rules}"#
        ),
    }
}

/// Resolve the Groq API key to use for this request: the user's own saved key
/// takes precedence, otherwise the server's default key. Returns a user-facing
/// error message when neither is available.
fn resolve_groq_key(user: &BabamulUser, config: &AppConfig) -> Result<String, String> {
    if let Some(encrypted) = &user.groq_api_key_encrypted {
        return decrypt_password(encrypted, config.api.auth.get_hashed_secret_key())
            .map_err(|e| format!("failed to decrypt saved Groq API key: {}", e));
    }
    match &config.babamul.groq_api_key {
        Some(key) if !key.trim().is_empty() => Ok(key.clone()),
        _ => Err(
            "No Groq API key is configured. Add your own Groq API key in your profile to use \
             natural-language filter generation."
                .to_string(),
        ),
    }
}

/// Strip a markdown code fence (```json ... ```) if the model wrapped its output.
fn strip_code_fence(content: &str) -> &str {
    let trimmed = content.trim();
    if let Some(rest) = trimmed.strip_prefix("```") {
        // Drop an optional language tag on the first line, then the trailing fence.
        let rest = rest.strip_prefix("json").unwrap_or(rest);
        let rest = rest.trim_start_matches('\n');
        return rest.strip_suffix("```").unwrap_or(rest).trim();
    }
    trimmed
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct LlmFilterRequest {
    /// Natural-language description of the alerts the user wants to find.
    pub query: String,
    /// Survey the filter targets (ZTF or LSST).
    pub survey: BabamulSurvey,
}

#[derive(Serialize, ToSchema)]
pub struct LlmFilterResponse {
    /// The generated block/condition filter tree.
    pub filters: serde_json::Value,
}

/// Generate a filter tree from a natural-language query using Groq.
#[utoipa::path(
    post,
    path = "/babamul/llm/filter",
    request_body = LlmFilterRequest,
    responses(
        (status = 200, description = "Filter generated successfully", body = LlmFilterResponse),
        (status = 400, description = "Invalid request or no Groq key configured"),
        (status = 401, description = "Unauthorized"),
        (status = 502, description = "Groq API error")
    ),
    tags=["Babamul"]
)]
#[post("/llm/filter")]
pub async fn post_llm_filter(
    current_user: Option<web::ReqData<BabamulUser>>,
    config: web::Data<AppConfig>,
    body: web::Json<LlmFilterRequest>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };

    let query = body.query.trim();
    if query.is_empty() {
        return response::bad_request("query cannot be empty");
    }
    if query.len() > MAX_QUERY_LEN {
        return response::bad_request(&format!(
            "query cannot exceed {} characters",
            MAX_QUERY_LEN
        ));
    }

    let api_key = match resolve_groq_key(&current_user, &config) {
        Ok(key) => key,
        Err(msg) => return response::bad_request(&msg),
    };

    let system_prompt = build_system_prompt(body.survey);
    let request_body = serde_json::json!({
        "model": config.babamul.groq_model,
        "messages": [
            { "role": "system", "content": system_prompt },
            { "role": "user", "content": query },
        ],
        "temperature": 0.1,
        "max_tokens": 2048,
    });

    let client = reqwest::Client::new();
    let groq_response = match client
        .post(GROQ_API_URL)
        .bearer_auth(&api_key)
        .json(&request_body)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!("Failed to reach Groq API: {}", e);
            return HttpResponse::BadGateway().json(response::ApiResponseBody::error(
                "Failed to reach the Groq API. Please try again.",
            ));
        }
    };

    let status = groq_response.status();
    let raw = groq_response.text().await.unwrap_or_default();
    if !status.is_success() {
        tracing::warn!("Groq API returned {}: {}", status, raw);
        // Surface auth/quota problems with the user's own key clearly, but do
        // not echo the raw provider body (it can contain key fragments).
        let msg = if status.as_u16() == 401 {
            "Groq rejected the API key. Check the key saved in your profile."
        } else if status.as_u16() == 429 {
            "Groq rate limit reached. Please wait a moment and try again."
        } else {
            "Groq API error while generating the filter."
        };
        return HttpResponse::BadGateway().json(response::ApiResponseBody::error(msg));
    }

    let parsed: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Failed to parse Groq response envelope: {}", e);
            return HttpResponse::BadGateway()
                .json(response::ApiResponseBody::error("Malformed response from Groq."));
        }
    };

    let content = parsed
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .trim();
    if content.is_empty() {
        return HttpResponse::BadGateway()
            .json(response::ApiResponseBody::error("Empty response from the LLM."));
    }

    let json_str = strip_code_fence(content);
    let filters: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("LLM returned non-JSON filter content: {}", e);
            return response::bad_request(
                "The LLM did not return a valid filter. Try rephrasing your description.",
            );
        }
    };

    // Basic shape validation: a non-empty array whose first element looks like a node.
    let valid = filters
        .as_array()
        .and_then(|arr| arr.first())
        .map(|first| first.get("id").is_some() && first.get("category").is_some())
        .unwrap_or(false);
    if !valid {
        return response::bad_request(
            "The LLM returned an invalid filter structure. Try rephrasing your description.",
        );
    }

    response::ok("filter generated successfully", serde_json::json!({ "filters": filters }))
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct SetGroqKeyRequest {
    /// The user's Groq API key (e.g. "gsk_...").
    pub groq_api_key: String,
}

/// Save (or replace) the authenticated user's Groq API key.
#[utoipa::path(
    put,
    path = "/babamul/profile/groq-key",
    request_body = SetGroqKeyRequest,
    responses(
        (status = 200, description = "Groq API key saved"),
        (status = 400, description = "Invalid key"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[put("/profile/groq-key")]
pub async fn put_groq_key(
    db: web::Data<Database>,
    config: web::Data<AppConfig>,
    current_user: Option<web::ReqData<BabamulUser>>,
    body: web::Json<SetGroqKeyRequest>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };

    let key = body.groq_api_key.trim();
    if key.is_empty() {
        return response::bad_request("Groq API key cannot be empty");
    }
    // Groq keys are short opaque strings; guard against obviously bad input.
    if key.len() > 200 {
        return response::bad_request("Groq API key is too long");
    }

    let encrypted = match encrypt_password(key, config.api.auth.get_hashed_secret_key()) {
        Ok(enc) => enc,
        Err(e) => {
            tracing::error!("Failed to encrypt Groq API key: {}", e);
            return response::internal_error("Failed to encrypt Groq API key");
        }
    };

    let collection: Collection<BabamulUser> = db.collection("babamul_users");
    match collection
        .update_one(
            doc! { "_id": &current_user.id },
            doc! { "$set": { "groq_api_key_encrypted": encrypted } },
        )
        .await
    {
        Ok(_) => response::ok_no_data("Groq API key saved successfully"),
        Err(e) => {
            tracing::error!("Failed to save Groq API key: {}", e);
            response::internal_error("Failed to save Groq API key")
        }
    }
}

/// Remove the authenticated user's saved Groq API key.
#[utoipa::path(
    delete,
    path = "/babamul/profile/groq-key",
    responses(
        (status = 200, description = "Groq API key removed"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[actix_web::delete("/profile/groq-key")]
pub async fn delete_groq_key(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<BabamulUser>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };

    let collection: Collection<BabamulUser> = db.collection("babamul_users");
    match collection
        .update_one(
            doc! { "_id": &current_user.id },
            doc! { "$unset": { "groq_api_key_encrypted": "" } },
        )
        .await
    {
        Ok(_) => response::ok_no_data("Groq API key removed successfully"),
        Err(e) => {
            tracing::error!("Failed to remove Groq API key: {}", e);
            response::internal_error("Failed to remove Groq API key")
        }
    }
}
