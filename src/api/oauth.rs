//! OAuth 2.0 / OpenID Connect sign-in for Babamul accounts.
//!
//! Three providers are supported: Google and ORCID (both OIDC) and GitHub
//! (plain OAuth 2.0 plus its REST API).  The whole exchange happens
//! server-side — the browser never sees a client secret, and the only thing
//! that comes back to the web app is a Babamul JWT.
//!
//! Every provider is driven through the authorization-code flow with PKCE.
//! GitHub does not require PKCE, but sending a challenge it ignores is
//! harmless, so the flow stays uniform across providers.

use crate::conf::{AppConfig, OAuthProviderConfig};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

/// An identity provider Babamul can delegate authentication to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OAuthProviderKind {
    Google,
    Github,
    Orcid,
}

impl OAuthProviderKind {
    pub const ALL: [OAuthProviderKind; 3] = [
        OAuthProviderKind::Google,
        OAuthProviderKind::Github,
        OAuthProviderKind::Orcid,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            OAuthProviderKind::Google => "google",
            OAuthProviderKind::Github => "github",
            OAuthProviderKind::Orcid => "orcid",
        }
    }

    /// Human-readable name for the login button.
    pub fn display_name(&self) -> &'static str {
        match self {
            OAuthProviderKind::Google => "Google",
            OAuthProviderKind::Github => "GitHub",
            OAuthProviderKind::Orcid => "ORCID",
        }
    }

    pub fn from_path_segment(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "google" => Some(OAuthProviderKind::Google),
            "github" => Some(OAuthProviderKind::Github),
            "orcid" => Some(OAuthProviderKind::Orcid),
            _ => None,
        }
    }

    /// Credentials for this provider, or `None` when it isn't fully configured.
    pub fn config<'a>(&self, config: &'a AppConfig) -> Option<&'a OAuthProviderConfig> {
        let oauth = &config.babamul.oauth;
        let provider = match self {
            OAuthProviderKind::Google => &oauth.google,
            OAuthProviderKind::Github => &oauth.github,
            OAuthProviderKind::Orcid => &oauth.orcid,
        };
        provider.is_configured().then_some(provider)
    }

    fn authorize_url(&self, sandbox: bool) -> &'static str {
        match self {
            OAuthProviderKind::Google => "https://accounts.google.com/o/oauth2/v2/auth",
            OAuthProviderKind::Github => "https://github.com/login/oauth/authorize",
            OAuthProviderKind::Orcid if sandbox => "https://sandbox.orcid.org/oauth/authorize",
            OAuthProviderKind::Orcid => "https://orcid.org/oauth/authorize",
        }
    }

    fn token_url(&self, sandbox: bool) -> &'static str {
        match self {
            OAuthProviderKind::Google => "https://oauth2.googleapis.com/token",
            OAuthProviderKind::Github => "https://github.com/login/oauth/access_token",
            OAuthProviderKind::Orcid if sandbox => "https://sandbox.orcid.org/oauth/token",
            OAuthProviderKind::Orcid => "https://orcid.org/oauth/token",
        }
    }

    fn scope(&self) -> &'static str {
        match self {
            OAuthProviderKind::Google => "openid email profile",
            // `user:email` is needed because a GitHub user's primary email is
            // often private and absent from the plain /user payload.
            OAuthProviderKind::Github => "read:user user:email",
            OAuthProviderKind::Orcid => "openid",
        }
    }
}

impl fmt::Display for OAuthProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A verified external identity, normalised across providers.
#[derive(Debug, Clone)]
pub struct ExternalIdentity {
    pub provider: OAuthProviderKind,
    /// Stable, provider-scoped user identifier (Google `sub`, GitHub numeric
    /// id, ORCID iD).  This — never the email — is the join key.
    pub subject: String,
    pub email: Option<String>,
    /// Whether the provider asserts the email address has been verified.
    /// Only a verified email may be auto-linked to an existing account.
    pub email_verified: bool,
    pub name: Option<String>,
    /// Set only for ORCID sign-ins.
    pub orcid_id: Option<String>,
}

#[derive(Debug)]
pub struct OAuthError(pub String);

impl fmt::Display for OAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for OAuthError {}

fn err<T>(msg: impl Into<String>) -> Result<T, OAuthError> {
    Err(OAuthError(msg.into()))
}

/// One leg of the PKCE handshake: a high-entropy verifier kept server-side and
/// the S256 challenge handed to the provider.
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

impl Pkce {
    pub fn generate() -> Self {
        // 64 chars from the unreserved set — comfortably inside RFC 7636's
        // 43..128 range.
        let verifier = crate::api::routes::babamul::generate_random_string(64);
        let digest = Sha256::digest(verifier.as_bytes());
        let challenge = URL_SAFE_NO_PAD.encode(digest);
        Pkce {
            verifier,
            challenge,
        }
    }
}

/// The redirect URI the provider must send the browser back to.
///
/// This has to be byte-identical to the value registered with the provider and
/// to the one replayed during the token exchange, so it is built in exactly one
/// place.
pub fn redirect_uri(config: &AppConfig, provider: OAuthProviderKind) -> Result<String, OAuthError> {
    let base = match &config.babamul.oauth.redirect_base_url {
        Some(base) if !base.trim().is_empty() => base.trim().trim_end_matches('/').to_string(),
        _ => {
            return err("babamul.oauth.redirect_base_url is not configured");
        }
    };
    Ok(format!("{}/babamul/oauth/{}/callback", base, provider))
}

/// Build the provider's authorization URL for the start of the flow.
pub fn authorization_url(
    config: &AppConfig,
    provider: OAuthProviderKind,
    state: &str,
    pkce_challenge: &str,
) -> Result<String, OAuthError> {
    let provider_config = match provider.config(config) {
        Some(c) => c,
        None => return err(format!("Provider {} is not enabled", provider)),
    };
    let redirect = redirect_uri(config, provider)?;
    let sandbox = config.babamul.oauth.orcid_sandbox;

    let mut url = match url::Url::parse(provider.authorize_url(sandbox)) {
        Ok(url) => url,
        Err(e) => return err(format!("Invalid authorize URL: {}", e)),
    };
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("response_type", "code")
            .append_pair("client_id", &provider_config.client_id)
            .append_pair("redirect_uri", &redirect)
            .append_pair("scope", provider.scope())
            .append_pair("state", state)
            .append_pair("code_challenge", pkce_challenge)
            .append_pair("code_challenge_method", "S256");
        if provider == OAuthProviderKind::Google {
            // Without this Google silently reuses a previously granted consent
            // and, for users with several accounts, skips the chooser.
            query.append_pair("prompt", "select_account");
        }
    }
    Ok(url.to_string())
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    id_token: Option<String>,
    /// ORCID returns the authenticated ORCID iD alongside the token.
    orcid: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

/// Trade the one-time authorization code for tokens, then resolve the caller's
/// identity with the provider.
pub async fn exchange_code_for_identity(
    config: &AppConfig,
    provider: OAuthProviderKind,
    code: &str,
    pkce_verifier: &str,
) -> Result<ExternalIdentity, OAuthError> {
    let provider_config = match provider.config(config) {
        Some(c) => c,
        None => return err(format!("Provider {} is not enabled", provider)),
    };
    let redirect = redirect_uri(config, provider)?;
    let sandbox = config.babamul.oauth.orcid_sandbox;

    let client = http_client()?;
    let params = [
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", redirect.as_str()),
        ("client_id", provider_config.client_id.as_str()),
        ("client_secret", provider_config.client_secret.as_str()),
        ("code_verifier", pkce_verifier),
    ];

    let response = client
        .post(provider.token_url(sandbox))
        // GitHub defaults to a form-encoded response body unless asked otherwise.
        .header("Accept", "application/json")
        .form(&params)
        .send()
        .await
        .map_err(|e| OAuthError(format!("Token request to {} failed: {}", provider, e)))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| OAuthError(format!("Could not read {} token response: {}", provider, e)))?;

    let token: TokenResponse = serde_json::from_str(&body).map_err(|e| {
        OAuthError(format!(
            "Unexpected {} token response (HTTP {}): {}",
            provider, status, e
        ))
    })?;

    if let Some(error) = token.error {
        let description = token.error_description.unwrap_or_default();
        return err(format!(
            "{} rejected the authorization code: {} {}",
            provider, error, description
        ));
    }

    match provider {
        OAuthProviderKind::Google => {
            let id_token = match token.id_token {
                Some(t) => t,
                None => return err("Google did not return an id_token"),
            };
            google_identity(&id_token)
        }
        OAuthProviderKind::Github => {
            let access_token = match token.access_token {
                Some(t) => t,
                None => return err("GitHub did not return an access_token"),
            };
            github_identity(&client, &access_token).await
        }
        OAuthProviderKind::Orcid => {
            // ORCID puts the authenticated iD directly in the token response;
            // fall back to the id_token's `sub` if that is ever missing.
            let orcid_id = match token.orcid {
                Some(id) => id,
                None => match token.id_token.as_deref().and_then(|t| {
                    decode_jwt_claims(t)
                        .ok()
                        .and_then(|c| c.get("sub").and_then(|v| v.as_str().map(String::from)))
                }) {
                    Some(id) => id,
                    None => return err("ORCID did not return an ORCID iD"),
                },
            };
            let name = token
                .id_token
                .as_deref()
                .and_then(|t| decode_jwt_claims(t).ok())
                .and_then(|c| {
                    c.get("name")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                        .or_else(|| {
                            let given = c.get("given_name").and_then(|v| v.as_str());
                            let family = c.get("family_name").and_then(|v| v.as_str());
                            match (given, family) {
                                (Some(g), Some(f)) => Some(format!("{} {}", g, f)),
                                (Some(g), None) => Some(g.to_string()),
                                (None, Some(f)) => Some(f.to_string()),
                                (None, None) => None,
                            }
                        })
                });
            let email = orcid_public_email(&client, &orcid_id, sandbox).await;
            Ok(ExternalIdentity {
                provider,
                subject: orcid_id.clone(),
                email_verified: email.is_some(),
                email,
                name,
                orcid_id: Some(orcid_id),
            })
        }
    }
}

fn http_client() -> Result<reqwest::Client, OAuthError> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .user_agent("babamul-api")
        .build()
        .map_err(|e| OAuthError(format!("Failed to build HTTP client: {}", e)))
}

/// Read the claims out of a JWT **without** verifying its signature.
///
/// Safe here and only here: the token was just received over TLS directly from
/// the provider's token endpoint in response to a request authenticated with
/// our client secret, so there is no untrusted party in the path.  Never use
/// this on a token that arrived from a browser.
fn decode_jwt_claims(token: &str) -> Result<serde_json::Value, OAuthError> {
    let payload = match token.split('.').nth(1) {
        Some(p) => p,
        None => return err("Malformed id_token"),
    };
    let decoded = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|e| OAuthError(format!("Could not decode id_token payload: {}", e)))?;
    serde_json::from_slice(&decoded)
        .map_err(|e| OAuthError(format!("Could not parse id_token claims: {}", e)))
}

fn google_identity(id_token: &str) -> Result<ExternalIdentity, OAuthError> {
    let claims = decode_jwt_claims(id_token)?;
    let subject = match claims.get("sub").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return err("Google id_token has no subject"),
    };
    let email = claims
        .get("email")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty());
    // Google encodes email_verified as either a bool or the string "true".
    let email_verified = match claims.get("email_verified") {
        Some(serde_json::Value::Bool(b)) => *b,
        Some(serde_json::Value::String(s)) => s == "true",
        _ => false,
    };
    Ok(ExternalIdentity {
        provider: OAuthProviderKind::Google,
        subject,
        email,
        email_verified,
        name: claims
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        orcid_id: None,
    })
}

#[derive(Deserialize)]
struct GithubUser {
    id: i64,
    login: String,
    name: Option<String>,
    email: Option<String>,
}

#[derive(Deserialize)]
struct GithubEmail {
    email: String,
    primary: bool,
    verified: bool,
}

async fn github_identity(
    client: &reqwest::Client,
    access_token: &str,
) -> Result<ExternalIdentity, OAuthError> {
    let user: GithubUser = client
        .get("https://api.github.com/user")
        .bearer_auth(access_token)
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .map_err(|e| OAuthError(format!("GitHub /user request failed: {}", e)))?
        .error_for_status()
        .map_err(|e| OAuthError(format!("GitHub /user returned an error: {}", e)))?
        .json()
        .await
        .map_err(|e| OAuthError(format!("Could not parse GitHub /user response: {}", e)))?;

    // The profile email is whatever the user chose to display publicly, which
    // may be absent or unverified; /user/emails is authoritative.
    let mut email = None;
    let mut email_verified = false;
    match client
        .get("https://api.github.com/user/emails")
        .bearer_auth(access_token)
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
    {
        Ok(response) => match response.json::<Vec<GithubEmail>>().await {
            Ok(emails) => {
                if let Some(primary) = emails
                    .iter()
                    .find(|e| e.primary && e.verified)
                    .or_else(|| emails.iter().find(|e| e.verified))
                {
                    email = Some(primary.email.trim().to_lowercase());
                    email_verified = true;
                }
            }
            Err(e) => tracing::warn!("Could not parse GitHub /user/emails response: {}", e),
        },
        Err(e) => tracing::warn!("GitHub /user/emails request failed: {}", e),
    }

    if email.is_none() {
        email = user
            .email
            .map(|e| e.trim().to_lowercase())
            .filter(|e| !e.is_empty());
    }

    Ok(ExternalIdentity {
        provider: OAuthProviderKind::Github,
        subject: user.id.to_string(),
        email,
        email_verified,
        name: user.name.or(Some(user.login)),
        orcid_id: None,
    })
}

#[derive(Deserialize)]
struct OrcidEmailRecord {
    email: Option<String>,
    verified: Option<bool>,
}

#[derive(Deserialize)]
struct OrcidEmails {
    #[serde(default)]
    email: Vec<OrcidEmailRecord>,
}

/// Fetch an ORCID record's public email, if the researcher published one.
///
/// Most ORCID users keep their email private, so `None` is the common case and
/// the caller falls back to a `{orcid-id}@orcid.org` placeholder.
async fn orcid_public_email(
    client: &reqwest::Client,
    orcid_id: &str,
    sandbox: bool,
) -> Option<String> {
    let host = if sandbox {
        "https://pub.sandbox.orcid.org"
    } else {
        "https://pub.orcid.org"
    };
    let url = format!("{}/v3.0/{}/email", host, orcid_id);
    let response = match client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await
    {
        Ok(response) => response,
        Err(e) => {
            tracing::warn!("ORCID public email lookup failed for {}: {}", orcid_id, e);
            return None;
        }
    };
    if !response.status().is_success() {
        return None;
    }
    let emails: OrcidEmails = match response.json().await {
        Ok(emails) => emails,
        Err(e) => {
            tracing::warn!(
                "Could not parse ORCID email response for {}: {}",
                orcid_id,
                e
            );
            return None;
        }
    };
    emails
        .email
        .into_iter()
        .filter(|record| record.verified.unwrap_or(true))
        .filter_map(|record| record.email)
        .map(|email| email.trim().to_lowercase())
        .find(|email| !email.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_challenge_is_the_s256_of_the_verifier() {
        let pkce = Pkce::generate();
        assert_eq!(pkce.verifier.len(), 64);
        let expected = URL_SAFE_NO_PAD.encode(Sha256::digest(pkce.verifier.as_bytes()));
        assert_eq!(pkce.challenge, expected);
        // Base64url must not be padded (RFC 7636 §4.2).
        assert!(!pkce.challenge.contains('='));
    }

    #[test]
    fn provider_round_trips_through_its_path_segment() {
        for provider in OAuthProviderKind::ALL {
            assert_eq!(
                OAuthProviderKind::from_path_segment(provider.as_str()),
                Some(provider)
            );
        }
        assert_eq!(
            OAuthProviderKind::from_path_segment("GitHub"),
            Some(OAuthProviderKind::Github)
        );
        assert_eq!(OAuthProviderKind::from_path_segment("facebook"), None);
    }

    #[test]
    fn google_identity_reads_claims_from_an_unsigned_id_token() {
        let claims = serde_json::json!({
            "sub": "1234567890",
            "email": "  Researcher@Example.ORG ",
            "email_verified": true,
            "name": "A Researcher",
        });
        let token = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        );
        let identity = google_identity(&token).unwrap();
        assert_eq!(identity.subject, "1234567890");
        assert_eq!(identity.email.as_deref(), Some("researcher@example.org"));
        assert!(identity.email_verified);
        assert_eq!(identity.name.as_deref(), Some("A Researcher"));
    }

    #[test]
    fn google_email_verified_accepts_the_string_form() {
        let claims =
            serde_json::json!({ "sub": "1", "email": "a@b.org", "email_verified": "true" });
        let token = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        );
        assert!(google_identity(&token).unwrap().email_verified);
    }

    #[test]
    fn google_identity_rejects_a_token_without_a_subject() {
        let claims = serde_json::json!({ "email": "a@b.org" });
        let token = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        );
        assert!(google_identity(&token).is_err());
    }
}
