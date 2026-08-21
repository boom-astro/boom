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

    let now = flare::Time::now().to_utc().timestamp();

    match provider {
        OAuthProviderKind::Google => {
            let id_token = match token.id_token {
                Some(t) => t,
                None => return err("Google did not return an id_token"),
            };
            google_identity(&id_token, &provider_config.client_id, now)
        }
        OAuthProviderKind::Github => {
            let access_token = match token.access_token {
                Some(t) => t,
                None => return err("GitHub did not return an access_token"),
            };
            github_identity(&client, &access_token).await
        }
        OAuthProviderKind::Orcid => {
            // ORCID's id_token is optional in practice — the iD comes back as a
            // top-level field of the token response — but when one is present
            // it must survive the same checks as any other before we read it.
            let claims = match token.id_token.as_deref() {
                Some(id_token) => {
                    let claims = decode_jwt_claims(id_token)?;
                    let issuer = if sandbox {
                        "https://sandbox.orcid.org"
                    } else {
                        "https://orcid.org"
                    };
                    validate_id_token_claims(&claims, &[issuer], &provider_config.client_id, now)?;
                    Some(claims)
                }
                None => None,
            };

            let orcid_id = match token.orcid {
                Some(id) => id,
                None => match claims
                    .as_ref()
                    .and_then(|c| c.get("sub"))
                    .and_then(|v| v.as_str())
                {
                    Some(id) => id.to_string(),
                    None => return err("ORCID did not return an ORCID iD"),
                },
            };

            let name = claims.as_ref().and_then(|c| {
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
                // `orcid_public_email` only returns addresses ORCID explicitly
                // marks verified, so anything that survives it is trustworthy.
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
/// Permitted here, and only here, by OIDC Core §3.1.3.7: the token was just
/// received over TLS directly from the provider's token endpoint, in response
/// to a request authenticated with our client secret, so TLS server validation
/// stands in for checking the signature. Never use this on a token that
/// arrived from a browser.
///
/// Callers must still run [`validate_id_token_claims`] — TLS says the bytes
/// came from the host we dialled, not that the token was minted for us.
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

/// Tolerance for clock drift between us and the provider.
const ID_TOKEN_CLOCK_SKEW_SECONDS: i64 = 120;

/// Check the claims every OIDC relying party is required to check: that the
/// token was minted by the issuer we expect, *for us*, and hasn't expired.
///
/// Cheap (no network, no JWKS) and catches the failure modes that skipping
/// signature verification leaves open — chiefly a token issued for a different
/// client being accepted as one of ours, and a misconfigured endpoint handing
/// back somebody else's token.
fn validate_id_token_claims(
    claims: &serde_json::Value,
    expected_issuers: &[&str],
    client_id: &str,
    now: i64,
) -> Result<(), OAuthError> {
    let issuer = claims.get("iss").and_then(|v| v.as_str()).unwrap_or("");
    if !expected_issuers.contains(&issuer) {
        return err(format!(
            "id_token issuer {:?} is not one of {:?}",
            issuer, expected_issuers
        ));
    }

    // `aud` is a single string for the common case, an array when the token is
    // addressed to several clients.
    let audience_matches = match claims.get("aud") {
        Some(serde_json::Value::String(aud)) => aud == client_id,
        Some(serde_json::Value::Array(auds)) => auds
            .iter()
            .any(|aud| aud.as_str().is_some_and(|aud| aud == client_id)),
        _ => false,
    };
    if !audience_matches {
        return err("id_token was not issued for this client");
    }

    match claims.get("exp").and_then(|v| v.as_i64()) {
        Some(exp) if exp + ID_TOKEN_CLOCK_SKEW_SECONDS > now => Ok(()),
        Some(_) => err("id_token has expired"),
        None => err("id_token has no expiry"),
    }
}

fn google_identity(
    id_token: &str,
    client_id: &str,
    now: i64,
) -> Result<ExternalIdentity, OAuthError> {
    let claims = decode_jwt_claims(id_token)?;
    validate_id_token_claims(
        &claims,
        // Google issues both forms and treats them as equivalent.
        &["https://accounts.google.com", "accounts.google.com"],
        client_id,
        now,
    )?;
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
/// Most ORCID users keep their email private, so `None` is the common case;
/// the caller then sends the user through the confirm-by-email flow rather
/// than inventing an address nobody has vouched for.
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
        // Default to *not* verified. An address ORCID won't explicitly vouch
        // for could otherwise be auto-linked to an existing Babamul account
        // with the same email. Being strict costs little now that the fallback
        // is the confirm-by-email flow rather than a hard failure.
        .filter(|record| record.verified.unwrap_or(false))
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

    const TEST_CLIENT_ID: &str = "test-client-id";
    const TEST_NOW: i64 = 1_700_000_000;

    /// Build an unsigned id_token whose registered claims pass validation, so
    /// each test can vary only the thing it is actually about.
    fn google_token(extra: serde_json::Value) -> String {
        let mut claims = serde_json::json!({
            "iss": "https://accounts.google.com",
            "aud": TEST_CLIENT_ID,
            "exp": TEST_NOW + 3600,
        });
        for (key, value) in extra.as_object().unwrap() {
            claims[key] = value.clone();
        }
        format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        )
    }

    #[test]
    fn google_identity_reads_claims_from_an_id_token() {
        let token = google_token(serde_json::json!({
            "sub": "1234567890",
            "email": "  Researcher@Example.ORG ",
            "email_verified": true,
            "name": "A Researcher",
        }));
        let identity = google_identity(&token, TEST_CLIENT_ID, TEST_NOW).unwrap();
        assert_eq!(identity.subject, "1234567890");
        assert_eq!(identity.email.as_deref(), Some("researcher@example.org"));
        assert!(identity.email_verified);
        assert_eq!(identity.name.as_deref(), Some("A Researcher"));
    }

    #[test]
    fn google_email_verified_accepts_the_string_form() {
        let token = google_token(
            serde_json::json!({ "sub": "1", "email": "a@b.org", "email_verified": "true" }),
        );
        assert!(
            google_identity(&token, TEST_CLIENT_ID, TEST_NOW)
                .unwrap()
                .email_verified
        );
    }

    #[test]
    fn google_identity_rejects_a_token_without_a_subject() {
        let token = google_token(serde_json::json!({ "email": "a@b.org" }));
        assert!(google_identity(&token, TEST_CLIENT_ID, TEST_NOW).is_err());
    }

    #[test]
    fn id_token_must_come_from_the_expected_issuer() {
        let claims = serde_json::json!({
            "iss": "https://accounts.evil.example",
            "aud": TEST_CLIENT_ID,
            "exp": TEST_NOW + 3600,
        });
        assert!(validate_id_token_claims(
            &claims,
            &["https://accounts.google.com"],
            TEST_CLIENT_ID,
            TEST_NOW
        )
        .is_err());
    }

    #[test]
    fn id_token_must_be_addressed_to_this_client() {
        // A token minted for a different client is the failure that skipping
        // signature verification would otherwise let through.
        let claims = serde_json::json!({
            "iss": "https://accounts.google.com",
            "aud": "some-other-clients-id",
            "exp": TEST_NOW + 3600,
        });
        assert!(validate_id_token_claims(
            &claims,
            &["https://accounts.google.com"],
            TEST_CLIENT_ID,
            TEST_NOW
        )
        .is_err());

        // Multi-audience tokens are valid as long as we are one of them.
        let claims = serde_json::json!({
            "iss": "https://accounts.google.com",
            "aud": ["another-client", TEST_CLIENT_ID],
            "exp": TEST_NOW + 3600,
        });
        assert!(validate_id_token_claims(
            &claims,
            &["https://accounts.google.com"],
            TEST_CLIENT_ID,
            TEST_NOW
        )
        .is_ok());
    }

    #[test]
    fn id_token_must_not_be_expired_but_tolerates_clock_skew() {
        let with_exp = |exp: i64| {
            serde_json::json!({
                "iss": "https://accounts.google.com",
                "aud": TEST_CLIENT_ID,
                "exp": exp,
            })
        };
        let validate = |claims: &serde_json::Value| {
            validate_id_token_claims(
                claims,
                &["https://accounts.google.com"],
                TEST_CLIENT_ID,
                TEST_NOW,
            )
        };
        assert!(validate(&with_exp(TEST_NOW - 3600)).is_err());
        // Just inside the skew allowance.
        assert!(validate(&with_exp(TEST_NOW - 60)).is_ok());
        assert!(validate(&with_exp(TEST_NOW + 3600)).is_ok());

        // A token with no expiry at all is refused rather than treated as
        // eternal.
        let claims = serde_json::json!({
            "iss": "https://accounts.google.com",
            "aud": TEST_CLIENT_ID,
        });
        assert!(validate(&claims).is_err());
    }
}
