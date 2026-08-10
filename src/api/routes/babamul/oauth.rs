//! Routes implementing social sign-in for Babamul.
//!
//! ```text
//!   browser ──GET /babamul/oauth/{provider}/start──▶ api
//!   api ──302──▶ provider consent screen
//!   provider ──302 ?code&state──▶ /babamul/oauth/{provider}/callback
//!   api ──code+verifier──▶ provider token endpoint  (server-to-server)
//!   api ──302 {webapp}/oauth/callback#access_token=…──▶ browser
//! ```
//!
//! The JWT comes back in the URL *fragment*, which browsers never send to a
//! server, so the token stays out of access logs and `Referer` headers.

use crate::api::auth::{hash_token, AuthProvider};
use crate::api::email::EmailService;
use crate::api::models::response;
use crate::api::oauth::{
    authorization_url, exchange_code_for_identity, ExternalIdentity, OAuthProviderKind, Pkce,
};
use crate::api::routes::babamul::{
    create_babamul_jwt, generate_random_string, BabamulUser, LinkedIdentity,
};
use crate::conf::AppConfig;
use actix_web::{get, post, web, HttpResponse};
use mongodb::bson::doc;
use mongodb::Database;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use utoipa::ToSchema;

const OAUTH_STATES_COLLECTION: &str = "babamul_oauth_states";
const PENDING_IDENTITIES_COLLECTION: &str = "babamul_pending_identities";

/// How long a user has to supply and confirm an email address after signing in
/// with a provider that didn't give us one.
const PENDING_IDENTITY_TTL_SECONDS: i64 = 1800;

/// Wrong codes tolerated before a ticket is burned, so a 8-character code
/// cannot be brute-forced.
const MAX_VERIFICATION_ATTEMPTS: i32 = 5;

/// A pending authorization request, keyed by the opaque `state` value.
///
/// Consumed exactly once by the callback (`find_one_and_delete`), which is what
/// makes the state both a CSRF token and a replay guard.
#[derive(Serialize, Deserialize, Debug)]
struct PendingAuthorization {
    #[serde(rename = "_id")]
    state: String,
    provider: String,
    pkce_verifier: String,
    /// Relative path in the web app to land on after a successful login
    redirect_to: Option<String>,
    created_at: i64,
    expires_at: i64,
    /// Same instant as `expires_at`, as a BSON date. MongoDB's TTL monitor only
    /// understands date fields, so the index in
    /// [`ensure_oauth_state_index`] hangs off this one.
    expires_at_date: mongodb::bson::DateTime,
}

/// A provider-verified identity waiting on an email address.
///
/// Created when a sign-in succeeds but yields no email we can trust — the
/// normal ORCID case. It holds the authenticated identity while the user
/// supplies an address and proves they control it; no account exists until
/// that confirmation lands.
#[derive(Serialize, Deserialize, Debug)]
struct PendingIdentity {
    #[serde(rename = "_id")]
    ticket: String,
    provider: String,
    subject: String,
    orcid_id: Option<String>,
    name: Option<String>,
    redirect_to: Option<String>,
    /// Address the user typed, set once `/oauth/complete` is called
    email: Option<String>,
    /// SHA-256 of the confirmation code — never the code itself
    code_hash: Option<String>,
    code_expires_at: Option<i64>,
    #[serde(default)]
    attempts: i32,
    created_at: i64,
    expires_at: i64,
    /// TTL index field; see [`ensure_oauth_state_index`]
    expires_at_date: mongodb::bson::DateTime,
}

#[derive(Serialize, Clone, ToSchema)]
pub struct OAuthProviderInfo {
    /// Slug used in the OAuth route paths, e.g. `google`
    pub id: String,
    /// Label for the sign-in button, e.g. `Google`
    pub name: String,
    /// Absolute path the browser should navigate to in order to start sign-in
    pub start_url: String,
}

/// List the social sign-in providers this deployment has configured
#[utoipa::path(
    get,
    path = "/babamul/oauth/providers",
    responses(
        (status = 200, description = "Configured providers", body = Vec<OAuthProviderInfo>)
    ),
    tags=["Babamul"]
)]
#[get("/oauth/providers")]
pub async fn get_oauth_providers(config: web::Data<AppConfig>) -> HttpResponse {
    // A provider whose redirect base URL is missing would fail at /start, so
    // treat the whole feature as off rather than advertising a broken button.
    let providers: Vec<OAuthProviderInfo> = if config.babamul.oauth.redirect_base_url.is_none() {
        Vec::new()
    } else {
        OAuthProviderKind::ALL
            .iter()
            .filter(|provider| provider.config(&config).is_some())
            .map(|provider| OAuthProviderInfo {
                id: provider.as_str().to_string(),
                name: provider.display_name().to_string(),
                start_url: format!("/babamul/oauth/{}/start", provider),
            })
            .collect()
    };
    response::ok_ser("success", providers)
}

#[derive(Deserialize)]
pub struct OAuthStartQuery {
    /// Where to send the user in the web app once signed in. Only in-app
    /// absolute paths are honoured; anything else is ignored so this cannot be
    /// turned into an open redirect.
    pub redirect_to: Option<String>,
}

/// Begin a social sign-in flow by redirecting to the provider
#[utoipa::path(
    get,
    path = "/babamul/oauth/{provider}/start",
    params(
        ("provider" = String, Path, description = "One of `google`, `github`, `orcid`"),
        ("redirect_to" = Option<String>, Query, description = "In-app path to land on after login")
    ),
    responses(
        (status = 302, description = "Redirect to the provider's consent screen"),
        (status = 404, description = "Unknown or disabled provider"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[get("/oauth/{provider}/start")]
pub async fn get_oauth_start(
    db: web::Data<Database>,
    config: web::Data<AppConfig>,
    path: web::Path<String>,
    query: web::Query<OAuthStartQuery>,
) -> HttpResponse {
    let provider = match OAuthProviderKind::from_path_segment(&path) {
        Some(provider) => provider,
        None => return response::not_found("Unknown sign-in provider"),
    };
    if provider.config(&config).is_none() {
        return response::not_found("Sign-in provider is not enabled");
    }

    let state = generate_random_string(48);
    let pkce = Pkce::generate();

    let authorize_url = match authorization_url(&config, provider, &state, &pkce.challenge) {
        Ok(url) => url,
        Err(e) => {
            tracing::error!("Could not build {} authorization URL: {}", provider, e);
            return response::internal_error("Sign-in is misconfigured");
        }
    };

    let now = flare::Time::now().to_utc().timestamp();
    let expires_at = now + config.babamul.oauth.state_ttl_seconds;
    let pending = PendingAuthorization {
        state,
        provider: provider.as_str().to_string(),
        pkce_verifier: pkce.verifier,
        redirect_to: query.redirect_to.as_deref().and_then(safe_redirect_path),
        created_at: now,
        expires_at,
        expires_at_date: mongodb::bson::DateTime::from_millis(expires_at * 1000),
    };

    let states: mongodb::Collection<PendingAuthorization> = db.collection(OAUTH_STATES_COLLECTION);
    if let Err(e) = states.insert_one(&pending).await {
        tracing::error!("Could not store OAuth state: {}", e);
        return response::internal_error("Could not start sign-in");
    }

    HttpResponse::Found()
        .insert_header(("Location", authorize_url))
        .insert_header(("Cache-Control", "no-store"))
        .finish()
}

#[derive(Deserialize)]
pub struct OAuthCallbackQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}

/// Complete a social sign-in and hand the web app a Babamul JWT
#[utoipa::path(
    get,
    path = "/babamul/oauth/{provider}/callback",
    params(
        ("provider" = String, Path, description = "One of `google`, `github`, `orcid`"),
        ("code" = Option<String>, Query, description = "Authorization code from the provider"),
        ("state" = Option<String>, Query, description = "Opaque state issued by /start")
    ),
    responses(
        (status = 302, description = "Redirect back to the web app with a token or an error"),
        (status = 404, description = "Unknown or disabled provider")
    ),
    tags=["Babamul"]
)]
#[get("/oauth/{provider}/callback")]
pub async fn get_oauth_callback(
    db: web::Data<Database>,
    auth: web::Data<AuthProvider>,
    config: web::Data<AppConfig>,
    path: web::Path<String>,
    query: web::Query<OAuthCallbackQuery>,
) -> HttpResponse {
    let provider = match OAuthProviderKind::from_path_segment(&path) {
        Some(provider) => provider,
        None => return response::not_found("Unknown sign-in provider"),
    };
    if provider.config(&config).is_none() {
        return response::not_found("Sign-in provider is not enabled");
    }

    // The user declined consent, or the provider refused the request.
    if let Some(error) = &query.error {
        tracing::info!(
            "{} sign-in returned an error: {} {}",
            provider,
            error,
            query.error_description.as_deref().unwrap_or("")
        );
        return redirect_to_webapp(&config, None, Some("Sign-in was cancelled"), None);
    }

    let (code, state) = match (&query.code, &query.state) {
        (Some(code), Some(state)) if !code.is_empty() && !state.is_empty() => (code, state),
        _ => {
            return redirect_to_webapp(
                &config,
                None,
                Some("Sign-in response was incomplete"),
                None,
            );
        }
    };

    // Consuming the state here is what prevents both CSRF and code replay: a
    // second callback with the same state finds nothing.
    let states: mongodb::Collection<PendingAuthorization> = db.collection(OAUTH_STATES_COLLECTION);
    let pending = match states.find_one_and_delete(doc! { "_id": state }).await {
        Ok(Some(pending)) => pending,
        Ok(None) => {
            return redirect_to_webapp(
                &config,
                None,
                Some("Sign-in request expired or was already used. Please try again."),
                None,
            );
        }
        Err(e) => {
            tracing::error!("Could not look up OAuth state: {}", e);
            return redirect_to_webapp(&config, None, Some("Sign-in failed"), None);
        }
    };

    let now = flare::Time::now().to_utc().timestamp();
    if pending.expires_at <= now {
        return redirect_to_webapp(
            &config,
            None,
            Some("Sign-in request expired. Please try again."),
            None,
        );
    }
    // The state is scoped to the provider that issued it; a mismatch means the
    // callback was replayed against a different provider's endpoint.
    if pending.provider != provider.as_str() {
        return redirect_to_webapp(&config, None, Some("Sign-in request was invalid"), None);
    }

    let identity =
        match exchange_code_for_identity(&config, provider, code, &pending.pkce_verifier).await {
            Ok(identity) => identity,
            Err(e) => {
                tracing::error!("{} sign-in failed: {}", provider, e);
                return redirect_to_webapp(
                    &config,
                    None,
                    Some("Could not verify your account with the sign-in provider"),
                    None,
                );
            }
        };

    let user = match resolve_identity(&db, &config, &identity, pending.redirect_to.as_deref()).await
    {
        Ok(Resolution::SignedIn(user)) => *user,
        Ok(Resolution::NeedsEmail {
            ticket,
            suggested_email,
        }) => {
            // The provider gave us nothing we can trust as an email address, so
            // the user has to supply one and prove they control it before an
            // account exists.
            return redirect_to_email_prompt(
                &config,
                provider,
                &ticket,
                suggested_email.as_deref(),
            );
        }
        Ok(Resolution::PendingActivation { email }) => {
            // Signing in again does not skip the verification they never
            // finished; send them back to the confirmation step.
            return redirect_to_webapp(
                &config,
                None,
                Some(&format!(
                    "Your account still needs email confirmation. Check {} for the code, or sign in again to get a new one.",
                    email
                )),
                None,
            );
        }
        Err(ResolveError::Conflict(message)) => {
            return redirect_to_webapp(&config, None, Some(&message), None);
        }
        Err(ResolveError::Internal(e)) => {
            tracing::error!("Could not resolve {} identity: {}", provider, e);
            return redirect_to_webapp(&config, None, Some("Sign-in failed"), None);
        }
    };

    match create_babamul_jwt(&auth, &user.id).await {
        Ok((token, expires_in)) => redirect_to_webapp(
            &config,
            Some((token, expires_in)),
            None,
            pending.redirect_to.as_deref(),
        ),
        Err(e) => {
            tracing::error!("Failed to create JWT after {} sign-in: {}", provider, e);
            redirect_to_webapp(&config, None, Some("Sign-in failed"), None)
        }
    }
}

enum ResolveError {
    /// The sign-in is valid but cannot be attached to an account; the message
    /// is safe to show the user.
    Conflict(String),
    Internal(String),
}

/// Outcome of matching an external identity against the account store.
enum Resolution {
    /// Ready to issue a token. Boxed because `BabamulUser` dwarfs the other
    /// variants, which would otherwise pad the whole enum to its size.
    SignedIn(Box<BabamulUser>),
    /// The provider gave us no email we can trust, so the user must supply one
    /// and confirm it by mail before an account exists. The ticket carries the
    /// verified identity across that detour.
    NeedsEmail {
        ticket: String,
        suggested_email: Option<String>,
    },
    /// The linked account exists but never finished email confirmation.
    PendingActivation { email: String },
}

/// Match an external identity to a Babamul account.
///
/// Resolution order:
/// 1. An account already linked to this `(provider, subject)` pair. The join
///    key is the provider's stable id, never the email.
/// 2. An account whose email matches a **verified** provider email — the
///    identity is linked to it, so someone who signed up with a password can
///    later just press the Google button.
/// 3. Otherwise, if the provider vouched for an email, a fresh activated
///    account.
/// 4. Otherwise — no email, or one the provider would not vouch for — a pending
///    ticket, and the caller sends the user off to supply and confirm an
///    address. This is the usual ORCID path, since most researchers keep their
///    email private.
async fn resolve_identity(
    db: &Database,
    config: &AppConfig,
    identity: &ExternalIdentity,
    redirect_to: Option<&str>,
) -> Result<Resolution, ResolveError> {
    let users: mongodb::Collection<BabamulUser> = db.collection("babamul_users");
    let provider = identity.provider.as_str();
    let now = flare::Time::now().to_utc().timestamp();

    // 1. Already linked.
    match users
        .find_one(doc! {
            "identities": {
                "$elemMatch": { "provider": provider, "subject": &identity.subject }
            }
        })
        .await
    {
        Ok(Some(user)) => {
            if user.is_activated {
                return Ok(Resolution::SignedIn(Box::new(user)));
            }
            // Only the provider re-asserting the very address on the account
            // can stand in for the confirmation the user owes us. Otherwise
            // this is an ORCID account still waiting on its email check, and
            // signing in again must not be a way around it.
            let provider_vouches_for_this_email =
                identity.email_verified && identity.email.as_deref() == Some(user.email.as_str());
            if !provider_vouches_for_this_email {
                return Ok(Resolution::PendingActivation { email: user.email });
            }
            if let Err(e) = users
                .update_one(
                    doc! { "_id": &user.id },
                    doc! { "$set": { "is_activated": true, "activation_code": mongodb::bson::Bson::Null } },
                )
                .await
            {
                return Err(ResolveError::Internal(format!(
                    "Could not activate user {}: {}",
                    user.id, e
                )));
            }
            let mut user = user;
            user.is_activated = true;
            return Ok(Resolution::SignedIn(Box::new(user)));
        }
        Ok(None) => {}
        Err(e) => {
            return Err(ResolveError::Internal(format!(
                "Identity lookup failed: {}",
                e
            )))
        }
    }

    // 2 & 3 need an email the provider is willing to vouch for. Without one we
    // ask the user, rather than trusting an address nobody verified.
    let verified_email = match (identity.email_verified, identity.email.as_deref()) {
        (true, Some(email)) => email,
        _ => {
            let ticket = store_pending_identity(db, config, identity, redirect_to).await?;
            return Ok(Resolution::NeedsEmail {
                ticket,
                suggested_email: identity.email.clone(),
            });
        }
    };

    let linked = LinkedIdentity {
        provider: provider.to_string(),
        subject: identity.subject.clone(),
        email: Some(verified_email.to_string()),
        linked_at: now,
    };

    // 2. Link to the existing account with that address.
    match users.find_one(doc! { "email": verified_email }).await {
        Ok(Some(existing)) => {
            let user =
                link_identity_to_user(db, &existing, &linked, identity.orcid_id.as_deref()).await?;
            return Ok(Resolution::SignedIn(Box::new(user)));
        }
        Ok(None) => {}
        Err(e) => {
            return Err(ResolveError::Internal(format!(
                "Email lookup failed: {}",
                e
            )))
        }
    }

    // 3. New account, already activated: the provider authenticated the user
    //    and vouched for the address, so there is nothing left to confirm.
    let user = new_social_user(
        verified_email.to_string(),
        derive_username(identity, verified_email),
        linked,
        identity.orcid_id.clone(),
        now,
    )?;
    insert_social_user(&users, user)
        .await
        .map(|user| Resolution::SignedIn(Box::new(user)))
}

/// Attach a verified identity to an existing account, activating it if the
/// confirmation it was waiting on has now been satisfied.
async fn link_identity_to_user(
    db: &Database,
    existing: &BabamulUser,
    linked: &LinkedIdentity,
    orcid_id: Option<&str>,
) -> Result<BabamulUser, ResolveError> {
    let users: mongodb::Collection<BabamulUser> = db.collection("babamul_users");
    let mut set = doc! { "is_activated": true, "activation_code": mongodb::bson::Bson::Null };
    if let Some(orcid_id) = orcid_id {
        set.insert("orcid_id", orcid_id);
    }
    let linked_bson = mongodb::bson::to_bson(linked)
        .map_err(|e| ResolveError::Internal(format!("Could not encode identity: {}", e)))?;
    users
        .update_one(
            doc! { "_id": &existing.id },
            doc! { "$set": set, "$push": { "identities": linked_bson } },
        )
        .await
        .map_err(|e| {
            ResolveError::Internal(format!(
                "Could not link identity to user {}: {}",
                existing.id, e
            ))
        })?;

    let mut user = existing.clone();
    user.is_activated = true;
    user.activation_code = None;
    if let Some(orcid_id) = orcid_id {
        user.orcid_id = Some(orcid_id.to_string());
    }
    user.identities.push(linked.clone());
    Ok(user)
}

/// Build an activated account backed only by an external identity.
fn new_social_user(
    email: String,
    username: String,
    linked: LinkedIdentity,
    orcid_id: Option<String>,
    now: i64,
) -> Result<BabamulUser, ResolveError> {
    Ok(BabamulUser {
        id: uuid::Uuid::new_v4().to_string(),
        username,
        email,
        // Social accounts have no password. Storing the hash of an
        // unguessable random string keeps the field's shape without leaving a
        // usable credential; `/forgot-password` lets the user set a real one.
        password_hash: bcrypt::hash(generate_random_string(48), bcrypt::DEFAULT_COST).map_err(
            |e| ResolveError::Internal(format!("Could not hash placeholder password: {}", e)),
        )?,
        activation_code: None,
        is_activated: true,
        created_at: now,
        kafka_credentials: Vec::new(),
        tokens: Vec::new(),
        password_reset_token_hash: None,
        password_reset_token_expires_at: None,
        password_last_changed_at: None,
        identities: vec![linked],
        orcid_id,
    })
}

async fn insert_social_user(
    users: &mongodb::Collection<BabamulUser>,
    user: BabamulUser,
) -> Result<BabamulUser, ResolveError> {
    match users.insert_one(&user).await {
        Ok(_) => Ok(user),
        Err(e) if e.to_string().contains("E11000 duplicate key error") => {
            // Lost a race against a concurrent sign-up with the same email.
            // Retrying is the fix, not a password: the account now exists, so
            // the next attempt takes the link-to-existing-account path. Telling
            // the user to sign in with a password would be a dead end, since a
            // social account has none until they set one via forgot-password.
            Err(ResolveError::Conflict(
                "That account was just created by another sign-in. Please try signing in again."
                    .to_string(),
            ))
        }
        Err(e) => Err(ResolveError::Internal(format!(
            "Could not create user: {}",
            e
        ))),
    }
}

/// Build a display username from the provider's profile, falling back to the
/// local part of the email (the same rule the email sign-up path uses).
fn derive_username(identity: &ExternalIdentity, email: &str) -> String {
    let sanitize = |raw: &str| -> String {
        raw.chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '_' || *c == '-')
            .collect()
    };

    if let Some(name) = identity.name.as_deref() {
        let candidate = sanitize(&name.replace(' ', "-"));
        if !candidate.is_empty() {
            return candidate;
        }
    }
    let candidate = sanitize(email.split('@').next().unwrap_or(""));
    if !candidate.is_empty() {
        return candidate;
    }
    format!("{}-{}", identity.provider, generate_random_string(8))
}

/// Only in-app absolute paths may be used as a post-login destination.
///
/// `//evil.example` and `https://evil.example` are both rejected, so a crafted
/// `redirect_to` cannot bounce the user off-site with a fresh token.
fn safe_redirect_path(raw: &str) -> Option<String> {
    let path = raw.trim();
    if path.starts_with('/') && !path.starts_with("//") && !path.contains('\\') {
        Some(path.to_string())
    } else {
        None
    }
}

/// Send the browser back to the web app, carrying either a token or an error in
/// the URL fragment.
fn redirect_to_webapp(
    config: &AppConfig,
    token: Option<(String, Option<usize>)>,
    error: Option<&str>,
    redirect_to: Option<&str>,
) -> HttpResponse {
    let webapp_url = match config.babamul.webapp_url.as_deref() {
        Some(url) if !url.trim().is_empty() => url.trim().trim_end_matches('/').to_string(),
        _ => {
            // Nowhere to bounce to — surface the outcome directly instead of
            // redirecting into the void.
            return match error {
                Some(message) => response::bad_request(message),
                None => response::internal_error("babamul.webapp_url is not configured"),
            };
        }
    };

    let mut fragment = url::form_urlencoded::Serializer::new(String::new());
    match token {
        Some((access_token, expires_in)) => {
            fragment.append_pair("access_token", &access_token);
            fragment.append_pair("token_type", "Bearer");
            if let Some(expires_in) = expires_in {
                fragment.append_pair("expires_in", &expires_in.to_string());
            }
            if let Some(next) = redirect_to {
                fragment.append_pair("next", next);
            }
        }
        None => {
            fragment.append_pair("error", error.unwrap_or("Sign-in failed"));
        }
    }

    HttpResponse::Found()
        .insert_header((
            "Location",
            format!("{}/oauth/callback#{}", webapp_url, fragment.finish()),
        ))
        .insert_header(("Cache-Control", "no-store"))
        .finish()
}

/// Send the browser to the "confirm your email" page, carrying the ticket that
/// stands in for the identity we just authenticated.
fn redirect_to_email_prompt(
    config: &AppConfig,
    provider: OAuthProviderKind,
    ticket: &str,
    suggested_email: Option<&str>,
) -> HttpResponse {
    let webapp_url = match config.babamul.webapp_url.as_deref() {
        Some(url) if !url.trim().is_empty() => url.trim().trim_end_matches('/').to_string(),
        _ => return response::internal_error("babamul.webapp_url is not configured"),
    };

    let mut fragment = url::form_urlencoded::Serializer::new(String::new());
    fragment.append_pair("ticket", ticket);
    fragment.append_pair("provider", provider.as_str());
    fragment.append_pair("provider_name", provider.display_name());
    if let Some(email) = suggested_email {
        // Prefill only — the address still has to be confirmed by mail, so an
        // unverified provider email is a convenience, not a credential.
        fragment.append_pair("suggested_email", email);
    }

    HttpResponse::Found()
        .insert_header((
            "Location",
            format!("{}/oauth/complete#{}", webapp_url, fragment.finish()),
        ))
        .insert_header(("Cache-Control", "no-store"))
        .finish()
}

/// Park a provider-verified identity until the user confirms an email address.
async fn store_pending_identity(
    db: &Database,
    config: &AppConfig,
    identity: &ExternalIdentity,
    redirect_to: Option<&str>,
) -> Result<String, ResolveError> {
    let _ = config;
    let now = flare::Time::now().to_utc().timestamp();
    let expires_at = now + PENDING_IDENTITY_TTL_SECONDS;
    let ticket = generate_random_string(48);
    let pending = PendingIdentity {
        ticket: ticket.clone(),
        provider: identity.provider.as_str().to_string(),
        subject: identity.subject.clone(),
        orcid_id: identity.orcid_id.clone(),
        name: identity.name.clone(),
        redirect_to: redirect_to.map(str::to_string),
        email: None,
        code_hash: None,
        code_expires_at: None,
        attempts: 0,
        created_at: now,
        expires_at,
        expires_at_date: mongodb::bson::DateTime::from_millis(expires_at * 1000),
    };
    let pending_identities: mongodb::Collection<PendingIdentity> =
        db.collection(PENDING_IDENTITIES_COLLECTION);
    pending_identities
        .insert_one(&pending)
        .await
        .map_err(|e| ResolveError::Internal(format!("Could not store pending identity: {}", e)))?;
    Ok(ticket)
}

async fn load_pending_identity(
    db: &Database,
    ticket: &str,
) -> Result<Option<PendingIdentity>, mongodb::error::Error> {
    let now = flare::Time::now().to_utc().timestamp();
    db.collection::<PendingIdentity>(PENDING_IDENTITIES_COLLECTION)
        // Filter on the timestamp rather than trusting the TTL monitor, which
        // only sweeps about once a minute.
        .find_one(doc! { "_id": ticket, "expires_at": { "$gt": now } })
        .await
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct OAuthCompletePost {
    /// Ticket handed to the web app by the sign-in redirect
    pub ticket: String,
    pub email: String,
}

#[derive(Serialize, Clone, ToSchema)]
pub struct OAuthCompleteResponse {
    pub message: String,
    pub email: String,
}

/// Supply an email address for a social sign-in that didn't come with one
///
/// Emails a confirmation code and returns; no account is touched until
/// `/babamul/oauth/verify` receives that code back.
#[utoipa::path(
    post,
    path = "/babamul/oauth/complete",
    request_body = OAuthCompletePost,
    responses(
        (status = 200, description = "Confirmation code sent", body = OAuthCompleteResponse),
        (status = 400, description = "Invalid email, or expired/unknown ticket"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[post("/oauth/complete")]
pub async fn post_oauth_complete(
    db: web::Data<Database>,
    config: web::Data<AppConfig>,
    email_service: web::Data<EmailService>,
    body: web::Json<OAuthCompletePost>,
) -> HttpResponse {
    let email = body.email.trim().to_lowercase();
    if !crate::api::routes::babamul::is_valid_email(&email) {
        return response::bad_request("Invalid email address");
    }

    let pending = match load_pending_identity(&db, body.ticket.trim()).await {
        Ok(Some(pending)) => pending,
        Ok(None) => {
            return response::bad_request(
                "This sign-in request has expired. Please sign in again.",
            );
        }
        Err(e) => {
            tracing::error!("Could not load pending identity: {}", e);
            return response::internal_error("Database error");
        }
    };

    // Codes are typed by hand, so keep them short; the attempt cap in
    // `/oauth/verify` is what makes that safe.
    let code = generate_random_string(8).to_uppercase();
    let now = flare::Time::now().to_utc().timestamp();

    let pending_identities: mongodb::Collection<PendingIdentity> =
        db.collection(PENDING_IDENTITIES_COLLECTION);
    if let Err(e) = pending_identities
        .update_one(
            doc! { "_id": &pending.ticket },
            doc! {
                "$set": {
                    "email": &email,
                    "code_hash": hash_token(&code),
                    "code_expires_at": pending.expires_at.min(now + PENDING_IDENTITY_TTL_SECONDS),
                    // A fresh code deserves a fresh budget of attempts.
                    "attempts": 0,
                }
            },
        )
        .await
    {
        tracing::error!("Could not attach email to pending identity: {}", e);
        return response::internal_error("Could not send confirmation code");
    }

    let provider_name = OAuthProviderKind::from_path_segment(&pending.provider)
        .map(|p| p.display_name())
        .unwrap_or("your account");

    if email_service.is_enabled() {
        if let Err(e) = email_service.send_identity_verification_email(
            &email,
            provider_name,
            &code,
            &pending.ticket,
            &config.api.domain,
            &config.babamul.webapp_url,
        ) {
            tracing::error!("Failed to send confirmation email to {}: {}", email, e);
            return response::internal_error(
                "Could not send the confirmation email. Please try again.",
            );
        }
    } else {
        tracing::info!(
            "Email service disabled - confirmation code for {} ({}): {}",
            email,
            pending.ticket,
            code
        );
    }

    HttpResponse::Ok().json(OAuthCompleteResponse {
        message: format!(
            "A confirmation code has been sent to {}. Enter it to finish signing in.",
            email
        ),
        email,
    })
}

#[derive(Deserialize, Clone, ToSchema)]
pub struct OAuthVerifyPost {
    pub ticket: String,
    pub code: String,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Serialize, Clone, ToSchema)]
pub struct OAuthVerifyResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: Option<usize>,
    /// In-app path the user was heading to when they started signing in
    pub next: Option<String>,
}

/// Confirm the emailed code and finish a social sign-in
///
/// On success the identity is linked to the account with that address — or a
/// new account is created — and a Babamul JWT is returned.
#[utoipa::path(
    post,
    path = "/babamul/oauth/verify",
    request_body = OAuthVerifyPost,
    responses(
        (status = 200, description = "Signed in", body = OAuthVerifyResponse),
        (status = 400, description = "Wrong or expired code, or unknown ticket"),
        (status = 429, description = "Too many wrong codes; start over"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Babamul"]
)]
#[post("/oauth/verify")]
pub async fn post_oauth_verify(
    db: web::Data<Database>,
    auth: web::Data<AuthProvider>,
    body: web::Json<OAuthVerifyPost>,
) -> HttpResponse {
    let ticket = body.ticket.trim();
    let pending_identities: mongodb::Collection<PendingIdentity> =
        db.collection(PENDING_IDENTITIES_COLLECTION);

    let pending = match load_pending_identity(&db, ticket).await {
        Ok(Some(pending)) => pending,
        Ok(None) => {
            return response::bad_request(
                "This sign-in request has expired. Please sign in again.",
            );
        }
        Err(e) => {
            tracing::error!("Could not load pending identity: {}", e);
            return response::internal_error("Database error");
        }
    };

    let (email, code_hash) = match (&pending.email, &pending.code_hash) {
        (Some(email), Some(code_hash)) => (email.clone(), code_hash.clone()),
        _ => return response::bad_request("No confirmation code has been requested yet"),
    };

    let now = flare::Time::now().to_utc().timestamp();
    if pending.code_expires_at.unwrap_or(0) <= now {
        return response::bad_request("That code has expired. Please sign in again.");
    }

    if pending.attempts >= MAX_VERIFICATION_ATTEMPTS {
        // Burn the ticket rather than leaving it to be ground down.
        let _ = pending_identities.delete_one(doc! { "_id": ticket }).await;
        return HttpResponse::TooManyRequests().json(
            crate::api::models::response::ApiResponseBody::error(
                "Too many incorrect codes. Please sign in again.",
            ),
        );
    }

    if hash_token(body.code.trim().to_uppercase().as_str()) != code_hash {
        if let Err(e) = pending_identities
            .update_one(doc! { "_id": ticket }, doc! { "$inc": { "attempts": 1 } })
            .await
        {
            tracing::error!("Could not record a failed confirmation attempt: {}", e);
        }
        return response::bad_request("Incorrect code");
    }

    let provider = match OAuthProviderKind::from_path_segment(&pending.provider) {
        Some(provider) => provider,
        None => {
            tracing::error!("Pending identity has unknown provider {}", pending.provider);
            return response::internal_error("Sign-in failed");
        }
    };

    // The user proved they control this mailbox, which is exactly the assurance
    // a password reset relies on — so the identity may be attached to an
    // account that already owns the address.
    let identity = ExternalIdentity {
        provider,
        subject: pending.subject.clone(),
        email: Some(email.clone()),
        email_verified: true,
        name: pending.name.clone(),
        orcid_id: pending.orcid_id.clone(),
    };
    let linked = LinkedIdentity {
        provider: pending.provider.clone(),
        subject: pending.subject.clone(),
        email: Some(email.clone()),
        linked_at: now,
    };

    let users: mongodb::Collection<BabamulUser> = db.collection("babamul_users");
    let user = match users.find_one(doc! { "email": &email }).await {
        Ok(Some(existing)) => {
            match link_identity_to_user(&db, &existing, &linked, pending.orcid_id.as_deref()).await
            {
                Ok(user) => user,
                Err(e) => return resolve_error_response(e),
            }
        }
        Ok(None) => {
            let user = match new_social_user(
                email.clone(),
                derive_username(&identity, &email),
                linked,
                pending.orcid_id.clone(),
                now,
            ) {
                Ok(user) => user,
                Err(e) => return resolve_error_response(e),
            };
            match insert_social_user(&users, user).await {
                Ok(user) => user,
                Err(e) => return resolve_error_response(e),
            }
        }
        Err(e) => {
            tracing::error!("Email lookup failed during confirmation: {}", e);
            return response::internal_error("Database error");
        }
    };

    // Single-use: the ticket dies with the sign-in it authorised.
    if let Err(e) = pending_identities.delete_one(doc! { "_id": ticket }).await {
        tracing::error!("Could not clear a used pending identity: {}", e);
    }

    match create_babamul_jwt(&auth, &user.id).await {
        Ok((token, expires_in)) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(OAuthVerifyResponse {
                access_token: token,
                token_type: "Bearer".into(),
                expires_in,
                next: pending.redirect_to,
            }),
        Err(e) => {
            tracing::error!("Failed to create JWT after email confirmation: {}", e);
            response::internal_error("Could not complete sign-in")
        }
    }
}

fn resolve_error_response(error: ResolveError) -> HttpResponse {
    match error {
        ResolveError::Conflict(message) => response::bad_request(&message),
        ResolveError::Internal(e) => {
            tracing::error!("Sign-in failed: {}", e);
            response::internal_error("Sign-in failed")
        }
    }
}

/// Ensure the short-lived OAuth records expire on their own.
///
/// Called once at startup. Without these the collections would grow forever
/// with abandoned sign-in attempts, since only completed flows delete their own
/// state.
pub async fn ensure_oauth_state_index(db: &Database) -> Result<(), mongodb::error::Error> {
    let ttl_index = || {
        mongodb::IndexModel::builder()
            .keys(doc! { "expires_at_date": 1 })
            .options(
                mongodb::options::IndexOptions::builder()
                    .expire_after(std::time::Duration::from_secs(0))
                    .build(),
            )
            .build()
    };
    db.collection::<PendingAuthorization>(OAUTH_STATES_COLLECTION)
        .create_index(ttl_index())
        .await?;
    db.collection::<PendingIdentity>(PENDING_IDENTITIES_COLLECTION)
        .create_index(ttl_index())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::oauth::OAuthProviderKind;

    fn identity(name: Option<&str>, email: &str) -> ExternalIdentity {
        ExternalIdentity {
            provider: OAuthProviderKind::Google,
            subject: "sub-1".to_string(),
            email: Some(email.to_string()),
            email_verified: true,
            name: name.map(str::to_string),
            orcid_id: None,
        }
    }

    #[test]
    fn username_prefers_the_provider_display_name() {
        assert_eq!(
            derive_username(
                &identity(Some("Ada Lovelace"), "ada@example.org"),
                "ada@example.org"
            ),
            "Ada-Lovelace"
        );
    }

    #[test]
    fn username_falls_back_to_the_email_local_part() {
        assert_eq!(
            derive_username(&identity(None, "ada.l@example.org"), "ada.l@example.org"),
            "ada.l"
        );
        // A name made entirely of characters we strip is not a usable username.
        assert_eq!(
            derive_username(&identity(Some("☆☆☆"), "ada@example.org"), "ada@example.org"),
            "ada"
        );
    }

    #[test]
    fn redirect_paths_must_stay_in_the_app() {
        assert_eq!(safe_redirect_path("/query"), Some("/query".to_string()));
        assert_eq!(
            safe_redirect_path("  /profile  "),
            Some("/profile".to_string())
        );
        assert_eq!(safe_redirect_path("//evil.example"), None);
        assert_eq!(safe_redirect_path("https://evil.example"), None);
        assert_eq!(safe_redirect_path("/\\evil.example"), None);
        assert_eq!(safe_redirect_path("query"), None);
    }
}
