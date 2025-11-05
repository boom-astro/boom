use crate::api::auth::AuthProvider;
use actix_web::{get, post, web, HttpRequest, HttpResponse};
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use utoipa::ToSchema;

#[derive(Deserialize, Clone, ToSchema)]
pub struct AuthPost {
    pub username: String,
    pub password: String,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Deserialize, Serialize, Clone, ToSchema)]
pub struct AuthResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: Option<usize>,
}

#[derive(Deserialize, Serialize, Clone, ToSchema)]
pub struct FailedAuthResponse {
    pub error: String,
    pub error_description: String,
}

/// Authenticate a user
#[utoipa::path(
    post,
    path = "/auth",
    request_body(content = AuthPost, content_type = "application/x-www-form-urlencoded"),
    responses(
        (status = 200, description = "Successful authentication", body = AuthResponse),
        (status = 401, description = "Invalid Client", body = FailedAuthResponse),
        (status = 400, description = "Invalid Request", body = FailedAuthResponse),
    ),
    tags=["Auth"]
)]
#[post("/auth")]
pub async fn post_auth(auth: web::Data<AuthProvider>, body: web::Form<AuthPost>) -> HttpResponse {
    // Check if the user exists and the password matches
    match auth
        .create_token_for_user(&body.username, &body.password)
        .await
    {
        Ok((token, expires_in)) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(AuthResponse {
                access_token: token,
                token_type: "Bearer".into(),
                expires_in,
            }),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound
                || e.kind() == std::io::ErrorKind::InvalidInput
            {
                // if the error is NotFound (username or password is incorrect), return a 401
                HttpResponse::Unauthorized().json(FailedAuthResponse {
                    error: "invalid_client".into(),
                    error_description: "Invalid username or password".into(),
                })
            } else {
                // for other errors, return a 400
                HttpResponse::BadRequest().json(FailedAuthResponse {
                    error: "invalid_request".into(),
                    error_description: format!("{}", e),
                })
            }
        }
    }
}

use openidconnect::core::{
    CoreAuthDisplay, CoreClaimName, CoreClaimType, CoreClient, CoreClientAuthMethod, CoreGrantType,
    CoreIdTokenClaims, CoreIdTokenVerifier, CoreJsonWebKey, CoreJweContentEncryptionAlgorithm,
    CoreJweKeyManagementAlgorithm, CoreResponseMode, CoreResponseType, CoreRevocableToken,
    CoreSubjectIdentifierType,
};
use openidconnect::reqwest;
use openidconnect::{
    AdditionalProviderMetadata, AuthenticationFlow, AuthorizationCode, ClientId, ClientSecret,
    CsrfToken, IssuerUrl, Nonce, OAuth2TokenResponse, ProviderMetadata, RedirectUrl, RevocationUrl,
    Scope,
};

// Teach openidconnect-rs about a Google custom extension to the OpenID Discovery response that we can use as the RFC
// 7009 OAuth 2.0 Token Revocation endpoint. For more information about the Google specific Discovery response see the
// Google OpenID Connect service documentation at: https://developers.google.com/identity/protocols/oauth2/openid-connect#discovery
#[derive(Clone, Debug, Deserialize, Serialize)]
struct RevocationEndpointProviderMetadata {
    revocation_endpoint: String,
}
impl AdditionalProviderMetadata for RevocationEndpointProviderMetadata {}
type GoogleProviderMetadata = ProviderMetadata<
    RevocationEndpointProviderMetadata,
    CoreAuthDisplay,
    CoreClientAuthMethod,
    CoreClaimName,
    CoreClaimType,
    CoreGrantType,
    CoreJweContentEncryptionAlgorithm,
    CoreJweKeyManagementAlgorithm,
    CoreJsonWebKey,
    CoreResponseMode,
    CoreResponseType,
    CoreSubjectIdentifierType,
>;

#[derive(Deserialize, Serialize, Debug)]
pub enum OAuthProvider {
    #[serde(rename = "google")]
    Google,
    // Add other providers as needed
}

#[get("/login/{provider}")]
// let's implement an endpoint to login using a google account, using OAuth2
pub async fn oauth2_login(provider: web::Path<OAuthProvider>) -> HttpResponse {
    // This function will handle the OAuth2 flow for Google login
    println!("Logging in with provider: {:?}", provider);
    let google_client_id = ClientId::new(
        std::env::var("GOOGLE_CLIENT_ID")
            .expect("Missing the GOOGLE_CLIENT_ID environment variable."),
    );
    let google_client_secret = ClientSecret::new(
        std::env::var("GOOGLE_CLIENT_SECRET")
            .expect("Missing the GOOGLE_CLIENT_SECRET environment variable."),
    );
    let issuer_url = match IssuerUrl::new("https://accounts.google.com".to_string()) {
        Ok(url) => url,
        Err(err) => {
            return HttpResponse::InternalServerError()
                .body(format!("Invalid issuer URL: {}", err));
        }
    };

    // Fetch Google's OpenID Connect discovery document.
    let http_client = match reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            return HttpResponse::InternalServerError()
                .body(format!("Failed to create HTTP client: {}", err));
        }
    };

    let provider_metadata =
        match GoogleProviderMetadata::discover_async(issuer_url, &http_client).await {
            Ok(metadata) => metadata,
            Err(err) => {
                return HttpResponse::InternalServerError()
                    .body(format!("Failed to discover OpenID Provider: {}", err));
            }
        };

    let revocation_endpoint = provider_metadata
        .additional_metadata()
        .revocation_endpoint
        .clone();

    // Set up the config for the Google OAuth2 process.
    let redirect_uri =
        match RedirectUrl::new("http://localhost:4000/login/google/callback".to_string()) {
            Ok(url) => url,
            Err(err) => {
                return HttpResponse::InternalServerError()
                    .body(format!("Invalid redirect URL: {}", err));
            }
        };
    let revocation_url = match RevocationUrl::new(revocation_endpoint) {
        Ok(url) => url,
        Err(err) => {
            return HttpResponse::InternalServerError()
                .body(format!("Invalid revocation endpoint URL: {}", err));
        }
    };
    let client = CoreClient::from_provider_metadata(
        provider_metadata,
        google_client_id,
        Some(google_client_secret),
    )
    .set_redirect_uri(redirect_uri)
    .set_revocation_url(revocation_url);

    // Generate the authorization URL to which we'll redirect the user.
    let (authorize_url, csrf_state, nonce) = client
        .authorize_url(
            AuthenticationFlow::<CoreResponseType>::AuthorizationCode,
            CsrfToken::new_random,
            Nonce::new_random,
        )
        // This example is requesting access to the "calendar" features and the user's profile.
        .add_scope(Scope::new("email".to_string()))
        .add_scope(Scope::new("profile".to_string()))
        .url();
    println!("CSRF state: {}", csrf_state.secret());
    println!("Nonce: {}", nonce.secret());

    // set the CSRF state and nonce in an http-only cookie to validate in the callback
    // redirect the user to the authorize_url
    actix_web::HttpResponse::Found()
        .cookie(
            actix_web::cookie::Cookie::build("oauth_csrf_state", csrf_state.secret().to_string())
                .http_only(true)
                .finish(),
        )
        .cookie(
            actix_web::cookie::Cookie::build("oauth_nonce", nonce.secret().to_string())
                .http_only(true)
                .finish(),
        )
        .insert_header(("Location", authorize_url.to_string()))
        .finish()
}

#[derive(Deserialize, Serialize, Debug)]
struct OAuthCallbackQuery {
    code: String,
    state: String,
    scope: Option<String>,
    authuser: Option<String>,
    prompt: Option<String>,
}

// implement a dummy callback endpoint for google oauth2, where we read the body as simple text
#[get("/login/{provider}/callback")]
pub async fn oauth2_callback(
    provider: web::Path<OAuthProvider>,
    query: web::Query<OAuthCallbackQuery>,
    req: HttpRequest,
) -> HttpResponse {
    println!("OAuth2 callback from provider: {:?}", provider);
    println!("Query code: {:?}", query.code);
    println!("Query state: {:?}", query.state);
    println!("Query scope: {:?}", query.scope);
    println!("Query authuser: {:?}", query.authuser);
    println!("Query prompt: {:?}", query.prompt);
    let code = AuthorizationCode::new(query.code.clone());
    let state = CsrfToken::new(query.state.clone());
    println!("Google returned the following code:\n{}\n", code.secret());
    println!("Google returned the following state:\n{}", state.secret(),);
    // get the oauth_csrf_state and oauth_nonce cookies from the request
    let cookies = req.cookies().unwrap();
    // debug, print all cookies
    for cookie in cookies.iter() {
        println!("Cookie: {}={}", cookie.name(), cookie.value());
    }
    let csrf_cookie = cookies.iter().find(|c| c.name() == "oauth_csrf_state");
    let nonce_cookie = cookies.iter().find(|c| c.name() == "oauth_nonce");
    if csrf_cookie.is_none() || nonce_cookie.is_none() {
        return HttpResponse::BadRequest().body("Missing CSRF state or nonce cookies");
    }
    let csrf_cookie = csrf_cookie.unwrap();
    let nonce_cookie = nonce_cookie.unwrap();
    let csrf_state = CsrfToken::new(csrf_cookie.value().to_string());
    let nonce = Nonce::new(nonce_cookie.value().to_string());
    // validate that the state returned by google matches the csrf_state cookie
    if state.secret() != csrf_state.secret() {
        return HttpResponse::BadRequest().body("Invalid CSRF state");
    }

    let google_client_id = ClientId::new(
        std::env::var("GOOGLE_CLIENT_ID")
            .expect("Missing the GOOGLE_CLIENT_ID environment variable."),
    );
    let google_client_secret = ClientSecret::new(
        std::env::var("GOOGLE_CLIENT_SECRET")
            .expect("Missing the GOOGLE_CLIENT_SECRET environment variable."),
    );
    let issuer_url = match IssuerUrl::new("https://accounts.google.com".to_string()) {
        Ok(url) => url,
        Err(err) => {
            return HttpResponse::InternalServerError()
                .body(format!("Invalid issuer URL: {}", err));
        }
    };

    // Fetch Google's OpenID Connect discovery document.
    let http_client = match reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            return HttpResponse::InternalServerError()
                .body(format!("Failed to create HTTP client: {}", err));
        }
    };

    let provider_metadata =
        match GoogleProviderMetadata::discover_async(issuer_url, &http_client).await {
            Ok(metadata) => metadata,
            Err(err) => {
                return HttpResponse::InternalServerError()
                    .body(format!("Failed to discover OpenID Provider: {}", err));
            }
        };

    let revocation_endpoint = provider_metadata
        .additional_metadata()
        .revocation_endpoint
        .clone();

    // Set up the config for the Google OAuth2 process.
    let redirect_uri =
        match RedirectUrl::new("http://localhost:4000/login/google/callback".to_string()) {
            Ok(url) => url,
            Err(err) => {
                return HttpResponse::InternalServerError()
                    .body(format!("Invalid redirect URL: {}", err));
            }
        };
    let revocation_url = match RevocationUrl::new(revocation_endpoint) {
        Ok(url) => url,
        Err(err) => {
            return HttpResponse::InternalServerError()
                .body(format!("Invalid revocation endpoint URL: {}", err));
        }
    };
    let client = CoreClient::from_provider_metadata(
        provider_metadata,
        google_client_id,
        Some(google_client_secret),
    )
    .set_redirect_uri(redirect_uri)
    .set_revocation_url(revocation_url);
    // Exchange the code with a token.
    let token_response = match client.exchange_code(code) {
        Ok(response) => response,
        Err(err) => {
            return HttpResponse::InternalServerError()
                .body(format!("Failed to exchange code for token: {}", err));
        }
    };
    let token_response = match token_response.request_async(&http_client).await {
        Ok(response) => response,
        Err(err) => {
            return HttpResponse::InternalServerError()
                .body(format!("Failed to contact token endpoint: {}", err));
        }
    };

    println!(
        "Google returned access token:\n{}\n",
        token_response.access_token().secret()
    );
    println!("Google returned scopes: {:?}", token_response.scopes());

    let id_token_verifier: CoreIdTokenVerifier = client.id_token_verifier();
    let id_token_claims: &CoreIdTokenClaims = match token_response
        .extra_fields()
        .id_token()
        .expect("Server did not return an ID token")
        .claims(&id_token_verifier, &nonce)
    {
        Ok(claims) => claims,
        Err(err) => {
            return HttpResponse::InternalServerError()
                .body(format!("Failed to verify ID token: {}", err));
        }
    };
    println!("Google returned ID token: {:?}", id_token_claims);
    HttpResponse::Ok().body(format!(
        "OAuth2 callback from provider: {:?}, query: {:?}",
        provider, query
    ))
}
