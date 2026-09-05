use crate::utils::{
    cutouts::{CutoutCache, CutoutStorage},
    enums::Survey,
    o11y::logging::as_error,
};
use chrono::NaiveDate;
use config::{Config, File, Value};
use dotenvy;
use mongodb::bson::{doc, Document};
use mongodb::Database;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
use std::{collections::HashMap, path::Path};
use tracing::{debug, error, info, instrument, warn};

const DEFAULT_CONFIG_PATH: &str = "config.yaml";

static HASHED_SECRET_KEY: OnceLock<[u8; 32]> = OnceLock::new();

#[derive(thiserror::Error, Debug)]
pub enum BoomConfigError {
    #[error("failed to load config ({0})")]
    InvalidConfigError(#[from] config::ConfigError),
    #[error("failed to connect to database using config")]
    ConnectMongoError(#[from] mongodb::error::Error),
    #[error("failed to connect to redis using config")]
    ConnectRedisError(#[from] redis::RedisError),
    #[error("could not find config file")]
    ConfigFileNotFound,
    #[error("missing key in config: {0}")]
    MissingKeyError(String),
    #[error("failed to deserialize config: {0}")]
    InvalidSecretError(String),
    #[error("cutout storage error: {0}")]
    CutoutStorageError(#[from] crate::utils::cutouts::CutoutStorageError),
    #[error("invalid crossmatch config: {0}")]
    UnknownCrossmatchCatalog(String),
}

/// Load environment variables from a .env file if it exists.
/// This function should be called early in the application startup,
/// typically before any configuration loading.
///
/// The function looks for .env files in this order:
/// 1. .env in the current working directory
/// 2. .env in the parent directory (useful when running from subdirs)
/// 3. If none found, continues without error (env vars may be set by system)
pub fn load_dotenv() {
    if Path::new(".env").exists() {
        match dotenvy::dotenv() {
            Ok(_) => debug!("Loaded environment variables from .env file"),
            Err(e) => warn!("Found .env file but failed to load it: {}", e),
        }
        return;
    }

    if Path::new("../.env").exists() {
        match dotenvy::from_path("../.env") {
            Ok(_) => debug!("Loaded environment variables from ../.env file"),
            Err(e) => warn!("Found ../.env file but failed to load it: {}", e),
        }
        return;
    }

    info!("No .env file found, using system environment variables only");
}

#[instrument(err)]
pub fn load_raw_config(filepath: &str) -> Result<Config, BoomConfigError> {
    let path = Path::new(filepath);

    if !path.exists() {
        return Err(BoomConfigError::ConfigFileNotFound);
    }

    load_dotenv();

    let conf = Config::builder()
        .add_source(File::from(path))
        .add_source(env_source())
        .build()?;

    Ok(conf)
}

/// Accept a list as either a YAML sequence or a comma-separated string.
///
/// A list has no natural single-variable form, and these lists have to be
/// settable from the environment -- `babamul.admin_emails` decides who may
/// mutate the data, so it belongs with the other deployment settings rather
/// than only in a file.
///
/// Done as a field deserializer rather than by turning on the config crate's
/// `list_separator`, which only takes effect with `try_parsing` and would then
/// coerce *every* env value that looks numeric into an integer -- including a
/// password that happens to be all digits.
///
/// Blank entries are dropped, so a trailing comma or a stray space is not a
/// silent extra "" entry that matches nothing.
fn comma_separated<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum SequenceOrString {
        Sequence(Vec<String>),
        String(String),
    }

    Ok(match SequenceOrString::deserialize(deserializer)? {
        SequenceOrString::Sequence(items) => items,
        SequenceOrString::String(value) => value
            .split(',')
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
            .collect(),
    })
}

/// The `BOOM_*` environment overlay applied on top of `config.yaml`.
///
/// Split out from [`load_raw_config`] so tests can exercise the exact source
/// production uses while feeding it a fake environment via
/// [`config::Environment::source`], rather than mutating the process's own.
fn env_source() -> config::Environment {
    config::Environment::with_prefix("boom")
        .prefix_separator("_")
        .separator("__")
        // Compose renders every `${VAR:-}` as `VAR=`, blanking the YAML. See AGENTS.md.
        .ignore_empty(true)
}

#[instrument(skip_all, err)]
async fn _build_db(db_conf: &DatabaseConfig) -> Result<Database, BoomConfigError> {
    let mut uri = if db_conf.srv {
        "mongodb+srv://".to_string()
    } else {
        "mongodb://".to_string()
    };

    let using_auth = !db_conf.username.is_empty() && !db_conf.password.is_empty();

    if using_auth {
        uri.push_str(&db_conf.username);
        uri.push(':');
        uri.push_str(&db_conf.password);
        uri.push('@');
    }

    uri.push_str(&db_conf.host);
    uri.push(':');
    uri.push_str(&db_conf.port.to_string());

    uri.push('/');
    uri.push_str(&db_conf.name);

    uri.push_str("?directConnection=true");

    if using_auth {
        uri.push_str("&authSource=admin");
    }

    if let Some(replica_set) = &db_conf.replica_set {
        uri.push_str(&format!("&replicaSet={}", replica_set));
    }

    uri.push_str(&format!("&maxPoolSize={}", db_conf.max_pool_size));

    let client_mongo = mongodb::Client::with_uri_str(&uri).await?;
    let db = client_mongo.database(&db_conf.name);

    Ok(db)
}

#[instrument(skip_all, err)]
async fn build_db(conf: &AppConfig) -> Result<Database, BoomConfigError> {
    let db_conf = &conf.database;

    _build_db(db_conf).await
}

#[instrument(skip_all, err)]
async fn build_redis_conn(
    redis_conf: &RedisConfig,
) -> Result<redis::aio::MultiplexedConnection, BoomConfigError> {
    let uri = format!("redis://{}:{}/", redis_conf.host, redis_conf.port);

    let client_redis =
        redis::Client::open(uri).inspect_err(as_error!("failed to connect to redis"))?;

    let con = client_redis
        .get_multiplexed_async_connection()
        .await
        .inspect_err(as_error!("failed to get multiplexed connection"))?;

    Ok(con)
}

#[instrument(skip_all, err)]
async fn build_cutout_cache_conn(
    cache_conf: &CutoutCacheConfig,
) -> Result<redis::aio::MultiplexedConnection, BoomConfigError> {
    let uri = format!("redis://{}:{}/", cache_conf.host, cache_conf.port);
    let client =
        redis::Client::open(uri).inspect_err(as_error!("failed to connect to cutout cache"))?;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .inspect_err(as_error!(
            "failed to get multiplexed connection for cutout cache"
        ))?;
    if let Err(e) = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(&cache_conf.max_memory)
        .query_async::<()>(&mut con)
        .await
    {
        warn!(
            "Failed to set maxmemory '{}' on cutout cache (may already be configured externally): {:?}",
            cache_conf.max_memory, e
        );
    }
    Ok(con)
}

#[instrument(skip_all, err)]
async fn build_redis(
    conf: &AppConfig,
) -> Result<redis::aio::MultiplexedConnection, BoomConfigError> {
    build_redis_conn(&conf.redis).await
}

fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

#[instrument(skip_all, err)]
async fn build_cutout_storage(
    survey: &Survey,
    conf: &AppConfig,
) -> Result<CutoutStorage, BoomConfigError> {
    let storage = match &conf.cutouts_storage {
        CutoutsStorage::S3(s3_conf) => {
            let credentials_static_str = string_to_static_str(s3_conf.credentials_provider.clone());
            let credentials = aws_sdk_s3::config::Credentials::new(
                s3_conf.access_key.clone(),
                s3_conf.secret_key.clone(),
                None,
                None,
                credentials_static_str,
            );
            let region = aws_sdk_s3::config::Region::new(s3_conf.region.clone());

            let mut s3_config_builder =
                aws_config::defaults(aws_sdk_s3::config::BehaviorVersion::latest())
                    .region(region)
                    .credentials_provider(credentials);
            if let Some(endpoint_url) = &s3_conf.endpoint_url {
                s3_config_builder = s3_config_builder.endpoint_url(endpoint_url.clone());
            }
            let s3_config = s3_config_builder.load().await;

            let rustfs_client = aws_sdk_s3::Client::from_conf(
                aws_sdk_s3::Config::from(&s3_config)
                    .to_builder()
                    .force_path_style(true)
                    .build(),
            );
            let bucket_name = s3_conf.bucket_name.clone();
            let key_prefix = survey.to_string().to_lowercase();

            let redis_conn =
                build_cutout_cache_conn(&s3_conf.cache)
                    .await
                    .inspect_err(as_error!(
                        "failed to build redis connection for cutout cache"
                    ))?;
            let cache = CutoutCache::new(redis_conn, s3_conf.cache.ttl_seconds, key_prefix.clone());

            let compress_stamps = matches!(survey, Survey::Lsst);
            CutoutStorage::from_s3(
                rustfs_client,
                bucket_name,
                key_prefix,
                None,
                cache,
                compress_stamps,
            )
            .await
            .inspect_err(as_error!("failed to create cutout storage"))?
        }
        CutoutsStorage::Mongo(mongo_conf) => {
            let db = _build_db(mongo_conf).await?;
            CutoutStorage::from_mongo(db, survey).await
        }
    };

    Ok(storage)
}

#[derive(Debug, Clone)]
pub struct CatalogXmatchConfig {
    pub catalog: String,
    pub radius: f64, // in radians
    pub projection: Document,
    pub use_distance: bool,
    pub distance_key: Option<String>,
    pub distance_max: Option<f64>,      // in kpc
    pub distance_max_near: Option<f64>, // in arcsec
    pub max_results: Option<usize>,
    /// Field naming a row's object type, e.g. DESI's `spectype`.
    pub type_key: Option<String>,
    /// Values of `type_key` that mean the row is a star rather than a galaxy.
    pub stellar_types: Vec<String>,
}

impl CatalogXmatchConfig {
    pub fn new(
        catalog: &str,
        radius: f64,
        projection: Document,
        use_distance: bool,
        distance_key: Option<String>,
        distance_max: Option<f64>,
        distance_max_near: Option<f64>,
        max_results: Option<usize>,
        type_key: Option<String>,
        stellar_types: Vec<String>,
    ) -> CatalogXmatchConfig {
        CatalogXmatchConfig {
            catalog: catalog.to_string(),
            radius: radius * std::f64::consts::PI / 180.0 / 3600.0, // convert arcsec to radians
            projection,
            use_distance,
            distance_key,
            distance_max,
            distance_max_near,
            max_results,
            type_key,
            stellar_types,
        }
    }

    #[instrument(skip_all, err)]
    fn from_config(config_value: Value) -> Result<CatalogXmatchConfig, BoomConfigError> {
        let hashmap_xmatch = config_value.into_table()?;
        let required = |key: &str| {
            hashmap_xmatch
                .get(key)
                .cloned()
                .ok_or_else(|| BoomConfigError::MissingKeyError(key.to_string()))
        };

        let catalog = required("catalog")?.into_string()?;
        let radius = required("radius")?.into_float()?;
        let projection = required("projection")?.into_table()?;

        let use_distance = hashmap_xmatch
            .get("use_distance")
            .cloned()
            .map(Value::into_bool)
            .transpose()?
            .unwrap_or(false);

        let distance_key = hashmap_xmatch
            .get("distance_key")
            .cloned()
            .map(Value::into_string)
            .transpose()?;

        let distance_max = hashmap_xmatch
            .get("distance_max")
            .cloned()
            .map(Value::into_float)
            .transpose()?;

        let distance_max_near = hashmap_xmatch
            .get("distance_max_near")
            .cloned()
            .map(Value::into_float)
            .transpose()?;

        let mut projection_doc = Document::new();
        for (key, value) in projection {
            projection_doc.insert(key, value.into_int()?);
        }

        if use_distance {
            if distance_key.is_none() {
                panic!("must provide a distance_key if use_distance is true");
            }

            if distance_max.is_none() {
                panic!("must provide a distance_max if use_distance is true");
            }

            if distance_max_near.is_none() {
                panic!("must provide a distance_max_near if use_distance is true");
            }
        }

        let max_results = match hashmap_xmatch.get("max_results") {
            Some(max_results) => {
                let value = max_results.clone().into_int()?;
                if value <= 0 {
                    panic!("max_results must be greater than 0");
                }
                Some(value as usize)
            }
            None => None,
        };

        if max_results.is_some() && use_distance {
            panic!("cannot use max_results with distance filtering");
        }

        let type_key = hashmap_xmatch
            .get("type_key")
            .cloned()
            .map(Value::into_string)
            .transpose()?;

        let stellar_types = match hashmap_xmatch.get("stellar_types") {
            Some(values) => values
                .clone()
                .into_array()?
                .into_iter()
                .map(Value::into_string)
                .collect::<Result<Vec<String>, _>>()?,
            None => Vec::new(),
        };

        Ok(CatalogXmatchConfig::new(
            &catalog,
            radius,
            projection_doc,
            use_distance,
            distance_key,
            distance_max,
            distance_max_near,
            max_results,
            type_key,
            stellar_types,
        ))
    }
}

impl<'de> Deserialize<'de> for CatalogXmatchConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = Value::deserialize(deserializer).map_err(serde::de::Error::custom)?;
        CatalogXmatchConfig::from_config(v).map_err(serde::de::Error::custom)
    }
}

fn default_bucket_name() -> String {
    "boom-cutouts".to_string()
}

#[derive(Deserialize, Debug, Clone)]
pub struct S3CutoutsStorageConfig {
    #[serde(default = "default_bucket_name")]
    pub bucket_name: String,
    pub region: String,
    /// Custom endpoint URL for S3-compatible services (rustfs, MinIO, Wasabi, …).
    /// Leave unset when pointing at AWS S3 — the SDK derives the endpoint from the region.
    #[serde(default)]
    pub endpoint_url: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    pub credentials_provider: String,
    pub cache: CutoutCacheConfig,
}

#[derive(Debug, Clone)]
pub enum CutoutsStorage {
    S3(S3CutoutsStorageConfig),
    Mongo(DatabaseConfig),
}

impl<'de> Deserialize<'de> for CutoutsStorage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        // The config crate's single-pass deserializer rules out #[serde(tag = "type")].
        let map = serde_json::Value::deserialize(deserializer).map_err(D::Error::custom)?;

        let storage_type = map
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| D::Error::missing_field("type"))?;

        match storage_type {
            "mongo" => serde_json::from_value::<DatabaseConfig>(map)
                .map(CutoutsStorage::Mongo)
                .map_err(D::Error::custom),
            "s3" => serde_json::from_value::<S3CutoutsStorageConfig>(map)
                .map(CutoutsStorage::S3)
                .map_err(D::Error::custom),
            other => Err(D::Error::custom(format!(
                "unknown cutouts_storage type {:?}; expected \"mongo\" or \"s3\"",
                other
            ))),
        }
    }
}

fn default_kafka_server() -> String {
    "localhost:9092".to_string()
}

fn default_subscription_window_days() -> u64 {
    1
}

/// `Unset` is spelled `""` rather than a missing key, so that an empty env
/// override deserializes instead of erroring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SecurityProtocol {
    #[default]
    #[serde(rename = "")]
    Unset,
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            SecurityProtocol::Unset | SecurityProtocol::Plaintext => "PLAINTEXT",
            SecurityProtocol::Ssl => "SSL",
            SecurityProtocol::SaslPlaintext => "SASL_PLAINTEXT",
            SecurityProtocol::SaslSsl => "SASL_SSL",
        }
    }

    pub fn uses_sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }

    pub fn uses_tls(self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }
}

#[derive(Debug, Clone, Default)]
pub struct KafkaSecurity {
    protocol: SecurityProtocol,
    username: Option<String>,
    password: Option<String>,
    ssl_ca_location: Option<String>,
}

impl KafkaSecurity {
    pub fn protocol(&self) -> SecurityProtocol {
        match self.protocol {
            SecurityProtocol::Unset if self.has_credentials() => SecurityProtocol::SaslPlaintext,
            SecurityProtocol::Unset => SecurityProtocol::Plaintext,
            explicit => explicit,
        }
    }

    pub fn has_credentials(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn ssl_ca_location(&self) -> Option<&str> {
        self.ssl_ca_location.as_deref()
    }
}

/// An empty string is an unset key, not a credential.
fn configured(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConsumerConfig {
    #[serde(default = "default_kafka_server")]
    pub server: String, // URL of the Kafka broker
    pub group_id: String,                           // Consumer group ID
    pub schema_registry: Option<String>,            // URL of the schema registry (if any)
    pub schema_github_fallback_url: Option<String>, // URL of the GitHub fallback for schemas (if any)
    pub username: Option<String>,                   // Username for authentication (if any)
    pub password: Option<String>,                   // Password for authentication (if any)
    #[serde(default)]
    pub security_protocol: SecurityProtocol, // Empty infers it from the credentials
    pub ssl_ca_location: Option<String>,            // CA bundle path (only for a private CA)
    /// Days before the current one to stay subscribed to, for surveys whose
    /// topics are per-night. 1 (the default) keeps yesterday alongside today so
    /// a night spanning UTC midnight isn't cut off. Raise it temporarily to
    /// catch up after an upstream outage — bounded by upstream retention, which
    /// is about 7 days for ZTF. Ignored by surveys with a single static topic.
    #[serde(default = "default_subscription_window_days")]
    pub subscription_window_days: u64,
}

impl KafkaConsumerConfig {
    pub fn security(&self) -> KafkaSecurity {
        KafkaSecurity {
            protocol: self.security_protocol,
            username: configured(&self.username),
            password: configured(&self.password),
            ssl_ca_location: configured(&self.ssl_ca_location),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct KafkaProducerConfig {
    #[serde(default = "default_kafka_server")]
    pub server: String, // URL of the Kafka broker
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub consumer: HashMap<Survey, KafkaConsumerConfig>,
    #[serde(default)]
    pub producer: KafkaProducerConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AuthConfig {
    pub secret_key: String,
    pub token_expiration: usize, // in seconds
    pub admin_username: String,
    pub admin_password: String,
    pub admin_email: String,
}

impl AuthConfig {
    pub fn get_hashed_secret_key(&self) -> &[u8; 32] {
        HASHED_SECRET_KEY.get_or_init(|| {
            let mut hasher = Sha256::new();
            hasher.update(self.secret_key.as_bytes());
            hasher.finalize().into()
        })
    }
}

fn default_api_port() -> u16 {
    4000
}

#[derive(Deserialize, Debug, Clone)]
pub struct ApiConfig {
    pub domain: String,
    pub auth: AuthConfig,
    #[serde(default = "default_api_port")]
    pub port: u16,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DatabaseConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub max_pool_size: u32,
    pub replica_set: Option<String>,
    pub srv: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
}

impl Default for RedisConfig {
    fn default() -> Self {
        RedisConfig {
            host: "localhost".to_string(),
            port: 6379,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct CutoutCacheConfig {
    pub host: String,
    pub port: u16,
    pub ttl_seconds: u64,
    pub max_memory: String,
}

impl Default for CutoutCacheConfig {
    fn default() -> Self {
        CutoutCacheConfig {
            host: "localhost".to_string(),
            port: 6379,
            ttl_seconds: 30,
            max_memory: "1gb".to_string(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct BabamulConfig {
    pub enabled: bool,
    pub webapp_url: Option<String>,
    /// Emails of the Babamul accounts allowed to run data-mutating tasks from
    /// the admin page.
    ///
    /// Desired state: every account's `is_admin` is reconciled against this at
    /// API startup, so this list is the whole answer to "who can mutate the
    /// data" and it is reviewable in the deployment's config. Emails rather
    /// than usernames because an email is what the account signs in with.
    ///
    /// Settable as `BOOM_BABAMUL__ADMIN_EMAILS`, comma-separated.
    #[serde(default, deserialize_with = "comma_separated")]
    pub admin_emails: Vec<String>,
    /// Number of days to retain Kafka messages for Babamul topics
    #[serde(default = "default_babamul_retention_days")]
    pub retention_days: u32,
    /// Minimum number of minutes that must elapse between successive password resets (default: 15)
    #[serde(default = "default_password_reset_cooldown_minutes")]
    pub password_reset_cooldown_minutes: u32,
    /// Whether this deployment will create new accounts (default: true).
    ///
    /// Set to `false` for a pre-release deployment that is open only to
    /// accounts that already exist. Every path that would mint one honors it —
    /// password sign-up and social sign-in alike — so it cannot be sidestepped
    /// by calling the API directly or by pressing a sign-in button the web app
    /// still shows. Signing in with an account that already exists, including
    /// linking a new provider to it, is unaffected.
    ///
    /// The web app has its own build-time `VITE_PRERELEASE_MODE`, which decides
    /// what the UI *offers*; this decides what the API *allows*. Set both.
    #[serde(default = "default_babamul_registration_enabled")]
    pub registration_enabled: bool,
    /// Social sign-in (Google / GitHub / ORCID) configuration
    #[serde(default)]
    pub oauth: OAuthConfig,
}

impl Default for BabamulConfig {
    fn default() -> Self {
        BabamulConfig {
            enabled: false,
            admin_emails: Vec::new(),
            webapp_url: None,
            retention_days: default_babamul_retention_days(),
            password_reset_cooldown_minutes: default_password_reset_cooldown_minutes(),
            registration_enabled: default_babamul_registration_enabled(),
            oauth: OAuthConfig::default(),
        }
    }
}

/// Credentials for a single OAuth 2.0 / OIDC identity provider.
///
/// **Set these from the environment, not `config.yaml`** — the YAML files are
/// committed, and a `client_secret:` key sitting in one is an invitation to
/// paste a live secret into the repo. The env vars are
/// `BOOM_BABAMUL__OAUTH__{GOOGLE,GITHUB,ORCID}__CLIENT_{ID,SECRET}`.
///
/// There is deliberately no `enabled` flag: a provider is on exactly when both
/// halves of its credential are present. That keeps the on/off switch in the
/// same place as the secret, so a provider can never be switched on without
/// one — it fails closed instead of sending users to a consent screen that
/// will reject them. Same shape as [`PostHogConfig`], where an empty
/// `project_api_key` disables analytics.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct OAuthProviderConfig {
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
}

impl OAuthProviderConfig {
    pub fn is_configured(&self) -> bool {
        !self.client_id.is_empty() && !self.client_secret.is_empty()
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct OAuthConfig {
    #[serde(default)]
    pub google: OAuthProviderConfig,
    #[serde(default)]
    pub github: OAuthProviderConfig,
    #[serde(default)]
    pub orcid: OAuthProviderConfig,
    /// Public base URL the API is reachable at, e.g. `https://api.babamul.org`.
    /// The redirect URI registered with each provider must be
    /// `{redirect_base_url}/babamul/oauth/{provider}/callback`.
    pub redirect_base_url: Option<String>,
    /// Point ORCID at its sandbox (`sandbox.orcid.org`) instead of production.
    #[serde(default)]
    pub orcid_sandbox: bool,
    /// Seconds an in-flight authorization request stays valid (default: 600)
    #[serde(default = "default_oauth_state_ttl_seconds")]
    pub state_ttl_seconds: i64,
}

impl Default for OAuthConfig {
    fn default() -> Self {
        OAuthConfig {
            google: OAuthProviderConfig::default(),
            github: OAuthProviderConfig::default(),
            orcid: OAuthProviderConfig::default(),
            redirect_base_url: None,
            orcid_sandbox: false,
            state_ttl_seconds: default_oauth_state_ttl_seconds(),
        }
    }
}

fn default_oauth_state_ttl_seconds() -> i64 {
    600
}

fn default_babamul_retention_days() -> u32 {
    3
}

fn default_password_reset_cooldown_minutes() -> u32 {
    15
}

fn default_babamul_registration_enabled() -> bool {
    true
}

/// Server-side PostHog product analytics.
///
/// Analytics are only sent when `project_api_key` is non-empty, so leaving it
/// unset (the default) disables capture entirely without any other change.
/// The key is a PostHog *project* (write-only, publicly shippable) key — the
/// same class of key the web app already ships in `VITE_PUBLIC_POSTHOG_KEY`.
#[derive(Deserialize, Debug, Clone)]
pub struct PostHogConfig {
    /// PostHog project API key. Empty disables analytics.
    #[serde(default)]
    pub project_api_key: String,
    /// PostHog ingestion host, e.g. `https://us.i.posthog.com`.
    #[serde(default = "default_posthog_host")]
    pub host: String,
    /// How often the buffered event queue is flushed to PostHog, in seconds.
    #[serde(default = "default_posthog_flush_interval_seconds")]
    pub flush_interval_seconds: u64,
    /// Maximum number of events buffered before excess events are dropped.
    ///
    /// Analytics must never apply backpressure to API requests, so the queue is
    /// bounded and overflow is dropped (and counted) rather than awaited.
    #[serde(default = "default_posthog_queue_capacity")]
    pub queue_capacity: usize,
    /// How often Kafka consumer-group consumption is sampled, in seconds.
    #[serde(default = "default_posthog_consumption_interval_seconds")]
    pub consumption_interval_seconds: u64,
}

impl Default for PostHogConfig {
    fn default() -> Self {
        PostHogConfig {
            project_api_key: String::new(),
            host: default_posthog_host(),
            flush_interval_seconds: default_posthog_flush_interval_seconds(),
            queue_capacity: default_posthog_queue_capacity(),
            consumption_interval_seconds: default_posthog_consumption_interval_seconds(),
        }
    }
}

impl PostHogConfig {
    /// Whether analytics capture is enabled (i.e. a project key is configured).
    pub fn is_enabled(&self) -> bool {
        !self.project_api_key.trim().is_empty()
    }
}

fn default_posthog_host() -> String {
    "https://us.i.posthog.com".to_string()
}

fn default_posthog_flush_interval_seconds() -> u64 {
    10
}

fn default_posthog_queue_capacity() -> usize {
    10_000
}

fn default_posthog_consumption_interval_seconds() -> u64 {
    5 * 60
}

#[derive(Deserialize, Debug, Clone)]
pub struct WorkerConfig {
    pub n_workers: usize,
}

fn default_enrichment_batch_size() -> usize {
    750
}

#[derive(Deserialize, Debug, Clone)]
pub struct EnrichmentWorkerConfig {
    pub n_workers: usize,
    /// Alerts processed per enrichment batch. Serves two roles at once: the
    /// queue RPOP cap (max alerts pulled per worker iteration) and the fixed
    /// ONNX batch dimension. Every GPU inference runs at exactly this many
    /// rows — partial batches are zero-padded — so ORT builds a single memory
    /// plan and the BFC arena stays stable instead of growing per distinct
    /// input shape. 750 is the proven stable shape on a 16 GB card
    /// (~10.3 GB footprint); 1000 OOMs (~15.7 GB).
    #[serde(default = "default_enrichment_batch_size")]
    pub batch_size: usize,
}

fn default_filter_refresh_interval_minutes() -> u64 {
    15
}

fn deserialize_filter_refresh_interval<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    const MIN_INTERVAL: u64 = 1;
    const MAX_INTERVAL: u64 = 60;
    if value < MIN_INTERVAL {
        return Err(serde::de::Error::custom(format!(
            "refresh_interval_minutes must be at least {} minutes, got {}",
            MIN_INTERVAL, value
        )));
    }
    if value > MAX_INTERVAL {
        return Err(serde::de::Error::custom(format!(
            "refresh_interval_minutes must be at most {} minutes, got {}",
            MAX_INTERVAL, value
        )));
    }
    Ok(value)
}

fn deserialize_command_interval<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let value = usize::deserialize(deserializer)?;
    const MIN_INTERVAL: usize = 100;
    const MAX_INTERVAL: usize = 60000;

    if value < MIN_INTERVAL {
        return Err(serde::de::Error::custom(format!(
            "command_interval must be at least {} ms, got {}",
            MIN_INTERVAL, value
        )));
    }
    if value > MAX_INTERVAL {
        return Err(serde::de::Error::custom(format!(
            "command_interval must be at most {} ms, got {}",
            MAX_INTERVAL, value
        )));
    }
    Ok(value)
}

fn deserialize_max_match_rate<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<u8>::deserialize(deserializer)?;
    if let Some(v) = value {
        if v == 0 || v > 100 {
            return Err(serde::de::Error::custom(format!(
                "max_match_rate must be between 1 and 100, got {}",
                v
            )));
        }
    }
    Ok(value)
}

#[derive(Deserialize, Debug, Clone)]
pub struct FilterWorkerConfig {
    pub n_workers: usize,
    #[serde(
        default = "default_filter_refresh_interval_minutes",
        deserialize_with = "deserialize_filter_refresh_interval"
    )]
    pub refresh_interval_minutes: u64,
    /// Maximum percentage of alerts that a filter is allowed
    /// to match before it is considered too permissive to activate. Required
    /// alongside `reference_night` to allow filter activation on this survey;
    /// if either is missing, filters cannot be activated.
    #[serde(default, deserialize_with = "deserialize_max_match_rate")]
    pub max_match_rate: Option<u8>,
    /// Reference observing night used to gauge how selective a filter is.
    /// Should be a recent, well-populated night for the survey. Required
    /// alongside `max_match_rate` to allow filter activation on this survey;
    /// if either is missing, filters cannot be activated.
    #[serde(default)]
    pub reference_night: Option<NaiveDate>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SurveyWorkerConfig {
    #[serde(deserialize_with = "deserialize_command_interval")]
    pub command_interval: usize, // in milliseconds
    pub alert: WorkerConfig,
    pub enrichment: EnrichmentWorkerConfig,
    pub filter: FilterWorkerConfig,
}

use serde::{de, Deserializer};

#[derive(Debug, Clone, Deserialize)]
pub struct GpuConfig {
    /// Whether to load ONNX models on GPU (CUDA) instead of CPU.
    /// Models are loaded once at startup and shared across all enrichment workers
    /// via `Arc<Mutex<...>>`.
    #[serde(default)]
    pub enabled: bool,
    /// CUDA device IDs available for GPU work. Default: [0].
    /// ONNX models are loaded on the first device. Additional devices are
    /// available for the GPU pool (future lightcurve fitting).
    /// Example for 8 GPUs: [0, 1, 2, 3, 4, 5, 6, 7].
    #[serde(
        default = "default_gpu_device_ids",
        deserialize_with = "deserialize_device_ids"
    )]
    pub device_ids: Vec<i32>,
}

fn deserialize_device_ids<'de, D>(deserializer: D) -> Result<Vec<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    struct DeviceIdsVisitor;
    impl<'de> de::Visitor<'de> for DeviceIdsVisitor {
        type Value = Vec<i32>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a list of integers or a comma-separated string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let ids = v
                .split(',')
                .map(|s| s.trim().parse::<i32>())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| E::custom("invalid integer in device_ids string"))?;
            Ok(ids)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut ids = Vec::new();
            while let Some(val) = seq.next_element()? {
                ids.push(val);
            }
            Ok(ids)
        }
    }
    deserializer.deserialize_any(DeviceIdsVisitor)
}

impl GpuConfig {
    /// Whether models load on CUDA: enabled, with at least one device.
    pub fn is_active(&self) -> bool {
        self.enabled && !self.device_ids.is_empty()
    }
}

impl Default for GpuConfig {
    fn default() -> Self {
        GpuConfig {
            enabled: false,
            device_ids: default_gpu_device_ids(),
        }
    }
}

fn default_gpu_device_ids() -> Vec<i32> {
    vec![0]
}

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    pub api: ApiConfig,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub redis: RedisConfig,
    #[serde(default)]
    pub babamul: BabamulConfig,
    #[serde(default)]
    pub posthog: PostHogConfig,
    pub kafka: KafkaConfig,
    /// Archival catalogs this deployment should hold, as kebab-case slugs.
    ///
    /// Desired state, not actual: nothing converges automatically. See
    /// `docs/catalogs.md`.
    ///
    /// Settable as `BOOM_CATALOGS`, comma-separated.
    #[serde(default, deserialize_with = "comma_separated")]
    pub catalogs: Vec<String>,
    #[serde(default)]
    pub crossmatch: HashMap<Survey, Vec<CatalogXmatchConfig>>,
    #[serde(default)]
    pub workers: HashMap<Survey, SurveyWorkerConfig>,
    #[serde(default)]
    pub gpu: GpuConfig,
    pub cutouts_storage: CutoutsStorage,
}

impl AppConfig {
    #[instrument(err)]
    pub fn from_default_path() -> Result<Self, BoomConfigError> {
        load_config(None)
    }

    #[instrument(err)]
    pub fn from_path(config_path: &str) -> Result<Self, BoomConfigError> {
        load_config(Some(config_path))
    }

    #[instrument(err)]
    pub fn from_test_config() -> Result<Self, BoomConfigError> {
        let mut current_dir = std::env::current_dir().expect("Failed to get current directory");
        let test_config_path = loop {
            let tests_dir = current_dir.join("tests");
            let test_config = tests_dir.join("config.test.yaml");

            if test_config.exists() {
                break test_config;
            }

            if let Some(parent) = current_dir.parent() {
                current_dir = parent.to_path_buf();
            } else {
                panic!("Could not find workspace root with tests/config.test.yaml");
            }
        };

        load_config(Some(test_config_path.to_str().expect("Invalid path")))
    }

    /// Validate that all required secrets are present
    fn validate_secrets(&self) -> Result<(), String> {
        if self.database.password.is_empty() {
            return Err(
                "Database password must be set via BOOM_DATABASE__PASSWORD environment variable"
                    .to_string(),
            );
        }

        if self.api.auth.secret_key.is_empty() {
            return Err(
                "API secret key must be set via BOOM_API__AUTH__SECRET_KEY environment variable"
                    .to_string(),
            );
        }

        if self.api.auth.admin_password.is_empty() {
            return Err("Admin password must be set via BOOM_API__AUTH__ADMIN_PASSWORD environment variable".to_string());
        }

        if self.api.auth.token_expiration == 0 {
            return Err("Token expiration must be greater than 0 for security reasons".to_string());
        }

        for (survey, consumer) in &self.kafka.consumer {
            let security = consumer.security();
            let protocol = security.protocol();
            if protocol.uses_sasl() && !security.has_credentials() {
                return Err(format!(
                    "kafka.consumer.{} is set to {} but has no credentials; set \
                     BOOM_KAFKA__CONSUMER__{}__USERNAME and BOOM_KAFKA__CONSUMER__{}__PASSWORD",
                    survey.as_str().to_lowercase(),
                    protocol.as_str(),
                    survey.as_str(),
                    survey.as_str(),
                ));
            }
        }

        Ok(())
    }

    #[instrument(skip_all, err)]
    pub async fn build_db(&self) -> Result<Database, BoomConfigError> {
        build_db(self).await
    }

    #[instrument(skip_all, err)]
    pub async fn build_redis(&self) -> Result<redis::aio::MultiplexedConnection, BoomConfigError> {
        build_redis(self).await
    }

    #[instrument(skip_all, err)]
    pub async fn build_cutout_storage(
        &self,
        survey: &Survey,
    ) -> Result<CutoutStorage, BoomConfigError> {
        match build_cutout_storage(survey, self).await {
            Ok(storage) => Ok(storage),
            Err(e) => {
                error!(
                    "Failed to build cutout storage for survey {:?}: {:?}",
                    survey, e
                );
                Err(e)
            }
        }
    }
}

#[instrument(err)]
pub fn load_config(config_path: Option<&str>) -> Result<AppConfig, BoomConfigError> {
    load_dotenv();

    let config_file = config_path.unwrap_or(DEFAULT_CONFIG_PATH);

    let config = load_raw_config(config_file)?;

    let app_config: AppConfig = config.try_deserialize()?;

    if let Err(e) = app_config.validate_secrets() {
        return Err(BoomConfigError::InvalidSecretError(e));
    }

    // A misspelled crossmatch catalog does not fail at query time -- it matches
    // nothing, and the alerts come out looking confidently unmatched. Fail
    // startup instead, where someone will see it.
    if let Err(e) = crate::catalogs::validate_crossmatch(&app_config.crossmatch) {
        return Err(BoomConfigError::UnknownCrossmatchCatalog(e));
    }

    debug!("Configuration loaded successfully");
    debug!("Database host: {}", app_config.database.host);
    debug!("Database name: {}", app_config.database.name);
    debug!("Admin username: {}", app_config.api.auth.admin_username);
    debug!("Admin email: {}", app_config.api.auth.admin_email);
    debug!("API port: {}", app_config.api.port);
    debug!(
        "Token expiration: {} seconds",
        app_config.api.auth.token_expiration
    );

    Ok(app_config)
}

pub async fn get_test_db() -> Database {
    let config = AppConfig::from_test_config().expect("Failed to load test config");
    config.build_db().await.unwrap()
}

pub async fn get_test_cutout_storage(survey: &Survey) -> CutoutStorage {
    let config = AppConfig::from_test_config().expect("Failed to load test config");
    config
        .build_cutout_storage(survey)
        .await
        .expect("Failed to build cutout storage")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn consumer_config(yaml: &str) -> KafkaConsumerConfig {
        Config::builder()
            .add_source(File::from_str(yaml, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap()
    }

    #[test]
    fn a_consumer_without_credentials_stays_on_plaintext() {
        let config = consumer_config("group_id: boom-ztf\n");
        assert_eq!(config.security().protocol(), SecurityProtocol::Plaintext);
    }

    #[test]
    fn credentials_alone_mean_sasl_plaintext() {
        let config = consumer_config("group_id: boom-lsst\nusername: user\npassword: pass\n");
        assert_eq!(
            config.security().protocol(),
            SecurityProtocol::SaslPlaintext
        );
    }

    #[test]
    fn an_empty_credential_is_not_a_credential() {
        let config = consumer_config("group_id: boom-ztf\nusername: \"\"\npassword: \"\"\n");
        assert_eq!(config.security().protocol(), SecurityProtocol::Plaintext);
        assert!(!config.security().has_credentials());
    }

    #[test]
    fn an_explicit_protocol_wins_over_the_inferred_one() {
        let config = consumer_config(
            "group_id: boom-ztf\nsecurity_protocol: SASL_SSL\nusername: user\npassword: pass\n",
        );
        assert_eq!(config.security().protocol(), SecurityProtocol::SaslSsl);
        assert!(config.security().protocol().uses_tls());
    }

    #[test]
    fn an_empty_protocol_means_unset_rather_than_a_parse_error() {
        let config = consumer_config("group_id: boom-ztf\nsecurity_protocol: \"\"\n");
        assert_eq!(config.security_protocol, SecurityProtocol::Unset);
    }

    /// `config.yaml` as a deployment with both OAuth URLs set would have it.
    const URLS_CONFIGURED_YAML: &str = "babamul:\n  webapp_url: https://example.org\n  oauth:\n    redirect_base_url: https://example.org/api\n";

    /// Build a config from [`URLS_CONFIGURED_YAML`] plus a *fake* environment.
    ///
    /// `Environment::source` substitutes the map for the real environment, so
    /// this exercises the production overlay without `set_var` — which would
    /// race the other tests in this binary reading the environment through
    /// `from_test_config`, and would clobber any `BOOM_*` values a developer's
    /// `.env` had already loaded into the process.
    fn config_with_env(vars: &[(&str, &str)]) -> Config {
        let env: HashMap<String, String> = vars
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        Config::builder()
            .add_source(File::from_str(
                URLS_CONFIGURED_YAML,
                config::FileFormat::Yaml,
            ))
            .add_source(env_source().source(Some(env)))
            .build()
            .unwrap()
    }

    #[test]
    fn admin_emails_can_be_set_as_a_comma_separated_env_var() {
        // A list has no natural single-variable form, and this one has to be
        // settable from the environment because it decides who may mutate the
        // data -- see AGENTS.md on secrets and deployment settings.
        let conf = config_with_env(&[(
            "BOOM_BABAMUL__ADMIN_EMAILS",
            "one@example.org,two@example.org",
        )]);
        assert_eq!(
            conf.get::<AdminEmails>("babamul").unwrap().admin_emails,
            vec!["one@example.org", "two@example.org"]
        );
    }

    #[test]
    fn a_single_admin_email_still_parses_as_a_list() {
        let conf = config_with_env(&[("BOOM_BABAMUL__ADMIN_EMAILS", "solo@example.org")]);
        assert_eq!(
            conf.get::<AdminEmails>("babamul").unwrap().admin_emails,
            vec!["solo@example.org"]
        );
    }

    #[test]
    fn admin_emails_tolerate_spacing_and_a_trailing_comma() {
        // A blank entry would match no account, but it would also make the
        // configured list look longer than it is.
        let conf = config_with_env(&[(
            "BOOM_BABAMUL__ADMIN_EMAILS",
            " one@example.org , two@example.org ,",
        )]);
        assert_eq!(
            conf.get::<AdminEmails>("babamul").unwrap().admin_emails,
            vec!["one@example.org", "two@example.org"]
        );
    }

    #[test]
    fn admin_emails_still_accept_a_yaml_sequence() {
        // The env form must not cost us the readable form in config.yaml.
        let conf = Config::builder()
            .add_source(File::from_str(
                "babamul:\n  admin_emails:\n    - one@example.org\n    - two@example.org\n",
                config::FileFormat::Yaml,
            ))
            .build()
            .unwrap();
        assert_eq!(
            conf.get::<AdminEmails>("babamul").unwrap().admin_emails,
            vec!["one@example.org", "two@example.org"]
        );
    }

    #[test]
    fn an_unset_admin_email_list_leaves_nobody_an_admin() {
        // Failing closed matters here: the alternative to "no admins" must not
        // be "everyone".
        let conf = config_with_env(&[]);
        assert_eq!(
            conf.get::<AdminEmails>("babamul").unwrap().admin_emails,
            Vec::<String>::new()
        );
    }

    #[test]
    fn the_catalog_inventory_can_be_set_as_a_comma_separated_env_var() {
        let conf = config_with_env(&[("BOOM_CATALOGS", "2mass,ned-lvs")]);
        #[derive(Deserialize)]
        struct Root {
            #[serde(default, deserialize_with = "comma_separated")]
            catalogs: Vec<String>,
        }
        assert_eq!(
            conf.try_deserialize::<Root>().unwrap().catalogs,
            vec!["2mass", "ned-lvs"]
        );
    }

    /// Just the field under test, so these do not need a whole valid AppConfig.
    #[derive(Deserialize)]
    struct AdminEmails {
        #[serde(default, deserialize_with = "comma_separated")]
        admin_emails: Vec<String>,
    }

    #[test]
    fn an_empty_env_var_does_not_blank_out_a_configured_value() {
        // Regression: an empty compose default hid social sign-in in production.
        let conf = config_with_env(&[
            ("BOOM_BABAMUL__WEBAPP_URL", ""),
            ("BOOM_BABAMUL__OAUTH__REDIRECT_BASE_URL", ""),
        ]);

        assert_eq!(
            conf.get::<String>("babamul.webapp_url").unwrap(),
            "https://example.org"
        );
        assert_eq!(
            conf.get::<String>("babamul.oauth.redirect_base_url")
                .unwrap(),
            "https://example.org/api"
        );
    }

    #[test]
    fn a_non_empty_env_var_still_overrides_the_file() {
        // The other half: ignoring empty vars must not ignore the environment.
        let conf = config_with_env(&[("BOOM_BABAMUL__WEBAPP_URL", "https://override.example")]);

        assert_eq!(
            conf.get::<String>("babamul.webapp_url").unwrap(),
            "https://override.example"
        );
    }

    #[test]
    fn oauth_provider_needs_a_client_id_and_secret_to_count_as_configured() {
        let mut provider = OAuthProviderConfig {
            client_id: "id".to_string(),
            client_secret: "secret".to_string(),
        };
        assert!(provider.is_configured());

        // A half-filled provider must fail closed, not hit a rejecting consent screen.
        provider.client_secret = String::new();
        assert!(!provider.is_configured());
        provider.client_secret = "secret".to_string();
        provider.client_id = String::new();
        assert!(!provider.is_configured());
    }

    #[test]
    fn oauth_config_defaults_are_off_with_a_usable_state_ttl() {
        // The serde defaults are separate from the Default impl.
        let config: OAuthConfig = serde_json::from_str("{}").unwrap();
        assert!(!config.google.is_configured());
        assert!(!config.github.is_configured());
        assert!(!config.orcid.is_configured());
        assert!(config.redirect_base_url.is_none());
        assert_eq!(config.state_ttl_seconds, 600);
        assert_eq!(
            config.state_ttl_seconds,
            OAuthConfig::default().state_ttl_seconds
        );
    }

    #[test]
    fn babamul_config_without_an_oauth_block_still_deserializes() {
        // Existing deployments' config.yaml files predate the oauth section.
        let config: BabamulConfig =
            serde_json::from_str(r#"{"enabled": true, "webapp_url": null}"#).unwrap();
        assert!(!config.oauth.google.is_configured());
    }

    #[test]
    fn test_gpu_config_defaults() {
        let config = GpuConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.device_ids, vec![0]);
    }

    #[test]
    fn test_gpu_config_deserialize_empty() {
        let json = "{}";
        let config: GpuConfig = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.device_ids, vec![0]);
    }

    #[test]
    fn test_gpu_config_deserialize_enabled_single_gpu() {
        let json = r#"{"enabled": true, "device_ids": [0]}"#;
        let config: GpuConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.device_ids, vec![0]);
    }

    #[test]
    fn test_gpu_config_deserialize_multi_gpu() {
        let json = r#"{"enabled": true, "device_ids": [0, 1, 2, 3, 4, 5, 6, 7]}"#;
        let config: GpuConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.device_ids, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_gpu_config_deserialize_partial() {
        let json = r#"{"enabled": true}"#;
        let config: GpuConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.device_ids, vec![0]);
    }

    #[test]
    fn test_gpu_config_deserialize_subset_of_devices() {
        let json = r#"{"enabled": true, "device_ids": [2, 5]}"#;
        let config: GpuConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.device_ids, vec![2, 5]);
    }

    #[test]
    fn test_gpu_config_is_active() {
        let active: GpuConfig = serde_json::from_str(r#"{"enabled": true}"#).unwrap();
        assert!(active.is_active());
        let disabled: GpuConfig = serde_json::from_str(r#"{"enabled": false}"#).unwrap();
        assert!(!disabled.is_active());
        let no_device: GpuConfig =
            serde_json::from_str(r#"{"enabled": true, "device_ids": []}"#).unwrap();
        assert!(!no_device.is_active());
    }
}
