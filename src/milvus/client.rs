//! gRPC client for a Milvus vector database.
//!
//! NRP's managed Milvus exposes gRPC only — there is no REST port reachable
//! from outside the cluster — so this talks the native protobuf API over TLS.
//!
//! Authentication and database selection are both carried as gRPC metadata on
//! every request, applied by [`AuthInterceptor`]:
//!
//! - `authorization: <base64(username:password)>` (note: no `Basic ` prefix,
//!   unlike HTTP basic auth)
//! - `dbname: <database>`
//!
//! Sending the database per-request rather than holding it as mutable client
//! state means a single client is safe to clone and share across workers.

use crate::conf::MilvusConfig;
use base64::{engine::general_purpose, Engine as _};
use std::time::Duration;
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::interceptor::InterceptedService,
    service::Interceptor,
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};
use tracing::{debug, info, instrument};

use super::proto::common::Status as MilvusStatus;
use super::proto::milvus::milvus_service_client::MilvusServiceClient;
use super::proto::milvus::{GetVersionRequest, ListDatabasesRequest, ShowCollectionsRequest};

#[derive(thiserror::Error, Debug)]
pub enum MilvusError {
    #[error("invalid milvus endpoint {0}")]
    InvalidEndpoint(String),
    #[error("invalid milvus credentials (could not be encoded as a gRPC header)")]
    InvalidCredentials,
    #[error("failed to connect to milvus")]
    ConnectError(#[source] tonic::transport::Error),
    #[error("milvus rpc failed")]
    RpcError(#[from] Status),
    #[error("milvus returned an error (code {code}): {reason}")]
    ServerError { code: i32, reason: String },
    #[error("milvus returned an empty status for {0}")]
    MissingStatus(&'static str),
    #[error("milvus is not enabled in the configuration")]
    NotEnabled,
    #[error("embedding has {got} dimensions but the collection expects {expected}")]
    DimensionMismatch { expected: i64, got: usize },
}

/// Attaches credentials and the target database to every outgoing request.
#[derive(Clone)]
pub struct AuthInterceptor {
    authorization: MetadataValue<Ascii>,
    dbname: Option<MetadataValue<Ascii>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());

        if let Some(dbname) = &self.dbname {
            request.metadata_mut().insert("dbname", dbname.clone());
        }

        Ok(request)
    }
}

/// A connected Milvus client.
///
/// Cheap to clone: the underlying tonic `Channel` shares one connection pool.
#[derive(Clone)]
pub struct MilvusClient {
    inner: MilvusServiceClient<InterceptedService<Channel, AuthInterceptor>>,
    config: MilvusConfig,
}

impl MilvusClient {
    /// Connect to Milvus using the supplied configuration.
    ///
    /// Returns [`MilvusError::NotEnabled`] if `milvus.enabled` is false, so that
    /// callers cannot accidentally dial a server the operator has switched off.
    #[instrument(skip_all, err, fields(endpoint = %config.endpoint(), database = %config.database))]
    pub async fn connect(config: &MilvusConfig) -> Result<Self, MilvusError> {
        if !config.enabled {
            return Err(MilvusError::NotEnabled);
        }

        let endpoint_url = config.endpoint();
        let mut endpoint = Endpoint::from_shared(endpoint_url.clone())
            .map_err(|_| MilvusError::InvalidEndpoint(endpoint_url.clone()))?
            .timeout(Duration::from_secs(config.timeout_seconds))
            .connect_timeout(Duration::from_secs(config.timeout_seconds));

        if config.tls {
            // NRP serves a standard Let's Encrypt certificate, so the platform
            // root store is all we need — no custom CA or client identity.
            let tls = ClientTlsConfig::new()
                .domain_name(config.host.clone())
                .with_native_roots();
            endpoint = endpoint
                .tls_config(tls)
                .map_err(MilvusError::ConnectError)?;
        }

        let channel = endpoint
            .connect()
            .await
            .map_err(MilvusError::ConnectError)?;

        let interceptor = Self::build_interceptor(config)?;
        let inner = MilvusServiceClient::with_interceptor(channel, interceptor);

        info!(endpoint = %endpoint_url, "connected to milvus");

        Ok(Self {
            inner,
            config: config.clone(),
        })
    }

    fn build_interceptor(config: &MilvusConfig) -> Result<AuthInterceptor, MilvusError> {
        // Milvus expects the raw base64 of "user:password", with no auth scheme.
        let encoded =
            general_purpose::STANDARD.encode(format!("{}:{}", config.username, config.password));
        let authorization: MetadataValue<Ascii> = encoded
            .parse()
            .map_err(|_| MilvusError::InvalidCredentials)?;

        let dbname = if config.database.is_empty() {
            None
        } else {
            Some(
                config
                    .database
                    .parse()
                    .map_err(|_| MilvusError::InvalidEndpoint(config.database.clone()))?,
            )
        };

        Ok(AuthInterceptor {
            authorization,
            dbname,
        })
    }

    pub fn config(&self) -> &MilvusConfig {
        &self.config
    }

    pub(super) fn service(
        &mut self,
    ) -> &mut MilvusServiceClient<InterceptedService<Channel, AuthInterceptor>> {
        &mut self.inner
    }

    /// The Milvus server version, useful for confirming the vendored protos
    /// still match what the server speaks.
    #[instrument(skip_all, err)]
    pub async fn version(&mut self) -> Result<String, MilvusError> {
        let response = self
            .inner
            .get_version(GetVersionRequest {})
            .await?
            .into_inner();

        check_status(response.status.as_ref(), "GetVersion")?;

        Ok(response.version)
    }

    /// Databases visible to these credentials.
    #[instrument(skip_all, err)]
    pub async fn list_databases(&mut self) -> Result<Vec<String>, MilvusError> {
        let response = self
            .inner
            .list_databases(ListDatabasesRequest::default())
            .await?
            .into_inner();

        check_status(response.status.as_ref(), "ListDatabases")?;

        Ok(response.db_names)
    }

    /// Collections in the configured database.
    #[instrument(skip_all, err)]
    pub async fn list_collections(&mut self) -> Result<Vec<String>, MilvusError> {
        let request = ShowCollectionsRequest {
            db_name: self.config.database.clone(),
            ..Default::default()
        };

        let response = self.inner.show_collections(request).await?.into_inner();

        check_status(response.status.as_ref(), "ShowCollections")?;

        debug!(
            count = response.collection_names.len(),
            "listed collections"
        );

        Ok(response.collection_names)
    }
}

/// Milvus reports application-level failures in a `Status` message rather than
/// as a gRPC error, so every response has to be checked explicitly.
pub(super) fn check_status(
    status: Option<&MilvusStatus>,
    operation: &'static str,
) -> Result<(), MilvusError> {
    let status = status.ok_or(MilvusError::MissingStatus(operation))?;

    // `code` is the modern field; `error_code` is the deprecated 2.x one. A
    // server may set either, so treat a non-zero value in either as a failure.
    #[allow(deprecated)]
    let failed = status.code != 0 || status.error_code != 0;

    if failed {
        #[allow(deprecated)]
        let code = if status.code != 0 {
            status.code
        } else {
            status.error_code
        };

        return Err(MilvusError::ServerError {
            code,
            reason: if status.reason.is_empty() {
                status.detail.clone()
            } else {
                status.reason.clone()
            },
        });
    }

    Ok(())
}
