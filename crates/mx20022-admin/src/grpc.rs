use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::auth::{authorize_request, AdminResource, AuthConfig, AuthError};
use crate::controller::{AdminController, AdminControllerError};
use crate::rate_limit::AdminRateLimiter;
use crate::tls::TlsConfig;

pub mod proto {
    tonic::include_proto!("mx20022.runtime.admin.v1");
}

#[derive(Clone)]
pub struct AdminGrpcService {
    controller: Arc<dyn AdminController>,
    auth: AuthConfig,
    rate_limiter: Arc<AdminRateLimiter>,
}

impl AdminGrpcService {
    pub fn new(controller: Arc<dyn AdminController>, auth: AuthConfig) -> Self {
        Self {
            controller,
            auth,
            rate_limiter: Arc::new(AdminRateLimiter::default()),
        }
    }
}

#[tonic::async_trait]
impl proto::admin_service_server::AdminService for AdminGrpcService {
    async fn get_health(
        &self,
        _request: Request<()>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        let dto = self
            .controller
            .get_health()
            .await
            .map_err(map_error_to_status)?;

        Ok(Response::new(proto::HealthResponse { ok: dto.ok }))
    }

    async fn get_ready(
        &self,
        request: Request<()>,
    ) -> Result<Response<proto::ReadyResponse>, Status> {
        self.authorize(&request, AdminResource::Ready).await?;
        let dto = self
            .controller
            .get_ready()
            .await
            .map_err(map_error_to_status)?;

        Ok(Response::new(proto::ReadyResponse {
            ready: dto.ready,
            details: dto.details,
        }))
    }

    async fn get_status(
        &self,
        request: Request<()>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        self.authorize(&request, AdminResource::Status).await?;
        let dto = self
            .controller
            .get_status()
            .await
            .map_err(map_error_to_status)?;

        Ok(Response::new(proto::StatusResponse {
            runtime: dto.runtime,
            pipelines: dto.pipelines,
            channels: dto.channels,
            store: dto.store,
            uptime_ms: dto.uptime_ms,
            store_ok: dto.store_ok,
            store_details: dto.store_details.unwrap_or_default(),
            in_flight_count: dto.in_flight_count as u64,
            pending_correlation_count: dto.pending_correlation_count as u64,
            dead_letter_count: dto.dead_letter_count as u64,
            config_version: dto.config_version,
            last_reload_result: dto.last_reload_result.unwrap_or_default(),
            last_reload_at: dto.last_reload_at.unwrap_or_default(),
        }))
    }

    async fn get_transaction(
        &self,
        request: Request<proto::GetTransactionRequest>,
    ) -> Result<Response<proto::TransactionResponse>, Status> {
        self.authorize(&request, AdminResource::Transaction).await?;
        let tx_id = request.into_inner().tx_id;
        let dto = self
            .controller
            .get_transaction(&tx_id)
            .await
            .map_err(map_error_to_status)?;

        Ok(Response::new(proto::TransactionResponse {
            tx_id: dto.tx_id,
            pipeline: dto.pipeline,
            message_type: dto.message_type,
            state: dto.state,
            received_at: dto.received_at,
            completed_at: dto.completed_at.unwrap_or_default(),
        }))
    }

    async fn reload(
        &self,
        request: Request<()>,
    ) -> Result<Response<proto::ReloadResponse>, Status> {
        self.authorize(&request, AdminResource::Reload).await?;
        let dto = self
            .controller
            .reload_config()
            .await
            .map_err(map_error_to_status)?;

        Ok(Response::new(proto::ReloadResponse {
            reloaded: dto.reloaded,
            details: dto.details,
        }))
    }
}

impl AdminGrpcService {
    async fn authorize<T>(
        &self,
        request: &Request<T>,
        resource: AdminResource,
    ) -> Result<(), Status> {
        let metadata = request.metadata();
        let bearer = metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        let mtls = metadata
            .get(self.auth.mtls_subject_header.as_str())
            .and_then(|value| value.to_str().ok());
        let rate_key = metadata
            .get("x-forwarded-for")
            .and_then(|value| value.to_str().ok())
            .or(metadata
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok()))
            .or(mtls)
            .or(bearer)
            .unwrap_or("anonymous");
        if !self.rate_limiter.allow(&format!("{resource:?}:{rate_key}")) {
            return Err(Status::resource_exhausted("rate limit exceeded"));
        }
        authorize_request(&self.auth, resource, bearer, mtls).map_err(map_auth_error)
    }
}

pub async fn serve(
    addr: &str,
    controller: Arc<dyn AdminController>,
    auth: AuthConfig,
) -> Result<(), GrpcHostError> {
    serve_with_tls(addr, controller, auth, None).await
}

pub async fn serve_with_tls(
    addr: &str,
    controller: Arc<dyn AdminController>,
    auth: AuthConfig,
    tls: Option<TlsConfig>,
) -> Result<(), GrpcHostError> {
    let socket: SocketAddr = addr
        .parse::<SocketAddr>()
        .map_err(|e| GrpcHostError::Bind(addr.to_string(), e.to_string()))?;

    let mut builder = Server::builder();

    if let Some(tls) = tls {
        let cert = std::fs::read_to_string(&tls.cert_path)
            .map_err(|e| GrpcHostError::Tls(format!("failed to read cert: {e}")))?;
        let key = std::fs::read_to_string(&tls.key_path)
            .map_err(|e| GrpcHostError::Tls(format!("failed to read key: {e}")))?;
        let identity = tonic::transport::Identity::from_pem(cert, key);
        let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);
        builder = builder
            .tls_config(tls_config)
            .map_err(|e| GrpcHostError::Tls(format!("tls config failed: {e}")))?;
        tracing::info!(addr = %addr, "admin gRPC host starting with TLS");
    } else {
        tracing::warn!("admin gRPC host starting without TLS");
    }

    builder
        .add_service(proto::admin_service_server::AdminServiceServer::new(
            AdminGrpcService::new(controller, auth),
        ))
        .serve(socket)
        .await
        .map_err(|e| GrpcHostError::Serve(e.to_string()))
}

fn map_error_to_status(error: AdminControllerError) -> Status {
    match error {
        AdminControllerError::NotFound => Status::not_found("not found"),
        AdminControllerError::Forbidden => Status::permission_denied("forbidden"),
        AdminControllerError::Internal(msg) => {
            tracing::error!(error = %msg, "admin request failed");
            Status::internal("internal server error")
        }
    }
}

fn map_auth_error(error: AuthError) -> Status {
    match error {
        AuthError::MissingBearer | AuthError::InvalidBearer => {
            Status::unauthenticated(error.to_string())
        }
        AuthError::Forbidden | AuthError::UntrustedMtlsSubject => {
            Status::permission_denied(error.to_string())
        }
        AuthError::MissingMtlsSubject => Status::unauthenticated(error.to_string()),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GrpcHostError {
    #[error("failed to bind grpc admin host on {0}: {1}")]
    Bind(String, String),
    #[error("failed to serve grpc admin host: {0}")]
    Serve(String),
    #[error("TLS configuration error: {0}")]
    Tls(String),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use mx20022_store::{Store, TransactionRecord};
    use mx20022_store_sqlite::SqliteStore;
    use tokio::sync::RwLock;

    use crate::controller::AdminController;
    use crate::service::{ReloadStatus, RuntimeStatusSnapshot, StoreBackedAdminController};

    use super::AdminGrpcService;

    #[tokio::test]
    async fn grpc_service_returns_status_and_tx() {
        let store: Arc<dyn Store> =
            Arc::new(SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize"));
        store
            .begin_transaction(&TransactionRecord {
                tx_id: "TX-GRPC-1".to_string(),
                pipeline: "demo".to_string(),
                source_channel: "http-in".to_string(),
                message_type: "pacs.008".to_string(),
                raw_message: "<Document/>".to_string(),
                state: "RECEIVED".to_string(),
                received_at: SystemTime::now(),
                completed_at: None,
                key_fields: HashMap::new(),
            })
            .await
            .expect("seed tx should succeed");

        let controller: Arc<dyn AdminController> = Arc::new(StoreBackedAdminController::new(
            store,
            RuntimeStatusSnapshot {
                runtime: "rt".to_string(),
                pipelines: vec!["demo".to_string()],
                channels: vec!["http-in".to_string()],
                store: "sqlite".to_string(),
                started_at: SystemTime::now(),
                reload_status: Arc::new(RwLock::new(ReloadStatus {
                    config_version: "cfg-v1".to_string(),
                    last_result: None,
                    last_reloaded_at: None,
                })),
            },
        ));
        let service = AdminGrpcService::new(controller, crate::auth::AuthConfig::default());

        let mut status_request = tonic::Request::new(());
        status_request.metadata_mut().insert(
            "authorization",
            "Bearer admin"
                .parse()
                .expect("valid authorization metadata"),
        );
        let status =
            <AdminGrpcService as super::proto::admin_service_server::AdminService>::get_status(
                &service,
                status_request,
            )
            .await
            .expect("status should return")
            .into_inner();
        assert_eq!(status.runtime, "rt");

        let mut tx_request = tonic::Request::new(super::proto::GetTransactionRequest {
            tx_id: "TX-GRPC-1".to_string(),
        });
        tx_request.metadata_mut().insert(
            "authorization",
            "Bearer admin"
                .parse()
                .expect("valid authorization metadata"),
        );
        let tx = <AdminGrpcService as super::proto::admin_service_server::AdminService>::get_transaction(
            &service,
            tx_request,
        )
        .await
        .expect("tx should return")
        .into_inner();
        assert_eq!(tx.tx_id, "TX-GRPC-1");
    }
}
