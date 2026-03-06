use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::controller::{AdminController, AdminControllerError};

pub mod proto {
    tonic::include_proto!("mx20022.runtime.admin.v1");
}

#[derive(Clone)]
pub struct AdminGrpcService {
    controller: Arc<dyn AdminController>,
}

impl AdminGrpcService {
    pub fn new(controller: Arc<dyn AdminController>) -> Self {
        Self { controller }
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
        _request: Request<()>,
    ) -> Result<Response<proto::ReadyResponse>, Status> {
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
        _request: Request<()>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
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
        }))
    }

    async fn get_transaction(
        &self,
        request: Request<proto::GetTransactionRequest>,
    ) -> Result<Response<proto::TransactionResponse>, Status> {
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
}

pub async fn serve(addr: &str, controller: Arc<dyn AdminController>) -> Result<(), GrpcHostError> {
    let socket: SocketAddr = addr
        .parse::<SocketAddr>()
        .map_err(|e| GrpcHostError::Bind(addr.to_string(), e.to_string()))?;

    Server::builder()
        .add_service(proto::admin_service_server::AdminServiceServer::new(
            AdminGrpcService::new(controller),
        ))
        .serve(socket)
        .await
        .map_err(|e| GrpcHostError::Serve(e.to_string()))
}

fn map_error_to_status(error: AdminControllerError) -> Status {
    match error {
        AdminControllerError::NotFound => Status::not_found("not found"),
        AdminControllerError::Forbidden => Status::permission_denied("forbidden"),
        AdminControllerError::Internal(msg) => Status::internal(msg),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GrpcHostError {
    #[error("failed to bind grpc admin host on {0}: {1}")]
    Bind(String, String),
    #[error("failed to serve grpc admin host: {0}")]
    Serve(String),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use mx20022_store::{Store, TransactionRecord};
    use mx20022_store_sqlite::SqliteStore;

    use crate::controller::AdminController;
    use crate::service::{RuntimeStatusSnapshot, StoreBackedAdminController};

    use super::AdminGrpcService;

    #[tokio::test]
    async fn grpc_service_returns_status_and_tx() {
        let store: Arc<dyn Store> = Arc::new(SqliteStore::new("sqlite::memory:"));
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
            },
        ));
        let service = AdminGrpcService::new(controller);

        let status =
            <AdminGrpcService as super::proto::admin_service_server::AdminService>::get_status(
                &service,
                tonic::Request::new(()),
            )
            .await
            .expect("status should return")
            .into_inner();
        assert_eq!(status.runtime, "rt");

        let tx = <AdminGrpcService as super::proto::admin_service_server::AdminService>::get_transaction(
            &service,
            tonic::Request::new(super::proto::GetTransactionRequest {
                tx_id: "TX-GRPC-1".to_string(),
            }),
        )
        .await
        .expect("tx should return")
        .into_inner();
        assert_eq!(tx.tx_id, "TX-GRPC-1");
    }
}
