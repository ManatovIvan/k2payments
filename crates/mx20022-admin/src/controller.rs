use async_trait::async_trait;

use crate::dto::{
    HealthResponseDto, ReadyResponseDto, ReloadResponseDto, StatusResponseDto,
    TransactionResponseDto,
};

#[async_trait]
pub trait AdminController: Send + Sync {
    async fn get_health(&self) -> Result<HealthResponseDto, AdminControllerError>;
    async fn get_ready(&self) -> Result<ReadyResponseDto, AdminControllerError>;
    async fn get_status(&self) -> Result<StatusResponseDto, AdminControllerError>;
    async fn get_transaction(
        &self,
        tx_id: &str,
    ) -> Result<TransactionResponseDto, AdminControllerError>;
    async fn reload_config(&self) -> Result<ReloadResponseDto, AdminControllerError>;
}

#[derive(Debug, thiserror::Error)]
pub enum AdminControllerError {
    #[error("resource not found")]
    NotFound,
    #[error("forbidden")]
    Forbidden,
    #[error("internal error: {0}")]
    Internal(String),
}
