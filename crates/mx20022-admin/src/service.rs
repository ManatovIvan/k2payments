use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use mx20022_store::Store;

use crate::controller::{AdminController, AdminControllerError};
use crate::dto::{HealthResponseDto, ReadyResponseDto, StatusResponseDto, TransactionResponseDto};

#[derive(Debug, Clone)]
pub struct RuntimeStatusSnapshot {
    pub runtime: String,
    pub pipelines: Vec<String>,
    pub channels: Vec<String>,
    pub store: String,
}

pub struct StoreBackedAdminController {
    store: Arc<dyn Store>,
    snapshot: RuntimeStatusSnapshot,
}

impl StoreBackedAdminController {
    pub fn new(store: Arc<dyn Store>, snapshot: RuntimeStatusSnapshot) -> Self {
        Self { store, snapshot }
    }
}

#[async_trait]
impl AdminController for StoreBackedAdminController {
    async fn get_health(&self) -> Result<HealthResponseDto, AdminControllerError> {
        let ok = match self.store.health().await {
            Ok(h) => h.ok,
            Err(_) => false,
        };
        Ok(HealthResponseDto { ok })
    }

    async fn get_ready(&self) -> Result<ReadyResponseDto, AdminControllerError> {
        let health = self
            .store
            .health()
            .await
            .map_err(|e| AdminControllerError::Internal(e.to_string()))?;

        Ok(ReadyResponseDto {
            ready: health.ok,
            details: health.details.unwrap_or_else(|| "ok".to_string()),
        })
    }

    async fn get_status(&self) -> Result<StatusResponseDto, AdminControllerError> {
        Ok(StatusResponseDto {
            runtime: self.snapshot.runtime.clone(),
            pipelines: self.snapshot.pipelines.clone(),
            channels: self.snapshot.channels.clone(),
            store: self.snapshot.store.clone(),
        })
    }

    async fn get_transaction(
        &self,
        tx_id: &str,
    ) -> Result<TransactionResponseDto, AdminControllerError> {
        let record = self
            .store
            .find_by_id(tx_id)
            .await
            .map_err(|e| AdminControllerError::Internal(e.to_string()))?
            .ok_or(AdminControllerError::NotFound)?;

        Ok(TransactionResponseDto {
            tx_id: record.tx_id,
            pipeline: record.pipeline,
            message_type: record.message_type,
            state: record.state,
            received_at: encode_time(record.received_at),
            completed_at: record.completed_at.map(encode_time),
        })
    }
}

fn encode_time(time: SystemTime) -> String {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis()
        .to_string()
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

    #[tokio::test]
    async fn returns_status_and_transaction_details() {
        let store: Arc<dyn Store> = Arc::new(SqliteStore::new("sqlite::memory:"));

        store
            .begin_transaction(&TransactionRecord {
                tx_id: "TX-123".to_string(),
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
            .expect("should insert transaction");

        let controller = StoreBackedAdminController::new(
            store,
            RuntimeStatusSnapshot {
                runtime: "test-runtime".to_string(),
                pipelines: vec!["demo".to_string()],
                channels: vec!["http-in".to_string()],
                store: "sqlite".to_string(),
            },
        );

        let status = controller.get_status().await.expect("status should return");
        assert_eq!(status.runtime, "test-runtime");

        let tx = controller
            .get_transaction("TX-123")
            .await
            .expect("tx should return");
        assert_eq!(tx.tx_id, "TX-123");
        assert_eq!(tx.pipeline, "demo");
    }
}
