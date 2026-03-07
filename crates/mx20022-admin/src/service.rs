// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use mx20022_store::Store;
use tokio::sync::RwLock;

use crate::controller::{AdminController, AdminControllerError};
use crate::dto::{
    HealthResponseDto, ReadyResponseDto, ReloadResponseDto, StatusResponseDto,
    TransactionResponseDto,
};

#[derive(Debug, Clone)]
pub struct RuntimeStatusSnapshot {
    pub runtime: String,
    pub pipelines: Vec<String>,
    pub channels: Vec<String>,
    pub store: String,
    pub started_at: SystemTime,
    pub reload_status: Arc<RwLock<ReloadStatus>>,
}

#[derive(Debug, Clone, Default)]
pub struct ReloadStatus {
    pub config_version: String,
    pub last_result: Option<String>,
    pub last_reloaded_at: Option<SystemTime>,
}

pub struct StoreBackedAdminController {
    store: Arc<dyn Store>,
    snapshot: RuntimeStatusSnapshot,
    reloader: Option<Arc<dyn RuntimeReloader>>,
}

impl StoreBackedAdminController {
    pub fn new(store: Arc<dyn Store>, snapshot: RuntimeStatusSnapshot) -> Self {
        Self {
            store,
            snapshot,
            reloader: None,
        }
    }

    pub fn with_reloader(mut self, reloader: Arc<dyn RuntimeReloader>) -> Self {
        self.reloader = Some(reloader);
        self
    }
}

#[async_trait]
pub trait RuntimeReloader: Send + Sync {
    async fn reload(&self) -> Result<String, AdminControllerError>;
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
        let store_health = self
            .store
            .health()
            .await
            .map_err(|e| AdminControllerError::Internal(e.to_string()))?;
        let pending_correlation_count = self
            .store
            .count_pending_expectations()
            .await
            .map_err(|e| AdminControllerError::Internal(e.to_string()))?;
        let dead_letter_count = self
            .store
            .count_dead_letters(None)
            .await
            .map_err(|e| AdminControllerError::Internal(e.to_string()))?;
        let in_flight_count = in_flight_transaction_count(self.store.as_ref()).await?;
        let reload_status = self.snapshot.reload_status.read().await.clone();

        Ok(StatusResponseDto {
            runtime: self.snapshot.runtime.clone(),
            pipelines: self.snapshot.pipelines.clone(),
            channels: self.snapshot.channels.clone(),
            store: self.snapshot.store.clone(),
            uptime_ms: elapsed_millis(self.snapshot.started_at).to_string(),
            store_ok: store_health.ok,
            store_details: store_health.details,
            in_flight_count,
            pending_correlation_count,
            dead_letter_count,
            config_version: reload_status.config_version,
            last_reload_result: reload_status.last_result,
            last_reload_at: reload_status.last_reloaded_at.map(encode_time),
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

    async fn reload_config(&self) -> Result<ReloadResponseDto, AdminControllerError> {
        let reloader = self
            .reloader
            .as_ref()
            .ok_or(AdminControllerError::Forbidden)?;
        let details = reloader.reload().await?;
        Ok(ReloadResponseDto {
            reloaded: true,
            details,
        })
    }
}

fn encode_time(time: SystemTime) -> String {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis()
        .to_string()
}

fn elapsed_millis(started_at: SystemTime) -> u128 {
    started_at
        .elapsed()
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis()
}

async fn in_flight_transaction_count(store: &dyn Store) -> Result<usize, AdminControllerError> {
    let states = [
        "RECEIVED",
        "PREPARING",
        "PREPARED",
        "COMMITTING",
        "ABORTING",
    ];
    store
        .count_transactions_by_states(&states)
        .await
        .map_err(|e| AdminControllerError::Internal(e.to_string()))
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

    #[tokio::test]
    async fn returns_status_and_transaction_details() {
        let store: Arc<dyn Store> =
            Arc::new(SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize"));

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
                started_at: SystemTime::now(),
                reload_status: Arc::new(RwLock::new(ReloadStatus {
                    config_version: "cfg-v1".to_string(),
                    last_result: None,
                    last_reloaded_at: None,
                })),
            },
        );

        let status = controller.get_status().await.expect("status should return");
        assert_eq!(status.runtime, "test-runtime");
        assert_eq!(status.config_version, "cfg-v1");

        let tx = controller
            .get_transaction("TX-123")
            .await
            .expect("tx should return");
        assert_eq!(tx.tx_id, "TX-123");
        assert_eq!(tx.pipeline, "demo");
    }
}
