// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::time::SystemTime;

use async_trait::async_trait;

/// Persisted transaction row tracked by the runtime lifecycle.
#[derive(Debug, Clone)]
pub struct TransactionRecord {
    pub tx_id: String,
    pub pipeline: String,
    pub source_channel: String,
    pub message_type: String,
    pub raw_message: String,
    pub state: String,
    pub received_at: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub key_fields: HashMap<String, String>,
}

/// Partial transaction mutation.
#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub state: Option<String>,
    pub error: Option<String>,
}

/// Terminal transaction outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Committed,
    Aborted,
    Poison,
}

/// Audit entry emitted for each context write.
#[derive(Debug, Clone)]
pub struct ContextEntry {
    pub tx_id: String,
    pub key: String,
    pub writer: String,
    pub written_at: SystemTime,
}

/// Transaction lookup filters.
#[derive(Debug, Clone)]
pub struct StoreQuery {
    pub pipeline: Option<String>,
    pub message_type: Option<String>,
    pub state: Option<String>,
    pub since: Option<SystemTime>,
    pub until: Option<SystemTime>,
    pub limit: Option<usize>,
}

/// Query result page.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub records: Vec<TransactionRecord>,
    pub total: usize,
}

/// Correlation expectation persisted for future response matching.
#[derive(Debug, Clone)]
pub struct Expectation {
    pub id: String,
    pub correlation_key: String,
    pub expected_message_type: String,
    pub timeout_at: SystemTime,
}

/// Expectation state update.
#[derive(Debug, Clone)]
pub struct ExpUpdate {
    pub state: Option<String>,
    pub matched_tx_id: Option<String>,
}

/// Failed transaction payload retained for replay.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub id: String,
    pub tx_id: String,
    pub reason: String,
    pub failed_at: SystemTime,
    pub raw_message: String,
}

/// Dead-letter list filters.
#[derive(Debug, Clone)]
pub struct DeadLetterQuery {
    pub pipeline: Option<String>,
    pub limit: Option<usize>,
}

/// Backend health report.
#[derive(Debug, Clone)]
pub struct StoreHealth {
    pub ok: bool,
    pub backend: String,
    pub details: Option<String>,
}

/// Persistence abstraction used by runtime and admin surfaces.
#[async_trait]
pub trait Store: Send + Sync {
    /// Insert or upsert a transaction record at the start of processing.
    async fn begin_transaction(&self, record: &TransactionRecord) -> Result<(), StoreError>;
    /// Apply an in-flight update to an existing transaction.
    async fn update_transaction(
        &self,
        tx_id: &str,
        update: TransactionUpdate,
    ) -> Result<(), StoreError>;
    /// Mark a transaction as terminal.
    async fn complete_transaction(&self, tx_id: &str, outcome: Outcome) -> Result<(), StoreError>;

    /// Persist one context audit entry.
    async fn append_context_entry(
        &self,
        tx_id: &str,
        entry: ContextEntry,
    ) -> Result<(), StoreError>;
    /// Persist multiple context audit entries.
    async fn batch_append_context_entries(
        &self,
        tx_id: &str,
        entries: &[ContextEntry],
    ) -> Result<(), StoreError> {
        for entry in entries {
            self.append_context_entry(tx_id, entry.clone()).await?;
        }
        Ok(())
    }
    /// Load ordered context audit entries for a transaction.
    async fn list_context_entries(&self, tx_id: &str) -> Result<Vec<ContextEntry>, StoreError>;

    /// Find transaction by ID.
    async fn find_by_id(&self, tx_id: &str) -> Result<Option<TransactionRecord>, StoreError>;
    /// Find transactions by message id key field.
    async fn find_by_message_id(&self, msg_id: &str) -> Result<Vec<TransactionRecord>, StoreError>;
    /// Find transactions by end-to-end id key field.
    async fn find_by_end_to_end_id(
        &self,
        e2e_id: &str,
    ) -> Result<Vec<TransactionRecord>, StoreError>;
    /// Find transactions by UETR key field.
    async fn find_by_uetr(&self, uetr: &str) -> Result<Vec<TransactionRecord>, StoreError>;
    /// Query transactions by filter.
    async fn query(&self, filter: StoreQuery) -> Result<QueryResult, StoreError>;

    async fn save_expectation(&self, exp: &Expectation) -> Result<(), StoreError>;
    async fn load_pending_expectations(&self) -> Result<Vec<Expectation>, StoreError>;
    async fn count_pending_expectations(&self) -> Result<usize, StoreError> {
        Ok(self.load_pending_expectations().await?.len())
    }
    async fn update_expectation(&self, id: &str, update: ExpUpdate) -> Result<(), StoreError>;

    async fn save_dead_letter(&self, letter: &DeadLetter) -> Result<(), StoreError>;
    async fn list_dead_letters(
        &self,
        filter: DeadLetterQuery,
    ) -> Result<Vec<DeadLetter>, StoreError>;
    async fn count_dead_letters(&self, pipeline: Option<&str>) -> Result<usize, StoreError> {
        Ok(self
            .list_dead_letters(DeadLetterQuery {
                pipeline: pipeline.map(ToString::to_string),
                limit: None,
            })
            .await?
            .len())
    }
    async fn replay_dead_letter(&self, id: &str) -> Result<(), StoreError>;

    async fn count_transactions_by_states(&self, states: &[&str]) -> Result<usize, StoreError> {
        let mut total = 0usize;
        for state in states {
            let result = self
                .query(StoreQuery {
                    pipeline: None,
                    message_type: None,
                    state: Some((*state).to_string()),
                    since: None,
                    until: None,
                    limit: None,
                })
                .await?;
            total = total.saturating_add(result.total);
        }
        Ok(total)
    }

    async fn health(&self) -> Result<StoreHealth, StoreError>;
    async fn compact(&self) -> Result<(), StoreError>;

    /// Flush any buffered writes and prepare for orderly process exit.
    /// Default is a no-op; backends that require explicit flush for durability
    /// (e.g. RocksDB WAL sync) should override.
    async fn shutdown(&self) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("store error: {message}")]
pub struct StoreError {
    message: String,
}

impl StoreError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
