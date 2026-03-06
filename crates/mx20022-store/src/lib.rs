use std::collections::HashMap;
use std::time::SystemTime;

use async_trait::async_trait;

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

#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub state: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Committed,
    Aborted,
    Poison,
}

#[derive(Debug, Clone)]
pub struct ContextEntry {
    pub tx_id: String,
    pub key: String,
    pub writer: String,
    pub written_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct StoreQuery {
    pub pipeline: Option<String>,
    pub message_type: Option<String>,
    pub state: Option<String>,
    pub since: Option<SystemTime>,
    pub until: Option<SystemTime>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub records: Vec<TransactionRecord>,
    pub total: usize,
}

#[derive(Debug, Clone)]
pub struct Expectation {
    pub id: String,
    pub correlation_key: String,
    pub expected_message_type: String,
    pub timeout_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ExpUpdate {
    pub state: Option<String>,
    pub matched_tx_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub id: String,
    pub tx_id: String,
    pub reason: String,
    pub failed_at: SystemTime,
    pub raw_message: String,
}

#[derive(Debug, Clone)]
pub struct DeadLetterQuery {
    pub pipeline: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct StoreHealth {
    pub ok: bool,
    pub backend: String,
    pub details: Option<String>,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn begin_transaction(&self, record: &TransactionRecord) -> Result<(), StoreError>;
    async fn update_transaction(
        &self,
        tx_id: &str,
        update: TransactionUpdate,
    ) -> Result<(), StoreError>;
    async fn complete_transaction(&self, tx_id: &str, outcome: Outcome) -> Result<(), StoreError>;

    async fn append_context_entry(
        &self,
        tx_id: &str,
        entry: ContextEntry,
    ) -> Result<(), StoreError>;

    async fn find_by_id(&self, tx_id: &str) -> Result<Option<TransactionRecord>, StoreError>;
    async fn find_by_message_id(&self, msg_id: &str) -> Result<Vec<TransactionRecord>, StoreError>;
    async fn find_by_end_to_end_id(
        &self,
        e2e_id: &str,
    ) -> Result<Vec<TransactionRecord>, StoreError>;
    async fn find_by_uetr(&self, uetr: &str) -> Result<Vec<TransactionRecord>, StoreError>;
    async fn query(&self, filter: StoreQuery) -> Result<QueryResult, StoreError>;

    async fn save_expectation(&self, exp: &Expectation) -> Result<(), StoreError>;
    async fn load_pending_expectations(&self) -> Result<Vec<Expectation>, StoreError>;
    async fn update_expectation(&self, id: &str, update: ExpUpdate) -> Result<(), StoreError>;

    async fn save_dead_letter(&self, letter: &DeadLetter) -> Result<(), StoreError>;
    async fn list_dead_letters(
        &self,
        filter: DeadLetterQuery,
    ) -> Result<Vec<DeadLetter>, StoreError>;
    async fn replay_dead_letter(&self, id: &str) -> Result<(), StoreError>;

    async fn health(&self) -> Result<StoreHealth, StoreError>;
    async fn compact(&self) -> Result<(), StoreError>;
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
