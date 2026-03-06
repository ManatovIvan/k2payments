use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponseDto {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyResponseDto {
    pub ready: bool,
    pub details: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponseDto {
    pub runtime: String,
    pub pipelines: Vec<String>,
    pub channels: Vec<String>,
    pub store: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResponseDto {
    pub tx_id: String,
    pub pipeline: String,
    pub message_type: String,
    pub state: String,
    pub received_at: String,
    pub completed_at: Option<String>,
}
