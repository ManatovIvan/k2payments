use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct TransactionRequest {
    pub tx_id: String,
    pub pipeline: String,
    pub source_channel: String,
    pub message_type: String,
    pub raw_message: String,
    pub key_fields: HashMap<String, String>,
}

impl TransactionRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.tx_id.trim().is_empty() {
            return Err(DomainError::Validation(
                "tx_id must not be empty".to_string(),
            ));
        }

        if self.pipeline.trim().is_empty() {
            return Err(DomainError::Validation(
                "pipeline must not be empty".to_string(),
            ));
        }

        if self.source_channel.trim().is_empty() {
            return Err(DomainError::Validation(
                "source_channel must not be empty".to_string(),
            ));
        }

        if self.message_type.trim().is_empty() {
            return Err(DomainError::Validation(
                "message_type must not be empty".to_string(),
            ));
        }

        if self.raw_message.trim().is_empty() {
            return Err(DomainError::Validation(
                "raw_message must not be empty".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("domain validation error: {0}")]
    Validation(String),
}
