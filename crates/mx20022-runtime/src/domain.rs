use std::collections::HashMap;
use std::time::SystemTime;

use mx20022_store::TransactionRecord;

#[derive(Debug, Clone)]
pub struct TransactionRequest {
    pub record: TransactionRecord,
}

impl TransactionRequest {
    pub fn new(
        tx_id: String,
        pipeline: String,
        source_channel: String,
        message_type: String,
        raw_message: String,
        key_fields: HashMap<String, String>,
        received_at: SystemTime,
    ) -> Self {
        Self {
            record: TransactionRecord {
                tx_id,
                pipeline,
                source_channel,
                message_type,
                raw_message,
                state: "RECEIVED".to_string(),
                received_at,
                completed_at: None,
                key_fields,
            },
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.record.tx_id.trim().is_empty() {
            return Err(DomainError::Validation(
                "tx_id must not be empty".to_string(),
            ));
        }

        if self.record.pipeline.trim().is_empty() {
            return Err(DomainError::Validation(
                "pipeline must not be empty".to_string(),
            ));
        }

        if self.record.source_channel.trim().is_empty() {
            return Err(DomainError::Validation(
                "source_channel must not be empty".to_string(),
            ));
        }

        if self.record.message_type.trim().is_empty() {
            return Err(DomainError::Validation(
                "message_type must not be empty".to_string(),
            ));
        }

        if self.record.raw_message.trim().is_empty() {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use super::{DomainError, TransactionRequest};

    fn request() -> TransactionRequest {
        TransactionRequest::new(
            "TX-1".to_string(),
            "demo".to_string(),
            "http-in".to_string(),
            "pacs.008".to_string(),
            "<Document/>".to_string(),
            HashMap::new(),
            SystemTime::now(),
        )
    }

    #[test]
    fn validates_happy_path() {
        assert!(request().validate().is_ok());
    }

    #[test]
    fn rejects_empty_required_fields() {
        let mut invalid = request();
        invalid.record.tx_id = " ".to_string();
        assert!(matches!(
            invalid.validate(),
            Err(DomainError::Validation(_))
        ));

        invalid = request();
        invalid.record.pipeline = " ".to_string();
        assert!(matches!(
            invalid.validate(),
            Err(DomainError::Validation(_))
        ));

        invalid = request();
        invalid.record.source_channel = " ".to_string();
        assert!(matches!(
            invalid.validate(),
            Err(DomainError::Validation(_))
        ));

        invalid = request();
        invalid.record.message_type = " ".to_string();
        assert!(matches!(
            invalid.validate(),
            Err(DomainError::Validation(_))
        ));

        invalid = request();
        invalid.record.raw_message = " ".to_string();
        assert!(matches!(
            invalid.validate(),
            Err(DomainError::Validation(_))
        ));
    }
}
