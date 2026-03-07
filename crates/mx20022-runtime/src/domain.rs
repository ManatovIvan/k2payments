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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{DomainError, TransactionRequest};

    fn request() -> TransactionRequest {
        TransactionRequest {
            tx_id: "TX-1".to_string(),
            pipeline: "demo".to_string(),
            source_channel: "http-in".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: "<Document/>".to_string(),
            key_fields: HashMap::new(),
        }
    }

    #[test]
    fn validates_happy_path() {
        assert!(request().validate().is_ok());
    }

    #[test]
    fn rejects_empty_required_fields() {
        let mut invalid = request();
        invalid.tx_id = " ".to_string();
        assert!(matches!(
            invalid.validate(),
            Err(DomainError::Validation(_))
        ));

        invalid = request();
        invalid.pipeline = " ".to_string();
        assert!(matches!(
            invalid.validate(),
            Err(DomainError::Validation(_))
        ));

        invalid = request();
        invalid.source_channel = " ".to_string();
        assert!(matches!(
            invalid.validate(),
            Err(DomainError::Validation(_))
        ));

        invalid = request();
        invalid.message_type = " ".to_string();
        assert!(matches!(
            invalid.validate(),
            Err(DomainError::Validation(_))
        ));

        invalid = request();
        invalid.raw_message = " ".to_string();
        assert!(matches!(
            invalid.validate(),
            Err(DomainError::Validation(_))
        ));
    }
}
