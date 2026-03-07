use std::time::SystemTime;

use mx20022_runtime_core::transaction_manager::Outcome as RuntimeOutcome;
use mx20022_store::{Outcome as StoreOutcome, TransactionRecord};

use crate::domain::TransactionRequest;

pub struct TransactionUseCase;

impl TransactionUseCase {
    pub fn begin_record(
        request: &TransactionRequest,
        received_at: SystemTime,
    ) -> TransactionRecord {
        TransactionRecord {
            tx_id: request.tx_id.clone(),
            pipeline: request.pipeline.clone(),
            source_channel: request.source_channel.clone(),
            message_type: request.message_type.clone(),
            raw_message: request.raw_message.clone(),
            state: "RECEIVED".to_string(),
            received_at,
            completed_at: None,
            key_fields: request.key_fields.clone(),
        }
    }

    pub fn map_outcome(outcome: RuntimeOutcome) -> StoreOutcome {
        match outcome {
            RuntimeOutcome::Committed => StoreOutcome::Committed,
            RuntimeOutcome::Aborted => StoreOutcome::Aborted,
            RuntimeOutcome::Poison => StoreOutcome::Poison,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use mx20022_runtime_core::transaction_manager::Outcome as RuntimeOutcome;
    use mx20022_store::Outcome as StoreOutcome;

    use super::TransactionUseCase;
    use crate::domain::TransactionRequest;

    #[test]
    fn begin_record_maps_request_fields() {
        let request = TransactionRequest {
            tx_id: "TX-1".to_string(),
            pipeline: "demo".to_string(),
            source_channel: "http-in".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: "<Document/>".to_string(),
            key_fields: HashMap::new(),
        };
        let now = SystemTime::now();
        let record = TransactionUseCase::begin_record(&request, now);

        assert_eq!(record.tx_id, "TX-1");
        assert_eq!(record.pipeline, "demo");
        assert_eq!(record.state, "RECEIVED");
        assert_eq!(record.received_at, now);
        assert!(record.completed_at.is_none());
    }

    #[test]
    fn map_outcome_covers_all_variants() {
        assert_eq!(
            TransactionUseCase::map_outcome(RuntimeOutcome::Committed),
            StoreOutcome::Committed
        );
        assert_eq!(
            TransactionUseCase::map_outcome(RuntimeOutcome::Aborted),
            StoreOutcome::Aborted
        );
        assert_eq!(
            TransactionUseCase::map_outcome(RuntimeOutcome::Poison),
            StoreOutcome::Poison
        );
    }
}
