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
