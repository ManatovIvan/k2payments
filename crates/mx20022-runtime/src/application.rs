// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use mx20022_runtime_core::transaction_manager::Outcome as RuntimeOutcome;
use mx20022_store::Outcome as StoreOutcome;

pub struct TransactionUseCase;

impl TransactionUseCase {
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
    use mx20022_runtime_core::transaction_manager::Outcome as RuntimeOutcome;
    use mx20022_store::Outcome as StoreOutcome;

    use super::TransactionUseCase;

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
