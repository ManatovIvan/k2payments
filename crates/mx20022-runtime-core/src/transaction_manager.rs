use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use tracing::warn;

use crate::context::{Context, ContextError};
use crate::participant::{Action, Participant, ParticipantError};
use crate::state::LifecycleState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Committed,
    Aborted,
    Poison,
}

#[derive(Debug)]
pub struct ParticipantResult {
    pub participant: String,
    pub action: Option<Action>,
    pub duration: Duration,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct TransactionReport {
    pub tx_id: String,
    pub outcome: Outcome,
    pub started_at: SystemTime,
    pub completed_at: SystemTime,
    pub participant_results: Vec<ParticipantResult>,
}

#[derive(Default)]
pub struct TransactionManager {
    participants: Vec<Arc<dyn Participant>>,
}

impl TransactionManager {
    pub fn new(participants: Vec<Arc<dyn Participant>>) -> Self {
        Self { participants }
    }

    pub fn add_participant(&mut self, participant: Arc<dyn Participant>) {
        self.participants.push(participant);
    }

    pub async fn process(&self, ctx: &mut Context) -> Result<TransactionReport, TransactionError> {
        let started_at = SystemTime::now();
        let mut results = Vec::with_capacity(self.participants.len());
        let mut prepared_indexes = Vec::new();

        ctx.transition_to(LifecycleState::Preparing)?;

        for (index, participant) in self.participants.iter().enumerate() {
            let tick = SystemTime::now();
            let prepare_result = participant.prepare(ctx).await;
            let duration = elapsed(tick);

            match prepare_result {
                Ok(Action::Prepared) => {
                    prepared_indexes.push(index);
                    results.push(ParticipantResult {
                        participant: participant.name().to_string(),
                        action: Some(Action::Prepared),
                        duration,
                        error: None,
                    });
                }
                Ok(Action::Aborted) => {
                    results.push(ParticipantResult {
                        participant: participant.name().to_string(),
                        action: Some(Action::Aborted),
                        duration,
                        error: None,
                    });

                    let abort_outcome = self
                        .abort_prepared(ctx, &prepared_indexes, &mut results)
                        .await;
                    let completed_at = SystemTime::now();
                    return Ok(TransactionReport {
                        tx_id: ctx.transaction_id().to_string(),
                        outcome: abort_outcome,
                        started_at,
                        completed_at,
                        participant_results: results,
                    });
                }
                Err(error) => {
                    results.push(ParticipantResult {
                        participant: participant.name().to_string(),
                        action: None,
                        duration,
                        error: Some(error.to_string()),
                    });

                    let abort_outcome = self
                        .abort_prepared(ctx, &prepared_indexes, &mut results)
                        .await;
                    let completed_at = SystemTime::now();
                    return Ok(TransactionReport {
                        tx_id: ctx.transaction_id().to_string(),
                        outcome: abort_outcome,
                        started_at,
                        completed_at,
                        participant_results: results,
                    });
                }
            }
        }

        ctx.transition_to(LifecycleState::Prepared)?;
        ctx.transition_to(LifecycleState::Committing)?;

        for index in &prepared_indexes {
            let participant = &self.participants[*index];
            let tick = SystemTime::now();
            let result = participant.commit(ctx).await;
            let duration = elapsed(tick);

            if let Err(error) = result {
                let error_message = error.to_string();
                if let Some(existing) = results
                    .iter_mut()
                    .find(|entry| entry.participant == participant.name())
                {
                    existing.error = Some(error_message);
                } else {
                    results.push(ParticipantResult {
                        participant: participant.name().to_string(),
                        action: Some(Action::Prepared),
                        duration,
                        error: Some(error_message),
                    });
                }

                ctx.transition_to(LifecycleState::Poison)?;

                return Ok(TransactionReport {
                    tx_id: ctx.transaction_id().to_string(),
                    outcome: Outcome::Poison,
                    started_at,
                    completed_at: SystemTime::now(),
                    participant_results: results,
                });
            }
        }

        ctx.transition_to(LifecycleState::Committed)?;

        Ok(TransactionReport {
            tx_id: ctx.transaction_id().to_string(),
            outcome: Outcome::Committed,
            started_at,
            completed_at: SystemTime::now(),
            participant_results: results,
        })
    }

    async fn abort_prepared(
        &self,
        ctx: &mut Context,
        prepared_indexes: &[usize],
        results: &mut Vec<ParticipantResult>,
    ) -> Outcome {
        if let Err(e) = ctx.transition_to(LifecycleState::Aborting) {
            warn!("failed to transition to Aborting, poisoning transaction: {e}");
            return Outcome::Poison;
        }

        for index in prepared_indexes.iter().rev() {
            let participant = &self.participants[*index];
            let tick = SystemTime::now();
            let abort_result = participant.abort(ctx).await;
            let duration = elapsed(tick);

            match abort_result {
                Ok(()) => {
                    results.push(ParticipantResult {
                        participant: participant.name().to_string(),
                        action: Some(Action::Aborted),
                        duration,
                        error: None,
                    });
                }
                Err(error) => {
                    results.push(ParticipantResult {
                        participant: participant.name().to_string(),
                        action: Some(Action::Aborted),
                        duration,
                        error: Some(error.to_string()),
                    });
                    if let Err(e) = ctx.transition_to(LifecycleState::Poison) {
                        warn!("failed to transition to Poison after abort error: {e}");
                    }
                    return Outcome::Poison;
                }
            }
        }

        if let Err(e) = ctx.transition_to(LifecycleState::Aborted) {
            warn!("failed to transition to Aborted, poisoning transaction: {e}");
            return Outcome::Poison;
        }

        Outcome::Aborted
    }
}

fn elapsed(start: SystemTime) -> Duration {
    start.elapsed().unwrap_or_else(|_| Duration::from_secs(0))
}

#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Participant(#[from] ParticipantError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::SystemTime;

    use async_trait::async_trait;

    use crate::context::{Context, ContextMeta};
    use crate::participant::{Action, Participant, ParticipantError};
    use crate::state::LifecycleState;
    use crate::transaction_manager::{Outcome, TransactionManager};

    struct AlwaysPrepare;

    #[async_trait]
    impl Participant for AlwaysPrepare {
        fn name(&self) -> &str {
            "always-prepare"
        }

        async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
            Ok(Action::Prepared)
        }
    }

    struct AlwaysAbort;

    #[async_trait]
    impl Participant for AlwaysAbort {
        fn name(&self) -> &str {
            "always-abort"
        }

        async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
            Ok(Action::Aborted)
        }
    }

    fn test_context() -> Context {
        Context::new(ContextMeta {
            transaction_id: "TX-1".to_string(),
            received_at: SystemTime::now(),
            pipeline: "test".to_string(),
            source_channel: "inbound".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: "<xml />".to_string(),
        })
    }

    #[tokio::test]
    async fn commits_when_all_participants_prepare() {
        let participants: Vec<Arc<dyn Participant>> =
            vec![Arc::new(AlwaysPrepare), Arc::new(AlwaysPrepare)];
        let manager = TransactionManager::new(participants);
        let mut ctx = test_context();

        let report = manager
            .process(&mut ctx)
            .await
            .expect("process should succeed");

        assert_eq!(report.outcome, Outcome::Committed);
        assert_eq!(ctx.state(), LifecycleState::Committed);
    }

    #[tokio::test]
    async fn aborts_when_a_participant_votes_abort() {
        let participants: Vec<Arc<dyn Participant>> =
            vec![Arc::new(AlwaysPrepare), Arc::new(AlwaysAbort)];
        let manager = TransactionManager::new(participants);
        let mut ctx = test_context();

        let report = manager
            .process(&mut ctx)
            .await
            .expect("process should succeed");

        assert_eq!(report.outcome, Outcome::Aborted);
        assert_eq!(ctx.state(), LifecycleState::Aborted);
    }
}
