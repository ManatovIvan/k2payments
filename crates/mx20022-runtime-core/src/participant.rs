use async_trait::async_trait;

use crate::context::Context;

/// Vote returned by `prepare` to continue or abort the transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Prepared,
    Aborted,
}

/// Transaction pipeline participant with three-phase lifecycle hooks.
#[async_trait]
pub trait Participant: Send + Sync {
    /// Stable participant identifier used in logs and audit trail entries.
    fn name(&self) -> &str;

    /// Validate/read context and vote whether processing can continue.
    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError>;

    /// Execute side effects after all participants have prepared successfully.
    async fn commit(&self, _ctx: &mut Context) -> Result<(), ParticipantError> {
        Ok(())
    }

    /// Compensate/rollback side effects for already-prepared participants.
    async fn abort(&self, _ctx: &mut Context) -> Result<(), ParticipantError> {
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("participant error: {message}")]
pub struct ParticipantError {
    message: String,
}

impl ParticipantError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
