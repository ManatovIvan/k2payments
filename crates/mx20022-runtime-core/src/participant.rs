use async_trait::async_trait;

use crate::context::Context;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Prepared,
    Aborted,
}

#[async_trait]
pub trait Participant: Send + Sync {
    fn name(&self) -> &str;

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError>;

    async fn commit(&self, _ctx: &mut Context) -> Result<(), ParticipantError> {
        Ok(())
    }

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
