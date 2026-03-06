use async_trait::async_trait;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

use crate::business_rule_validator::{BusinessRuleValidator, ValidationScheme};

pub struct CbprRuleValidator {
    inner: BusinessRuleValidator,
}

impl Default for CbprRuleValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl CbprRuleValidator {
    pub fn new() -> Self {
        Self {
            inner: BusinessRuleValidator::new().with_scheme(ValidationScheme::Cbpr),
        }
    }
}

#[async_trait]
impl Participant for CbprRuleValidator {
    fn name(&self) -> &str {
        "cbpr-rule-validator"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        self.inner.prepare(ctx).await
    }
}
