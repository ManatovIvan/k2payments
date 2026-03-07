// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use async_trait::async_trait;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

use crate::business_rule_validator::{BusinessRuleValidator, ValidationScheme};

pub struct SepaRuleValidator {
    inner: BusinessRuleValidator,
}

impl Default for SepaRuleValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl SepaRuleValidator {
    pub fn new() -> Self {
        Self {
            inner: BusinessRuleValidator::new().with_scheme(ValidationScheme::Sepa),
        }
    }
}

#[async_trait]
impl Participant for SepaRuleValidator {
    fn name(&self) -> &str {
        "sepa-rule-validator"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        self.inner.prepare(ctx).await
    }
}
