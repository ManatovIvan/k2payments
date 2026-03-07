// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use async_trait::async_trait;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

use crate::business_rule_validator::{BusinessRuleValidator, ValidationScheme};

pub struct FednowRuleValidator {
    inner: BusinessRuleValidator,
}

impl Default for FednowRuleValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl FednowRuleValidator {
    pub fn new() -> Self {
        Self {
            inner: BusinessRuleValidator::new().with_scheme(ValidationScheme::FedNow),
        }
    }
}

#[async_trait]
impl Participant for FednowRuleValidator {
    fn name(&self) -> &str {
        "fednow-rule-validator"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        self.inner.prepare(ctx).await
    }
}
