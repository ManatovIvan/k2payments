// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use async_trait::async_trait;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

pub struct ErrorResponseBuilder {
    overwrite_existing: bool,
}

impl Default for ErrorResponseBuilder {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ErrorResponseBuilder {
    pub fn new(overwrite_existing: bool) -> Self {
        Self { overwrite_existing }
    }

    fn build_error_xml(&self, ctx: &Context, reason: &str) -> String {
        let tx_id = crate::escape_xml(ctx.transaction_id());
        let reason = crate::escape_xml(reason);
        format!(
            "<Document><FIToFIPmtStsRpt><GrpHdr><MsgId>{tx_id}</MsgId></GrpHdr><TxInfAndSts><OrgnlMsgId>{tx_id}</OrgnlMsgId><TxSts>RJCT</TxSts><StsRsnInf><Rsn><Prtry>{reason}</Prtry></Rsn></StsRsnInf></TxInfAndSts></FIToFIPmtStsRpt></Document>",
        )
    }
}

#[async_trait]
impl Participant for ErrorResponseBuilder {
    fn name(&self) -> &str {
        "error-response-builder"
    }

    async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
        Ok(Action::Prepared)
    }

    async fn abort(&self, ctx: &mut Context) -> Result<(), ParticipantError> {
        if !self.overwrite_existing && ctx.get_or_none::<String>("response.xml").is_some() {
            return Ok(());
        }

        let reason = if ctx
            .get_or_none::<Vec<mx20022_validate::ValidationError>>("schema.validation.errors")
            .is_some()
        {
            "SCHEMA_VALIDATION_FAILED"
        } else if ctx
            .get_or_none::<Vec<mx20022_validate::ValidationError>>("business.validation.errors")
            .is_some()
        {
            "BUSINESS_VALIDATION_FAILED"
        } else if ctx.get_or_none::<bool>("duplicate.detected") == Some(&true) {
            "DUPLICATE_DETECTED"
        } else if ctx.get_or_none::<bool>("rate_limiter.exceeded") == Some(&true) {
            "RATE_LIMIT_EXCEEDED"
        } else if ctx.get_or_none::<bool>("circuit_breaker.open") == Some(&true) {
            "CIRCUIT_OPEN"
        } else {
            "PROCESSING_ABORTED"
        };

        let response = self.build_error_xml(ctx, reason);
        ctx.put_with_writer("response.xml", self.name(), response);
        ctx.put_with_writer("response.message_type", self.name(), "pacs.002".to_string());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use mx20022_runtime_core::context::{Context, ContextMeta};
    use mx20022_runtime_core::participant::Participant;

    use super::ErrorResponseBuilder;

    fn context() -> Context {
        Context::new(ContextMeta {
            transaction_id: "TX-ERR".to_string(),
            received_at: SystemTime::now(),
            pipeline: "p".to_string(),
            source_channel: "c".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: "<Document/>".to_string(),
        })
    }

    #[tokio::test]
    async fn writes_duplicate_reason_when_duplicate_detected() {
        let mut ctx = context();
        ctx.put_with_writer("duplicate.detected", "test", true);
        let participant = ErrorResponseBuilder::new(false);
        participant
            .abort(&mut ctx)
            .await
            .expect("abort should succeed");

        let response = ctx
            .get::<String>("response.xml")
            .expect("response should be set");
        assert!(response.contains("DUPLICATE_DETECTED"));
    }
}
