use async_trait::async_trait;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

pub struct StatusResponseBuilder {
    auto_pacs002: bool,
}

impl Default for StatusResponseBuilder {
    fn default() -> Self {
        Self::new(true)
    }
}

impl StatusResponseBuilder {
    pub fn new(auto_pacs002: bool) -> Self {
        Self { auto_pacs002 }
    }

    fn build_status_xml(&self, ctx: &Context, tx_status: &str, reason: Option<&str>) -> String {
        let tx_id = crate::escape_xml(ctx.transaction_id());
        let reason_xml = reason
            .map(|r| {
                let r = crate::escape_xml(r);
                format!("<StsRsnInf><Rsn><Prtry>{r}</Prtry></Rsn></StsRsnInf>")
            })
            .unwrap_or_default();

        format!(
            "<Document><FIToFIPmtStsRpt><GrpHdr><MsgId>{tx_id}</MsgId></GrpHdr><TxInfAndSts><OrgnlMsgId>{tx_id}</OrgnlMsgId><TxSts>{tx_status}</TxSts>{reason_xml}</TxInfAndSts></FIToFIPmtStsRpt></Document>",
        )
    }
}

#[async_trait]
impl Participant for StatusResponseBuilder {
    fn name(&self) -> &str {
        "status-response-builder"
    }

    async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
        Ok(Action::Prepared)
    }

    async fn commit(&self, ctx: &mut Context) -> Result<(), ParticipantError> {
        if !self.auto_pacs002 {
            return Ok(());
        }
        let response = self.build_status_xml(ctx, "ACTC", None);
        ctx.put_with_writer("response.xml", self.name(), response);
        ctx.put_with_writer("response.message_type", self.name(), "pacs.002".to_string());
        Ok(())
    }

    async fn abort(&self, ctx: &mut Context) -> Result<(), ParticipantError> {
        if !self.auto_pacs002 {
            return Ok(());
        }
        let response = self.build_status_xml(ctx, "RJCT", Some("PROCESSING_ABORTED"));
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

    use super::StatusResponseBuilder;

    fn context(raw: &str) -> Context {
        Context::new(ContextMeta {
            transaction_id: "TX-2".to_string(),
            received_at: SystemTime::now(),
            pipeline: "p".to_string(),
            source_channel: "c".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: raw.to_string(),
        })
    }

    #[tokio::test]
    async fn writes_commit_status_to_context() {
        let mut ctx = context("<Document/>");
        let participant = StatusResponseBuilder::new(true);
        participant
            .commit(&mut ctx)
            .await
            .expect("commit should succeed");

        let response = ctx
            .get::<String>("response.xml")
            .expect("response should be present");
        assert!(response.contains("<TxSts>ACTC</TxSts>"));
    }

    #[tokio::test]
    async fn writes_abort_status_to_context() {
        let mut ctx = context("<Document/>");
        let participant = StatusResponseBuilder::new(true);
        participant
            .abort(&mut ctx)
            .await
            .expect("abort should succeed");

        let response = ctx
            .get::<String>("response.xml")
            .expect("response should be present");
        assert!(response.contains("<TxSts>RJCT</TxSts>"));
        assert!(response.contains("PROCESSING_ABORTED"));
    }
}
