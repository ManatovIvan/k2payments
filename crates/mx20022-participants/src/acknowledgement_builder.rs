use async_trait::async_trait;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

pub struct AcknowledgementBuilder {
    overwrite_existing: bool,
}

impl Default for AcknowledgementBuilder {
    fn default() -> Self {
        Self::new(false)
    }
}

impl AcknowledgementBuilder {
    pub fn new(overwrite_existing: bool) -> Self {
        Self { overwrite_existing }
    }
}

#[async_trait]
impl Participant for AcknowledgementBuilder {
    fn name(&self) -> &str {
        "acknowledgement-builder"
    }

    async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
        Ok(Action::Prepared)
    }

    async fn commit(&self, ctx: &mut Context) -> Result<(), ParticipantError> {
        if !self.overwrite_existing && ctx.get_or_none::<String>("response.xml").is_some() {
            return Ok(());
        }

        let ack = format!(
            "<AppHdr><BizMsgIdr>{}</BizMsgIdr><MsgDefIdr>head.001.001.04</MsgDefIdr><BizSvc>ACK</BizSvc></AppHdr>",
            ctx.transaction_id()
        );
        ctx.put_with_writer("response.xml", self.name(), ack);
        ctx.put_with_writer("response.message_type", self.name(), "head.001".to_string());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use mx20022_runtime_core::context::{Context, ContextMeta};
    use mx20022_runtime_core::participant::Participant;

    use super::AcknowledgementBuilder;

    fn context() -> Context {
        Context::new(ContextMeta {
            transaction_id: "TX-ACK".to_string(),
            received_at: SystemTime::now(),
            pipeline: "p".to_string(),
            source_channel: "c".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: "<Document/>".to_string(),
        })
    }

    #[tokio::test]
    async fn writes_ack_on_commit() {
        let mut ctx = context();
        let participant = AcknowledgementBuilder::new(false);
        participant
            .commit(&mut ctx)
            .await
            .expect("commit should succeed");
        let response = ctx
            .get::<String>("response.xml")
            .expect("response should exist");
        assert!(response.contains("<BizSvc>ACK</BizSvc>"));
    }
}
