// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use async_trait::async_trait;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

pub struct MessageLogger {
    mask_fields: Vec<&'static str>,
    tag: String,
}

impl Default for MessageLogger {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageLogger {
    pub fn new() -> Self {
        Self {
            mask_fields: vec!["DbtrAcct", "CdtrAcct", "DbtrNm", "CdtrNm", "Adr"],
            tag: "inbound".to_string(),
        }
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = tag.into();
        self
    }

    pub fn with_mask_fields(mut self, fields: Vec<&'static str>) -> Self {
        self.mask_fields = fields;
        self
    }

    fn mask_xml(&self, raw_xml: &str) -> String {
        let mut out = raw_xml.to_string();

        for field in &self.mask_fields {
            out = mask_tag(&out, field);
        }

        out
    }
}

fn mask_tag(input: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");

    let mut result = String::with_capacity(input.len());
    let mut rest = input;

    loop {
        let Some(start) = rest.find(&open) else {
            result.push_str(rest);
            break;
        };

        let (prefix, after_prefix) = rest.split_at(start + open.len());
        result.push_str(prefix);

        let Some(end_rel) = after_prefix.find(&close) else {
            result.push_str(after_prefix);
            break;
        };

        result.push_str("***");
        let (_, after_value) = after_prefix.split_at(end_rel);
        rest = after_value;
    }

    result
}

#[async_trait]
impl Participant for MessageLogger {
    fn name(&self) -> &str {
        "message-logger"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        let masked = self.mask_xml(ctx.raw_message());

        tracing::info!(
            participant = self.name(),
            tag = %self.tag,
            tx_id = %ctx.transaction_id(),
            pipeline = %ctx.pipeline(),
            msg_type = %ctx.message_type(),
            payload = %masked,
            "message processed"
        );

        Ok(Action::Prepared)
    }
}

#[cfg(test)]
mod tests {
    use super::{mask_tag, MessageLogger};

    #[test]
    fn masks_sensitive_tags() {
        let xml = "<DbtrAcct>1234</DbtrAcct><Amt>10</Amt>";
        let masked = mask_tag(xml, "DbtrAcct");

        assert!(masked.contains("<DbtrAcct>***</DbtrAcct>"));
        assert!(masked.contains("<Amt>10</Amt>"));
    }

    #[test]
    fn logger_masks_multiple_fields() {
        let logger = MessageLogger::new();
        let xml = "<DbtrNm>John</DbtrNm><CdtrNm>Jane</CdtrNm>";
        let masked = logger.mask_xml(xml);

        assert!(masked.contains("<DbtrNm>***</DbtrNm>"));
        assert!(masked.contains("<CdtrNm>***</CdtrNm>"));
    }
}
