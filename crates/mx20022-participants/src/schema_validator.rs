// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use async_trait::async_trait;
use mx20022_parse::{de, envelope};
use mx20022_validate::typed;
use mx20022_validate::ValidationResult;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

pub struct SchemaValidator;

impl Default for SchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaValidator {
    pub fn new() -> Self {
        Self
    }

    fn validate_message(&self, xml: &str) -> Result<ValidationResult, ParticipantError> {
        let msg_id = envelope::detect_message_type(xml)
            .map_err(|e| ParticipantError::new(format!("schema-validator: {e}")))?;
        let message_type = msg_id.dotted();

        let result = match message_type.as_str() {
            "pacs.002.001.14" => {
                let doc: mx20022_model::generated::pacs::pacs_002_001_14::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "pacs.004.001.11" => {
                let doc: mx20022_model::generated::pacs::pacs_004_001_11::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "pacs.008.001.13" => {
                let doc: mx20022_model::generated::pacs::pacs_008_001_13::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "pacs.009.001.10" => {
                let doc: mx20022_model::generated::pacs::pacs_009_001_10::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "pacs.028.001.05" => {
                let doc: mx20022_model::generated::pacs::pacs_028_001_05::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "pain.001.001.11" => {
                let doc: mx20022_model::generated::pain::pain_001_001_11::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "pain.002.001.13" => {
                let doc: mx20022_model::generated::pain::pain_002_001_13::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "pain.013.001.09" => {
                let doc: mx20022_model::generated::pain::pain_013_001_09::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "camt.029.001.12" => {
                let doc: mx20022_model::generated::camt::camt_029_001_12::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "camt.053.001.11" => {
                let doc: mx20022_model::generated::camt::camt_053_001_11::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "camt.054.001.11" => {
                let doc: mx20022_model::generated::camt::camt_054_001_11::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "camt.056.001.11" => {
                let doc: mx20022_model::generated::camt::camt_056_001_11::Document =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            "head.001.001.04" => {
                let doc: mx20022_model::generated::head::BusinessApplicationHeaderV04 =
                    de::from_str(xml).map_err(map_parse_error)?;
                typed::validate_message(&doc)
            }
            other => {
                return Err(ParticipantError::new(format!(
                    "schema-validator: unsupported message type {other}"
                )))
            }
        };

        Ok(result)
    }
}

fn map_parse_error(error: mx20022_parse::ParseError) -> ParticipantError {
    ParticipantError::new(format!("schema-validator: parse failed: {error}"))
}

#[async_trait]
impl Participant for SchemaValidator {
    fn name(&self) -> &str {
        "schema-validator"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        let result = self.validate_message(ctx.raw_message())?;
        if !result.is_valid() {
            ctx.put_with_writer(
                "schema.validation.errors",
                self.name(),
                result.errors.clone(),
            );
            return Err(ParticipantError::new(format!(
                "schema-validator: {} error(s) found",
                result.error_count()
            )));
        }
        Ok(Action::Prepared)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use mx20022_runtime_core::context::{Context, ContextMeta};
    use mx20022_runtime_core::participant::Participant;

    use super::SchemaValidator;

    fn context(raw: &str) -> Context {
        Context::new(ContextMeta {
            transaction_id: "TX-1".to_string(),
            received_at: SystemTime::now(),
            pipeline: "p".to_string(),
            source_channel: "c".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: raw.to_string(),
        })
    }

    #[tokio::test]
    async fn accepts_well_formed_xml() {
        let mut ctx = context(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13">
  <FIToFICstmrCdtTrf>
    <GrpHdr>
      <MsgId>PACS008-20240101-001</MsgId>
      <CreDtTm>2024-01-01T12:00:00Z</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <SttlmInf>
        <SttlmMtd>CLRG</SttlmMtd>
      </SttlmInf>
    </GrpHdr>
    <CdtTrfTxInf>
      <PmtId>
        <EndToEndId>E2E-20240101-001</EndToEndId>
        <UETR>97ed4827-7b6f-4491-a06f-b548d5a7512d</UETR>
      </PmtId>
      <IntrBkSttlmAmt Ccy="USD">1000.00</IntrBkSttlmAmt>
      <IntrBkSttlmDt>2024-01-01</IntrBkSttlmDt>
      <ChrgBr>SLEV</ChrgBr>
      <Dbtr>
        <Nm>Alice Smith</Nm>
      </Dbtr>
      <DbtrAcct>
        <Id>
          <IBAN>GB82WEST12345698765432</IBAN>
        </Id>
      </DbtrAcct>
      <DbtrAgt>
        <FinInstnId>
          <BICFI>AAAAGB2LXXX</BICFI>
        </FinInstnId>
      </DbtrAgt>
      <CdtrAgt>
        <FinInstnId>
          <BICFI>BBBBUS33XXX</BICFI>
        </FinInstnId>
      </CdtrAgt>
      <Cdtr>
        <Nm>Bob Jones</Nm>
      </Cdtr>
      <CdtrAcct>
        <Id>
          <IBAN>DE89370400440532013000</IBAN>
        </Id>
      </CdtrAcct>
    </CdtTrfTxInf>
  </FIToFICstmrCdtTrf>
</Document>"#,
        );
        let participant = SchemaValidator::new();
        let result = participant.prepare(&mut ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn rejects_malformed_xml() {
        let mut ctx = context(
            r#"<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13"><A></Document>"#,
        );
        let participant = SchemaValidator::new();
        let result = participant.prepare(&mut ctx).await;
        assert!(result.is_err());
    }
}
