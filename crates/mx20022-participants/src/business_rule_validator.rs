// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use async_trait::async_trait;
use mx20022_parse::envelope::detect_message_type;
use mx20022_validate::schemes::{
    cbpr::CbprPlusValidator,
    fednow::FedNowValidator,
    sepa::SepaValidator,
    xml_scan::{extract_all_attributes, extract_all_elements, extract_element},
    SchemeValidator,
};
use mx20022_validate::{RuleRegistry, Severity, ValidationError, ValidationResult};

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationScheme {
    FedNow,
    Sepa,
    Cbpr,
}

pub struct BusinessRuleValidator {
    registry: RuleRegistry,
    scheme: Option<ValidationScheme>,
}

impl Default for BusinessRuleValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl BusinessRuleValidator {
    pub fn new() -> Self {
        Self {
            registry: RuleRegistry::with_defaults(),
            scheme: None,
        }
    }

    pub fn with_scheme(mut self, scheme: ValidationScheme) -> Self {
        self.scheme = Some(scheme);
        self
    }

    fn validate_message(&self, xml: &str) -> Result<ValidationResult, ParticipantError> {
        let mut result = ValidationResult::default();

        for (idx, value) in extract_all_elements(xml, "BICFI").into_iter().enumerate() {
            let path = format!("//BICFI[{}]", idx + 1);
            result
                .errors
                .extend(self.registry.validate_field(value, &path, &["BIC_CHECK"]));
        }
        for (idx, value) in extract_all_elements(xml, "BIC").into_iter().enumerate() {
            let path = format!("//BIC[{}]", idx + 1);
            result
                .errors
                .extend(self.registry.validate_field(value, &path, &["BIC_CHECK"]));
        }

        for (idx, value) in extract_all_elements(xml, "IBAN").into_iter().enumerate() {
            let path = format!("//IBAN[{}]", idx + 1);
            result
                .errors
                .extend(self.registry.validate_field(value, &path, &["IBAN_CHECK"]));
        }

        for (idx, value) in extract_all_elements(xml, "Ccy").into_iter().enumerate() {
            let path = format!("//Ccy[{}]", idx + 1);
            result.errors.extend(
                self.registry
                    .validate_field(value, &path, &["CURRENCY_CHECK"]),
            );
        }
        for (idx, value) in extract_all_attributes(xml, "Ccy").into_iter().enumerate() {
            let path = format!("//@Ccy[{}]", idx + 1);
            result.errors.extend(
                self.registry
                    .validate_field(value, &path, &["CURRENCY_CHECK"]),
            );
        }

        for (idx, value) in extract_all_elements(xml, "LEI").into_iter().enumerate() {
            let path = format!("//LEI[{}]", idx + 1);
            result
                .errors
                .extend(self.registry.validate_field(value, &path, &["LEI_CHECK"]));
        }

        for (idx, value) in extract_all_elements(xml, "CreDtTm").into_iter().enumerate() {
            let path = format!("//CreDtTm[{}]", idx + 1);
            result.errors.extend(
                self.registry
                    .validate_field(value, &path, &["DATETIME_CHECK"]),
            );
        }

        for (idx, value) in extract_all_elements(xml, "IntrBkSttlmDt")
            .into_iter()
            .enumerate()
        {
            let path = format!("//IntrBkSttlmDt[{}]", idx + 1);
            result
                .errors
                .extend(self.registry.validate_field(value, &path, &["DATE_CHECK"]));
        }

        let msg_id_present = extract_element(xml, "BizMsgIdr")
            .or_else(|| extract_element(xml, "MsgId"))
            .is_some();
        if !msg_id_present {
            result.errors.push(ValidationError::new(
                "//GrpHdr/MsgId",
                Severity::Warning,
                "MSG_ID_MISSING",
                "No message identifier (BizMsgIdr / MsgId) found in document",
            ));
        }

        if let Some(scheme) = self.scheme {
            let msg_id = detect_message_type(xml)
                .map_err(|e| ParticipantError::new(format!("business-rule-validator: {e}")))?;
            let validator = scheme_validator(scheme);
            result.merge(validator.validate(xml, &msg_id.dotted()));
        }

        Ok(result)
    }
}

fn scheme_validator(scheme: ValidationScheme) -> Box<dyn SchemeValidator> {
    match scheme {
        ValidationScheme::FedNow => Box::new(FedNowValidator::new()),
        ValidationScheme::Sepa => Box::new(SepaValidator::new()),
        ValidationScheme::Cbpr => Box::new(CbprPlusValidator::new()),
    }
}

#[async_trait]
impl Participant for BusinessRuleValidator {
    fn name(&self) -> &str {
        "business-rule-validator"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        let result = self.validate_message(ctx.raw_message())?;
        if !result.is_valid() {
            ctx.put_with_writer(
                "business.validation.errors",
                self.name(),
                result.errors.clone(),
            );
            return Err(ParticipantError::new(format!(
                "business-rule-validator: {} error(s) found",
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

    use super::BusinessRuleValidator;

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
    async fn accepts_valid_amount_currency() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
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
</Document>"#;
        let mut ctx = context(xml);
        let participant = BusinessRuleValidator::new();
        let result = participant.prepare(&mut ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn rejects_disallowed_currency() {
        let xml = r#"<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13"><IntrBkSttlmAmt Ccy="ZZZ">10.00</IntrBkSttlmAmt></Document>"#;
        let mut ctx = context(xml);
        let participant = BusinessRuleValidator::new();
        let result = participant.prepare(&mut ctx).await;
        assert!(result.is_err());
    }
}
