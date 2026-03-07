// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use async_trait::async_trait;
use mx20022_validate::schemes::xml_scan::{extract_all_attributes, extract_element};

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

#[derive(Debug, Clone)]
pub struct RouteRule {
    pub destination: String,
    pub message_type: Option<String>,
    pub currency: Option<String>,
    pub bic_prefix: Option<String>,
}

pub struct RoutingEngine {
    rules: Vec<RouteRule>,
    default_route: Option<String>,
}

impl RoutingEngine {
    pub fn new(default_route: Option<String>) -> Self {
        Self {
            rules: Vec::new(),
            default_route,
        }
    }

    pub fn with_rule(mut self, rule: RouteRule) -> Self {
        self.rules.push(rule);
        self
    }

    fn resolve_destination(&self, ctx: &Context) -> Option<String> {
        let xml = ctx.raw_message();
        let currency = extract_all_attributes(xml, "Ccy")
            .first()
            .map(|value| value.to_string())
            .or_else(|| extract_element(xml, "Ccy").map(ToString::to_string));
        let debtor_bic = extract_element(xml, "BICFI")
            .or_else(|| extract_element(xml, "BIC"))
            .map(ToString::to_string);

        for rule in &self.rules {
            if let Some(message_type) = &rule.message_type {
                if message_type != ctx.message_type() {
                    continue;
                }
            }
            if let Some(currency_rule) = &rule.currency {
                if currency.as_deref() != Some(currency_rule.as_str()) {
                    continue;
                }
            }
            if let Some(bic_prefix) = &rule.bic_prefix {
                if !debtor_bic
                    .as_deref()
                    .map(|bic| bic.starts_with(bic_prefix))
                    .unwrap_or(false)
                {
                    continue;
                }
            }

            return Some(rule.destination.clone());
        }

        self.default_route.clone()
    }
}

#[async_trait]
impl Participant for RoutingEngine {
    fn name(&self) -> &str {
        "routing-engine"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        let destination = self.resolve_destination(ctx).ok_or_else(|| {
            ParticipantError::new("routing-engine: no route matched and no default route set")
        })?;
        ctx.put_with_writer("routing.destination", self.name(), destination);
        Ok(Action::Prepared)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use mx20022_runtime_core::context::{Context, ContextMeta};
    use mx20022_runtime_core::participant::{Action, Participant};

    use super::{RouteRule, RoutingEngine};

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
    async fn matches_route_by_currency() {
        let mut ctx = context(
            "<Document><FIToFICstmrCdtTrf><CdtTrfTxInf><IntrBkSttlmAmt Ccy=\"EUR\">100</IntrBkSttlmAmt></CdtTrfTxInf></FIToFICstmrCdtTrf></Document>",
        );
        let participant =
            RoutingEngine::new(Some("default-out".to_string())).with_rule(RouteRule {
                destination: "sepa-out".to_string(),
                message_type: Some("pacs.008".to_string()),
                currency: Some("EUR".to_string()),
                bic_prefix: None,
            });

        let action = participant.prepare(&mut ctx).await.expect("prepare");
        assert_eq!(action, Action::Prepared);
        let route = ctx
            .get::<String>("routing.destination")
            .expect("route should be set");
        assert_eq!(route, "sepa-out");
    }

    #[tokio::test]
    async fn falls_back_to_default_route() {
        let mut ctx = context("<Document/>");
        let participant = RoutingEngine::new(Some("default-out".to_string()));

        let action = participant.prepare(&mut ctx).await.expect("prepare");
        assert_eq!(action, Action::Prepared);
        let route = ctx
            .get::<String>("routing.destination")
            .expect("route should be set");
        assert_eq!(route, "default-out");
    }
}
