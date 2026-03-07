//! Built-in participants for v0.1 foundations.

pub(crate) fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub mod acknowledgement_builder;
pub mod business_rule_validator;
pub mod cbpr_rule_validator;
pub mod circuit_breaker;
pub mod duplicate_checker;
pub mod error_response_builder;
pub mod fednow_rule_validator;
pub mod message_logger;
pub mod rate_limiter;
pub mod routing_engine;
pub mod schema_validator;
pub mod sepa_rule_validator;
pub mod status_response_builder;
