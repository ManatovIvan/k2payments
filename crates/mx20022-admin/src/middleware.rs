// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareStage {
    Authentication,
    Authorization,
    RateLimit,
    Validation,
    ErrorTransform,
    StructuredLogging,
}

pub const DEFAULT_MIDDLEWARE_CHAIN: &[MiddlewareStage] = &[
    MiddlewareStage::Authentication,
    MiddlewareStage::Authorization,
    MiddlewareStage::RateLimit,
    MiddlewareStage::Validation,
    MiddlewareStage::ErrorTransform,
    MiddlewareStage::StructuredLogging,
];
