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
