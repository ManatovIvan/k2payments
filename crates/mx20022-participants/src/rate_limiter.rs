use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitScope {
    Global,
    MessageType,
    SourceChannel,
}

#[derive(Debug, Clone)]
struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

pub struct RateLimiter {
    rate_per_second: f64,
    burst: f64,
    scope: LimitScope,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl RateLimiter {
    pub fn new(rate_per_second: f64, burst: f64, scope: LimitScope) -> Self {
        Self {
            rate_per_second,
            burst,
            scope,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    fn bucket_key(&self, ctx: &Context) -> String {
        match self.scope {
            LimitScope::Global => "global".to_string(),
            LimitScope::MessageType => ctx.message_type().to_string(),
            LimitScope::SourceChannel => ctx.source_channel().to_string(),
        }
    }
}

#[async_trait]
impl Participant for RateLimiter {
    fn name(&self) -> &str {
        "rate-limiter"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        let now = Instant::now();
        let key = self.bucket_key(ctx);
        let mut buckets = self
            .buckets
            .lock()
            .map_err(|_| ParticipantError::new("rate-limiter: lock poisoned"))?;

        let bucket = buckets.entry(key).or_insert(Bucket {
            tokens: self.burst,
            last_refill: now,
        });

        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + (elapsed * self.rate_per_second)).min(self.burst);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            return Ok(Action::Prepared);
        }

        ctx.put_with_writer("rate_limiter.exceeded", self.name(), true);
        Ok(Action::Aborted)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use mx20022_runtime_core::context::{Context, ContextMeta};
    use mx20022_runtime_core::participant::{Action, Participant};

    use super::{LimitScope, RateLimiter};

    fn context() -> Context {
        Context::new(ContextMeta {
            transaction_id: "TX-1".to_string(),
            received_at: SystemTime::now(),
            pipeline: "p".to_string(),
            source_channel: "http-in".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: "<Document/>".to_string(),
        })
    }

    #[tokio::test]
    async fn aborts_when_burst_is_consumed() {
        let participant = RateLimiter::new(100.0, 1.0, LimitScope::Global);
        let mut ctx1 = context();
        let mut ctx2 = context();

        let first = participant.prepare(&mut ctx1).await.expect("first request");
        let second = participant
            .prepare(&mut ctx2)
            .await
            .expect("second request");
        assert_eq!(first, Action::Prepared);
        assert_eq!(second, Action::Aborted);
    }
}
