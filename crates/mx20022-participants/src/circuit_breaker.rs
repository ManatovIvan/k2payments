// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Mutex;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

#[derive(Debug, Clone)]
struct CircuitState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
}

pub struct CircuitBreaker {
    failure_threshold: u32,
    open_duration: Duration,
    state: Mutex<CircuitState>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, open_duration: Duration) -> Self {
        Self {
            failure_threshold: failure_threshold.max(1),
            open_duration,
            state: Mutex::new(CircuitState {
                consecutive_failures: 0,
                open_until: None,
            }),
        }
    }
}

#[async_trait]
impl Participant for CircuitBreaker {
    fn name(&self) -> &str {
        "circuit-breaker"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        let now = Instant::now();
        let mut state = self.state.lock().await;
        if let Some(until) = state.open_until {
            if now < until {
                ctx.put_with_writer("circuit_breaker.open", self.name(), true);
                return Ok(Action::Aborted);
            }
            state.open_until = None;
            state.consecutive_failures = 0;
        }
        Ok(Action::Prepared)
    }

    async fn commit(&self, _ctx: &mut Context) -> Result<(), ParticipantError> {
        let mut state = self.state.lock().await;
        state.consecutive_failures = 0;
        state.open_until = None;
        Ok(())
    }

    async fn abort(&self, _ctx: &mut Context) -> Result<(), ParticipantError> {
        let mut state = self.state.lock().await;
        state.consecutive_failures += 1;
        if state.consecutive_failures >= self.failure_threshold {
            state.open_until = Some(Instant::now() + self.open_duration);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use mx20022_runtime_core::context::{Context, ContextMeta};
    use mx20022_runtime_core::participant::{Action, Participant};

    use super::CircuitBreaker;

    fn context(tx_id: &str) -> Context {
        Context::new(ContextMeta {
            transaction_id: tx_id.to_string(),
            received_at: SystemTime::now(),
            pipeline: "p".to_string(),
            source_channel: "c".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: "<Document/>".to_string(),
        })
    }

    #[tokio::test]
    async fn opens_after_threshold_and_blocks_prepare() {
        let participant = CircuitBreaker::new(2, Duration::from_secs(60));
        let mut first = context("TX-1");
        let mut second = context("TX-2");
        let mut third = context("TX-3");

        assert_eq!(
            participant.prepare(&mut first).await.expect("prepare"),
            Action::Prepared
        );
        participant.abort(&mut first).await.expect("abort");
        participant.abort(&mut second).await.expect("abort");

        assert_eq!(
            participant.prepare(&mut third).await.expect("prepare"),
            Action::Aborted
        );
    }

    #[tokio::test]
    async fn recovers_after_open_duration_expires() {
        let participant = CircuitBreaker::new(1, Duration::from_millis(10));
        let mut first = context("TX-1");
        let mut second = context("TX-2");

        participant.abort(&mut first).await.expect("abort");
        assert_eq!(
            participant.prepare(&mut second).await.expect("prepare"),
            Action::Aborted
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            participant.prepare(&mut second).await.expect("prepare"),
            Action::Prepared
        );
    }
}
