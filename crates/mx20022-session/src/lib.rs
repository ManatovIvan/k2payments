use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Disconnected,
    Connecting,
    Authenticating,
    Active,
    Draining,
    Failed,
}

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub id: String,
    pub heartbeat_interval: Duration,
    pub reconnect_backoff: Duration,
    pub max_reconnect_backoff: Duration,
}

#[derive(Debug, Clone)]
pub struct SessionSnapshot {
    pub id: String,
    pub state: SessionState,
    pub sent_seq: u64,
    pub recv_seq: u64,
    pub reconnect_attempts: u64,
}

#[derive(Clone)]
pub struct SessionManager {
    inner: Arc<RwLock<SessionInner>>,
}

struct SessionInner {
    config: SessionConfig,
    state: SessionState,
    sent_seq: u64,
    recv_seq: u64,
    reconnect_attempts: u64,
}

impl SessionManager {
    pub fn new(config: SessionConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SessionInner {
                config,
                state: SessionState::Disconnected,
                sent_seq: 0,
                recv_seq: 0,
                reconnect_attempts: 0,
            })),
        }
    }

    pub async fn connect(&self) -> Result<(), SessionError> {
        let mut inner = self.inner.write().await;
        if inner.state != SessionState::Disconnected && inner.state != SessionState::Failed {
            return Err(SessionError::InvalidState {
                from: inner.state,
                to: SessionState::Connecting,
            });
        }

        inner.state = SessionState::Connecting;
        inner.state = SessionState::Authenticating;
        inner.state = SessionState::Active;
        inner.reconnect_attempts = 0;

        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), SessionError> {
        let mut inner = self.inner.write().await;
        inner.state = SessionState::Disconnected;
        Ok(())
    }

    pub async fn mark_failed(&self) {
        let mut inner = self.inner.write().await;
        inner.state = SessionState::Failed;
        inner.reconnect_attempts += 1;
    }

    pub async fn begin_drain(&self) {
        let mut inner = self.inner.write().await;
        inner.state = SessionState::Draining;
    }

    pub async fn next_send_sequence(&self) -> Result<u64, SessionError> {
        let mut inner = self.inner.write().await;
        if inner.state != SessionState::Active {
            return Err(SessionError::NotActive(inner.state));
        }

        inner.sent_seq += 1;
        Ok(inner.sent_seq)
    }

    pub async fn register_received_sequence(&self, sequence: u64) -> Result<(), SessionError> {
        let mut inner = self.inner.write().await;
        if inner.state != SessionState::Active && inner.state != SessionState::Draining {
            return Err(SessionError::NotActive(inner.state));
        }

        if sequence <= inner.recv_seq {
            return Err(SessionError::SequenceRollback {
                current: inner.recv_seq,
                received: sequence,
            });
        }

        inner.recv_seq = sequence;
        Ok(())
    }

    pub async fn snapshot(&self) -> SessionSnapshot {
        let inner = self.inner.read().await;
        SessionSnapshot {
            id: inner.config.id.clone(),
            state: inner.state,
            sent_seq: inner.sent_seq,
            recv_seq: inner.recv_seq,
            reconnect_attempts: inner.reconnect_attempts,
        }
    }

    pub fn spawn_heartbeat_task(self) {
        tokio::spawn(async move {
            loop {
                let (state, interval) = {
                    let inner = self.inner.read().await;
                    (inner.state, inner.config.heartbeat_interval)
                };

                if state == SessionState::Disconnected {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }

                tokio::time::sleep(interval).await;
                if let Err(error) = self.heartbeat_tick().await {
                    tracing::warn!(error = %error, "session heartbeat failed");
                }
            }
        });
    }

    async fn heartbeat_tick(&self) -> Result<(), SessionError> {
        let inner = self.inner.read().await;
        if inner.state == SessionState::Active {
            tracing::debug!(session_id = %inner.config.id, "session heartbeat tick");
        }
        Ok(())
    }

    pub async fn reconnect_after_failure(&self) -> Result<(), SessionError> {
        let delay = {
            let inner = self.inner.read().await;
            if inner.state != SessionState::Failed {
                return Err(SessionError::InvalidState {
                    from: inner.state,
                    to: SessionState::Connecting,
                });
            }

            let factor = 1u32 << (inner.reconnect_attempts.min(8) as u32);
            let calc = inner.config.reconnect_backoff.saturating_mul(factor);
            std::cmp::min(calc, inner.config.max_reconnect_backoff)
        };

        tokio::time::sleep(delay).await;
        self.connect().await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("invalid state transition: {from:?} -> {to:?}")]
    InvalidState {
        from: SessionState,
        to: SessionState,
    },
    #[error("session not active: current state {0:?}")]
    NotActive(SessionState),
    #[error("received sequence rollback: current={current}, received={received}")]
    SequenceRollback { current: u64, received: u64 },
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{SessionConfig, SessionManager, SessionState};

    #[tokio::test]
    async fn tracks_sequence_and_state() {
        let session = SessionManager::new(SessionConfig {
            id: "session-1".to_string(),
            heartbeat_interval: Duration::from_millis(5),
            reconnect_backoff: Duration::from_millis(5),
            max_reconnect_backoff: Duration::from_millis(20),
        });

        session.connect().await.expect("connect should succeed");
        let seq = session
            .next_send_sequence()
            .await
            .expect("sequence should increment");
        assert_eq!(seq, 1);

        session
            .register_received_sequence(10)
            .await
            .expect("recv sequence should update");

        let snapshot = session.snapshot().await;
        assert_eq!(snapshot.state, SessionState::Active);
        assert_eq!(snapshot.recv_seq, 10);

        session.begin_drain().await;
        let snapshot = session.snapshot().await;
        assert_eq!(snapshot.state, SessionState::Draining);
    }
}
