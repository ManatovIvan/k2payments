use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use mx20022_channels::auth::{authorize_inbound, InboundAuthConfig, InboundAuthContext};
use mx20022_channels::{
    ChannelError, ChannelHealth, DeliveryReceipt, InboundChannel, InboundMessage, OutboundChannel,
    OutboundMessage,
};
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone)]
pub struct HttpInboundConfig {
    pub name: String,
    pub bind: String,
    pub content_type: String,
    pub auth: InboundAuthConfig,
}

#[derive(Clone)]
struct InboundState {
    sender: mpsc::Sender<InboundMessage>,
    content_type: String,
    paused: Arc<RwLock<bool>>,
    auth: InboundAuthConfig,
}

pub struct HttpInboundChannel {
    config: HttpInboundConfig,
    paused: Arc<RwLock<bool>>,
}

impl HttpInboundChannel {
    pub fn new(config: HttpInboundConfig) -> Self {
        Self {
            config,
            paused: Arc::new(RwLock::new(false)),
        }
    }
}

#[async_trait]
impl InboundChannel for HttpInboundChannel {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn run(&self, sender: mpsc::Sender<InboundMessage>) -> Result<(), ChannelError> {
        let state = InboundState {
            sender,
            content_type: self.config.content_type.clone(),
            paused: Arc::clone(&self.paused),
            auth: self.config.auth.clone(),
        };

        let app = Router::new()
            .route("/", post(handle_post))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(&self.config.bind)
            .await
            .map_err(|e| ChannelError::new(format!("failed to bind inbound channel: {e}")))?;

        axum::serve(listener, app)
            .await
            .map_err(|e| ChannelError::new(format!("inbound channel serve failed: {e}")))
    }

    async fn shutdown(&self) -> Result<(), ChannelError> {
        Ok(())
    }

    async fn health(&self) -> Result<ChannelHealth, ChannelError> {
        Ok(ChannelHealth {
            ok: true,
            message: Some(format!("listening on {}", self.config.bind)),
        })
    }

    async fn pause(&self) -> Result<(), ChannelError> {
        *self.paused.write().await = true;
        Ok(())
    }

    async fn resume(&self) -> Result<(), ChannelError> {
        *self.paused.write().await = false;
        Ok(())
    }
}

async fn handle_post(
    State(state): State<InboundState>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, String) {
    if *state.paused.read().await {
        return (StatusCode::SERVICE_UNAVAILABLE, String::new());
    }

    if let Err(error) = authorize_inbound(
        &state.auth,
        InboundAuthContext {
            authorization_header: headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            mtls_subject: headers
                .get(state.auth.mtls_subject_header.as_str())
                .and_then(|value| value.to_str().ok()),
        },
    ) {
        let code = if error.to_string().contains("forbidden")
            || error.to_string().contains("untrusted mTLS")
        {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::UNAUTHORIZED
        };
        return (code, String::new());
    }

    let payload = String::from_utf8_lossy(&body).to_string();
    let result = state
        .sender
        .send(InboundMessage {
            raw: payload,
            content_type: state.content_type.clone(),
        })
        .await;

    match result {
        Ok(_) => (StatusCode::ACCEPTED, String::new()),
        Err(err) => {
            tracing::error!(error=%err, "failed to enqueue inbound message");
            (StatusCode::INTERNAL_SERVER_ERROR, String::new())
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpOutboundConfig {
    pub name: String,
    pub endpoint: String,
    pub content_type: String,
}

pub struct HttpOutboundChannel {
    config: HttpOutboundConfig,
    client: reqwest::Client,
}

impl HttpOutboundChannel {
    pub fn new(config: HttpOutboundConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl OutboundChannel for HttpOutboundChannel {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn send(&self, msg: OutboundMessage) -> Result<DeliveryReceipt, ChannelError> {
        let content_type = if msg.content_type.is_empty() {
            self.config.content_type.as_str()
        } else {
            msg.content_type.as_str()
        };

        let response = self
            .client
            .post(&self.config.endpoint)
            .header("content-type", content_type)
            .body(msg.raw)
            .send()
            .await
            .map_err(|e| ChannelError::new(format!("failed outbound HTTP request: {e}")))?;

        if !response.status().is_success() {
            return Err(ChannelError::new(format!(
                "downstream did not acknowledge delivery: {}",
                response.status()
            )));
        }

        Ok(DeliveryReceipt {
            id: format!("http:{}", now_millis()),
        })
    }

    async fn shutdown(&self) -> Result<(), ChannelError> {
        Ok(())
    }

    async fn health(&self) -> Result<ChannelHealth, ChannelError> {
        Ok(ChannelHealth {
            ok: true,
            message: Some(format!("endpoint={}", self.config.endpoint)),
        })
    }
}

fn now_millis() -> u128 {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis()
}
