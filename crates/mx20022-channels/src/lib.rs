use async_trait::async_trait;

pub mod auth;

#[derive(Debug, Clone)]
pub struct InboundMessage {
    pub raw: String,
    pub content_type: String,
}

#[derive(Debug, Clone)]
pub struct OutboundMessage {
    pub raw: String,
    pub content_type: String,
}

#[derive(Debug, Clone)]
pub struct DeliveryReceipt {
    pub id: String,
}

#[derive(Debug, Clone)]
pub struct ChannelHealth {
    pub ok: bool,
    pub message: Option<String>,
}

#[async_trait]
pub trait InboundChannel: Send + Sync {
    fn name(&self) -> &str;
    async fn run(
        &self,
        sender: tokio::sync::mpsc::Sender<InboundMessage>,
    ) -> Result<(), ChannelError>;
    async fn shutdown(&self) -> Result<(), ChannelError>;
    async fn health(&self) -> Result<ChannelHealth, ChannelError>;
    async fn pause(&self) -> Result<(), ChannelError>;
    async fn resume(&self) -> Result<(), ChannelError>;
}

#[async_trait]
pub trait OutboundChannel: Send + Sync {
    fn name(&self) -> &str;
    async fn send(&self, msg: OutboundMessage) -> Result<DeliveryReceipt, ChannelError>;
    async fn shutdown(&self) -> Result<(), ChannelError>;
    async fn health(&self) -> Result<ChannelHealth, ChannelError>;
}

#[derive(Debug, thiserror::Error)]
#[error("channel error: {message}")]
pub struct ChannelError {
    message: String,
}

impl ChannelError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
