use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::StreamExt;
use mx20022_channels::{
    ChannelError, ChannelHealth, DeliveryReceipt, InboundChannel, InboundMessage, OutboundChannel,
    OutboundMessage,
};
use tokio::sync::{mpsc, Mutex};

#[derive(Debug, Clone)]
pub struct NatsInboundConfig {
    pub name: String,
    pub endpoint: String,
    pub subject: String,
    pub queue_group: Option<String>,
    pub content_type: String,
}

#[derive(Clone)]
pub struct NatsInboundChannel {
    config: NatsInboundConfig,
    paused: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
}

impl NatsInboundChannel {
    pub fn new(config: NatsInboundConfig) -> Self {
        Self {
            config,
            paused: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            connected: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl InboundChannel for NatsInboundChannel {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn run(&self, sender: mpsc::Sender<InboundMessage>) -> Result<(), ChannelError> {
        let client = async_nats::connect(&self.config.endpoint)
            .await
            .map_err(|e| ChannelError::new(format!("nats connect failed: {e}")))?;
        self.connected.store(true, Ordering::Relaxed);

        let mut subscription = match &self.config.queue_group {
            Some(group) => client
                .queue_subscribe(self.config.subject.clone(), group.clone())
                .await
                .map_err(|e| ChannelError::new(format!("nats queue subscribe failed: {e}")))?,
            None => client
                .subscribe(self.config.subject.clone())
                .await
                .map_err(|e| ChannelError::new(format!("nats subscribe failed: {e}")))?,
        };

        while !self.shutdown.load(Ordering::Relaxed) {
            if self.paused.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }

            let Some(message) = subscription.next().await else {
                break;
            };

            sender
                .send(InboundMessage {
                    raw: String::from_utf8(message.payload.to_vec())
                        .map_err(|_| ChannelError::new("nats payload is not valid UTF-8"))?,
                    content_type: self.config.content_type.clone(),
                })
                .await
                .map_err(|e| ChannelError::new(format!("nats inbound enqueue failed: {e}")))?;
        }

        self.connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ChannelError> {
        self.shutdown.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn health(&self) -> Result<ChannelHealth, ChannelError> {
        Ok(ChannelHealth {
            ok: self.connected.load(Ordering::Relaxed) && !self.shutdown.load(Ordering::Relaxed),
            message: Some(if self.paused.load(Ordering::Relaxed) {
                "paused".to_string()
            } else if self.connected.load(Ordering::Relaxed) {
                "connected".to_string()
            } else {
                "disconnected".to_string()
            }),
        })
    }

    async fn pause(&self) -> Result<(), ChannelError> {
        self.paused.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn resume(&self) -> Result<(), ChannelError> {
        self.paused.store(false, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct NatsOutboundConfig {
    pub name: String,
    pub endpoint: String,
    pub subject: String,
}

#[derive(Clone)]
pub struct NatsOutboundChannel {
    config: NatsOutboundConfig,
    client: Arc<Mutex<Option<async_nats::Client>>>,
    shutdown: Arc<AtomicBool>,
}

impl NatsOutboundChannel {
    pub fn new(config: NatsOutboundConfig) -> Self {
        Self {
            config,
            client: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn get_client(&self) -> Result<async_nats::Client, ChannelError> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let client = async_nats::connect(&self.config.endpoint)
            .await
            .map_err(|e| ChannelError::new(format!("nats connect failed: {e}")))?;
        *guard = Some(client.clone());
        Ok(client)
    }
}

#[async_trait]
impl OutboundChannel for NatsOutboundChannel {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn send(&self, msg: OutboundMessage) -> Result<DeliveryReceipt, ChannelError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(ChannelError::new("channel is shut down"));
        }

        let client = self.get_client().await?;
        client
            .publish(self.config.subject.clone(), msg.raw.into_bytes().into())
            .await
            .map_err(|e| ChannelError::new(format!("nats publish failed: {e}")))?;
        client
            .flush()
            .await
            .map_err(|e| ChannelError::new(format!("nats flush failed: {e}")))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_millis();
        Ok(DeliveryReceipt {
            id: format!("nats-{now}"),
        })
    }

    async fn shutdown(&self) -> Result<(), ChannelError> {
        self.shutdown.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn health(&self) -> Result<ChannelHealth, ChannelError> {
        Ok(ChannelHealth {
            ok: !self.shutdown.load(Ordering::Relaxed),
            message: Some("ok".to_string()),
        })
    }
}
