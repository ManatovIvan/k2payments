use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use mx20022_channels::{
    ChannelError, ChannelHealth, DeliveryReceipt, InboundChannel, InboundMessage, OutboundChannel,
    OutboundMessage,
};
use tokio::sync::{mpsc, Mutex};

#[derive(Debug, Clone)]
pub struct AmqpInboundConfig {
    pub name: String,
    pub url: String,
    pub queue: String,
    pub consumer_tag: String,
    pub content_type: String,
}

#[derive(Clone)]
pub struct AmqpInboundChannel {
    config: AmqpInboundConfig,
    paused: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
}

impl AmqpInboundChannel {
    pub fn new(config: AmqpInboundConfig) -> Self {
        Self {
            config,
            paused: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            connected: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl InboundChannel for AmqpInboundChannel {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn run(&self, sender: mpsc::Sender<InboundMessage>) -> Result<(), ChannelError> {
        let connection = Connection::connect(&self.config.url, ConnectionProperties::default())
            .await
            .map_err(|e| ChannelError::new(format!("amqp connect failed: {e}")))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| ChannelError::new(format!("amqp create channel failed: {e}")))?;
        self.connected.store(true, Ordering::Relaxed);

        channel
            .queue_declare(
                &self.config.queue,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| ChannelError::new(format!("amqp queue declare failed: {e}")))?;

        let mut consumer = channel
            .basic_consume(
                &self.config.queue,
                &self.config.consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| ChannelError::new(format!("amqp basic consume failed: {e}")))?;

        while !self.shutdown.load(Ordering::Relaxed) {
            if self.paused.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }

            let Some(delivery) = consumer.next().await else {
                break;
            };
            let delivery =
                delivery.map_err(|e| ChannelError::new(format!("amqp delivery failed: {e}")))?;

            sender
                .send(InboundMessage {
                    raw: String::from_utf8(delivery.data.to_vec())
                        .map_err(|_| ChannelError::new("amqp payload is not valid UTF-8"))?,
                    content_type: self.config.content_type.clone(),
                })
                .await
                .map_err(|e| ChannelError::new(format!("amqp enqueue failed: {e}")))?;

            delivery
                .ack(BasicAckOptions::default())
                .await
                .map_err(|e| ChannelError::new(format!("amqp ack failed: {e}")))?;
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
pub struct AmqpOutboundConfig {
    pub name: String,
    pub url: String,
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Clone)]
pub struct AmqpOutboundChannel {
    config: AmqpOutboundConfig,
    channel: Arc<Mutex<Option<Channel>>>,
    shutdown: Arc<AtomicBool>,
}

impl AmqpOutboundChannel {
    pub fn new(config: AmqpOutboundConfig) -> Self {
        Self {
            config,
            channel: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn get_channel(&self) -> Result<Channel, ChannelError> {
        let mut guard = self.channel.lock().await;
        if let Some(channel) = guard.as_ref() {
            return Ok(channel.clone());
        }

        let connection = Connection::connect(&self.config.url, ConnectionProperties::default())
            .await
            .map_err(|e| ChannelError::new(format!("amqp connect failed: {e}")))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| ChannelError::new(format!("amqp create channel failed: {e}")))?;
        *guard = Some(channel.clone());
        Ok(channel)
    }
}

#[async_trait]
impl OutboundChannel for AmqpOutboundChannel {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn send(&self, msg: OutboundMessage) -> Result<DeliveryReceipt, ChannelError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(ChannelError::new("channel is shut down"));
        }

        let channel = self.get_channel().await?;
        let confirm = channel
            .basic_publish(
                &self.config.exchange,
                &self.config.routing_key,
                BasicPublishOptions::default(),
                msg.raw.as_bytes(),
                BasicProperties::default().with_content_type(msg.content_type.into()),
            )
            .await
            .map_err(|e| ChannelError::new(format!("amqp publish failed: {e}")))?
            .await
            .map_err(|e| ChannelError::new(format!("amqp publish confirm failed: {e}")))?;

        if !confirm.is_ack() {
            return Err(ChannelError::new("amqp publish not acknowledged"));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_millis();
        Ok(DeliveryReceipt {
            id: format!("amqp-{now}"),
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
