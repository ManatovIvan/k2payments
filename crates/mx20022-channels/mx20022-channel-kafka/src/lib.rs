use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::StreamExt;
use mx20022_channels::{
    ChannelError, ChannelHealth, DeliveryReceipt, InboundChannel, InboundMessage, OutboundChannel,
    OutboundMessage,
};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use tokio::sync::{mpsc, Mutex};

#[derive(Debug, Clone)]
pub struct KafkaInboundConfig {
    pub name: String,
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub content_type: String,
    pub security_protocol: Option<String>,
    pub ssl_ca_location: Option<String>,
}

#[derive(Clone)]
pub struct KafkaInboundChannel {
    config: KafkaInboundConfig,
    paused: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
}

impl KafkaInboundChannel {
    pub fn new(config: KafkaInboundConfig) -> Self {
        Self {
            config,
            paused: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            connected: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl InboundChannel for KafkaInboundChannel {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn run(&self, sender: mpsc::Sender<InboundMessage>) -> Result<(), ChannelError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "false");
        if let Some(protocol) = self.config.security_protocol.as_deref() {
            client_config.set("security.protocol", protocol);
        }
        if let Some(ca_location) = self.config.ssl_ca_location.as_deref() {
            client_config.set("ssl.ca.location", ca_location);
        }

        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| ChannelError::new(format!("kafka consumer create failed: {e}")))?;

        consumer
            .subscribe(&[&self.config.topic])
            .map_err(|e| ChannelError::new(format!("kafka subscribe failed: {e}")))?;
        self.connected.store(true, Ordering::Relaxed);

        let mut stream = consumer.stream();
        while !self.shutdown.load(Ordering::Relaxed) {
            if self.paused.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }

            let Some(message) = stream.next().await else {
                break;
            };
            let message =
                message.map_err(|e| ChannelError::new(format!("kafka recv failed: {e}")))?;

            let payload = match message.payload() {
                Some(value) => String::from_utf8(value.to_vec())
                    .map_err(|_| ChannelError::new("kafka payload is not valid UTF-8"))?,
                None => String::new(),
            };
            sender
                .send(InboundMessage {
                    raw: payload,
                    content_type: self.config.content_type.clone(),
                })
                .await
                .map_err(|e| ChannelError::new(format!("kafka enqueue failed: {e}")))?;
            consumer
                .commit_message(&message, rdkafka::consumer::CommitMode::Sync)
                .map_err(|e| ChannelError::new(format!("kafka commit failed: {e}")))?;
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
pub struct KafkaOutboundConfig {
    pub name: String,
    pub brokers: String,
    pub topic: String,
    pub security_protocol: Option<String>,
    pub ssl_ca_location: Option<String>,
}

#[derive(Clone)]
pub struct KafkaOutboundChannel {
    config: KafkaOutboundConfig,
    producer: Arc<Mutex<Option<FutureProducer>>>,
    shutdown: Arc<AtomicBool>,
}

impl KafkaOutboundChannel {
    pub fn new(config: KafkaOutboundConfig) -> Self {
        Self {
            config,
            producer: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn get_producer(&self) -> Result<FutureProducer, ChannelError> {
        let mut guard = self.producer.lock().await;
        if let Some(producer) = guard.as_ref() {
            return Ok(producer.clone());
        }

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("message.timeout.ms", "5000");
        if let Some(protocol) = self.config.security_protocol.as_deref() {
            client_config.set("security.protocol", protocol);
        }
        if let Some(ca_location) = self.config.ssl_ca_location.as_deref() {
            client_config.set("ssl.ca.location", ca_location);
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| ChannelError::new(format!("kafka producer create failed: {e}")))?;

        *guard = Some(producer.clone());
        Ok(producer)
    }
}

#[async_trait]
impl OutboundChannel for KafkaOutboundChannel {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn send(&self, msg: OutboundMessage) -> Result<DeliveryReceipt, ChannelError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(ChannelError::new("channel is shut down"));
        }

        let producer = self.get_producer().await?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_millis();
        let key = format!("mx-{now}");

        let record = FutureRecord::to(&self.config.topic)
            .payload(&msg.raw)
            .key(&key)
            .headers(
                rdkafka::message::OwnedHeaders::new().insert(rdkafka::message::Header {
                    key: "content-type",
                    value: Some(msg.content_type.as_str()),
                }),
            );

        let delivery = producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| ChannelError::new(format!("kafka publish failed: {e}")))?;

        Ok(DeliveryReceipt {
            id: format!("{}-{}", delivery.0, delivery.1),
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

#[cfg(test)]
mod tests {
    use super::{
        KafkaInboundChannel, KafkaInboundConfig, KafkaOutboundChannel, KafkaOutboundConfig,
    };
    use mx20022_channels::{InboundChannel, OutboundChannel, OutboundMessage};

    #[tokio::test]
    async fn inbound_pause_resume_updates_health_message() {
        let channel = KafkaInboundChannel::new(KafkaInboundConfig {
            name: "kafka-in".to_string(),
            brokers: "127.0.0.1:9092".to_string(),
            topic: "mx.inbound".to_string(),
            group_id: "mxruntime".to_string(),
            content_type: "application/xml".to_string(),
            security_protocol: None,
            ssl_ca_location: None,
        });

        channel.pause().await.expect("pause should succeed");
        let paused = channel.health().await.expect("health should succeed");
        assert_eq!(paused.message.as_deref(), Some("paused"));

        channel.resume().await.expect("resume should succeed");
        let resumed = channel.health().await.expect("health should succeed");
        assert_eq!(resumed.message.as_deref(), Some("disconnected"));
    }

    #[tokio::test]
    async fn outbound_send_fails_after_shutdown() {
        let channel = KafkaOutboundChannel::new(KafkaOutboundConfig {
            name: "kafka-out".to_string(),
            brokers: "127.0.0.1:9092".to_string(),
            topic: "mx.outbound".to_string(),
            security_protocol: None,
            ssl_ca_location: None,
        });
        channel.shutdown().await.expect("shutdown should succeed");

        let err = channel
            .send(OutboundMessage {
                raw: "<Document/>".to_string(),
                content_type: "application/xml".to_string(),
            })
            .await
            .expect_err("send should fail once channel is shut down");
        assert!(err.to_string().contains("shut down"));
    }
}
