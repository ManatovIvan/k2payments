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
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "true")
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

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.brokers)
            .set("message.timeout.ms", "5000")
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
