use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mx20022_channel_amqp::{AmqpInboundChannel, AmqpInboundConfig};
use mx20022_channel_file::FileInboundChannel;
use mx20022_channel_grpc::{GrpcInboundChannel, GrpcInboundConfig};
use mx20022_channel_http::{HttpInboundChannel, HttpInboundConfig};
use mx20022_channel_kafka::{KafkaInboundChannel, KafkaInboundConfig};
use mx20022_channel_nats::{NatsInboundChannel, NatsInboundConfig};
use mx20022_channel_tcp::{TcpFraming, TcpInboundChannel, TcpInboundConfig};
use mx20022_channels::{InboundChannel, InboundMessage};
use mx20022_config::{ChannelSection, RuntimeConfig};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

use crate::app::RuntimeApp;

static TX_COUNTER: AtomicU64 = AtomicU64::new(1);

pub async fn run_pipelines(app: Arc<RuntimeApp>, config: RuntimeConfig) -> Result<(), EngineError> {
    let mut tasks = JoinSet::new();
    let mut started = 0usize;

    for pipeline in &config.pipelines {
        let Some(channel_cfg) = config.channels.get(&pipeline.channel_in) else {
            continue;
        };

        let channel_name = pipeline.channel_in.clone();
        let pipeline_name = pipeline.name.clone();
        let message_type_default = pipeline
            .message_types
            .first()
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        let inbound: Arc<dyn InboundChannel> =
            match (channel_cfg.channel_type.as_str(), channel_cfg.mode.as_str()) {
                ("http", "server") => Arc::new(HttpInboundChannel::new(HttpInboundConfig {
                    name: channel_name.clone(),
                    bind: extract_bind(channel_cfg)?,
                    content_type: "application/xml".to_string(),
                })),
                ("file", "watch") => Arc::new(FileInboundChannel::new(
                    channel_name.clone(),
                    extract_required(channel_cfg, "directory")?,
                    extract_optional(channel_cfg, "pattern").unwrap_or_else(|| "*.xml".to_string()),
                    Duration::from_millis(
                        extract_u64(channel_cfg, "poll_interval_ms").unwrap_or(1000),
                    ),
                    extract_optional(channel_cfg, "move_processed_to").map(Into::into),
                    extract_optional(channel_cfg, "move_failed_to").map(Into::into),
                )),
                ("grpc", "server") => Arc::new(GrpcInboundChannel::new(GrpcInboundConfig {
                    name: channel_name.clone(),
                    bind: extract_bind(channel_cfg)?,
                })),
                ("tcp", "server") => Arc::new(TcpInboundChannel::new(TcpInboundConfig {
                    name: channel_name.clone(),
                    bind: extract_bind(channel_cfg)?,
                    framing: extract_tcp_framing(channel_cfg),
                    content_type: extract_optional(channel_cfg, "content_type")
                        .unwrap_or_else(|| "application/xml".to_string()),
                })),
                ("nats", "subscriber") => Arc::new(NatsInboundChannel::new(NatsInboundConfig {
                    name: channel_name.clone(),
                    endpoint: extract_required(channel_cfg, "endpoint")
                        .or_else(|_| extract_required(channel_cfg, "url"))?,
                    subject: extract_required(channel_cfg, "subject")?,
                    queue_group: extract_optional(channel_cfg, "queue_group"),
                    content_type: extract_optional(channel_cfg, "content_type")
                        .unwrap_or_else(|| "application/xml".to_string()),
                })),
                ("kafka", "consumer") => Arc::new(KafkaInboundChannel::new(KafkaInboundConfig {
                    name: channel_name.clone(),
                    brokers: extract_string_list_or_single(channel_cfg, "brokers")
                        .or_else(|| extract_optional(channel_cfg, "bootstrap_servers"))
                        .ok_or_else(|| {
                            EngineError::Config(
                                "kafka channel requires `brokers` or `bootstrap_servers`"
                                    .to_string(),
                            )
                        })?,
                    topic: extract_required(channel_cfg, "topic")?,
                    group_id: extract_optional(channel_cfg, "group_id")
                        .unwrap_or_else(|| format!("mxruntime-{}", channel_name)),
                    content_type: extract_optional(channel_cfg, "content_type")
                        .unwrap_or_else(|| "application/xml".to_string()),
                })),
                ("amqp", "consumer") => Arc::new(AmqpInboundChannel::new(AmqpInboundConfig {
                    name: channel_name.clone(),
                    url: extract_required(channel_cfg, "url")?,
                    queue: extract_required(channel_cfg, "queue")?,
                    consumer_tag: extract_optional(channel_cfg, "consumer_tag")
                        .unwrap_or_else(|| format!("mxruntime-{}", channel_name)),
                    content_type: extract_optional(channel_cfg, "content_type")
                        .unwrap_or_else(|| "application/xml".to_string()),
                })),
                _ => {
                    tracing::warn!(
                        pipeline = %pipeline.name,
                        channel = %pipeline.channel_in,
                        channel_type = %channel_cfg.channel_type,
                        mode = %channel_cfg.mode,
                        "skipping unsupported inbound channel"
                    );
                    continue;
                }
            };

        let (tx, mut rx) = mpsc::channel::<InboundMessage>(1024);
        let inbound_channel = Arc::clone(&inbound);
        tasks.spawn(async move {
            inbound_channel
                .run(tx)
                .await
                .map_err(|e| EngineError::ChannelRun {
                    pipeline: pipeline_name,
                    channel: channel_name,
                    message: e.to_string(),
                })
        });

        let app_for_pipeline = Arc::clone(&app);
        let pipeline_name = pipeline.name.clone();
        let source_channel = pipeline.channel_in.clone();
        let default_message_type = message_type_default;
        let max_concurrent = pipeline.max_concurrent.unwrap_or(1000);
        tasks.spawn(async move {
            let semaphore = Arc::new(Semaphore::new(max_concurrent));

            while let Some(msg) = rx.recv().await {
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| EngineError::Concurrency(e.to_string()))?;

                let app = Arc::clone(&app_for_pipeline);
                let pipeline = pipeline_name.clone();
                let source_channel = source_channel.clone();
                let message_type = infer_message_type(&msg, &default_message_type);
                let raw = msg.raw;

                tokio::spawn(async move {
                    let _permit = permit;
                    let tx_id = next_tx_id();

                    if let Err(error) = app
                        .process(&pipeline, tx_id.clone(), source_channel, message_type, raw)
                        .await
                    {
                        tracing::error!(
                            tx_id = %tx_id,
                            pipeline = %pipeline,
                            error = %error,
                            "pipeline processing failed"
                        );
                    }
                });
            }

            Ok(())
        });

        started += 1;
        tracing::info!(
            pipeline = %pipeline.name,
            channel = %pipeline.channel_in,
            max_concurrent,
            "started inbound pipeline"
        );
    }

    if started == 0 {
        return Err(EngineError::NoSupportedPipelines);
    }

    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(EngineError::TaskJoin(error.to_string())),
        }
    }

    Ok(())
}

fn extract_bind(channel_cfg: &ChannelSection) -> Result<String, EngineError> {
    extract_required(channel_cfg, "bind")
}

fn extract_required(channel_cfg: &ChannelSection, key: &str) -> Result<String, EngineError> {
    channel_cfg
        .extra
        .get(key)
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
        .ok_or_else(|| EngineError::Config(format!("channel requires `{key}`")))
}

fn extract_optional(channel_cfg: &ChannelSection, key: &str) -> Option<String> {
    channel_cfg
        .extra
        .get(key)
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
}

fn extract_u64(channel_cfg: &ChannelSection, key: &str) -> Option<u64> {
    channel_cfg
        .extra
        .get(key)
        .and_then(|v| v.as_integer())
        .and_then(|v| u64::try_from(v).ok())
}

fn extract_string_list_or_single(channel_cfg: &ChannelSection, key: &str) -> Option<String> {
    let value = channel_cfg.extra.get(key)?;
    if let Some(v) = value.as_str() {
        return Some(v.to_string());
    }
    if let Some(values) = value.as_array() {
        let items = values
            .iter()
            .filter_map(|v| v.as_str().map(ToString::to_string))
            .collect::<Vec<_>>();
        if items.is_empty() {
            None
        } else {
            Some(items.join(","))
        }
    } else {
        None
    }
}

fn extract_tcp_framing(channel_cfg: &ChannelSection) -> TcpFraming {
    match extract_optional(channel_cfg, "framing").as_deref() {
        Some("delimiter") => {
            let delimiter = extract_u64(channel_cfg, "delimiter_byte")
                .and_then(|v| u8::try_from(v).ok())
                .unwrap_or(b'\n');
            TcpFraming::Delimiter(delimiter)
        }
        _ => TcpFraming::LengthPrefixed,
    }
}

fn infer_message_type(msg: &InboundMessage, fallback: &str) -> String {
    if msg.content_type.contains("json") {
        return "json-message".to_string();
    }

    fallback.to_string()
}

fn next_tx_id() -> String {
    let count = TX_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis();
    format!("TX-{}-{}", now, count)
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("config error: {0}")]
    Config(String),
    #[error("no supported pipelines were configured")]
    NoSupportedPipelines,
    #[error("channel task failed for pipeline `{pipeline}` channel `{channel}`: {message}")]
    ChannelRun {
        pipeline: String,
        channel: String,
        message: String,
    },
    #[error("concurrency error: {0}")]
    Concurrency(String),
    #[error("task join error: {0}")]
    TaskJoin(String),
}
