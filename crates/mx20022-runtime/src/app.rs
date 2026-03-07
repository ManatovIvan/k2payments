#[cfg(not(any(
    feature = "store-sqlite",
    feature = "store-postgres",
    feature = "store-rocksdb"
)))]
compile_error!(
    "at least one store backend feature must be enabled: store-sqlite, store-postgres, or store-rocksdb"
);

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::RwLock;

#[cfg(feature = "channel-amqp")]
use mx20022_channel_amqp::{AmqpOutboundChannel, AmqpOutboundConfig};
#[cfg(feature = "channel-file")]
use mx20022_channel_file::FileOutboundChannel;
#[cfg(feature = "channel-grpc")]
use mx20022_channel_grpc::{GrpcOutboundChannel, GrpcOutboundConfig};
#[cfg(feature = "channel-http")]
use mx20022_channel_http::{HttpOutboundChannel, HttpOutboundConfig};
#[cfg(feature = "channel-kafka")]
use mx20022_channel_kafka::{KafkaOutboundChannel, KafkaOutboundConfig};
#[cfg(feature = "channel-nats")]
use mx20022_channel_nats::{NatsOutboundChannel, NatsOutboundConfig};
#[cfg(feature = "channel-tcp")]
use mx20022_channel_tcp::{TcpFraming, TcpOutboundChannel, TcpOutboundConfig};
use mx20022_channels::{OutboundChannel, OutboundMessage};
use mx20022_config::{ChannelSection, ParticipantConfig, RuntimeConfig};
use mx20022_correlation::{CorrelationEngine, CorrelationLookupKey};
use mx20022_participants::acknowledgement_builder::AcknowledgementBuilder;
use mx20022_participants::business_rule_validator::BusinessRuleValidator;
use mx20022_participants::cbpr_rule_validator::CbprRuleValidator;
use mx20022_participants::circuit_breaker::CircuitBreaker;
use mx20022_participants::duplicate_checker::{DuplicateChecker, DuplicateKey};
use mx20022_participants::error_response_builder::ErrorResponseBuilder;
use mx20022_participants::fednow_rule_validator::FednowRuleValidator;
use mx20022_participants::message_logger::MessageLogger;
use mx20022_participants::rate_limiter::{LimitScope, RateLimiter};
use mx20022_participants::routing_engine::{RouteRule, RoutingEngine};
use mx20022_participants::schema_validator::SchemaValidator;
use mx20022_participants::sepa_rule_validator::SepaRuleValidator;
use mx20022_participants::status_response_builder::StatusResponseBuilder;
use mx20022_runtime_core::context::{Context, ContextMeta};
use mx20022_runtime_core::participant::Participant;
use mx20022_runtime_core::transaction_manager::{TransactionManager, TransactionReport};
use mx20022_store::{Store, StoreQuery};
#[cfg(feature = "store-postgres")]
use mx20022_store_postgres::PostgresStore;
#[cfg(feature = "store-rocksdb")]
use mx20022_store_rocksdb::RocksDbStore;
#[cfg(feature = "store-sqlite")]
use mx20022_store_sqlite::SqliteStore;

use crate::application::TransactionUseCase;
use crate::domain::{DomainError, TransactionRequest};

struct ActiveTransactionGuard {
    pipeline: String,
}

impl ActiveTransactionGuard {
    fn new(pipeline: impl Into<String>) -> Self {
        let pipeline = pipeline.into();
        mx20022_metrics::inc_active_transactions(&pipeline);
        Self { pipeline }
    }
}

impl Drop for ActiveTransactionGuard {
    fn drop(&mut self) {
        mx20022_metrics::dec_active_transactions(&self.pipeline);
    }
}

pub struct RuntimeApp {
    pipelines: RwLock<HashMap<String, PipelineRuntime>>,
    store: Arc<dyn Store>,
    correlation: Arc<CorrelationEngine>,
    runtime_name: String,
    instance_id: String,
    channel_names: Vec<String>,
    store_backend: String,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryReport {
    pub attempted: usize,
    pub recovered: usize,
    pub failed: usize,
}

struct PipelineRuntime {
    message_types: Vec<String>,
    participant_names: Vec<String>,
    manager: Arc<TransactionManager>,
    channel_out: Option<String>,
    outbound: Option<Arc<dyn OutboundChannel>>,
    timeout_ms: Option<u64>,
}

impl Clone for PipelineRuntime {
    fn clone(&self) -> Self {
        Self {
            message_types: self.message_types.clone(),
            participant_names: self.participant_names.clone(),
            manager: Arc::clone(&self.manager),
            channel_out: self.channel_out.clone(),
            outbound: self.outbound.as_ref().map(Arc::clone),
            timeout_ms: self.timeout_ms,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReloadReport {
    pub pipelines_reloaded: usize,
    pub participants_reloaded: usize,
}

impl RuntimeApp {
    pub async fn from_config(config: &RuntimeConfig) -> Result<Self, RuntimeBuildError> {
        let store: Arc<dyn Store> = match config.store.backend.as_str() {
            #[cfg(feature = "store-sqlite")]
            "sqlite" => Arc::new(SqliteStore::with_pool_size(
                config.store.url.clone(),
                config.store.pool_size,
            )?),
            #[cfg(feature = "store-postgres")]
            "postgres" => Arc::new(
                PostgresStore::connect_with_pool_size(
                    config.store.url.clone(),
                    config.store.pool_size,
                )
                .await?,
            ),
            #[cfg(feature = "store-rocksdb")]
            "rocksdb" => Arc::new(RocksDbStore::open(config.store.url.clone())?),
            other => {
                return Err(RuntimeBuildError::UnsupportedStoreBackend(
                    other.to_string(),
                ))
            }
        };

        let correlation = Arc::new(CorrelationEngine::new(Arc::clone(&store)).await?);
        let scan_interval_ms = config
            .runtime
            .correlation_scan_interval_ms
            .unwrap_or(10_000);
        if scan_interval_ms > 0 {
            Arc::clone(&correlation).spawn_timeout_worker(Duration::from_millis(scan_interval_ms));
        }

        let mut pipelines = HashMap::new();

        for pipeline_cfg in &config.pipelines {
            let participants = build_participants(&pipeline_cfg.participants, Arc::clone(&store))?;
            let channel_out = pipeline_cfg.channel_out.clone();
            let outbound = if let Some(channel_name) = channel_out.as_ref() {
                let channel_cfg = config.channels.get(channel_name).ok_or_else(|| {
                    RuntimeBuildError::Channel(format!(
                        "pipeline `{}` references missing channel_out `{}`",
                        pipeline_cfg.name, channel_name
                    ))
                })?;
                Some(build_outbound_channel(channel_name, channel_cfg)?)
            } else {
                None
            };
            let runtime = PipelineRuntime {
                message_types: pipeline_cfg.message_types.clone(),
                participant_names: pipeline_cfg
                    .participants
                    .iter()
                    .map(|participant| participant.name.clone())
                    .collect(),
                manager: Arc::new(TransactionManager::new(participants)),
                channel_out,
                outbound,
                timeout_ms: pipeline_cfg.timeout_ms,
            };
            pipelines.insert(pipeline_cfg.name.clone(), runtime);
        }

        Ok(Self {
            pipelines: RwLock::new(pipelines),
            store,
            correlation,
            runtime_name: config.runtime.name.clone(),
            instance_id: config.runtime.instance_id.clone(),
            channel_names: config.channels.keys().cloned().collect(),
            store_backend: config.store.backend.clone(),
        })
    }

    pub async fn pipeline_count(&self) -> usize {
        self.pipelines.read().await.len()
    }

    pub async fn pipeline_names(&self) -> Vec<String> {
        self.pipelines.read().await.keys().cloned().collect()
    }

    pub fn channel_names(&self) -> Vec<String> {
        self.channel_names.clone()
    }

    pub fn runtime_name(&self) -> &str {
        &self.runtime_name
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn store_backend(&self) -> &str {
        &self.store_backend
    }

    pub fn store_handle(&self) -> Arc<dyn Store> {
        Arc::clone(&self.store)
    }

    pub async fn accepts_message_type(&self, pipeline: &str, message_type: &str) -> bool {
        let pipelines = self.pipelines.read().await;
        let Some(runtime) = pipelines.get(pipeline) else {
            return false;
        };

        if runtime.message_types.is_empty() {
            return true;
        }

        runtime.message_types.iter().any(|mt| mt == message_type)
    }

    pub async fn process(
        &self,
        pipeline: &str,
        tx_id: impl Into<String>,
        source_channel: impl Into<String>,
        message_type: impl Into<String>,
        raw_message: impl Into<String>,
    ) -> Result<TransactionReport, RuntimeBuildError> {
        let started = SystemTime::now();
        let runtime = self
            .pipelines
            .read()
            .await
            .get(pipeline)
            .cloned()
            .ok_or_else(|| RuntimeBuildError::UnknownPipeline(pipeline.to_string()))?;

        let request = TransactionRequest {
            tx_id: tx_id.into(),
            pipeline: pipeline.to_string(),
            source_channel: source_channel.into(),
            message_type: message_type.into(),
            raw_message: raw_message.into(),
            key_fields: HashMap::new(),
        };
        request.validate()?;

        if !self
            .accepts_message_type(pipeline, &request.message_type)
            .await
        {
            return Err(RuntimeBuildError::MessageTypeNotAccepted {
                pipeline: pipeline.to_string(),
                message_type: request.message_type.clone(),
            });
        }

        let now = SystemTime::now();
        let _active_guard = ActiveTransactionGuard::new(pipeline.to_string());
        let mut ctx = Context::new(ContextMeta {
            transaction_id: request.tx_id.clone(),
            received_at: now,
            pipeline: pipeline.to_string(),
            source_channel: request.source_channel.clone(),
            message_type: request.message_type.clone(),
            raw_message: request.raw_message.clone(),
        });

        self.store
            .begin_transaction(&TransactionUseCase::begin_record(&request, now))
            .await
            .map_err(RuntimeBuildError::Store)?;

        let mut report = match runtime.timeout_ms.filter(|timeout_ms| *timeout_ms > 0) {
            Some(timeout_ms) => {
                let timed = tokio::time::timeout(
                    Duration::from_millis(timeout_ms),
                    runtime.manager.process(&mut ctx),
                )
                .await;
                match timed {
                    Ok(result) => result.map_err(RuntimeBuildError::Processing)?,
                    Err(_) => {
                        let context_entries = context_entries_for_tx(&request.tx_id, &ctx);
                        self.store
                            .batch_append_context_entries(&request.tx_id, &context_entries)
                            .await
                            .map_err(RuntimeBuildError::Store)?;
                        self.store
                            .complete_transaction(&request.tx_id, mx20022_store::Outcome::Poison)
                            .await
                            .map_err(RuntimeBuildError::Store)?;
                        let duration_seconds = started
                            .elapsed()
                            .unwrap_or_else(|_| Duration::from_secs(0))
                            .as_secs_f64();
                        mx20022_metrics::record_transaction_duration(
                            pipeline,
                            &request.message_type,
                            duration_seconds,
                        );
                        mx20022_metrics::record_transaction_total(
                            pipeline,
                            &request.message_type,
                            "poison",
                        );
                        return Err(RuntimeBuildError::PipelineTimeout {
                            pipeline: pipeline.to_string(),
                            timeout_ms,
                        });
                    }
                }
            }
            None => runtime
                .manager
                .process(&mut ctx)
                .await
                .map_err(RuntimeBuildError::Processing)?,
        };

        let mut outbound_error = None::<String>;
        if report.outcome == mx20022_runtime_core::transaction_manager::Outcome::Committed {
            if let Some(outbound) = runtime.outbound.as_ref() {
                if let Some(payload) = ctx.get_or_none::<String>("response.xml") {
                    let content_type = ctx
                        .get_or_none::<String>("response.content_type")
                        .cloned()
                        .unwrap_or_else(|| "application/xml".to_string());
                    if let Err(error) = outbound
                        .send(OutboundMessage {
                            raw: payload.clone(),
                            content_type,
                        })
                        .await
                    {
                        outbound_error = Some(error.to_string());
                        report.outcome = mx20022_runtime_core::transaction_manager::Outcome::Poison;
                    }
                } else {
                    tracing::warn!(
                        tx_id = %request.tx_id,
                        pipeline = %pipeline,
                        channel_out = ?runtime.channel_out,
                        "committed transaction has channel_out configured but no response.xml in context"
                    );
                }
            }
        }

        let context_entries = context_entries_for_tx(&request.tx_id, &ctx);
        self.store
            .batch_append_context_entries(&request.tx_id, &context_entries)
            .await
            .map_err(RuntimeBuildError::Store)?;

        self.store
            .complete_transaction(
                &request.tx_id,
                TransactionUseCase::map_outcome(report.outcome),
            )
            .await
            .map_err(RuntimeBuildError::Store)?;

        if report.outcome == mx20022_runtime_core::transaction_manager::Outcome::Committed {
            if let Some(key) = ctx.get_or_none::<CorrelationLookupKey>("correlation.lookup_key") {
                self.correlation
                    .match_response(key.clone(), request.tx_id.clone())
                    .await
                    .map_err(RuntimeBuildError::Correlation)?;
            }
            if let Some(expectation) =
                ctx.get_or_none::<mx20022_store::Expectation>("correlation.expectation")
            {
                self.correlation
                    .register(expectation.clone())
                    .await
                    .map_err(RuntimeBuildError::Correlation)?;
            }
        }

        let duration_seconds = started
            .elapsed()
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs_f64();
        mx20022_metrics::record_transaction_duration(
            pipeline,
            &request.message_type,
            duration_seconds,
        );
        mx20022_metrics::record_transaction_total(
            pipeline,
            &request.message_type,
            match report.outcome {
                mx20022_runtime_core::transaction_manager::Outcome::Committed => "committed",
                mx20022_runtime_core::transaction_manager::Outcome::Aborted => "aborted",
                mx20022_runtime_core::transaction_manager::Outcome::Poison => "poison",
            },
        );
        if let Some(error) = outbound_error {
            return Err(RuntimeBuildError::Outbound(error));
        }
        Ok(report)
    }

    pub async fn recover_incomplete_transactions(
        &self,
        limit: usize,
    ) -> Result<RecoveryReport, RuntimeBuildError> {
        let mut report = RecoveryReport::default();
        let states = [
            "RECEIVED",
            "PREPARING",
            "PREPARED",
            "COMMITTING",
            "ABORTING",
        ];

        for state in states {
            let remaining = limit.saturating_sub(report.attempted);
            if remaining == 0 {
                break;
            }

            let result = self
                .store
                .query(StoreQuery {
                    pipeline: None,
                    message_type: None,
                    state: Some(state.to_string()),
                    since: None,
                    until: None,
                    limit: Some(remaining),
                })
                .await
                .map_err(RuntimeBuildError::Store)?;

            for record in result.records {
                report.attempted += 1;
                let recovery = self
                    .process(
                        &record.pipeline,
                        record.tx_id.clone(),
                        record.source_channel.clone(),
                        record.message_type.clone(),
                        record.raw_message.clone(),
                    )
                    .await;

                match recovery {
                    Ok(_) => report.recovered += 1,
                    Err(error) => {
                        report.failed += 1;
                        tracing::error!(
                            tx_id = %record.tx_id,
                            pipeline = %record.pipeline,
                            state = %record.state,
                            error = %error,
                            "startup recovery replay failed"
                        );
                    }
                }
            }
        }

        Ok(report)
    }

    pub async fn reload_participant_configs(
        &self,
        config: &RuntimeConfig,
    ) -> Result<ReloadReport, RuntimeBuildError> {
        let current = self.pipelines.read().await;

        if current.len() != config.pipelines.len() {
            return Err(RuntimeBuildError::TopologyReloadNotAllowed(
                "pipeline count changed; restart is required".to_string(),
            ));
        }

        for pipeline_cfg in &config.pipelines {
            let Some(existing) = current.get(&pipeline_cfg.name) else {
                return Err(RuntimeBuildError::TopologyReloadNotAllowed(format!(
                    "pipeline `{}` does not exist in running topology",
                    pipeline_cfg.name
                )));
            };
            if existing.message_types != pipeline_cfg.message_types {
                return Err(RuntimeBuildError::TopologyReloadNotAllowed(format!(
                    "pipeline `{}` message_types changed; restart is required",
                    pipeline_cfg.name
                )));
            }
            if existing.channel_out != pipeline_cfg.channel_out {
                return Err(RuntimeBuildError::TopologyReloadNotAllowed(format!(
                    "pipeline `{}` channel_out changed; restart is required",
                    pipeline_cfg.name
                )));
            }
            if existing.timeout_ms != pipeline_cfg.timeout_ms {
                return Err(RuntimeBuildError::TopologyReloadNotAllowed(format!(
                    "pipeline `{}` timeout_ms changed; restart is required",
                    pipeline_cfg.name
                )));
            }

            let incoming_names = pipeline_cfg
                .participants
                .iter()
                .map(|participant| participant.name.clone())
                .collect::<Vec<_>>();
            if existing.participant_names != incoming_names {
                return Err(RuntimeBuildError::TopologyReloadNotAllowed(format!(
                    "pipeline `{}` participant order/topology changed; restart is required",
                    pipeline_cfg.name
                )));
            }
        }
        drop(current);

        let mut rebuilt = HashMap::new();
        for pipeline_cfg in &config.pipelines {
            let participants =
                build_participants(&pipeline_cfg.participants, Arc::clone(&self.store))?;
            let channel_out = pipeline_cfg.channel_out.clone();
            let outbound = if let Some(channel_name) = channel_out.as_ref() {
                let channel_cfg = config.channels.get(channel_name).ok_or_else(|| {
                    RuntimeBuildError::Channel(format!(
                        "pipeline `{}` references missing channel_out `{}`",
                        pipeline_cfg.name, channel_name
                    ))
                })?;
                Some(build_outbound_channel(channel_name, channel_cfg)?)
            } else {
                None
            };
            rebuilt.insert(
                pipeline_cfg.name.clone(),
                PipelineRuntime {
                    message_types: pipeline_cfg.message_types.clone(),
                    participant_names: pipeline_cfg
                        .participants
                        .iter()
                        .map(|participant| participant.name.clone())
                        .collect(),
                    manager: Arc::new(TransactionManager::new(participants)),
                    channel_out,
                    outbound,
                    timeout_ms: pipeline_cfg.timeout_ms,
                },
            );
        }

        let mut pipelines = self.pipelines.write().await;
        for (name, runtime) in rebuilt {
            pipelines.insert(name, runtime);
        }

        Ok(ReloadReport {
            pipelines_reloaded: config.pipelines.len(),
            participants_reloaded: config.pipelines.iter().map(|p| p.participants.len()).sum(),
        })
    }
}

fn context_entries_for_tx(tx_id: &str, ctx: &Context) -> Vec<mx20022_store::ContextEntry> {
    ctx.audit_log()
        .iter()
        .map(|entry| mx20022_store::ContextEntry {
            tx_id: tx_id.to_string(),
            key: entry.key.clone(),
            writer: entry.writer.clone(),
            written_at: entry.written_at,
        })
        .collect()
}

fn build_outbound_channel(
    channel_name: &str,
    channel_cfg: &ChannelSection,
) -> Result<Arc<dyn OutboundChannel>, RuntimeBuildError> {
    match (channel_cfg.channel_type.as_str(), channel_cfg.mode.as_str()) {
        #[cfg(feature = "channel-http")]
        ("http", "client") => Ok(Arc::new(HttpOutboundChannel::new(HttpOutboundConfig {
            name: channel_name.to_string(),
            endpoint: extract_required(channel_cfg, "endpoint")
                .or_else(|_| extract_required(channel_cfg, "url"))?,
            content_type: extract_optional(channel_cfg, "content_type")
                .unwrap_or_else(|| "application/xml".to_string()),
        }))),
        #[cfg(feature = "channel-grpc")]
        ("grpc", "client") => Ok(Arc::new(GrpcOutboundChannel::new(GrpcOutboundConfig {
            name: channel_name.to_string(),
            endpoint: extract_required(channel_cfg, "endpoint")
                .or_else(|_| extract_required(channel_cfg, "url"))?,
        }))),
        #[cfg(feature = "channel-tcp")]
        ("tcp", "client") => Ok(Arc::new(TcpOutboundChannel::new(TcpOutboundConfig {
            name: channel_name.to_string(),
            endpoint: extract_required(channel_cfg, "endpoint")
                .or_else(|_| extract_required(channel_cfg, "url"))?,
            framing: extract_tcp_framing(channel_cfg),
            content_type: extract_optional(channel_cfg, "content_type")
                .unwrap_or_else(|| "application/xml".to_string()),
        }))),
        #[cfg(feature = "channel-file")]
        ("file", "write") => Ok(Arc::new(FileOutboundChannel::new(
            channel_name.to_string(),
            extract_required(channel_cfg, "directory")?,
            extract_optional(channel_cfg, "extension").unwrap_or_else(|| "xml".to_string()),
        ))),
        #[cfg(feature = "channel-nats")]
        ("nats", "publisher") => Ok(Arc::new(NatsOutboundChannel::new(NatsOutboundConfig {
            name: channel_name.to_string(),
            endpoint: extract_required(channel_cfg, "endpoint")
                .or_else(|_| extract_required(channel_cfg, "url"))?,
            subject: extract_required(channel_cfg, "subject")?,
        }))),
        #[cfg(feature = "channel-kafka")]
        ("kafka", "producer") => Ok(Arc::new(KafkaOutboundChannel::new(KafkaOutboundConfig {
            name: channel_name.to_string(),
            brokers: extract_string_list_or_single(channel_cfg, "brokers")
                .or_else(|| extract_optional(channel_cfg, "bootstrap_servers"))
                .ok_or_else(|| {
                    RuntimeBuildError::Channel(format!(
                        "channel `{channel_name}` requires `brokers` or `bootstrap_servers`"
                    ))
                })?,
            topic: extract_required(channel_cfg, "topic")?,
        }))),
        #[cfg(feature = "channel-amqp")]
        ("amqp", "publisher") => Ok(Arc::new(AmqpOutboundChannel::new(AmqpOutboundConfig {
            name: channel_name.to_string(),
            url: extract_required(channel_cfg, "url")?,
            exchange: extract_optional(channel_cfg, "exchange").unwrap_or_default(),
            routing_key: extract_required(channel_cfg, "routing_key")
                .or_else(|_| extract_required(channel_cfg, "queue"))?,
        }))),
        _ => Err(RuntimeBuildError::Channel(format!(
            "unsupported outbound channel `{channel_name}` type=`{}` mode=`{}`",
            channel_cfg.channel_type, channel_cfg.mode
        ))),
    }
}

fn extract_required(channel_cfg: &ChannelSection, key: &str) -> Result<String, RuntimeBuildError> {
    channel_cfg
        .extra
        .get(key)
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
        .ok_or_else(|| RuntimeBuildError::Channel(format!("channel requires `{key}`")))
}

fn extract_optional(channel_cfg: &ChannelSection, key: &str) -> Option<String> {
    channel_cfg
        .extra
        .get(key)
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
}

#[cfg(feature = "channel-kafka")]
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

#[cfg(feature = "channel-tcp")]
fn extract_u64(channel_cfg: &ChannelSection, key: &str) -> Option<u64> {
    channel_cfg
        .extra
        .get(key)
        .and_then(|v| v.as_integer())
        .and_then(|v| u64::try_from(v).ok())
}

#[cfg(feature = "channel-tcp")]
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

fn build_participants(
    configs: &[ParticipantConfig],
    store: Arc<dyn Store>,
) -> Result<Vec<Arc<dyn Participant>>, RuntimeBuildError> {
    let mut participants: Vec<Arc<dyn Participant>> = Vec::new();

    for cfg in configs {
        match cfg.name.as_str() {
            "message-logger" => {
                let mut participant = MessageLogger::new();
                if let Some(tag) = cfg.config.get("tag").and_then(|v| v.as_str()) {
                    participant = participant.with_tag(tag.to_string());
                }
                participants.push(Arc::new(participant));
            }
            "schema-validator" => participants.push(Arc::new(SchemaValidator::new())),
            "fednow-rule-validator" => participants.push(Arc::new(FednowRuleValidator::new())),
            "sepa-rule-validator" => participants.push(Arc::new(SepaRuleValidator::new())),
            "cbpr-rule-validator" => participants.push(Arc::new(CbprRuleValidator::new())),
            "business-rule-validator" => {
                let mut validator = BusinessRuleValidator::new();
                if let Some(scheme) = cfg.config.get("scheme").and_then(|v| v.as_str()) {
                    validator = validator.with_scheme(match scheme {
                        "fednow" => {
                            mx20022_participants::business_rule_validator::ValidationScheme::FedNow
                        }
                        "sepa" => {
                            mx20022_participants::business_rule_validator::ValidationScheme::Sepa
                        }
                        "cbpr" => {
                            mx20022_participants::business_rule_validator::ValidationScheme::Cbpr
                        }
                        other => {
                            return Err(RuntimeBuildError::UnknownParticipant(format!(
                                "business-rule-validator scheme `{other}`"
                            )));
                        }
                    });
                }
                participants.push(Arc::new(validator));
            }
            "status-response-builder" => {
                let auto = cfg
                    .config
                    .get("auto_pacs002")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                participants.push(Arc::new(StatusResponseBuilder::new(auto)));
            }
            "acknowledgement-builder" => {
                let overwrite = cfg
                    .config
                    .get("overwrite_existing")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
                participants.push(Arc::new(AcknowledgementBuilder::new(overwrite)));
            }
            "error-response-builder" => {
                let overwrite = cfg
                    .config
                    .get("overwrite_existing")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
                participants.push(Arc::new(ErrorResponseBuilder::new(overwrite)));
            }
            "duplicate-checker" => {
                let keys = cfg
                    .config
                    .get("keys")
                    .and_then(|value| value.as_array())
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|item| item.as_str())
                            .map(|item| match item {
                                "message_id" | "msg_id" => Ok(DuplicateKey::MessageId),
                                "end_to_end_id" | "e2e_id" => Ok(DuplicateKey::EndToEndId),
                                "uetr" => Ok(DuplicateKey::Uetr),
                                other => Err(RuntimeBuildError::UnknownParticipant(format!(
                                    "duplicate-checker key `{other}`"
                                ))),
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?
                    .unwrap_or_else(|| {
                        vec![
                            DuplicateKey::MessageId,
                            DuplicateKey::EndToEndId,
                            DuplicateKey::Uetr,
                        ]
                    });
                participants.push(Arc::new(
                    DuplicateChecker::new(Arc::clone(&store)).with_keys(keys),
                ));
            }
            "routing-engine" => {
                let default_route = cfg
                    .config
                    .get("default_route")
                    .and_then(|value| value.as_str())
                    .map(ToString::to_string);
                let mut engine = RoutingEngine::new(default_route);

                if let Some(rules) = cfg.config.get("rules").and_then(|value| value.as_array()) {
                    for rule in rules {
                        let table = rule.as_table().ok_or_else(|| {
                            RuntimeBuildError::UnknownParticipant(
                                "routing-engine rule must be an inline table".to_string(),
                            )
                        })?;
                        let destination = table
                            .get("destination")
                            .and_then(|value| value.as_str())
                            .ok_or_else(|| {
                                RuntimeBuildError::UnknownParticipant(
                                    "routing-engine rule requires destination".to_string(),
                                )
                            })?
                            .to_string();
                        engine = engine.with_rule(RouteRule {
                            destination,
                            message_type: table
                                .get("message_type")
                                .and_then(|value| value.as_str())
                                .map(ToString::to_string),
                            currency: table
                                .get("currency")
                                .and_then(|value| value.as_str())
                                .map(ToString::to_string),
                            bic_prefix: table
                                .get("bic_prefix")
                                .and_then(|value| value.as_str())
                                .map(ToString::to_string),
                        });
                    }
                }

                participants.push(Arc::new(engine));
            }
            "rate-limiter" => {
                let rate = cfg
                    .config
                    .get("rate_per_second")
                    .and_then(|value| value.as_float())
                    .or_else(|| {
                        cfg.config
                            .get("rate_per_second")
                            .and_then(|value| value.as_integer().map(|v| v as f64))
                    })
                    .unwrap_or(100.0);
                let burst = cfg
                    .config
                    .get("burst")
                    .and_then(|value| value.as_float())
                    .or_else(|| {
                        cfg.config
                            .get("burst")
                            .and_then(|value| value.as_integer().map(|v| v as f64))
                    })
                    .unwrap_or(rate.max(1.0));
                let scope = match cfg
                    .config
                    .get("scope")
                    .and_then(|value| value.as_str())
                    .unwrap_or("global")
                {
                    "global" => LimitScope::Global,
                    "message_type" => LimitScope::MessageType,
                    "source_channel" => LimitScope::SourceChannel,
                    other => {
                        return Err(RuntimeBuildError::UnknownParticipant(format!(
                            "rate-limiter scope `{other}`"
                        )))
                    }
                };
                participants.push(Arc::new(RateLimiter::new(
                    rate.max(0.1),
                    burst.max(1.0),
                    scope,
                )));
            }
            "circuit-breaker" => {
                let threshold = cfg
                    .config
                    .get("failure_threshold")
                    .and_then(|value| value.as_integer())
                    .and_then(|value| u32::try_from(value).ok())
                    .unwrap_or(5);
                let open_ms = cfg
                    .config
                    .get("open_ms")
                    .and_then(|value| value.as_integer())
                    .and_then(|value| u64::try_from(value).ok())
                    .unwrap_or(30_000);
                participants.push(Arc::new(CircuitBreaker::new(
                    threshold,
                    Duration::from_millis(open_ms),
                )));
            }
            other => {
                return Err(RuntimeBuildError::UnknownParticipant(other.to_string()));
            }
        }
    }

    Ok(participants)
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeBuildError {
    #[error("unknown participant `{0}`")]
    UnknownParticipant(String),
    #[error("unsupported store backend `{0}`")]
    UnsupportedStoreBackend(String),
    #[error("unknown pipeline `{0}`")]
    UnknownPipeline(String),
    #[error("topology reload is not allowed: {0}")]
    TopologyReloadNotAllowed(String),
    #[error("message type `{message_type}` not accepted by pipeline `{pipeline}`")]
    MessageTypeNotAccepted {
        pipeline: String,
        message_type: String,
    },
    #[error("channel configuration error: {0}")]
    Channel(String),
    #[error("pipeline `{pipeline}` timed out after {timeout_ms}ms")]
    PipelineTimeout { pipeline: String, timeout_ms: u64 },
    #[error("outbound delivery failed: {0}")]
    Outbound(String),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Store(#[from] mx20022_store::StoreError),
    #[error(transparent)]
    Processing(mx20022_runtime_core::transaction_manager::TransactionError),
    #[error(transparent)]
    Correlation(#[from] mx20022_correlation::CorrelationError),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use mx20022_config::RuntimeConfig;
    use mx20022_runtime_core::transaction_manager::Outcome;
    use mx20022_store::{Store, TransactionRecord};

    use crate::app::RuntimeApp;

    const TEST_CONFIG: &str = r#"
[runtime]
name = "test-runtime"
instance_id = "local-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:8080"

[[pipeline]]
name = "demo"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "message-logger", config = {} },
]
"#;

    #[tokio::test]
    async fn builds_runtime_from_valid_config() {
        let config = RuntimeConfig::parse(TEST_CONFIG).expect("config should parse");
        let app = RuntimeApp::from_config(&config)
            .await
            .expect("app should build");

        assert_eq!(app.pipeline_count().await, 1);
        assert!(app.accepts_message_type("demo", "pacs.008").await);
        assert!(!app.accepts_message_type("demo", "pacs.002").await);
    }

    #[tokio::test]
    async fn processes_message_through_pipeline() {
        let config = RuntimeConfig::parse(TEST_CONFIG).expect("config should parse");
        let app = RuntimeApp::from_config(&config)
            .await
            .expect("app should build");

        let report = app
            .process("demo", "TX-42", "http-in", "pacs.008", "<Document/>")
            .await
            .expect("process should succeed");

        assert_eq!(report.outcome, Outcome::Committed);
    }

    const DUPLICATE_GUARD_CONFIG: &str = r#"
[runtime]
name = "test-runtime"
instance_id = "local-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:8080"

[[pipeline]]
name = "duplicate-guard"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "error-response-builder", config = { overwrite_existing = true } },
  { name = "duplicate-checker", config = { keys = ["message_id"] } },
]
"#;

    const OUTBOUND_CONFIG: &str = r#"
[runtime]
name = "test-runtime"
instance_id = "local-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:8080"

[channels.http-out]
type = "http"
mode = "client"
endpoint = "http://127.0.0.1:9/outbox"

[[pipeline]]
name = "outbound"
channel_in = "http-in"
channel_out = "http-out"
message_types = ["pacs.008"]
participants = [
  { name = "acknowledgement-builder", config = {} },
]
"#;

    #[tokio::test]
    async fn duplicate_checker_aborts_pipeline_when_message_id_exists() {
        let config = RuntimeConfig::parse(DUPLICATE_GUARD_CONFIG).expect("config should parse");
        let app = RuntimeApp::from_config(&config)
            .await
            .expect("app should build");

        let store: Arc<dyn Store> = app.store_handle();
        let mut key_fields = HashMap::new();
        key_fields.insert("message_id".to_string(), "MSG-DUP-1".to_string());
        store
            .begin_transaction(&TransactionRecord {
                tx_id: "TX-OLD".to_string(),
                pipeline: "duplicate-guard".to_string(),
                source_channel: "http-in".to_string(),
                message_type: "pacs.008".to_string(),
                raw_message: "<Document/>".to_string(),
                state: "COMMITTED".to_string(),
                received_at: SystemTime::now(),
                completed_at: Some(SystemTime::now()),
                key_fields,
            })
            .await
            .expect("seed tx should succeed");

        let xml = "<Document><FIToFICstmrCdtTrf><GrpHdr><MsgId>MSG-DUP-1</MsgId></GrpHdr></FIToFICstmrCdtTrf></Document>";
        let report = app
            .process("duplicate-guard", "TX-NEW", "http-in", "pacs.008", xml)
            .await
            .expect("process should return report");

        assert_eq!(report.outcome, Outcome::Aborted);
    }

    #[tokio::test]
    async fn outbound_delivery_failure_marks_transaction_poison() {
        let config = RuntimeConfig::parse(OUTBOUND_CONFIG).expect("config should parse");
        let app = RuntimeApp::from_config(&config)
            .await
            .expect("app should build");
        let err = app
            .process("outbound", "TX-OUT-1", "http-in", "pacs.008", "<Document/>")
            .await
            .expect_err("outbound send should fail");
        assert!(
            err.to_string().contains("outbound delivery failed"),
            "unexpected error: {err}"
        );

        let record = app
            .store_handle()
            .find_by_id("TX-OUT-1")
            .await
            .expect("lookup")
            .expect("record");
        assert_eq!(record.state, "POISON");
    }

    const RECOVERY_CONFIG: &str = r#"
[runtime]
name = "test-runtime"
instance_id = "local-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:8080"

[[pipeline]]
name = "recovery"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "message-logger", config = {} },
]
"#;

    #[tokio::test]
    async fn recovers_incomplete_transactions_from_store() {
        let config = RuntimeConfig::parse(RECOVERY_CONFIG).expect("config should parse");
        let app = RuntimeApp::from_config(&config)
            .await
            .expect("app should build");

        let store: Arc<dyn Store> = app.store_handle();
        store
            .begin_transaction(&TransactionRecord {
                tx_id: "TX-REC-1".to_string(),
                pipeline: "recovery".to_string(),
                source_channel: "http-in".to_string(),
                message_type: "pacs.008".to_string(),
                raw_message: "<Document/>".to_string(),
                state: "PREPARING".to_string(),
                received_at: SystemTime::now(),
                completed_at: None,
                key_fields: HashMap::new(),
            })
            .await
            .expect("seed tx should succeed");

        let report = app
            .recover_incomplete_transactions(10)
            .await
            .expect("recovery should run");
        assert_eq!(report.attempted, 1);
        assert_eq!(report.recovered, 1);
        assert_eq!(report.failed, 0);

        let updated = store
            .find_by_id("TX-REC-1")
            .await
            .expect("lookup should succeed")
            .expect("record should exist");
        assert_eq!(updated.state, "COMMITTED");
    }

    const RELOAD_CONFIG_BASE: &str = r#"
[runtime]
name = "test-runtime"
instance_id = "local-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:8080"

[[pipeline]]
name = "reloadable"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "rate-limiter", config = { rate_per_second = 10, burst = 20, scope = "global" } },
  { name = "message-logger", config = { tag = "v1" } },
]
"#;

    const RELOAD_CONFIG_UPDATED: &str = r#"
[runtime]
name = "test-runtime"
instance_id = "local-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:8080"

[[pipeline]]
name = "reloadable"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "rate-limiter", config = { rate_per_second = 100, burst = 200, scope = "source_channel" } },
  { name = "message-logger", config = { tag = "v2" } },
]
"#;

    const RELOAD_CONFIG_TOPOLOGY_CHANGE: &str = r#"
[runtime]
name = "test-runtime"
instance_id = "local-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:8080"

[[pipeline]]
name = "reloadable"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "message-logger", config = { tag = "v2" } },
]
"#;

    #[tokio::test]
    async fn reloads_participant_configs_when_topology_is_unchanged() {
        let base = RuntimeConfig::parse(RELOAD_CONFIG_BASE).expect("base config should parse");
        let app = RuntimeApp::from_config(&base)
            .await
            .expect("app should build");
        let updated =
            RuntimeConfig::parse(RELOAD_CONFIG_UPDATED).expect("updated config should parse");

        let report = app
            .reload_participant_configs(&updated)
            .await
            .expect("reload should succeed");
        assert_eq!(report.pipelines_reloaded, 1);
        assert_eq!(report.participants_reloaded, 2);
    }

    #[tokio::test]
    async fn rejects_reload_when_participant_topology_changes() {
        let base = RuntimeConfig::parse(RELOAD_CONFIG_BASE).expect("base config should parse");
        let app = RuntimeApp::from_config(&base)
            .await
            .expect("app should build");
        let changed = RuntimeConfig::parse(RELOAD_CONFIG_TOPOLOGY_CHANGE)
            .expect("changed config should parse");

        let error = app
            .reload_participant_configs(&changed)
            .await
            .expect_err("reload should fail");
        assert!(
            error
                .to_string()
                .contains("participant order/topology changed"),
            "unexpected error: {error}"
        );
    }
}
