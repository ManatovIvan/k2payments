use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use mx20022_config::{ParticipantConfig, RuntimeConfig};
use mx20022_correlation::{CorrelationEngine, CorrelationLookupKey};
use mx20022_participants::business_rule_validator::BusinessRuleValidator;
use mx20022_participants::message_logger::MessageLogger;
use mx20022_participants::schema_validator::SchemaValidator;
use mx20022_participants::status_response_builder::StatusResponseBuilder;
use mx20022_runtime_core::context::{Context, ContextMeta};
use mx20022_runtime_core::participant::Participant;
use mx20022_runtime_core::transaction_manager::{TransactionManager, TransactionReport};
use mx20022_store::Store;
use mx20022_store_postgres::PostgresStore;
use mx20022_store_rocksdb::RocksDbStore;
use mx20022_store_sqlite::SqliteStore;

use crate::application::TransactionUseCase;
use crate::domain::{DomainError, TransactionRequest};

pub struct RuntimeApp {
    pipelines: HashMap<String, PipelineRuntime>,
    store: Arc<dyn Store>,
    correlation: Arc<CorrelationEngine>,
    runtime_name: String,
    instance_id: String,
    channel_names: Vec<String>,
    store_backend: String,
}

struct PipelineRuntime {
    message_types: Vec<String>,
    manager: TransactionManager,
}

impl RuntimeApp {
    pub async fn from_config(config: &RuntimeConfig) -> Result<Self, RuntimeBuildError> {
        let store: Arc<dyn Store> = match config.store.backend.as_str() {
            "sqlite" => Arc::new(SqliteStore::new(config.store.url.clone())),
            "postgres" => Arc::new(PostgresStore::connect(config.store.url.clone()).await?),
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
            let participants = build_participants(&pipeline_cfg.participants)?;
            let runtime = PipelineRuntime {
                message_types: pipeline_cfg.message_types.clone(),
                manager: TransactionManager::new(participants),
            };
            pipelines.insert(pipeline_cfg.name.clone(), runtime);
        }

        Ok(Self {
            pipelines,
            store,
            correlation,
            runtime_name: config.runtime.name.clone(),
            instance_id: config.runtime.instance_id.clone(),
            channel_names: config.channels.keys().cloned().collect(),
            store_backend: config.store.backend.clone(),
        })
    }

    pub fn pipeline_count(&self) -> usize {
        self.pipelines.len()
    }

    pub fn pipeline_names(&self) -> Vec<String> {
        self.pipelines.keys().cloned().collect()
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

    pub fn accepts_message_type(&self, pipeline: &str, message_type: &str) -> bool {
        let Some(runtime) = self.pipelines.get(pipeline) else {
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
            .get(pipeline)
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

        if !self.accepts_message_type(pipeline, &request.message_type) {
            return Err(RuntimeBuildError::MessageTypeNotAccepted {
                pipeline: pipeline.to_string(),
                message_type: request.message_type.clone(),
            });
        }

        let now = SystemTime::now();
        mx20022_metrics::set_active_transactions(pipeline, 1);
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

        let report = runtime
            .manager
            .process(&mut ctx)
            .await
            .map_err(RuntimeBuildError::Processing)?;

        for entry in ctx.audit_log() {
            self.store
                .append_context_entry(
                    &request.tx_id,
                    mx20022_store::ContextEntry {
                        tx_id: request.tx_id.clone(),
                        key: entry.key.clone(),
                        writer: entry.writer.clone(),
                        written_at: entry.written_at,
                    },
                )
                .await
                .map_err(RuntimeBuildError::Store)?;
        }

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
        mx20022_metrics::set_active_transactions(pipeline, 0);

        Ok(report)
    }
}

fn build_participants(
    configs: &[ParticipantConfig],
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
    #[error("message type `{message_type}` not accepted by pipeline `{pipeline}`")]
    MessageTypeNotAccepted {
        pipeline: String,
        message_type: String,
    },
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
    use mx20022_config::RuntimeConfig;
    use mx20022_runtime_core::transaction_manager::Outcome;

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

        assert_eq!(app.pipeline_count(), 1);
        assert!(app.accepts_message_type("demo", "pacs.008"));
        assert!(!app.accepts_message_type("demo", "pacs.002"));
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
}
