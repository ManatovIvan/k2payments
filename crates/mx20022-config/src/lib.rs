use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    pub runtime: RuntimeSection,
    pub store: StoreSection,
    #[serde(default)]
    pub channels: HashMap<String, ChannelSection>,
    #[serde(default, rename = "pipeline")]
    pub pipelines: Vec<PipelineSection>,
}

impl RuntimeConfig {
    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let cfg: RuntimeConfig = toml::from_str(content)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path).map_err(ConfigError::Io)?;
        Self::parse(&content)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.runtime.name.trim().is_empty() {
            return Err(ConfigError::Validation(
                "runtime.name must not be empty".to_string(),
            ));
        }

        if self.runtime.instance_id.trim().is_empty() {
            return Err(ConfigError::Validation(
                "runtime.instance_id must not be empty".to_string(),
            ));
        }

        if self.pipelines.is_empty() {
            return Err(ConfigError::Validation(
                "at least one [[pipeline]] is required".to_string(),
            ));
        }

        for pipeline in &self.pipelines {
            if pipeline.name.trim().is_empty() {
                return Err(ConfigError::Validation(
                    "pipeline.name must not be empty".to_string(),
                ));
            }

            if !self.channels.contains_key(&pipeline.channel_in) {
                return Err(ConfigError::Validation(format!(
                    "pipeline `{}` references missing channel_in `{}`",
                    pipeline.name, pipeline.channel_in
                )));
            }

            if let Some(channel_out) = &pipeline.channel_out {
                if !self.channels.contains_key(channel_out) {
                    return Err(ConfigError::Validation(format!(
                        "pipeline `{}` references missing channel_out `{}`",
                        pipeline.name, channel_out
                    )));
                }
            }

            if pipeline.participants.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "pipeline `{}` must include at least one participant",
                    pipeline.name
                )));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeSection {
    pub name: String,
    pub instance_id: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub metrics_bind: Option<String>,
    #[serde(default)]
    pub admin_bind: Option<String>,
    #[serde(default)]
    pub admin_grpc_bind: Option<String>,
    #[serde(default)]
    pub correlation_scan_interval_ms: Option<u64>,
}

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct StoreSection {
    pub backend: String,
    pub url: String,
    #[serde(default)]
    pub pool_size: Option<u32>,
    #[serde(default)]
    pub retention_days: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelSection {
    #[serde(rename = "type")]
    pub channel_type: String,
    pub mode: String,
    #[serde(flatten)]
    pub extra: HashMap<String, toml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineSection {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub channel_in: String,
    #[serde(default)]
    pub channel_out: Option<String>,
    #[serde(default)]
    pub message_types: Vec<String>,
    #[serde(default)]
    pub max_concurrent: Option<usize>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub participants: Vec<ParticipantConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParticipantConfig {
    pub name: String,
    #[serde(default)]
    pub config: HashMap<String, toml::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("io error: {0}")]
    Io(std::io::Error),
    #[error("toml parse error: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("config validation error: {0}")]
    Validation(String),
}
