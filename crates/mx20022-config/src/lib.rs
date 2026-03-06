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

        let auth = &self.runtime.admin_auth;
        match auth.mode.as_str() {
            "disabled" | "legacy_bearer" => {}
            "jwt_hs256" => {
                if auth
                    .jwt_hs256_secret
                    .as_ref()
                    .map(|value| value.trim().is_empty())
                    .unwrap_or(true)
                {
                    return Err(ConfigError::Validation(
                        "runtime.admin_auth.jwt_hs256_secret must be set when mode=jwt_hs256"
                            .to_string(),
                    ));
                }
            }
            other => {
                return Err(ConfigError::Validation(format!(
                    "runtime.admin_auth.mode `{other}` is invalid (expected disabled|legacy_bearer|jwt_hs256)"
                )));
            }
        }

        if auth.require_mtls_subject && auth.mtls_subject_header.trim().is_empty() {
            return Err(ConfigError::Validation(
                "runtime.admin_auth.mtls_subject_header must not be empty when require_mtls_subject=true".to_string(),
            ));
        }

        if let Some(limit) = self.runtime.recovery_startup_limit {
            if limit == 0 {
                return Err(ConfigError::Validation(
                    "runtime.recovery_startup_limit must be greater than 0".to_string(),
                ));
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
    #[serde(default)]
    pub participant_reload_poll_ms: Option<u64>,
    #[serde(default = "default_recover_incomplete_on_startup")]
    pub recover_incomplete_on_startup: bool,
    #[serde(default)]
    pub recovery_startup_limit: Option<usize>,
    #[serde(default)]
    pub admin_auth: AdminAuthSection,
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_recover_incomplete_on_startup() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdminAuthSection {
    #[serde(default = "default_admin_auth_mode")]
    pub mode: String,
    #[serde(default)]
    pub jwt_hs256_secret: Option<String>,
    #[serde(default)]
    pub jwt_issuer: Option<String>,
    #[serde(default)]
    pub jwt_audience: Option<String>,
    #[serde(default = "default_ready_roles")]
    pub ready_roles: Vec<String>,
    #[serde(default = "default_status_roles")]
    pub status_roles: Vec<String>,
    #[serde(default = "default_tx_roles")]
    pub tx_roles: Vec<String>,
    #[serde(default = "default_reload_roles")]
    pub reload_roles: Vec<String>,
    #[serde(default)]
    pub require_mtls_subject: bool,
    #[serde(default = "default_mtls_subject_header")]
    pub mtls_subject_header: String,
    #[serde(default)]
    pub mtls_allowed_subjects: Vec<String>,
}

impl Default for AdminAuthSection {
    fn default() -> Self {
        Self {
            mode: default_admin_auth_mode(),
            jwt_hs256_secret: None,
            jwt_issuer: None,
            jwt_audience: None,
            ready_roles: default_ready_roles(),
            status_roles: default_status_roles(),
            tx_roles: default_tx_roles(),
            reload_roles: default_reload_roles(),
            require_mtls_subject: false,
            mtls_subject_header: default_mtls_subject_header(),
            mtls_allowed_subjects: Vec::new(),
        }
    }
}

fn default_admin_auth_mode() -> String {
    "legacy_bearer".to_string()
}

fn default_ready_roles() -> Vec<String> {
    vec!["admin.read".to_string(), "admin".to_string()]
}

fn default_status_roles() -> Vec<String> {
    vec!["admin.read".to_string(), "admin".to_string()]
}

fn default_tx_roles() -> Vec<String> {
    vec!["admin.tx.read".to_string(), "admin".to_string()]
}

fn default_reload_roles() -> Vec<String> {
    vec!["admin.write".to_string(), "admin".to_string()]
}

fn default_mtls_subject_header() -> String {
    "x-client-cert-subject".to_string()
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

#[cfg(test)]
mod tests {
    use super::RuntimeConfig;

    const BASE_CONFIG: &str = r#"
[runtime]
name = "runtime"
instance_id = "local"

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
participants = [{ name = "message-logger" }]
"#;

    #[test]
    fn rejects_invalid_admin_auth_mode() {
        let config = format!("{BASE_CONFIG}\n[runtime.admin_auth]\nmode = \"invalid\"\n");
        let result = RuntimeConfig::parse(&config);
        assert!(result.is_err());
    }

    #[test]
    fn requires_jwt_secret_when_jwt_mode_enabled() {
        let config = format!("{BASE_CONFIG}\n[runtime.admin_auth]\nmode = \"jwt_hs256\"\n");
        let result = RuntimeConfig::parse(&config);
        assert!(result.is_err());
    }

    #[test]
    fn accepts_jwt_mode_with_secret() {
        let config = format!(
            "{BASE_CONFIG}\n[runtime.admin_auth]\nmode = \"jwt_hs256\"\njwt_hs256_secret = \"secret\"\n"
        );
        let result = RuntimeConfig::parse(&config);
        assert!(result.is_ok());
    }
}
