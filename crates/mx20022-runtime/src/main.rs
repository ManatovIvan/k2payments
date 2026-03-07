use std::env;
use std::process;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use mx20022_admin::auth::{AuthConfig as AdminAuthConfig, AuthMode as AdminAuthMode};
use mx20022_admin::controller::{AdminController, AdminControllerError};
use mx20022_admin::grpc;
use mx20022_admin::host;
use mx20022_admin::service::{
    ReloadStatus, RuntimeReloader, RuntimeStatusSnapshot, StoreBackedAdminController,
};
use mx20022_admin::tls::TlsConfig as AdminTlsConfig;
use mx20022_config::RuntimeConfig;
use mx20022_runtime::app::RuntimeApp;
use mx20022_runtime::engine;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if let Err(error) = run().await {
        tracing::error!(error = %error, "runtime startup failed");
        process::exit(1);
    }
}

async fn run() -> Result<(), RuntimeBootstrapError> {
    let cli = parse_cli(env::args())?;
    let config = RuntimeConfig::load_from_path(&cli.config_path)?;
    let app = Arc::new(RuntimeApp::from_config(&config).await?);
    let reload_status = Arc::new(RwLock::new(ReloadStatus {
        config_version: compute_config_version(&cli.config_path)
            .unwrap_or_else(|| "unknown".to_string()),
        last_result: None,
        last_reloaded_at: None,
    }));
    let _reload_watcher = spawn_participant_reload_watcher(
        Arc::clone(&app),
        cli.config_path.clone(),
        Arc::clone(&reload_status),
        config.runtime.participant_reload_poll_ms,
    );

    tracing::info!(
        runtime = %app.runtime_name(),
        instance_id = %app.instance_id(),
        pipelines = app.pipeline_count().await,
        channels = app.channel_names().len(),
        store_backend = %app.store_backend(),
        "runtime configuration loaded"
    );

    tracing::debug!(pipelines = ?app.pipeline_names().await, "pipeline names loaded");

    if config.runtime.recover_incomplete_on_startup {
        let limit = config.runtime.recovery_startup_limit.unwrap_or(500);
        let recovery = app.recover_incomplete_transactions(limit).await?;
        tracing::info!(
            attempted = recovery.attempted,
            recovered = recovery.recovered,
            failed = recovery.failed,
            limit,
            "startup recovery run completed"
        );
    }

    let admin_bind = config
        .runtime
        .admin_bind
        .clone()
        .unwrap_or_else(|| "127.0.0.1:9090".to_string());
    let admin_grpc_bind = config
        .runtime
        .admin_grpc_bind
        .clone()
        .unwrap_or_else(|| "127.0.0.1:9091".to_string());
    let admin_auth = build_admin_auth(&config);
    let admin_tls = build_admin_tls(&config);
    let admin_cors_allowed_origins = config.runtime.admin_cors_allowed_origins.clone();
    let service_mode = (cli.run_pipelines, cli.serve_admin, cli.serve_admin_grpc);
    if matches!(admin_auth.mode, AdminAuthMode::Disabled)
        && (cli.serve_admin || cli.serve_admin_grpc)
    {
        tracing::warn!("admin auth is disabled while admin service is enabled");
    }

    match service_mode {
        (true, true, true) => {
            let controller =
                build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status))
                    .await;
            tracing::info!(bind = %admin_bind, grpc_bind = %admin_grpc_bind, "starting admin http+grpc hosts and pipeline engine");

            tokio::select! {
                res = engine::run_pipelines(Arc::clone(&app), config.clone()) => {
                    res.map_err(RuntimeBootstrapError::Engine)?;
                }
                res = host::serve_with_tls_and_cors(&admin_bind, Arc::clone(&controller), admin_auth.clone(), admin_tls.clone(), admin_cors_allowed_origins.clone()) => {
                    res.map_err(RuntimeBootstrapError::AdminHost)?;
                }
                res = grpc::serve_with_tls(&admin_grpc_bind, controller, admin_auth.clone(), admin_tls.clone()) => {
                    res.map_err(RuntimeBootstrapError::AdminGrpcHost)?;
                }
            }
        }
        (true, true, false) => {
            let controller =
                build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status))
                    .await;
            tracing::info!(bind = %admin_bind, "starting admin host and pipeline engine");

            tokio::select! {
                res = engine::run_pipelines(Arc::clone(&app), config.clone()) => {
                    res.map_err(RuntimeBootstrapError::Engine)?;
                }
                res = host::serve_with_tls_and_cors(&admin_bind, controller, admin_auth.clone(), admin_tls.clone(), admin_cors_allowed_origins.clone()) => {
                    res.map_err(RuntimeBootstrapError::AdminHost)?;
                }
            }
        }
        (true, false, true) => {
            let controller =
                build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status))
                    .await;
            tracing::info!(grpc_bind = %admin_grpc_bind, "starting admin grpc host and pipeline engine");

            tokio::select! {
                res = engine::run_pipelines(Arc::clone(&app), config.clone()) => {
                    res.map_err(RuntimeBootstrapError::Engine)?;
                }
                res = grpc::serve_with_tls(&admin_grpc_bind, controller, admin_auth.clone(), admin_tls.clone()) => {
                    res.map_err(RuntimeBootstrapError::AdminGrpcHost)?;
                }
            }
        }
        (true, false, false) => {
            tracing::info!("starting pipeline engine");
            engine::run_pipelines(Arc::clone(&app), config.clone())
                .await
                .map_err(RuntimeBootstrapError::Engine)?;
        }
        (false, true, true) => {
            let controller =
                build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status))
                    .await;
            tracing::info!(bind = %admin_bind, grpc_bind = %admin_grpc_bind, "starting admin http+grpc hosts");

            tokio::select! {
                res = host::serve_with_tls_and_cors(&admin_bind, Arc::clone(&controller), admin_auth.clone(), admin_tls.clone(), admin_cors_allowed_origins.clone()) => {
                    res.map_err(RuntimeBootstrapError::AdminHost)?;
                }
                res = grpc::serve_with_tls(&admin_grpc_bind, controller, admin_auth.clone(), admin_tls.clone()) => {
                    res.map_err(RuntimeBootstrapError::AdminGrpcHost)?;
                }
            }
        }
        (false, true, false) => {
            let controller =
                build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status))
                    .await;
            tracing::info!(bind = %admin_bind, "starting admin host");
            host::serve_with_tls_and_cors(
                &admin_bind,
                controller,
                admin_auth.clone(),
                admin_tls.clone(),
                admin_cors_allowed_origins.clone(),
            )
            .await
            .map_err(RuntimeBootstrapError::AdminHost)?;
        }
        (false, false, true) => {
            let controller =
                build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status))
                    .await;
            tracing::info!(grpc_bind = %admin_grpc_bind, "starting admin grpc host");
            grpc::serve_with_tls(&admin_grpc_bind, controller, admin_auth, admin_tls)
                .await
                .map_err(RuntimeBootstrapError::AdminGrpcHost)?;
        }
        (false, false, false) => {
            tracing::info!("mxruntime initialized with no active services (--no-pipelines)");
        }
    }

    Ok(())
}

fn spawn_participant_reload_watcher(
    app: Arc<RuntimeApp>,
    config_path: String,
    reload_status: Arc<RwLock<ReloadStatus>>,
    poll_ms: Option<u64>,
) -> Option<tokio::task::JoinHandle<()>> {
    let interval_ms = poll_ms?;
    if interval_ms == 0 {
        return None;
    }

    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
        let mut last_hash = None::<u64>;

        loop {
            ticker.tick().await;

            let bytes = match tokio::fs::read(&config_path).await {
                Ok(content) => content,
                Err(error) => {
                    mx20022_metrics::record_runtime_config_reload("error");
                    mx20022_metrics::record_runtime_config_reload_error("read");
                    tracing::warn!(path = %config_path, error = %error, "participant reload watcher failed to read config");
                    continue;
                }
            };
            let hash = hash_bytes(&bytes);
            if last_hash == Some(hash) {
                continue;
            }
            last_hash = Some(hash);

            let content = match String::from_utf8(bytes) {
                Ok(content) => content,
                Err(error) => {
                    mx20022_metrics::record_runtime_config_reload("error");
                    mx20022_metrics::record_runtime_config_reload_error("utf8");
                    tracing::warn!(path = %config_path, error = %error, "participant reload watcher read invalid UTF-8 config");
                    continue;
                }
            };

            let config = match RuntimeConfig::parse(&content) {
                Ok(config) => config,
                Err(error) => {
                    mx20022_metrics::record_runtime_config_reload("error");
                    mx20022_metrics::record_runtime_config_reload_error("parse");
                    tracing::warn!(path = %config_path, error = %error, "participant reload watcher failed to parse config");
                    continue;
                }
            };

            match app.reload_participant_configs(&config).await {
                Ok(report) => {
                    mx20022_metrics::record_runtime_config_reload("success");
                    let mut status = reload_status.write().await;
                    status.config_version = format!("h{:016x}", hash);
                    status.last_result = Some("success".to_string());
                    status.last_reloaded_at = Some(SystemTime::now());
                    tracing::info!(
                        pipelines = report.pipelines_reloaded,
                        participants = report.participants_reloaded,
                        "participant config watcher applied reload"
                    );
                }
                Err(error) => {
                    mx20022_metrics::record_runtime_config_reload("error");
                    mx20022_metrics::record_runtime_config_reload_error("apply");
                    let mut status = reload_status.write().await;
                    status.last_result = Some(format!("error:{error}"));
                    status.last_reloaded_at = Some(SystemTime::now());
                    tracing::warn!(error = %error, "participant reload watcher rejected config update");
                }
            }
        }
    }))
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

fn build_admin_tls(config: &RuntimeConfig) -> Option<AdminTlsConfig> {
    admin_tls_pair(config).map(|(cert_path, key_path)| AdminTlsConfig {
        cert_path,
        key_path,
    })
}

fn admin_tls_pair(config: &RuntimeConfig) -> Option<(String, String)> {
    match (
        &config.runtime.admin_tls_cert,
        &config.runtime.admin_tls_key,
    ) {
        (Some(cert), Some(key)) => Some((cert.clone(), key.clone())),
        (None, None) => None,
        _ => {
            tracing::error!("admin_tls_cert and admin_tls_key must both be set or both be absent");
            None
        }
    }
}

fn build_admin_auth(config: &RuntimeConfig) -> AdminAuthConfig {
    let mode = match config.runtime.admin_auth.mode.as_str() {
        "disabled" => AdminAuthMode::Disabled,
        "legacy_bearer" => AdminAuthMode::LegacyBearer,
        "jwt_hs256" => AdminAuthMode::JwtHs256,
        _ => AdminAuthMode::Disabled,
    };

    AdminAuthConfig {
        mode,
        jwt_hs256_secret: config.runtime.admin_auth.jwt_hs256_secret.clone(),
        legacy_bearer_token: config.runtime.admin_auth.legacy_bearer_token.clone(),
        legacy_readonly_token: config.runtime.admin_auth.legacy_readonly_token.clone(),
        jwt_issuer: config.runtime.admin_auth.jwt_issuer.clone(),
        jwt_audience: config.runtime.admin_auth.jwt_audience.clone(),
        ready_roles: config.runtime.admin_auth.ready_roles.clone(),
        status_roles: config.runtime.admin_auth.status_roles.clone(),
        tx_roles: config.runtime.admin_auth.tx_roles.clone(),
        reload_roles: config.runtime.admin_auth.reload_roles.clone(),
        require_mtls_subject: config.runtime.admin_auth.require_mtls_subject,
        mtls_subject_header: config.runtime.admin_auth.mtls_subject_header.clone(),
        mtls_allowed_subjects: config.runtime.admin_auth.mtls_allowed_subjects.clone(),
    }
}

async fn build_admin_controller(
    app: &Arc<RuntimeApp>,
    config_path: String,
    reload_status: Arc<RwLock<ReloadStatus>>,
) -> Arc<dyn AdminController> {
    let reloader: Arc<dyn RuntimeReloader> = Arc::new(AppConfigReloader {
        app: Arc::clone(app),
        config_path,
        reload_status: Arc::clone(&reload_status),
    });
    Arc::new(
        StoreBackedAdminController::new(
            app.store_handle(),
            RuntimeStatusSnapshot {
                runtime: app.runtime_name().to_string(),
                pipelines: app.pipeline_names().await,
                channels: app.channel_names(),
                store: app.store_backend().to_string(),
                started_at: SystemTime::now(),
                reload_status,
            },
        )
        .with_reloader(reloader),
    )
}

struct AppConfigReloader {
    app: Arc<RuntimeApp>,
    config_path: String,
    reload_status: Arc<RwLock<ReloadStatus>>,
}

#[async_trait]
impl RuntimeReloader for AppConfigReloader {
    async fn reload(&self) -> Result<String, AdminControllerError> {
        let bytes = tokio::fs::read(&self.config_path).await.map_err(|error| {
            mx20022_metrics::record_runtime_config_reload("error");
            mx20022_metrics::record_runtime_config_reload_error("read");
            AdminControllerError::Internal(format!("reload failed to read config: {error}"))
        })?;
        let raw = String::from_utf8(bytes).map_err(|error| {
            mx20022_metrics::record_runtime_config_reload("error");
            mx20022_metrics::record_runtime_config_reload_error("utf8");
            AdminControllerError::Internal(format!("reload failed to decode UTF-8 config: {error}"))
        })?;
        let version_hash = hash_bytes(raw.as_bytes());
        let config = RuntimeConfig::parse(&raw).map_err(|error| {
            mx20022_metrics::record_runtime_config_reload("error");
            mx20022_metrics::record_runtime_config_reload_error("parse");
            AdminControllerError::Internal(format!("reload failed to parse config: {error}"))
        })?;

        let report = match self.app.reload_participant_configs(&config).await {
            Ok(report) => report,
            Err(error) => {
                mx20022_metrics::record_runtime_config_reload("error");
                mx20022_metrics::record_runtime_config_reload_error("apply");
                let mut status = self.reload_status.write().await;
                status.last_result = Some(format!("error:{error}"));
                status.last_reloaded_at = Some(SystemTime::now());
                return Err(AdminControllerError::Internal(format!(
                    "reload failed: {error}"
                )));
            }
        };

        mx20022_metrics::record_runtime_config_reload("success");
        let mut status = self.reload_status.write().await;
        status.config_version = format!("h{:016x}", version_hash);
        status.last_result = Some("success".to_string());
        status.last_reloaded_at = Some(SystemTime::now());
        Ok(format!(
            "reloaded participant config for {} pipelines and {} participants",
            report.pipelines_reloaded, report.participants_reloaded
        ))
    }
}

fn compute_config_version(path: &str) -> Option<String> {
    std::fs::read(path)
        .ok()
        .map(|bytes| format!("h{:016x}", hash_bytes(&bytes)))
}

struct CliArgs {
    config_path: String,
    serve_admin: bool,
    serve_admin_grpc: bool,
    run_pipelines: bool,
}

fn parse_cli<I>(args: I) -> Result<CliArgs, RuntimeBootstrapError>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter().skip(1);
    let mut config_path = None;
    let mut serve_admin = false;
    let mut serve_admin_grpc = false;
    let mut run_pipelines = true;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(
                    args.next()
                        .ok_or(RuntimeBootstrapError::MissingConfigValue)?,
                );
            }
            "--serve-admin" => {
                serve_admin = true;
            }
            "--serve-admin-grpc" => {
                serve_admin_grpc = true;
            }
            "--no-pipelines" => {
                run_pipelines = false;
            }
            _ => {}
        }
    }

    let config_path = config_path.ok_or(RuntimeBootstrapError::MissingConfigFlag)?;

    Ok(CliArgs {
        config_path,
        serve_admin,
        serve_admin_grpc,
        run_pipelines,
    })
}

#[derive(Debug, thiserror::Error)]
enum RuntimeBootstrapError {
    #[error("missing required --config <path> argument")]
    MissingConfigFlag,
    #[error("--config was provided without a path")]
    MissingConfigValue,
    #[error(transparent)]
    Config(#[from] mx20022_config::ConfigError),
    #[error(transparent)]
    Build(#[from] mx20022_runtime::app::RuntimeBuildError),
    #[error(transparent)]
    AdminHost(#[from] mx20022_admin::host::HostError),
    #[error(transparent)]
    AdminGrpcHost(#[from] mx20022_admin::grpc::GrpcHostError),
    #[error(transparent)]
    Engine(#[from] engine::EngineError),
}
