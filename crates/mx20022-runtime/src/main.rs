mod app;
mod application;
mod domain;
mod engine;

use std::env;
use std::hash::{Hash, Hasher};
use std::process;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use app::RuntimeApp;
use async_trait::async_trait;
use mx20022_admin::auth::{AuthConfig as AdminAuthConfig, AuthMode as AdminAuthMode};
use mx20022_admin::controller::{AdminController, AdminControllerError};
use mx20022_admin::grpc;
use mx20022_admin::host;
use mx20022_admin::service::{
    ReloadStatus, RuntimeReloader, RuntimeStatusSnapshot, StoreBackedAdminController,
};
use mx20022_config::RuntimeConfig;
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
        pipelines = app.pipeline_count(),
        channels = app.channel_names().len(),
        store_backend = %app.store_backend(),
        "runtime configuration loaded"
    );

    tracing::debug!(pipelines = ?app.pipeline_names(), "pipeline names loaded");

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

    if cli.serve_admin && cli.serve_admin_grpc && cli.run_pipelines {
        let controller =
            build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status));
        tracing::info!(bind = %admin_bind, grpc_bind = %admin_grpc_bind, "starting admin http+grpc hosts and pipeline engine");

        tokio::select! {
            res = engine::run_pipelines(Arc::clone(&app), config.clone()) => {
                res.map_err(RuntimeBootstrapError::Engine)?;
            }
            res = host::serve(&admin_bind, Arc::clone(&controller), admin_auth.clone()) => {
                res.map_err(RuntimeBootstrapError::AdminHost)?;
            }
            res = grpc::serve(&admin_grpc_bind, controller, admin_auth.clone()) => {
                res.map_err(RuntimeBootstrapError::AdminGrpcHost)?;
            }
        }
    } else if cli.serve_admin && cli.run_pipelines {
        let controller =
            build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status));
        tracing::info!(bind = %admin_bind, "starting admin host and pipeline engine");

        tokio::select! {
            res = engine::run_pipelines(Arc::clone(&app), config.clone()) => {
                res.map_err(RuntimeBootstrapError::Engine)?;
            }
            res = host::serve(&admin_bind, controller, admin_auth.clone()) => {
                res.map_err(RuntimeBootstrapError::AdminHost)?;
            }
        }
    } else if cli.serve_admin_grpc && cli.run_pipelines {
        let controller =
            build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status));
        tracing::info!(grpc_bind = %admin_grpc_bind, "starting admin grpc host and pipeline engine");

        tokio::select! {
            res = engine::run_pipelines(Arc::clone(&app), config.clone()) => {
                res.map_err(RuntimeBootstrapError::Engine)?;
            }
            res = grpc::serve(&admin_grpc_bind, controller, admin_auth.clone()) => {
                res.map_err(RuntimeBootstrapError::AdminGrpcHost)?;
            }
        }
    } else if cli.run_pipelines {
        tracing::info!("starting pipeline engine");
        engine::run_pipelines(Arc::clone(&app), config)
            .await
            .map_err(RuntimeBootstrapError::Engine)?;
    } else if cli.serve_admin && cli.serve_admin_grpc {
        let controller =
            build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status));
        tracing::info!(bind = %admin_bind, grpc_bind = %admin_grpc_bind, "starting admin http+grpc hosts");

        tokio::select! {
            res = host::serve(&admin_bind, Arc::clone(&controller), admin_auth.clone()) => {
                res.map_err(RuntimeBootstrapError::AdminHost)?;
            }
            res = grpc::serve(&admin_grpc_bind, controller, admin_auth.clone()) => {
                res.map_err(RuntimeBootstrapError::AdminGrpcHost)?;
            }
        }
    } else if cli.serve_admin {
        let controller =
            build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status));
        tracing::info!(bind = %admin_bind, "starting admin host");
        host::serve(&admin_bind, controller, admin_auth.clone())
            .await
            .map_err(RuntimeBootstrapError::AdminHost)?;
    } else if cli.serve_admin_grpc {
        let controller =
            build_admin_controller(&app, cli.config_path.clone(), Arc::clone(&reload_status));
        tracing::info!(grpc_bind = %admin_grpc_bind, "starting admin grpc host");
        grpc::serve(&admin_grpc_bind, controller, admin_auth)
            .await
            .map_err(RuntimeBootstrapError::AdminGrpcHost)?;
    } else {
        tracing::info!("mxruntime initialized with no active services (--no-pipelines)");
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
                    let mut status = reload_status
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
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
                    let mut status = reload_status
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    status.last_result = Some(format!("error:{error}"));
                    status.last_reloaded_at = Some(SystemTime::now());
                    tracing::warn!(error = %error, "participant reload watcher rejected config update");
                }
            }
        }
    }))
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn build_admin_auth(config: &RuntimeConfig) -> AdminAuthConfig {
    let mode = match config.runtime.admin_auth.mode.as_str() {
        "disabled" => AdminAuthMode::Disabled,
        "jwt_hs256" => AdminAuthMode::JwtHs256,
        _ => AdminAuthMode::LegacyBearer,
    };

    AdminAuthConfig {
        mode,
        jwt_hs256_secret: config.runtime.admin_auth.jwt_hs256_secret.clone(),
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

fn build_admin_controller(
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
                pipelines: app.pipeline_names(),
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
        let raw = std::fs::read_to_string(&self.config_path).map_err(|error| {
            mx20022_metrics::record_runtime_config_reload("error");
            mx20022_metrics::record_runtime_config_reload_error("read");
            AdminControllerError::Internal(format!("reload failed to read config: {error}"))
        })?;
        let version_hash = hash_bytes(raw.as_bytes());
        let config = RuntimeConfig::parse(&raw).map_err(|error| {
            mx20022_metrics::record_runtime_config_reload("error");
            mx20022_metrics::record_runtime_config_reload_error("parse");
            AdminControllerError::Internal(format!("reload failed to parse config: {error}"))
        })?;

        let report = self
            .app
            .reload_participant_configs(&config)
            .await
            .map_err(|error| {
                mx20022_metrics::record_runtime_config_reload("error");
                mx20022_metrics::record_runtime_config_reload_error("apply");
                let mut status = self
                    .reload_status
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                status.last_result = Some(format!("error:{error}"));
                status.last_reloaded_at = Some(SystemTime::now());
                AdminControllerError::Internal(format!("reload failed: {error}"))
            })?;

        mx20022_metrics::record_runtime_config_reload("success");
        let mut status = self
            .reload_status
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
    Build(#[from] app::RuntimeBuildError),
    #[error(transparent)]
    AdminHost(#[from] mx20022_admin::host::HostError),
    #[error(transparent)]
    AdminGrpcHost(#[from] mx20022_admin::grpc::GrpcHostError),
    #[error(transparent)]
    Engine(#[from] engine::EngineError),
}
