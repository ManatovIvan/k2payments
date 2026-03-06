mod app;
mod application;
mod domain;
mod engine;

use std::env;
use std::process;
use std::sync::Arc;

use app::RuntimeApp;
use mx20022_admin::controller::AdminController;
use mx20022_admin::grpc;
use mx20022_admin::host;
use mx20022_admin::service::{RuntimeStatusSnapshot, StoreBackedAdminController};
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

    tracing::info!(
        runtime = %app.runtime_name(),
        instance_id = %app.instance_id(),
        pipelines = app.pipeline_count(),
        channels = app.channel_names().len(),
        store_backend = %app.store_backend(),
        "runtime configuration loaded"
    );

    tracing::debug!(pipelines = ?app.pipeline_names(), "pipeline names loaded");

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

    if cli.serve_admin && cli.serve_admin_grpc && cli.run_pipelines {
        let controller = build_admin_controller(&app);
        tracing::info!(bind = %admin_bind, grpc_bind = %admin_grpc_bind, "starting admin http+grpc hosts and pipeline engine");

        tokio::select! {
            res = engine::run_pipelines(Arc::clone(&app), config.clone()) => {
                res.map_err(RuntimeBootstrapError::Engine)?;
            }
            res = host::serve(&admin_bind, Arc::clone(&controller)) => {
                res.map_err(RuntimeBootstrapError::AdminHost)?;
            }
            res = grpc::serve(&admin_grpc_bind, controller) => {
                res.map_err(RuntimeBootstrapError::AdminGrpcHost)?;
            }
        }
    } else if cli.serve_admin && cli.run_pipelines {
        let controller = build_admin_controller(&app);
        tracing::info!(bind = %admin_bind, "starting admin host and pipeline engine");

        tokio::select! {
            res = engine::run_pipelines(Arc::clone(&app), config.clone()) => {
                res.map_err(RuntimeBootstrapError::Engine)?;
            }
            res = host::serve(&admin_bind, controller) => {
                res.map_err(RuntimeBootstrapError::AdminHost)?;
            }
        }
    } else if cli.serve_admin_grpc && cli.run_pipelines {
        let controller = build_admin_controller(&app);
        tracing::info!(grpc_bind = %admin_grpc_bind, "starting admin grpc host and pipeline engine");

        tokio::select! {
            res = engine::run_pipelines(Arc::clone(&app), config.clone()) => {
                res.map_err(RuntimeBootstrapError::Engine)?;
            }
            res = grpc::serve(&admin_grpc_bind, controller) => {
                res.map_err(RuntimeBootstrapError::AdminGrpcHost)?;
            }
        }
    } else if cli.run_pipelines {
        tracing::info!("starting pipeline engine");
        engine::run_pipelines(Arc::clone(&app), config)
            .await
            .map_err(RuntimeBootstrapError::Engine)?;
    } else if cli.serve_admin && cli.serve_admin_grpc {
        let controller = build_admin_controller(&app);
        tracing::info!(bind = %admin_bind, grpc_bind = %admin_grpc_bind, "starting admin http+grpc hosts");

        tokio::select! {
            res = host::serve(&admin_bind, Arc::clone(&controller)) => {
                res.map_err(RuntimeBootstrapError::AdminHost)?;
            }
            res = grpc::serve(&admin_grpc_bind, controller) => {
                res.map_err(RuntimeBootstrapError::AdminGrpcHost)?;
            }
        }
    } else if cli.serve_admin {
        let controller = build_admin_controller(&app);
        tracing::info!(bind = %admin_bind, "starting admin host");
        host::serve(&admin_bind, controller)
            .await
            .map_err(RuntimeBootstrapError::AdminHost)?;
    } else if cli.serve_admin_grpc {
        let controller = build_admin_controller(&app);
        tracing::info!(grpc_bind = %admin_grpc_bind, "starting admin grpc host");
        grpc::serve(&admin_grpc_bind, controller)
            .await
            .map_err(RuntimeBootstrapError::AdminGrpcHost)?;
    } else {
        tracing::info!("mxruntime initialized with no active services (--no-pipelines)");
    }

    Ok(())
}

fn build_admin_controller(app: &Arc<RuntimeApp>) -> Arc<dyn AdminController> {
    Arc::new(StoreBackedAdminController::new(
        app.store_handle(),
        RuntimeStatusSnapshot {
            runtime: app.runtime_name().to_string(),
            pipelines: app.pipeline_names(),
            channels: app.channel_names(),
            store: app.store_backend().to_string(),
        },
    ))
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
