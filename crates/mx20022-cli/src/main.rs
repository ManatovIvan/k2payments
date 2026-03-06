use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mx20022_admin::grpc::proto::admin_service_client::AdminServiceClient;
use mx20022_store::Store;
use mx20022_store_postgres::PostgresStore;
use mx20022_store_rocksdb::RocksDbStore;
use mx20022_store_sqlite::SqliteStore;

#[tokio::main]
async fn main() {
    if let Err(error) = run(env::args().collect()).await {
        eprintln!("mxctl error: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Vec<String>) -> Result<(), CliError> {
    if args.len() < 2 {
        return Err(CliError::Usage(usage()));
    }

    match args[1].as_str() {
        "status" => status_command(&args).await,
        "config" => {
            if args.get(2).map(String::as_str) != Some("validate") {
                return Err(CliError::Usage(
                    "usage: mxctl config validate <path>".to_string(),
                ));
            }

            let path = args.get(3).ok_or_else(|| {
                CliError::Usage("usage: mxctl config validate <path>".to_string())
            })?;

            let config = mx20022_config::RuntimeConfig::load_from_path(path)?;
            println!(
                "valid config: runtime={} instance={} pipelines={} channels={}",
                config.runtime.name,
                config.runtime.instance_id,
                config.pipelines.len(),
                config.channels.len()
            );
            Ok(())
        }
        "db" => db_command(&args).await,
        "tx" => tx_command(&args).await,
        _ => Err(CliError::Usage(usage())),
    }
}

async fn db_command(args: &[String]) -> Result<(), CliError> {
    let action = args.get(2).ok_or_else(|| {
        CliError::Usage("usage: mxctl db <migrate|rollback|seed> <config>".to_string())
    })?;
    let config_path = args.get(3).ok_or_else(|| {
        CliError::Usage("usage: mxctl db <migrate|rollback|seed> <config>".to_string())
    })?;

    let config = mx20022_config::RuntimeConfig::load_from_path(config_path)?;
    match config.store.backend.as_str() {
        "sqlite" => {
            let store = SqliteStore::new(config.store.url.clone());
            match action.as_str() {
                "migrate" => {
                    store.apply_migrations().await?;
                    println!("migrations applied to {}", store.database_url());
                    Ok(())
                }
                "rollback" => {
                    store.rollback_migrations().await?;
                    println!("migrations rolled back for {}", store.database_url());
                    Ok(())
                }
                "seed" => {
                    store.apply_dev_seed().await?;
                    println!("dev seed applied to {}", store.database_url());
                    Ok(())
                }
                _ => Err(CliError::Usage(
                    "usage: mxctl db <migrate|rollback|seed> <config>".to_string(),
                )),
            }
        }
        "postgres" => {
            let store = PostgresStore::connect(config.store.url.clone()).await?;
            match action.as_str() {
                "migrate" => {
                    store.apply_migrations().await?;
                    println!("migrations applied to {}", store.database_url());
                    Ok(())
                }
                "rollback" => {
                    store.rollback_migrations().await?;
                    println!("migrations rolled back for {}", store.database_url());
                    Ok(())
                }
                "seed" => {
                    store.apply_dev_seed().await?;
                    println!("dev seed applied to {}", store.database_url());
                    Ok(())
                }
                _ => Err(CliError::Usage(
                    "usage: mxctl db <migrate|rollback|seed> <config>".to_string(),
                )),
            }
        }
        "rocksdb" => {
            let store = RocksDbStore::open(config.store.url.clone())?;
            match action.as_str() {
                "migrate" | "rollback" | "seed" => {
                    println!(
                        "rocksdb backend does not use SQL migrations; store path={}",
                        store.path()
                    );
                    Ok(())
                }
                _ => Err(CliError::Usage(
                    "usage: mxctl db <migrate|rollback|seed> <config>".to_string(),
                )),
            }
        }
        other => Err(CliError::UnsupportedBackend(other.to_string())),
    }
}

async fn status_command(args: &[String]) -> Result<(), CliError> {
    if let Some(grpc_endpoint) = option_value(args, "--admin-grpc") {
        let mut client = AdminServiceClient::connect(grpc_endpoint.clone())
            .await
            .map_err(CliError::GrpcTransport)?;
        let payload = client
            .get_status(())
            .await
            .map_err(CliError::GrpcStatus)?
            .into_inner();
        println!(
            "runtime={} store={} pipelines={} channels={}",
            payload.runtime,
            payload.store,
            payload.pipelines.len(),
            payload.channels.len(),
        );
        return Ok(());
    }

    let admin_url =
        option_value(args, "--admin").unwrap_or_else(|| "http://127.0.0.1:9090".to_string());
    let token = option_value(args, "--token")
        .unwrap_or_else(|| env::var("MXCTL_TOKEN").unwrap_or_else(|_| "readonly".to_string()));

    let url = format!("{}/status", admin_url.trim_end_matches('/'));
    let response = reqwest::Client::new()
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !response.status().is_success() {
        return Err(CliError::AdminHttp {
            url,
            status: response.status().as_u16(),
            body: response.text().await.unwrap_or_else(|_| String::new()),
        });
    }

    let payload: serde_json::Value = response.json().await.map_err(CliError::Http)?;
    println!(
        "runtime={} store={} pipelines={} channels={}",
        payload
            .get("runtime")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-"),
        payload
            .get("store")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-"),
        payload
            .get("pipelines")
            .and_then(serde_json::Value::as_array)
            .map(|v| v.len())
            .unwrap_or(0),
        payload
            .get("channels")
            .and_then(serde_json::Value::as_array)
            .map(|v| v.len())
            .unwrap_or(0),
    );
    Ok(())
}

async fn tx_command(args: &[String]) -> Result<(), CliError> {
    let action = args.get(2).ok_or_else(|| {
        CliError::Usage(
            "usage: mxctl tx <show|search> ... (use `mxctl tx search --help`)".to_string(),
        )
    })?;

    match action.as_str() {
        "show" => {
            let config_path = args.get(3).ok_or_else(|| {
                CliError::Usage("usage: mxctl tx show <config> <tx_id>".to_string())
            })?;
            let tx_id = args.get(4).ok_or_else(|| {
                CliError::Usage("usage: mxctl tx show <config> <tx_id>".to_string())
            })?;

            let config = mx20022_config::RuntimeConfig::load_from_path(config_path)?;
            let tx = match config.store.backend.as_str() {
                "sqlite" => {
                    SqliteStore::new(config.store.url.clone())
                        .find_by_id(tx_id)
                        .await?
                }
                "postgres" => {
                    PostgresStore::connect(config.store.url.clone())
                        .await?
                        .find_by_id(tx_id)
                        .await?
                }
                "rocksdb" => {
                    RocksDbStore::open(config.store.url.clone())?
                        .find_by_id(tx_id)
                        .await?
                }
                other => return Err(CliError::UnsupportedBackend(other.to_string())),
            };
            match tx {
                Some(tx) => {
                    print_transaction_line(&tx);
                    Ok(())
                }
                None => Err(CliError::NotFound(tx_id.to_string())),
            }
        }
        "search" => tx_search_command(args).await,
        _ => Err(CliError::Usage(
            "usage: mxctl tx <show|search> ...".to_string(),
        )),
    }
}

async fn tx_search_command(args: &[String]) -> Result<(), CliError> {
    if args.iter().any(|arg| arg == "--help") {
        println!("{}", tx_search_usage());
        return Ok(());
    }

    let config_path = option_value(args, "--config")
        .ok_or_else(|| CliError::Usage(format!("missing --config\n{}", tx_search_usage())))?;

    let config = mx20022_config::RuntimeConfig::load_from_path(config_path)?;

    match config.store.backend.as_str() {
        "sqlite" => {
            let store = SqliteStore::new(config.store.url.clone());
            let records = search_records(&store, args).await?;
            print_transactions(&records);
            Ok(())
        }
        "postgres" => {
            let store = PostgresStore::connect(config.store.url.clone()).await?;
            let records = search_records(&store, args).await?;
            print_transactions(&records);
            Ok(())
        }
        "rocksdb" => {
            let store = RocksDbStore::open(config.store.url.clone())?;
            let records = search_records(&store, args).await?;
            print_transactions(&records);
            Ok(())
        }
        other => Err(CliError::UnsupportedBackend(other.to_string())),
    }
}

async fn search_records(
    store: &dyn Store,
    args: &[String],
) -> Result<Vec<mx20022_store::TransactionRecord>, CliError> {
    if let Some(msg_id) = option_value(args, "--msg-id") {
        return store
            .find_by_message_id(&msg_id)
            .await
            .map_err(CliError::Store);
    }
    if let Some(e2e_id) = option_value(args, "--e2e-id") {
        return store
            .find_by_end_to_end_id(&e2e_id)
            .await
            .map_err(CliError::Store);
    }
    if let Some(uetr) = option_value(args, "--uetr") {
        return store.find_by_uetr(&uetr).await.map_err(CliError::Store);
    }

    let filter = mx20022_store::StoreQuery {
        pipeline: option_value(args, "--pipeline"),
        message_type: option_value(args, "--message-type"),
        state: option_value(args, "--state"),
        since: option_value(args, "--since").map(parse_epoch_millis),
        until: option_value(args, "--until").map(parse_epoch_millis),
        limit: option_value(args, "--limit").and_then(|v| v.parse::<usize>().ok()),
    };

    let result = store.query(filter).await.map_err(CliError::Store)?;
    Ok(result.records)
}

fn parse_epoch_millis(value: String) -> SystemTime {
    let millis = value.parse::<u64>().unwrap_or(0);
    UNIX_EPOCH + Duration::from_millis(millis)
}

fn print_transactions(records: &[mx20022_store::TransactionRecord]) {
    if records.is_empty() {
        println!("no transactions found");
        return;
    }
    for tx in records {
        print_transaction_line(tx);
    }
}

fn print_transaction_line(tx: &mx20022_store::TransactionRecord) {
    println!(
        "tx_id={} pipeline={} type={} state={} received_at={} completed_at={}",
        tx.tx_id,
        tx.pipeline,
        tx.message_type,
        tx.state,
        encode_time(tx.received_at),
        tx.completed_at
            .map(encode_time)
            .unwrap_or_else(|| "-".to_string())
    );
}

fn encode_time(time: SystemTime) -> String {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis()
        .to_string()
}

fn usage() -> String {
    "usage: mxctl <status [--admin <url>] [--token <token>] [--admin-grpc <url>]|config validate <path>|db <migrate|rollback|seed> <config>|tx <show|search> ...>".to_string()
}

fn option_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2).find_map(|window| {
        if window[0] == flag {
            Some(window[1].clone())
        } else {
            None
        }
    })
}

fn tx_search_usage() -> String {
    "usage: mxctl tx search --config <path> [--msg-id <id>|--e2e-id <id>|--uetr <id>|--pipeline <name> --message-type <type> --state <state> --since <epoch_ms> --until <epoch_ms> --limit <n>]".to_string()
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("{0}")]
    Usage(String),
    #[error(transparent)]
    Config(#[from] mx20022_config::ConfigError),
    #[error(transparent)]
    Store(#[from] mx20022_store::StoreError),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    GrpcTransport(#[from] tonic::transport::Error),
    #[error(transparent)]
    GrpcStatus(#[from] tonic::Status),
    #[error("admin request failed for {url} with status={status}: {body}")]
    AdminHttp {
        url: String,
        status: u16,
        body: String,
    },
    #[error("unsupported store backend: {0}")]
    UnsupportedBackend(String),
    #[error("transaction not found: {0}")]
    NotFound(String),
}
