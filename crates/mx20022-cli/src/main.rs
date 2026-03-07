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
        "reload" => reload_command(&args).await,
        "channel" => channel_command(&args).await,
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
        "deadletter" => deadletter_command(&args).await,
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
            let store =
                SqliteStore::with_pool_size(config.store.url.clone(), config.store.pool_size)?;
            match action.as_str() {
                "migrate" => {
                    store.apply_migrations().await?;
                    println!("migrations applied to sqlite backend");
                    Ok(())
                }
                "rollback" => {
                    store.rollback_migrations().await?;
                    println!("migrations rolled back for sqlite backend");
                    Ok(())
                }
                "seed" => {
                    store.apply_dev_seed().await?;
                    println!("dev seed applied to sqlite backend");
                    Ok(())
                }
                _ => Err(CliError::Usage(
                    "usage: mxctl db <migrate|rollback|seed> <config>".to_string(),
                )),
            }
        }
        "postgres" => {
            let store = PostgresStore::connect_with_pool_size(
                config.store.url.clone(),
                config.store.pool_size,
            )
            .await?;
            match action.as_str() {
                "migrate" => {
                    store.apply_migrations().await?;
                    println!("migrations applied to postgres backend");
                    Ok(())
                }
                "rollback" => {
                    store.rollback_migrations().await?;
                    println!("migrations rolled back for postgres backend");
                    Ok(())
                }
                "seed" => {
                    store.apply_dev_seed().await?;
                    println!("dev seed applied to postgres backend");
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
        println!(
            "uptime_ms={} store_ok={} in_flight={} pending_correlation={} dead_letters={} config_version={} last_reload_result={} last_reload_at={}",
            payload.uptime_ms,
            payload.store_ok,
            payload.in_flight_count,
            payload.pending_correlation_count,
            payload.dead_letter_count,
            payload.config_version,
            if payload.last_reload_result.is_empty() { "-" } else { payload.last_reload_result.as_str() },
            if payload.last_reload_at.is_empty() { "-" } else { payload.last_reload_at.as_str() },
        );
        return Ok(());
    }

    let admin_url =
        option_value(args, "--admin").unwrap_or_else(|| "http://127.0.0.1:9090".to_string());
    let token = resolve_admin_token(args, "--token").ok_or_else(|| {
        CliError::Usage(
            "admin token is required; pass --token <token> or set MXCTL_TOKEN".to_string(),
        )
    })?;

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
    println!(
        "uptime_ms={} store_ok={} in_flight={} pending_correlation={} dead_letters={} config_version={} last_reload_result={} last_reload_at={}",
        payload
            .get("uptime_ms")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-"),
        payload
            .get("store_ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        payload
            .get("in_flight_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        payload
            .get("pending_correlation_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        payload
            .get("dead_letter_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        payload
            .get("config_version")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-"),
        payload
            .get("last_reload_result")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-"),
        payload
            .get("last_reload_at")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-"),
    );
    Ok(())
}

async fn reload_command(args: &[String]) -> Result<(), CliError> {
    if let Some(grpc_endpoint) = option_value(args, "--admin-grpc") {
        let mut client = AdminServiceClient::connect(grpc_endpoint.clone())
            .await
            .map_err(CliError::GrpcTransport)?;
        let payload = client
            .reload(())
            .await
            .map_err(CliError::GrpcStatus)?
            .into_inner();
        println!("reloaded={} details={}", payload.reloaded, payload.details);
        return Ok(());
    }

    let admin_url =
        option_value(args, "--admin").unwrap_or_else(|| "http://127.0.0.1:9090".to_string());
    let token = resolve_admin_token(args, "--token").ok_or_else(|| {
        CliError::Usage(
            "admin token is required; pass --token <token> or set MXCTL_TOKEN".to_string(),
        )
    })?;

    let url = format!("{}/reload", admin_url.trim_end_matches('/'));
    let response = reqwest::Client::new()
        .post(&url)
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
        "reloaded={} details={}",
        payload
            .get("reloaded")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        payload
            .get("details")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-"),
    );
    Ok(())
}

async fn channel_command(args: &[String]) -> Result<(), CliError> {
    let action = args.get(2).ok_or_else(|| {
        CliError::Usage(
            "usage: mxctl channel list [--admin <url>] [--token <token>] [--admin-grpc <url>]"
                .to_string(),
        )
    })?;
    match action.as_str() {
        "list" => channel_list_command(args).await,
        _ => Err(CliError::Usage(
            "usage: mxctl channel list [--admin <url>] [--token <token>] [--admin-grpc <url>]"
                .to_string(),
        )),
    }
}

async fn channel_list_command(args: &[String]) -> Result<(), CliError> {
    if let Some(grpc_endpoint) = option_value(args, "--admin-grpc") {
        let mut client = AdminServiceClient::connect(grpc_endpoint.clone())
            .await
            .map_err(CliError::GrpcTransport)?;
        let payload = client
            .get_status(())
            .await
            .map_err(CliError::GrpcStatus)?
            .into_inner();

        if payload.channels.is_empty() {
            println!("no channels configured");
            return Ok(());
        }

        for channel in payload.channels {
            println!("channel={}", channel);
        }
        return Ok(());
    }

    let admin_url =
        option_value(args, "--admin").unwrap_or_else(|| "http://127.0.0.1:9090".to_string());
    let token = resolve_admin_token(args, "--token").ok_or_else(|| {
        CliError::Usage(
            "admin token is required; pass --token <token> or set MXCTL_TOKEN".to_string(),
        )
    })?;
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
    let Some(channels) = payload
        .get("channels")
        .and_then(serde_json::Value::as_array)
    else {
        println!("no channels configured");
        return Ok(());
    };
    if channels.is_empty() {
        println!("no channels configured");
        return Ok(());
    }
    for channel in channels {
        if let Some(name) = channel.as_str() {
            println!("channel={}", name);
        }
    }
    Ok(())
}

async fn tx_command(args: &[String]) -> Result<(), CliError> {
    let action = args.get(2).ok_or_else(|| {
        CliError::Usage(
            "usage: mxctl tx <show|search|context> ... (use `mxctl tx search --help`)".to_string(),
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
                    SqliteStore::with_pool_size(config.store.url.clone(), config.store.pool_size)?
                        .find_by_id(tx_id)
                        .await?
                }
                "postgres" => {
                    PostgresStore::connect_with_pool_size(
                        config.store.url.clone(),
                        config.store.pool_size,
                    )
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
        "context" => {
            let config_path = args.get(3).ok_or_else(|| {
                CliError::Usage("usage: mxctl tx context <config> <tx_id>".to_string())
            })?;
            let tx_id = args.get(4).ok_or_else(|| {
                CliError::Usage("usage: mxctl tx context <config> <tx_id>".to_string())
            })?;

            let config = mx20022_config::RuntimeConfig::load_from_path(config_path)?;
            let entries = match config.store.backend.as_str() {
                "sqlite" => {
                    SqliteStore::with_pool_size(config.store.url.clone(), config.store.pool_size)?
                        .list_context_entries(tx_id)
                        .await?
                }
                "postgres" => {
                    PostgresStore::connect_with_pool_size(
                        config.store.url.clone(),
                        config.store.pool_size,
                    )
                    .await?
                    .list_context_entries(tx_id)
                    .await?
                }
                "rocksdb" => {
                    RocksDbStore::open(config.store.url.clone())?
                        .list_context_entries(tx_id)
                        .await?
                }
                other => return Err(CliError::UnsupportedBackend(other.to_string())),
            };

            if entries.is_empty() {
                println!("no context mutations found for tx_id={}", tx_id);
                return Ok(());
            }

            for entry in entries {
                println!(
                    "tx_id={} written_at={} writer={} key={}",
                    entry.tx_id,
                    encode_time(entry.written_at),
                    entry.writer,
                    entry.key
                );
            }
            Ok(())
        }
        "search" => tx_search_command(args).await,
        _ => Err(CliError::Usage(
            "usage: mxctl tx <show|search|context> ...".to_string(),
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
            let store =
                SqliteStore::with_pool_size(config.store.url.clone(), config.store.pool_size)?;
            let records = search_records(&store, args).await?;
            print_transactions(&records);
            Ok(())
        }
        "postgres" => {
            let store = PostgresStore::connect_with_pool_size(
                config.store.url.clone(),
                config.store.pool_size,
            )
            .await?;
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
    "usage: mxctl <status [--admin <url>] [--token <token>] [--admin-grpc <url>]|reload [--admin <url>] [--token <token>] [--admin-grpc <url>]|channel list [--admin <url>] [--token <token>] [--admin-grpc <url>]|config validate <path>|db <migrate|rollback|seed> <config>|tx <show|search|context> ...|deadletter <list|show|replay> ...>".to_string()
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

fn resolve_admin_token(args: &[String], flag: &str) -> Option<String> {
    option_value(args, flag)
        .or_else(|| env::var("MXCTL_TOKEN").ok())
        .filter(|value| !value.trim().is_empty())
}

fn tx_search_usage() -> String {
    "usage: mxctl tx search --config <path> [--msg-id <id>|--e2e-id <id>|--uetr <id>|--pipeline <name> --message-type <type> --state <state> --since <epoch_ms> --until <epoch_ms> --limit <n>]".to_string()
}

async fn deadletter_command(args: &[String]) -> Result<(), CliError> {
    let action = args
        .get(2)
        .ok_or_else(|| CliError::Usage(deadletter_usage()))?;
    let config_path = option_value(args, "--config")
        .ok_or_else(|| CliError::Usage(format!("missing --config\n{}", deadletter_usage())))?;
    let config = mx20022_config::RuntimeConfig::load_from_path(config_path)?;

    match config.store.backend.as_str() {
        "sqlite" => {
            let store =
                SqliteStore::with_pool_size(config.store.url.clone(), config.store.pool_size)?;
            run_deadletter_action(&store, action, args).await
        }
        "postgres" => {
            let store = PostgresStore::connect_with_pool_size(
                config.store.url.clone(),
                config.store.pool_size,
            )
            .await?;
            run_deadletter_action(&store, action, args).await
        }
        "rocksdb" => {
            let store = RocksDbStore::open(config.store.url.clone())?;
            run_deadletter_action(&store, action, args).await
        }
        other => Err(CliError::UnsupportedBackend(other.to_string())),
    }
}

async fn run_deadletter_action(
    store: &dyn Store,
    action: &str,
    args: &[String],
) -> Result<(), CliError> {
    match action {
        "list" => {
            let letters = store
                .list_dead_letters(mx20022_store::DeadLetterQuery {
                    pipeline: option_value(args, "--pipeline"),
                    limit: option_value(args, "--limit").and_then(|v| v.parse::<usize>().ok()),
                })
                .await
                .map_err(CliError::Store)?;

            if letters.is_empty() {
                println!("no dead letters found");
                return Ok(());
            }
            for letter in letters {
                println!(
                    "id={} tx_id={} failed_at={} reason={}",
                    letter.id,
                    letter.tx_id,
                    encode_time(letter.failed_at),
                    letter.reason
                );
            }
            Ok(())
        }
        "show" => {
            let id = option_value(args, "--id").ok_or_else(|| {
                CliError::Usage(
                    "usage: mxctl deadletter show --config <path> --id <dl_id>".to_string(),
                )
            })?;
            let letters = store
                .list_dead_letters(mx20022_store::DeadLetterQuery {
                    pipeline: None,
                    limit: None,
                })
                .await
                .map_err(CliError::Store)?;
            let letter = letters
                .into_iter()
                .find(|item| item.id == id)
                .ok_or_else(|| CliError::NotFound(id.clone()))?;
            println!(
                "id={}\ntx_id={}\nfailed_at={}\nreason={}\nraw_message={}",
                letter.id,
                letter.tx_id,
                encode_time(letter.failed_at),
                letter.reason,
                letter.raw_message
            );
            Ok(())
        }
        "replay" => {
            let id = option_value(args, "--id").ok_or_else(|| {
                CliError::Usage(
                    "usage: mxctl deadletter replay --config <path> --id <dl_id>".to_string(),
                )
            })?;
            store
                .replay_dead_letter(&id)
                .await
                .map_err(CliError::Store)?;
            println!("dead letter replayed: {id}");
            Ok(())
        }
        _ => Err(CliError::Usage(deadletter_usage())),
    }
}

fn deadletter_usage() -> String {
    "usage: mxctl deadletter <list|show|replay> --config <path> [--pipeline <name>] [--limit <n>] [--id <dl_id>]".to_string()
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
