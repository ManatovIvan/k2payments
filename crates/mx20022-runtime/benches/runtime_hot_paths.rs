use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mx20022_config::RuntimeConfig;
use mx20022_runtime::app::RuntimeApp;
use mx20022_store::StoreQuery;
use tempfile::TempDir;

fn build_config(sqlite_path: &str) -> RuntimeConfig {
    let raw = format!(
        r#"
[runtime]
name = "bench-runtime"
instance_id = "bench-01"

[store]
backend = "sqlite"
url = "{sqlite_path}"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:0"

[[pipeline]]
name = "bench"
channel_in = "http-in"
participants = [{{ name = "message-logger" }}]
"#
    );
    RuntimeConfig::parse(&raw).expect("benchmark runtime config should parse")
}

fn build_runtime(rt: &tokio::runtime::Runtime) -> (TempDir, Arc<RuntimeApp>) {
    let temp_dir = TempDir::new().expect("create temp dir");
    let sqlite_path = temp_dir.path().join("bench.db");
    let cfg = build_config(sqlite_path.to_str().expect("utf8 sqlite path"));
    let app = rt
        .block_on(RuntimeApp::from_config(&cfg))
        .expect("build runtime app");
    (temp_dir, Arc::new(app))
}

fn bench_runtime_process(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("create tokio runtime");
    let (_temp_dir, app) = build_runtime(&rt);
    let counter = AtomicU64::new(1);

    c.bench_function("runtime_app_process_message_logger", |b| {
        b.to_async(&rt).iter(|| async {
            let id = counter.fetch_add(1, Ordering::Relaxed);
            let report = app
                .process(
                    "bench",
                    format!("TX-BENCH-{id}"),
                    "http-in",
                    "pacs.008.001.08",
                    "<Document/>",
                )
                .await
                .expect("process benchmark transaction");
            black_box(report);
        });
    });
}

fn bench_store_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("create tokio runtime");
    let (_temp_dir, app) = build_runtime(&rt);
    let store = app.store_handle();

    rt.block_on(async {
        for id in 0..200_u64 {
            app.process(
                "bench",
                format!("TX-QUERY-{id}"),
                "http-in",
                "pacs.008.001.08",
                "<Document/>",
            )
            .await
            .expect("seed transaction");
        }
    });

    c.bench_function("store_query_committed", |b| {
        b.to_async(&rt).iter(|| async {
            let result = store
                .query(StoreQuery {
                    pipeline: Some("bench".to_string()),
                    message_type: None,
                    state: Some("COMMITTED".to_string()),
                    since: None,
                    until: None,
                    limit: Some(50),
                })
                .await
                .expect("query benchmark transactions");
            black_box(result.total);
        });
    });
}

criterion_group!(benches, bench_runtime_process, bench_store_query);
criterion_main!(benches);
