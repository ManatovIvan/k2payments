// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::time::SystemTime;

use mx20022_config::RuntimeConfig;
use mx20022_runtime::app::RuntimeApp;
use mx20022_runtime_core::transaction_manager::Outcome;
use mx20022_store::{StoreQuery, TransactionRecord};

// ── Fixture configs ────────────────────────────────────────────────────

const E2E_CONFIG: &str = r#"
[runtime]
name = "e2e-test"
instance_id = "test-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:0"

[[pipeline]]
name = "e2e"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "message-logger", config = {} },
  { name = "acknowledgement-builder", config = {} },
]
"#;

const MULTI_PARTICIPANT_CONFIG: &str = r#"
[runtime]
name = "e2e-test"
instance_id = "test-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:0"

[[pipeline]]
name = "multi"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "message-logger", config = { tag = "multi-test" } },
  { name = "acknowledgement-builder", config = {} },
]
"#;

const DUPLICATE_DETECTION_CONFIG: &str = r#"
[runtime]
name = "e2e-test"
instance_id = "test-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:0"

[[pipeline]]
name = "dedup"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "error-response-builder", config = { overwrite_existing = true } },
  { name = "duplicate-checker", config = { keys = ["message_id"] } },
]
"#;

const MULTI_PIPELINE_CONFIG: &str = r#"
[runtime]
name = "e2e-test"
instance_id = "test-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:0"

[[pipeline]]
name = "pacs008-pipeline"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "message-logger", config = { tag = "pacs008" } },
]

[[pipeline]]
name = "pacs002-pipeline"
channel_in = "http-in"
message_types = ["pacs.002"]
participants = [
  { name = "message-logger", config = { tag = "pacs002" } },
]
"#;

const RECOVERY_CONFIG: &str = r#"
[runtime]
name = "e2e-test"
instance_id = "test-01"

[store]
backend = "sqlite"
url = "sqlite::memory:"

[channels.http-in]
type = "http"
mode = "server"
bind = "127.0.0.1:0"

[[pipeline]]
name = "recovery"
channel_in = "http-in"
message_types = ["pacs.008"]
participants = [
  { name = "message-logger", config = {} },
]
"#;

// ── Helpers ────────────────────────────────────────────────────────────

fn pacs008_xml(msg_id: &str) -> String {
    format!(
        r#"<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13">
        <FIToFICstmrCdtTrf>
            <GrpHdr><MsgId>{msg_id}</MsgId><NbOfTxs>1</NbOfTxs></GrpHdr>
        </FIToFICstmrCdtTrf>
    </Document>"#
    )
}

// ── Tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn e2e_process_pacs008_commits_and_persists_context() {
    let config = RuntimeConfig::parse(E2E_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    let report = app
        .process("e2e", "TX-E2E-1", "http-in", "pacs.008", pacs008_xml("MSG-E2E-1"))
        .await
        .expect("process should succeed");

    assert_eq!(report.outcome, Outcome::Committed);

    let store = app.store_handle();

    let record = store
        .find_by_id("TX-E2E-1")
        .await
        .expect("find should succeed")
        .expect("record should exist");
    assert_eq!(record.state, "COMMITTED");
    assert!(record.completed_at.is_some());

    let entries = store
        .list_context_entries("TX-E2E-1")
        .await
        .expect("list context should succeed");
    assert!(
        !entries.is_empty(),
        "context entries should be persisted by acknowledgement-builder"
    );
}

#[tokio::test]
async fn e2e_multi_participant_commits_with_context_entries() {
    let config = RuntimeConfig::parse(MULTI_PARTICIPANT_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    let report = app
        .process(
            "multi",
            "TX-MULTI-1",
            "http-in",
            "pacs.008",
            pacs008_xml("MSG-MULTI-1"),
        )
        .await
        .expect("process should succeed");

    assert_eq!(report.outcome, Outcome::Committed);

    // Both participants should have produced results
    assert_eq!(
        report.participant_results.len(),
        2,
        "expected 2 participant results"
    );
    assert_eq!(report.participant_results[0].participant, "message-logger");
    assert_eq!(
        report.participant_results[1].participant,
        "acknowledgement-builder"
    );
    // Neither participant should have errored
    for pr in &report.participant_results {
        assert!(pr.error.is_none(), "{} errored: {:?}", pr.participant, pr.error);
    }

    let store = app.store_handle();

    let record = store
        .find_by_id("TX-MULTI-1")
        .await
        .expect("find should succeed")
        .expect("record should exist");
    assert_eq!(record.state, "COMMITTED");

    let entries = store
        .list_context_entries("TX-MULTI-1")
        .await
        .expect("list context should succeed");
    assert!(
        !entries.is_empty(),
        "expected context entries from acknowledgement-builder"
    );

    // acknowledgement-builder writes context entries (response.xml, content_type)
    let writers: Vec<&str> = entries.iter().map(|e| e.writer.as_str()).collect();
    assert!(
        writers.iter().all(|w| *w == "acknowledgement-builder"),
        "context entries should come from acknowledgement-builder, got: {writers:?}"
    );
}

#[tokio::test]
async fn e2e_duplicate_detection_aborts_transaction() {
    let config = RuntimeConfig::parse(DUPLICATE_DETECTION_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    // Seed the store with a committed transaction whose message_id is known
    let store = app.store_handle();
    let mut key_fields = HashMap::new();
    key_fields.insert("message_id".to_string(), "MSG-DUP-E2E".to_string());
    store
        .begin_transaction(&TransactionRecord {
            tx_id: "TX-SEED".to_string(),
            pipeline: "dedup".to_string(),
            source_channel: "http-in".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: "<Document/>".to_string(),
            state: "COMMITTED".to_string(),
            received_at: SystemTime::now(),
            completed_at: Some(SystemTime::now()),
            key_fields,
        })
        .await
        .expect("seed should succeed");

    // Process a message with the same message_id through the dedup pipeline
    let xml = r#"<Document><FIToFICstmrCdtTrf><GrpHdr><MsgId>MSG-DUP-E2E</MsgId></GrpHdr></FIToFICstmrCdtTrf></Document>"#;
    let report = app
        .process("dedup", "TX-DUP-E2E", "http-in", "pacs.008", xml)
        .await
        .expect("process should return report");

    assert_eq!(
        report.outcome,
        Outcome::Aborted,
        "duplicate message_id should cause abort"
    );
}

#[tokio::test]
async fn e2e_store_record_has_expected_fields() {
    let config = RuntimeConfig::parse(E2E_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    let xml = pacs008_xml("MSG-FIELDS-1");
    app.process("e2e", "TX-FIELDS-1", "http-in", "pacs.008", &xml)
        .await
        .expect("process should succeed");

    let store = app.store_handle();
    let record = store
        .find_by_id("TX-FIELDS-1")
        .await
        .expect("find should succeed")
        .expect("record should exist");

    assert_eq!(record.tx_id, "TX-FIELDS-1");
    assert_eq!(record.pipeline, "e2e");
    assert_eq!(record.source_channel, "http-in");
    assert_eq!(record.message_type, "pacs.008");
    assert!(
        record.raw_message.contains("MSG-FIELDS-1"),
        "raw_message should contain MsgId"
    );
    assert_eq!(record.state, "COMMITTED");
    assert!(record.completed_at.is_some());
    assert!(record.received_at <= SystemTime::now());
}

#[tokio::test]
async fn e2e_multiple_transactions_persist_independently() {
    let config = RuntimeConfig::parse(E2E_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    // Process 3 transactions through the same pipeline
    for i in 1..=3 {
        let report = app
            .process(
                "e2e",
                format!("TX-ISO-{i}"),
                "http-in",
                "pacs.008",
                pacs008_xml(&format!("MSG-ISO-{i}")),
            )
            .await
            .expect("process should succeed");
        assert_eq!(report.outcome, Outcome::Committed);
    }

    let store = app.store_handle();

    // Verify each transaction exists with correct identity
    for i in 1..=3 {
        let record = store
            .find_by_id(&format!("TX-ISO-{i}"))
            .await
            .expect("find should succeed")
            .expect("record should exist");
        assert_eq!(record.state, "COMMITTED");
        assert_eq!(record.tx_id, format!("TX-ISO-{i}"));
    }

    // Verify via store query: all 3 are committed in this pipeline
    let result = store
        .query(StoreQuery {
            pipeline: Some("e2e".to_string()),
            message_type: None,
            state: Some("COMMITTED".to_string()),
            since: None,
            until: None,
            limit: None,
        })
        .await
        .expect("query should succeed");
    assert_eq!(result.total, 3, "expected 3 committed transactions");
}

#[tokio::test]
async fn e2e_unknown_pipeline_returns_error() {
    let config = RuntimeConfig::parse(E2E_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    let err = app
        .process("nonexistent", "TX-ERR-1", "http-in", "pacs.008", "<Document/>")
        .await
        .expect_err("should fail for unknown pipeline");

    assert!(
        err.to_string().contains("unknown pipeline"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn e2e_rejected_message_type_returns_error() {
    let config = RuntimeConfig::parse(E2E_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    let err = app
        .process("e2e", "TX-ERR-2", "http-in", "pacs.002", "<Document/>")
        .await
        .expect_err("should fail for unsupported message type");

    let msg = err.to_string();
    assert!(
        msg.contains("not accepted"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn e2e_multi_pipeline_routing() {
    let config = RuntimeConfig::parse(MULTI_PIPELINE_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    assert_eq!(app.pipeline_count().await, 2);
    assert!(app.accepts_message_type("pacs008-pipeline", "pacs.008").await);
    assert!(app.accepts_message_type("pacs002-pipeline", "pacs.002").await);
    assert!(!app.accepts_message_type("pacs008-pipeline", "pacs.002").await);
    assert!(!app.accepts_message_type("pacs002-pipeline", "pacs.008").await);

    // Process through pipeline 1
    let report1 = app
        .process(
            "pacs008-pipeline",
            "TX-ROUTE-1",
            "http-in",
            "pacs.008",
            "<Document/>",
        )
        .await
        .expect("process should succeed");
    assert_eq!(report1.outcome, Outcome::Committed);

    // Process through pipeline 2
    let report2 = app
        .process(
            "pacs002-pipeline",
            "TX-ROUTE-2",
            "http-in",
            "pacs.002",
            "<Document/>",
        )
        .await
        .expect("process should succeed");
    assert_eq!(report2.outcome, Outcome::Committed);

    // Verify both are persisted with correct pipeline names
    let store = app.store_handle();

    let record1 = store
        .find_by_id("TX-ROUTE-1")
        .await
        .expect("find should succeed")
        .expect("record should exist");
    assert_eq!(record1.pipeline, "pacs008-pipeline");
    assert_eq!(record1.message_type, "pacs.008");

    let record2 = store
        .find_by_id("TX-ROUTE-2")
        .await
        .expect("find should succeed")
        .expect("record should exist");
    assert_eq!(record2.pipeline, "pacs002-pipeline");
    assert_eq!(record2.message_type, "pacs.002");
}

#[tokio::test]
async fn e2e_recovery_completes_incomplete_transactions() {
    let config = RuntimeConfig::parse(RECOVERY_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    // Seed the store with a PREPARING transaction (simulates crash mid-flight)
    let store = app.store_handle();
    store
        .begin_transaction(&TransactionRecord {
            tx_id: "TX-REC-E2E".to_string(),
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
        .expect("seed should succeed");

    // Run recovery
    let report = app
        .recover_incomplete_transactions(10)
        .await
        .expect("recovery should run");

    assert_eq!(report.attempted, 1);
    assert_eq!(report.recovered, 1);
    assert_eq!(report.failed, 0);

    // Verify the transaction was moved to COMMITTED
    let updated = store
        .find_by_id("TX-REC-E2E")
        .await
        .expect("lookup should succeed")
        .expect("record should exist");
    assert_eq!(updated.state, "COMMITTED");
    assert!(
        updated.completed_at.is_some(),
        "recovered transaction should have completed_at set"
    );
}

#[tokio::test]
async fn e2e_context_entries_record_participant_writes() {
    let config = RuntimeConfig::parse(E2E_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    app.process(
        "e2e",
        "TX-CTX-1",
        "http-in",
        "pacs.008",
        pacs008_xml("MSG-CTX-1"),
    )
    .await
    .expect("process should succeed");

    let store = app.store_handle();
    let entries = store
        .list_context_entries("TX-CTX-1")
        .await
        .expect("list context should succeed");

    assert!(!entries.is_empty(), "expected context entries");

    // Every entry must reference the correct transaction
    for entry in &entries {
        assert_eq!(entry.tx_id, "TX-CTX-1");
        assert!(
            !entry.writer.is_empty(),
            "context entry writer should not be empty"
        );
        assert!(
            !entry.key.is_empty(),
            "context entry key should not be empty"
        );
    }

    // Entries must be ordered by written_at (ascending)
    for window in entries.windows(2) {
        assert!(
            window[0].written_at <= window[1].written_at,
            "context entries should be ordered by written_at"
        );
    }
}
