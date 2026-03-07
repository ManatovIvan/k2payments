use mx20022_config::RuntimeConfig;
use mx20022_runtime::app::RuntimeApp;
use mx20022_runtime_core::transaction_manager::Outcome;

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

#[tokio::test]
async fn e2e_process_pacs008_commits_and_persists_context() {
    let config = RuntimeConfig::parse(E2E_CONFIG).expect("config should parse");
    let app = RuntimeApp::from_config(&config)
        .await
        .expect("app should build");

    let pacs008 = r#"<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13">
        <FIToFICstmrCdtTrf>
            <GrpHdr><MsgId>MSG-E2E-1</MsgId><NbOfTxs>1</NbOfTxs></GrpHdr>
        </FIToFICstmrCdtTrf>
    </Document>"#;

    let report = app
        .process("e2e", "TX-E2E-1", "http-in", "pacs.008", pacs008)
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
