/// Missing high-value tests for mx20022-store-sqlite.
///
/// This module is wired from `lib.rs` via `mod tests_missing;`.
///
/// Every test uses the in-memory SQLite store, so there are no external deps.
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use mx20022_store::{DeadLetterQuery, Outcome, Store, TransactionRecord};

    use crate::SqliteStore;

    // ---------------------------------------------------------------------------
    // Shared helpers
    // ---------------------------------------------------------------------------

    fn record_with_keys(
        tx_id: &str,
        message_id: &str,
        e2e_id: &str,
        uetr: &str,
    ) -> TransactionRecord {
        let mut key_fields = HashMap::new();
        key_fields.insert("message_id".to_string(), message_id.to_string());
        key_fields.insert("end_to_end_id".to_string(), e2e_id.to_string());
        key_fields.insert("uetr".to_string(), uetr.to_string());

        TransactionRecord {
            tx_id: tx_id.to_string(),
            pipeline: "demo".to_string(),
            source_channel: "http".to_string(),
            message_type: "pacs.008.001.13".to_string(),
            raw_message: "<Document/>".to_string(),
            state: "RECEIVED".to_string(),
            received_at: SystemTime::now(),
            completed_at: None,
            key_fields,
        }
    }

    // ===========================================================================
    // TEST 1: find_by_message_id / find_by_uetr / find_by_end_to_end_id
    //
    // WHY: These three methods were recently changed to delegate to
    // find_by_key_field, which issues a json_extract() SQL query.  The existing
    // test suite never calls them directly.  If the JSON column encoding or the
    // SQL template is wrong, searches silently return empty results — a silent
    // correctness failure that is undetectable without a real call.
    // ===========================================================================
    #[tokio::test]
    async fn find_by_message_id_returns_matching_records() {
        let store = SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize");

        store
            .begin_transaction(&record_with_keys("TX-A", "MSG-001", "E2E-001", "UETR-001"))
            .await
            .unwrap();
        store
            .begin_transaction(&record_with_keys("TX-B", "MSG-002", "E2E-002", "UETR-002"))
            .await
            .unwrap();

        let found = store.find_by_message_id("MSG-001").await.unwrap();
        assert_eq!(found.len(), 1, "exactly one record should match MSG-001");
        assert_eq!(found[0].tx_id, "TX-A");

        let empty = store.find_by_message_id("MSG-MISSING").await.unwrap();
        assert!(
            empty.is_empty(),
            "unknown message_id should return empty vec"
        );
    }

    #[tokio::test]
    async fn find_by_uetr_returns_matching_record() {
        let store = SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize");

        store
            .begin_transaction(&record_with_keys("TX-C", "MSG-003", "E2E-003", "UETR-ABC"))
            .await
            .unwrap();

        let found = store.find_by_uetr("UETR-ABC").await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].tx_id, "TX-C");
    }

    #[tokio::test]
    async fn find_by_end_to_end_id_returns_matching_record() {
        let store = SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize");

        store
            .begin_transaction(&record_with_keys("TX-D", "MSG-004", "E2E-XYZ", "UETR-004"))
            .await
            .unwrap();

        let found = store.find_by_end_to_end_id("E2E-XYZ").await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].tx_id, "TX-D");
    }

    // ===========================================================================
    // TEST 2: complete_transaction transitions are persisted correctly for all
    //         three Outcome variants (Committed, Aborted, Poison).
    //
    // WHY: The existing test only checks Outcome::Committed.  Aborted and Poison
    // go through the same SQL UPDATE but write different state strings.  A typo
    // ("ABORT" vs "ABORTED") would be invisible to the current test.
    // ===========================================================================
    #[tokio::test]
    async fn complete_transaction_persists_all_outcome_variants() {
        let store = SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize");

        for (tx_id, outcome, expected_state) in [
            ("TX-COMMIT", Outcome::Committed, "COMMITTED"),
            ("TX-ABORT", Outcome::Aborted, "ABORTED"),
            ("TX-POISON", Outcome::Poison, "POISON"),
        ] {
            store
                .begin_transaction(&record_with_keys(tx_id, tx_id, tx_id, tx_id))
                .await
                .unwrap_or_else(|e| panic!("begin_transaction for {tx_id} failed: {e}"));

            store
                .complete_transaction(tx_id, outcome)
                .await
                .unwrap_or_else(|e| panic!("complete_transaction for {tx_id} failed: {e}"));

            let record = store
                .find_by_id(tx_id)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("record {tx_id} should exist after complete"));

            assert_eq!(
                record.state, expected_state,
                "outcome {:?} should map to state {}",
                outcome, expected_state
            );
            assert!(
                record.completed_at.is_some(),
                "completed_at must be set for outcome {:?}",
                outcome
            );
        }
    }

    // ===========================================================================
    // TEST 3: replay_dead_letter returns an error for an unknown id.
    //
    // WHY: The implementation checks for existence and returns Err when the
    // dead_letter row is missing, but there is no test exercising the error path.
    // Callers (the CLI) rely on this error to produce "not found" messages.
    // ===========================================================================
    #[tokio::test]
    async fn replay_dead_letter_errors_for_unknown_id() {
        let store = SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize");

        let result = store.replay_dead_letter("NO-SUCH-ID").await;
        assert!(
            result.is_err(),
            "replay_dead_letter must return Err for an unknown dead letter id"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("NO-SUCH-ID"),
            "error should mention the missing id, got: {err_msg}"
        );
    }

    // ===========================================================================
    // TEST 4: list_dead_letters pipeline filter and limit are applied correctly.
    //
    // WHY: The query fetches all rows then filters in Rust.  If the pipeline
    // comparison is broken (e.g., wrong field name in join) the filter silently
    // passes everything.  The limit is applied after the loop — an off-by-one
    // would not be caught by the existing round-trip test which only inserts 1.
    // ===========================================================================
    #[tokio::test]
    async fn list_dead_letters_filters_by_pipeline_and_respects_limit() {
        use mx20022_store::DeadLetter;
        let store = SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize");

        // Insert three transactions, two on the demo pipeline.
        let mut rec_demo = record_with_keys("TX-DL-1", "M1", "E1", "U1");
        rec_demo.pipeline = "demo".to_string();
        let mut rec_other = record_with_keys("TX-DL-2", "M2", "E2", "U2");
        rec_other.pipeline = "other".to_string();
        let mut rec_demo_two = record_with_keys("TX-DL-3", "M3", "E3", "U3");
        rec_demo_two.pipeline = "demo".to_string();

        store.begin_transaction(&rec_demo).await.unwrap();
        store.begin_transaction(&rec_other).await.unwrap();
        store.begin_transaction(&rec_demo_two).await.unwrap();

        for (i, tx_id) in ["TX-DL-1", "TX-DL-3", "TX-DL-2"].iter().enumerate() {
            store
                .save_dead_letter(&DeadLetter {
                    id: format!("DL-{i}"),
                    tx_id: tx_id.to_string(),
                    reason: "test".to_string(),
                    failed_at: SystemTime::now(),
                    raw_message: "<Document/>".to_string(),
                })
                .await
                .unwrap();
        }

        // Pipeline filter: only "demo" letters (DL-0 and DL-1).
        let demo_letters = store
            .list_dead_letters(DeadLetterQuery {
                pipeline: Some("demo".to_string()),
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(
            demo_letters.len(),
            2,
            "only demo-pipeline dead letters should be returned"
        );

        // Limit: only 1 of the demo letters.
        let limited = store
            .list_dead_letters(DeadLetterQuery {
                pipeline: Some("demo".to_string()),
                limit: Some(1),
            })
            .await
            .unwrap();
        assert_eq!(limited.len(), 1, "limit=1 should return exactly 1 record");
    }

    // ===========================================================================
    // TEST 5: update_transaction for a non-existent tx_id returns an error.
    //
    // WHY: update_transaction first calls find_by_id and wraps the None case in
    // an explicit Err.  This is the only method that guards against updating a
    // phantom record.  No existing test exercises the missing-record error path,
    // so a refactor that accidentally drops the guard would go undetected.
    // ===========================================================================
    #[tokio::test]
    async fn update_transaction_errors_when_record_does_not_exist() {
        use mx20022_store::TransactionUpdate;
        let store = SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize");

        let result = store
            .update_transaction(
                "TX-PHANTOM",
                TransactionUpdate {
                    state: Some("PREPARING".to_string()),
                    error: None,
                },
            )
            .await;

        assert!(
            result.is_err(),
            "updating a non-existent transaction must return Err"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("TX-PHANTOM"),
            "error should name the missing tx_id, got: {msg}"
        );
    }
}
