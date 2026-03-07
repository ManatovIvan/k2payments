use std::sync::Arc;

use async_trait::async_trait;
use mx20022_store::Store;
use mx20022_validate::schemes::xml_scan::extract_element;

use mx20022_runtime_core::{
    context::Context,
    participant::{Action, Participant, ParticipantError},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicateKey {
    MessageId,
    EndToEndId,
    Uetr,
}

pub struct DuplicateChecker {
    store: Arc<dyn Store>,
    keys: Vec<DuplicateKey>,
}

impl DuplicateChecker {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self {
            store,
            keys: vec![
                DuplicateKey::MessageId,
                DuplicateKey::EndToEndId,
                DuplicateKey::Uetr,
            ],
        }
    }

    pub fn with_keys(mut self, keys: Vec<DuplicateKey>) -> Self {
        self.keys = keys;
        self
    }

    async fn detect_duplicate(
        &self,
        tx_id: &str,
        xml: &str,
    ) -> Result<Option<(DuplicateKey, String)>, ParticipantError> {
        let check_message_id = self.keys.contains(&DuplicateKey::MessageId).then(|| {
            extract_element(xml, "BizMsgIdr")
                .or_else(|| extract_element(xml, "MsgId"))
                .map(ToString::to_string)
        });
        let check_end_to_end_id = self
            .keys
            .contains(&DuplicateKey::EndToEndId)
            .then(|| extract_element(xml, "EndToEndId").map(ToString::to_string));
        let check_uetr = self
            .keys
            .contains(&DuplicateKey::Uetr)
            .then(|| extract_element(xml, "UETR").map(ToString::to_string));

        let (message_id_records, end_to_end_records, uetr_records) = tokio::join!(
            async {
                if let Some(Some(value)) = check_message_id.as_ref() {
                    self.store
                        .find_by_message_id(value)
                        .await
                        .map(Some)
                        .map_err(|e| ParticipantError::new(format!("duplicate-checker: {e}")))
                } else {
                    Ok(None)
                }
            },
            async {
                if let Some(Some(value)) = check_end_to_end_id.as_ref() {
                    self.store
                        .find_by_end_to_end_id(value)
                        .await
                        .map(Some)
                        .map_err(|e| ParticipantError::new(format!("duplicate-checker: {e}")))
                } else {
                    Ok(None)
                }
            },
            async {
                if let Some(Some(value)) = check_uetr.as_ref() {
                    self.store
                        .find_by_uetr(value)
                        .await
                        .map(Some)
                        .map_err(|e| ParticipantError::new(format!("duplicate-checker: {e}")))
                } else {
                    Ok(None)
                }
            }
        );

        let message_id_records = message_id_records?;
        let end_to_end_records = end_to_end_records?;
        let uetr_records = uetr_records?;

        for key in &self.keys {
            match key {
                DuplicateKey::MessageId => {
                    if let (Some(Some(value)), Some(records)) =
                        (check_message_id.as_ref(), message_id_records.as_ref())
                    {
                        if records.iter().any(|record| record.tx_id != tx_id) {
                            return Ok(Some((DuplicateKey::MessageId, value.clone())));
                        }
                    }
                }
                DuplicateKey::EndToEndId => {
                    if let (Some(Some(value)), Some(records)) =
                        (check_end_to_end_id.as_ref(), end_to_end_records.as_ref())
                    {
                        if records.iter().any(|record| record.tx_id != tx_id) {
                            return Ok(Some((DuplicateKey::EndToEndId, value.clone())));
                        }
                    }
                }
                DuplicateKey::Uetr => {
                    if let (Some(Some(value)), Some(records)) =
                        (check_uetr.as_ref(), uetr_records.as_ref())
                    {
                        if records.iter().any(|record| record.tx_id != tx_id) {
                            return Ok(Some((DuplicateKey::Uetr, value.clone())));
                        }
                    }
                }
            }
        }

        Ok(None)
    }
}

#[async_trait]
impl Participant for DuplicateChecker {
    fn name(&self) -> &str {
        "duplicate-checker"
    }

    async fn prepare(&self, ctx: &mut Context) -> Result<Action, ParticipantError> {
        let duplicate = self
            .detect_duplicate(ctx.transaction_id(), ctx.raw_message())
            .await?;
        if let Some((kind, value)) = duplicate {
            let key_label = match kind {
                DuplicateKey::MessageId => "message_id",
                DuplicateKey::EndToEndId => "end_to_end_id",
                DuplicateKey::Uetr => "uetr",
            };
            ctx.put_with_writer("duplicate.detected", self.name(), true);
            ctx.put_with_writer("duplicate.key", self.name(), key_label.to_string());
            ctx.put_with_writer("duplicate.value", self.name(), value);
            return Ok(Action::Aborted);
        }
        Ok(Action::Prepared)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use mx20022_runtime_core::context::{Context, ContextMeta};
    use mx20022_runtime_core::participant::{Action, Participant};
    use mx20022_store::{Store, TransactionRecord};
    use mx20022_store_sqlite::SqliteStore;

    use super::{DuplicateChecker, DuplicateKey};

    fn context(tx_id: &str, raw: &str) -> Context {
        Context::new(ContextMeta {
            transaction_id: tx_id.to_string(),
            received_at: SystemTime::now(),
            pipeline: "p".to_string(),
            source_channel: "c".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: raw.to_string(),
        })
    }

    fn xml(msg_id: &str, e2e: &str, uetr: &str) -> String {
        format!(
            "<Document><FIToFICstmrCdtTrf><GrpHdr><MsgId>{msg_id}</MsgId></GrpHdr><CdtTrfTxInf><PmtId><EndToEndId>{e2e}</EndToEndId><UETR>{uetr}</UETR></PmtId></CdtTrfTxInf></FIToFICstmrCdtTrf></Document>"
        )
    }

    #[tokio::test]
    async fn aborts_when_duplicate_message_id_exists() {
        let store: Arc<dyn Store> =
            Arc::new(SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize"));
        let mut keys = HashMap::new();
        keys.insert("message_id".to_string(), "MSG-1".to_string());
        store
            .begin_transaction(&TransactionRecord {
                tx_id: "TX-OLD".to_string(),
                pipeline: "p".to_string(),
                source_channel: "c".to_string(),
                message_type: "pacs.008".to_string(),
                raw_message: "<Document/>".to_string(),
                state: "COMMITTED".to_string(),
                received_at: SystemTime::now(),
                completed_at: Some(SystemTime::now()),
                key_fields: keys,
            })
            .await
            .expect("seed tx");

        let participant = DuplicateChecker::new(store).with_keys(vec![DuplicateKey::MessageId]);
        let mut ctx = context(
            "TX-NEW",
            xml("MSG-1", "E2E-1", "97ed4827-7b6f-4491-a06f-b548d5a7512d").as_str(),
        );

        let action = participant
            .prepare(&mut ctx)
            .await
            .expect("prepare should return");
        assert_eq!(action, Action::Aborted);
    }

    #[tokio::test]
    async fn prepares_when_no_duplicate_found() {
        let store: Arc<dyn Store> =
            Arc::new(SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize"));
        let participant = DuplicateChecker::new(store);
        let mut ctx = context(
            "TX-NEW",
            xml("MSG-1", "E2E-1", "97ed4827-7b6f-4491-a06f-b548d5a7512d").as_str(),
        );

        let action = participant
            .prepare(&mut ctx)
            .await
            .expect("prepare should return");
        assert_eq!(action, Action::Prepared);
    }

    #[tokio::test]
    async fn prepares_when_message_has_no_duplicate_keys() {
        let store: Arc<dyn Store> =
            Arc::new(SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize"));
        let participant = DuplicateChecker::new(store);
        let mut ctx = context("TX-NEW", "<Document><Any>value</Any></Document>");

        let action = participant
            .prepare(&mut ctx)
            .await
            .expect("prepare should return");
        assert_eq!(action, Action::Prepared);
    }

    #[tokio::test]
    async fn marks_first_matching_duplicate_key() {
        let store: Arc<dyn Store> =
            Arc::new(SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize"));
        let mut msg_keys = HashMap::new();
        msg_keys.insert("message_id".to_string(), "MSG-1".to_string());
        let mut uetr_keys = HashMap::new();
        uetr_keys.insert(
            "uetr".to_string(),
            "97ed4827-7b6f-4491-a06f-b548d5a7512d".to_string(),
        );

        store
            .begin_transaction(&TransactionRecord {
                tx_id: "TX-MSG".to_string(),
                pipeline: "p".to_string(),
                source_channel: "c".to_string(),
                message_type: "pacs.008".to_string(),
                raw_message: "<Document/>".to_string(),
                state: "COMMITTED".to_string(),
                received_at: SystemTime::now(),
                completed_at: Some(SystemTime::now()),
                key_fields: msg_keys,
            })
            .await
            .expect("seed msg");
        store
            .begin_transaction(&TransactionRecord {
                tx_id: "TX-UETR".to_string(),
                pipeline: "p".to_string(),
                source_channel: "c".to_string(),
                message_type: "pacs.008".to_string(),
                raw_message: "<Document/>".to_string(),
                state: "COMMITTED".to_string(),
                received_at: SystemTime::now(),
                completed_at: Some(SystemTime::now()),
                key_fields: uetr_keys,
            })
            .await
            .expect("seed uetr");

        let participant = DuplicateChecker::new(store).with_keys(vec![
            DuplicateKey::MessageId,
            DuplicateKey::EndToEndId,
            DuplicateKey::Uetr,
        ]);
        let mut ctx = context(
            "TX-NEW",
            xml("MSG-1", "E2E-1", "97ed4827-7b6f-4491-a06f-b548d5a7512d").as_str(),
        );

        let action = participant.prepare(&mut ctx).await.expect("prepare");
        assert_eq!(action, Action::Aborted);
        let key = ctx
            .get::<String>("duplicate.key")
            .expect("duplicate key should be recorded");
        assert_eq!(key, "message_id");
    }
}
