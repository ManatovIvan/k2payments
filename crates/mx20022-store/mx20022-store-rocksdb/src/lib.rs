use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use mx20022_store::{
    ContextEntry, DeadLetter, DeadLetterQuery, ExpUpdate, Expectation, Outcome, QueryResult, Store,
    StoreError, StoreHealth, StoreQuery, TransactionRecord, TransactionUpdate,
};
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use serde_json::{json, Map, Value};

pub struct RocksDbStore {
    path: String,
    db: Arc<DB>,
}

impl RocksDbStore {
    pub fn open(path: impl Into<String>) -> Result<Self, StoreError> {
        let path = path.into();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, &path)
            .map_err(|e| StoreError::new(format!("rocksdb open failed at {}: {e}", path)))?;
        Ok(Self {
            path,
            db: Arc::new(db),
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

#[async_trait]
impl Store for RocksDbStore {
    async fn begin_transaction(&self, record: &TransactionRecord) -> Result<(), StoreError> {
        let db = Arc::clone(&self.db);
        let value = transaction_to_value(record);
        let key = format!("tx:{}", record.tx_id);
        run_blocking(move || put_value(&db, &key, &value)).await
    }

    async fn update_transaction(
        &self,
        tx_id: &str,
        update: TransactionUpdate,
    ) -> Result<(), StoreError> {
        let mut record = self
            .find_by_id(tx_id)
            .await?
            .ok_or_else(|| StoreError::new(format!("transaction not found: {tx_id}")))?;

        if let Some(state) = update.state {
            record.state = state;
        }
        if update.error.is_some() && record.completed_at.is_none() {
            record.completed_at = Some(SystemTime::now());
        }
        self.begin_transaction(&record).await
    }

    async fn complete_transaction(&self, tx_id: &str, outcome: Outcome) -> Result<(), StoreError> {
        let state = match outcome {
            Outcome::Committed => "COMMITTED",
            Outcome::Aborted => "ABORTED",
            Outcome::Poison => "POISON",
        };
        self.update_transaction(
            tx_id,
            TransactionUpdate {
                state: Some(state.to_string()),
                error: None,
            },
        )
        .await
    }

    async fn append_context_entry(
        &self,
        tx_id: &str,
        entry: ContextEntry,
    ) -> Result<(), StoreError> {
        let db = Arc::clone(&self.db);
        let written = encode_time(entry.written_at);
        let key = format!("ctx:{tx_id}:{written}:{}", entry.key);
        let value = json!({
            "tx_id": tx_id,
            "key": entry.key,
            "writer": entry.writer,
            "written_at": written,
        });
        run_blocking(move || put_value(&db, &key, &value)).await
    }

    async fn batch_append_context_entries(
        &self,
        tx_id: &str,
        entries: &[ContextEntry],
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let db = Arc::clone(&self.db);
        let tx_id = tx_id.to_string();
        let entries = entries.to_vec();
        run_blocking(move || {
            let mut batch = WriteBatch::default();
            for entry in &entries {
                let written = encode_time(entry.written_at);
                let key = format!("ctx:{tx_id}:{written}:{}", entry.key);
                let value = json!({
                    "tx_id": tx_id,
                    "key": entry.key,
                    "writer": entry.writer,
                    "written_at": written,
                });
                let data = serde_json::to_vec(&value)
                    .map_err(|e| StoreError::new(format!("value encode failed: {e}")))?;
                batch.put(key.as_bytes(), data);
            }
            db.write(batch)
                .map_err(|e| StoreError::new(format!("rocksdb batch write failed: {e}")))?;
            Ok(())
        })
        .await
    }

    async fn list_context_entries(&self, tx_id: &str) -> Result<Vec<ContextEntry>, StoreError> {
        let db = Arc::clone(&self.db);
        let prefix = format!("ctx:{tx_id}:");
        run_blocking(move || {
            let mut entries = Vec::new();
            for item in db.iterator(IteratorMode::From(prefix.as_bytes(), Direction::Forward)) {
                let (key, value) =
                    item.map_err(|e| StoreError::new(format!("rocksdb iterator failed: {e}")))?;
                let key_str = String::from_utf8_lossy(&key);
                if !key_str.starts_with(&prefix) {
                    break;
                }
                let value: Value = serde_json::from_slice(&value)
                    .map_err(|e| StoreError::new(format!("context decode failed: {e}")))?;
                entries.push(ContextEntry {
                    tx_id: value
                        .get("tx_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    key: value
                        .get("key")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    writer: value
                        .get("writer")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    written_at: decode_time(
                        value
                            .get("written_at")
                            .and_then(Value::as_i64)
                            .unwrap_or_default(),
                    ),
                });
            }
            entries.sort_by_key(|entry| encode_time(entry.written_at));
            Ok(entries)
        })
        .await
    }

    async fn find_by_id(&self, tx_id: &str) -> Result<Option<TransactionRecord>, StoreError> {
        let db = Arc::clone(&self.db);
        let key = format!("tx:{tx_id}");
        run_blocking(move || {
            let raw = db
                .get(key)
                .map_err(|e| StoreError::new(format!("rocksdb get failed: {e}")))?;
            match raw {
                Some(bytes) => {
                    let value: Value = serde_json::from_slice(&bytes)
                        .map_err(|e| StoreError::new(format!("transaction decode failed: {e}")))?;
                    Ok(Some(transaction_from_value(&value)?))
                }
                None => Ok(None),
            }
        })
        .await
    }

    async fn find_by_message_id(&self, msg_id: &str) -> Result<Vec<TransactionRecord>, StoreError> {
        find_by_key_field(Arc::clone(&self.db), "message_id", msg_id).await
    }

    async fn find_by_end_to_end_id(
        &self,
        e2e_id: &str,
    ) -> Result<Vec<TransactionRecord>, StoreError> {
        find_by_key_field(Arc::clone(&self.db), "end_to_end_id", e2e_id).await
    }

    async fn find_by_uetr(&self, uetr: &str) -> Result<Vec<TransactionRecord>, StoreError> {
        find_by_key_field(Arc::clone(&self.db), "uetr", uetr).await
    }

    async fn query(&self, filter: StoreQuery) -> Result<QueryResult, StoreError> {
        let db = Arc::clone(&self.db);
        run_blocking(move || {
            let mut records = Vec::new();
            let prefix = "tx:";
            for item in db.iterator(IteratorMode::From(prefix.as_bytes(), Direction::Forward)) {
                let (key, value) =
                    item.map_err(|e| StoreError::new(format!("rocksdb iterator failed: {e}")))?;
                let key_str = String::from_utf8_lossy(&key);
                if !key_str.starts_with(prefix) {
                    break;
                }
                let value: Value = serde_json::from_slice(&value)
                    .map_err(|e| StoreError::new(format!("transaction decode failed: {e}")))?;
                let record = transaction_from_value(&value)?;

                if let Some(ref pipeline) = filter.pipeline {
                    if &record.pipeline != pipeline {
                        continue;
                    }
                }
                if let Some(ref message_type) = filter.message_type {
                    if &record.message_type != message_type {
                        continue;
                    }
                }
                if let Some(ref state) = filter.state {
                    if &record.state != state {
                        continue;
                    }
                }
                if let Some(since) = filter.since {
                    if record.received_at < since {
                        continue;
                    }
                }
                if let Some(until) = filter.until {
                    if record.received_at > until {
                        continue;
                    }
                }
                records.push(record);
                if let Some(limit) = filter.limit {
                    if records.len() >= limit {
                        break;
                    }
                }
            }

            Ok(QueryResult {
                total: records.len(),
                records,
            })
        })
        .await
    }

    async fn save_expectation(&self, exp: &Expectation) -> Result<(), StoreError> {
        let db = Arc::clone(&self.db);
        let key = format!("exp:{}", exp.id);
        let value = json!({
            "id": exp.id,
            "correlation_key": exp.correlation_key,
            "expected_message_type": exp.expected_message_type,
            "timeout_at": encode_time(exp.timeout_at),
            "state": "PENDING",
            "matched_tx_id": Value::Null,
        });
        run_blocking(move || put_value(&db, &key, &value)).await
    }

    async fn load_pending_expectations(&self) -> Result<Vec<Expectation>, StoreError> {
        let db = Arc::clone(&self.db);
        run_blocking(move || {
            let mut out = Vec::new();
            let prefix = "exp:";
            for item in db.iterator(IteratorMode::From(prefix.as_bytes(), Direction::Forward)) {
                let (key, value) =
                    item.map_err(|e| StoreError::new(format!("rocksdb iterator failed: {e}")))?;
                if !String::from_utf8_lossy(&key).starts_with(prefix) {
                    break;
                }
                let value: Value = serde_json::from_slice(&value)
                    .map_err(|e| StoreError::new(format!("expectation decode failed: {e}")))?;
                if value.get("state").and_then(Value::as_str) == Some("PENDING") {
                    out.push(expectation_from_value(&value)?);
                }
            }
            Ok(out)
        })
        .await
    }

    async fn update_expectation(&self, id: &str, update: ExpUpdate) -> Result<(), StoreError> {
        let db = Arc::clone(&self.db);
        let id = id.to_string();
        let key = format!("exp:{id}");
        run_blocking(move || {
            let raw = db
                .get(&key)
                .map_err(|e| StoreError::new(format!("rocksdb get failed: {e}")))?;
            let Some(raw) = raw else {
                return Err(StoreError::new(format!("expectation not found: {}", id)));
            };
            let mut value: Value = serde_json::from_slice(&raw)
                .map_err(|e| StoreError::new(format!("expectation decode failed: {e}")))?;
            if let Some(state) = update.state {
                value["state"] = Value::String(state);
            }
            if let Some(matched_tx_id) = update.matched_tx_id {
                value["matched_tx_id"] = Value::String(matched_tx_id);
            }
            put_value(&db, &key, &value)
        })
        .await
    }

    async fn save_dead_letter(&self, letter: &DeadLetter) -> Result<(), StoreError> {
        let db = Arc::clone(&self.db);
        let key = format!("dl:{}", letter.id);
        let value = json!({
            "id": letter.id,
            "tx_id": letter.tx_id,
            "reason": letter.reason,
            "failed_at": encode_time(letter.failed_at),
            "raw_message": letter.raw_message,
        });
        run_blocking(move || put_value(&db, &key, &value)).await
    }

    async fn list_dead_letters(
        &self,
        filter: DeadLetterQuery,
    ) -> Result<Vec<DeadLetter>, StoreError> {
        let db = Arc::clone(&self.db);
        run_blocking(move || {
            let mut out = Vec::new();
            let prefix = "dl:";
            for item in db.iterator(IteratorMode::From(prefix.as_bytes(), Direction::Forward)) {
                let (key, value) =
                    item.map_err(|e| StoreError::new(format!("rocksdb iterator failed: {e}")))?;
                if !String::from_utf8_lossy(&key).starts_with(prefix) {
                    break;
                }
                let value: Value = serde_json::from_slice(&value)
                    .map_err(|e| StoreError::new(format!("dead letter decode failed: {e}")))?;
                let dead_letter = dead_letter_from_value(&value)?;
                if let Some(ref required_pipeline) = filter.pipeline {
                    if let Some(tx) = load_tx_by_id(&db, &dead_letter.tx_id)? {
                        if &tx.pipeline != required_pipeline {
                            continue;
                        }
                    }
                }
                out.push(dead_letter);
                if let Some(limit) = filter.limit {
                    if out.len() >= limit {
                        break;
                    }
                }
            }
            Ok(out)
        })
        .await
    }

    async fn replay_dead_letter(&self, id: &str) -> Result<(), StoreError> {
        let db = Arc::clone(&self.db);
        let id = id.to_string();
        let key = format!("dl:{id}");
        run_blocking(move || {
            let exists = db
                .get(&key)
                .map_err(|e| StoreError::new(format!("rocksdb get failed: {e}")))?;
            if exists.is_none() {
                return Err(StoreError::new(format!("dead letter not found: {id}")));
            }
            db.delete(&key)
                .map_err(|e| StoreError::new(format!("rocksdb delete failed: {e}")))?;
            Ok(())
        })
        .await
    }

    async fn health(&self) -> Result<StoreHealth, StoreError> {
        let db = Arc::clone(&self.db);
        let _path = self.path.clone();
        run_blocking(move || {
            let _ = db
                .property_value("rocksdb.stats")
                .map_err(|e| StoreError::new(format!("rocksdb health failed: {e}")))?;
            Ok(StoreHealth {
                ok: true,
                backend: "rocksdb".to_string(),
                details: Some("backend=rocksdb".to_string()),
            })
        })
        .await
    }

    async fn compact(&self) -> Result<(), StoreError> {
        let db = Arc::clone(&self.db);
        run_blocking(move || {
            db.compact_range::<&[u8], &[u8]>(None, None);
            Ok(())
        })
        .await
    }
}

async fn find_by_key_field(
    db: Arc<DB>,
    field: &str,
    value: &str,
) -> Result<Vec<TransactionRecord>, StoreError> {
    let field = field.to_string();
    let value = value.to_string();
    run_blocking(move || {
        let mut out = Vec::new();
        let prefix = "tx:";
        for item in db.iterator(IteratorMode::From(prefix.as_bytes(), Direction::Forward)) {
            let (key, val) =
                item.map_err(|e| StoreError::new(format!("rocksdb iterator failed: {e}")))?;
            if !String::from_utf8_lossy(&key).starts_with(prefix) {
                break;
            }
            let json: Value = serde_json::from_slice(&val)
                .map_err(|e| StoreError::new(format!("transaction decode failed: {e}")))?;
            let tx = transaction_from_value(&json)?;
            if tx.key_fields.get(&field).map(String::as_str) == Some(value.as_str()) {
                out.push(tx);
            }
        }
        Ok(out)
    })
    .await
}

async fn run_blocking<T>(
    f: impl FnOnce() -> Result<T, StoreError> + Send + 'static,
) -> Result<T, StoreError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| StoreError::new(format!("rocksdb blocking task failed: {e}")))?
}

fn put_value(db: &DB, key: &str, value: &Value) -> Result<(), StoreError> {
    let data = serde_json::to_vec(value)
        .map_err(|e| StoreError::new(format!("value encode failed: {e}")))?;
    db.put(key.as_bytes(), data)
        .map_err(|e| StoreError::new(format!("rocksdb put failed: {e}")))
}

fn load_tx_by_id(db: &DB, tx_id: &str) -> Result<Option<TransactionRecord>, StoreError> {
    let key = format!("tx:{tx_id}");
    let raw = db
        .get(key)
        .map_err(|e| StoreError::new(format!("rocksdb get failed: {e}")))?;
    match raw {
        Some(bytes) => {
            let value: Value = serde_json::from_slice(&bytes)
                .map_err(|e| StoreError::new(format!("transaction decode failed: {e}")))?;
            Ok(Some(transaction_from_value(&value)?))
        }
        None => Ok(None),
    }
}

fn transaction_to_value(record: &TransactionRecord) -> Value {
    json!({
        "tx_id": record.tx_id,
        "pipeline": record.pipeline,
        "source_channel": record.source_channel,
        "message_type": record.message_type,
        "raw_message": record.raw_message,
        "state": record.state,
        "received_at": encode_time(record.received_at),
        "completed_at": record.completed_at.map(encode_time),
        "key_fields": record.key_fields,
    })
}

fn transaction_from_value(value: &Value) -> Result<TransactionRecord, StoreError> {
    let map = value
        .as_object()
        .ok_or_else(|| StoreError::new("transaction value is not an object"))?;
    Ok(TransactionRecord {
        tx_id: get_string(map, "tx_id")?,
        pipeline: get_string(map, "pipeline")?,
        source_channel: get_string(map, "source_channel")?,
        message_type: get_string(map, "message_type")?,
        raw_message: get_string(map, "raw_message")?,
        state: get_string(map, "state")?,
        received_at: decode_time(get_i64(map, "received_at")?),
        completed_at: map
            .get("completed_at")
            .and_then(Value::as_i64)
            .map(decode_time),
        key_fields: map
            .get("key_fields")
            .and_then(Value::as_object)
            .map(|fields| {
                fields
                    .iter()
                    .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default(),
    })
}

fn expectation_from_value(value: &Value) -> Result<Expectation, StoreError> {
    let map = value
        .as_object()
        .ok_or_else(|| StoreError::new("expectation value is not an object"))?;
    Ok(Expectation {
        id: get_string(map, "id")?,
        correlation_key: get_string(map, "correlation_key")?,
        expected_message_type: get_string(map, "expected_message_type")?,
        timeout_at: decode_time(get_i64(map, "timeout_at")?),
    })
}

fn dead_letter_from_value(value: &Value) -> Result<DeadLetter, StoreError> {
    let map = value
        .as_object()
        .ok_or_else(|| StoreError::new("dead letter value is not an object"))?;
    Ok(DeadLetter {
        id: get_string(map, "id")?,
        tx_id: get_string(map, "tx_id")?,
        reason: get_string(map, "reason")?,
        failed_at: decode_time(get_i64(map, "failed_at")?),
        raw_message: get_string(map, "raw_message")?,
    })
}

fn get_string(map: &Map<String, Value>, key: &str) -> Result<String, StoreError> {
    map.get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| StoreError::new(format!("missing string field `{key}`")))
}

fn get_i64(map: &Map<String, Value>, key: &str) -> Result<i64, StoreError> {
    map.get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| StoreError::new(format!("missing i64 field `{key}`")))
}

fn encode_time(time: SystemTime) -> i64 {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn decode_time(value: i64) -> SystemTime {
    if value <= 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH + Duration::from_millis(value as u64)
}
