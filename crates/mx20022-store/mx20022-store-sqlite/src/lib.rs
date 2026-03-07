// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use mx20022_store::{
    ContextEntry, DeadLetter, DeadLetterQuery, ExpUpdate, Expectation, Outcome, QueryResult, Store,
    StoreError, StoreHealth, StoreQuery, TransactionRecord, TransactionUpdate,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, QueryBuilder, Row, Sqlite};
use tokio::sync::OnceCell;

#[cfg(test)]
mod tests_missing;

pub const MIGRATION_0001_UP: &str = include_str!("../migrations/0001_initial_schema.up.sql");
pub const MIGRATION_0001_DOWN: &str = include_str!("../migrations/0001_initial_schema.down.sql");
pub const DEV_SEED_SQL: &str = include_str!("../seeds/dev_seed.sql");

pub struct SqliteStore {
    pool: Pool<Sqlite>,
    initialized: OnceCell<()>,
}

impl SqliteStore {
    pub fn new(database_url: impl Into<String>) -> Result<Self, StoreError> {
        Self::with_pool_size(database_url, None)
    }

    pub fn with_pool_size(
        database_url: impl Into<String>,
        pool_size: Option<u32>,
    ) -> Result<Self, StoreError> {
        let database_url = database_url.into();
        let connect_url = normalize_sqlite_url(&database_url);
        let connect_options = SqliteConnectOptions::from_str(&connect_url)
            .map_err(|e| StoreError::new(format!("invalid sqlite url `{database_url}`: {e}")))?
            .create_if_missing(true);
        let mut pool_options = SqlitePoolOptions::new();
        if let Some(size) = pool_size {
            pool_options = pool_options.max_connections(size.max(1));
        }
        let pool = pool_options.connect_lazy_with(connect_options);

        Ok(Self {
            pool,
            initialized: OnceCell::new(),
        })
    }

    pub async fn apply_migrations(&self) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        execute_batch(&self.pool, MIGRATION_0001_UP).await
    }

    pub async fn rollback_migrations(&self) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        execute_batch(&self.pool, MIGRATION_0001_DOWN).await
    }

    pub async fn apply_dev_seed(&self) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        execute_batch(&self.pool, DEV_SEED_SQL).await
    }

    async fn find_by_key_field(
        &self,
        field: &str,
        value: &str,
    ) -> Result<Vec<TransactionRecord>, StoreError> {
        self.ensure_initialized().await?;
        let rows = sqlx::query(
            "SELECT tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json
             FROM transactions WHERE json_extract(key_fields_json, '$.' || ?1) = ?2",
        )
        .bind(field)
        .bind(value)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("find_by_key_field failed: {e}")))?;

        rows.into_iter()
            .map(map_transaction_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn ensure_initialized(&self) -> Result<(), StoreError> {
        self.initialized
            .get_or_try_init(|| async {
                sqlx::query("PRAGMA foreign_keys = ON")
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StoreError::new(format!("failed to enable foreign_keys: {e}")))?;
                sqlx::query("PRAGMA journal_mode = WAL")
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StoreError::new(format!("failed to enable WAL mode: {e}")))?;
                execute_batch(&self.pool, MIGRATION_0001_UP).await?;
                Ok(())
            })
            .await?;

        Ok(())
    }
}

fn normalize_sqlite_url(url: &str) -> String {
    if url == "sqlite::memory:" {
        "sqlite::memory:".to_string()
    } else if url.starts_with("sqlite:") {
        url.to_string()
    } else {
        format!("sqlite:{url}")
    }
}

async fn execute_batch(pool: &Pool<Sqlite>, sql: &str) -> Result<(), StoreError> {
    for statement in sql.split(';') {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }

        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|e| StoreError::new(format!("sql batch statement failed: {e}")))?;
    }

    Ok(())
}

fn encode_time(time: SystemTime) -> String {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis();
    millis.to_string()
}

fn encode_time_i64(time: SystemTime) -> i64 {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn decode_time(value: &str) -> SystemTime {
    let millis = value.parse::<u128>().unwrap_or(0);
    let millis_u64 = u64::try_from(millis).unwrap_or(u64::MAX);
    UNIX_EPOCH + Duration::from_millis(millis_u64)
}

fn encode_key_fields(fields: &HashMap<String, String>) -> String {
    serde_json::to_string(fields).unwrap_or_else(|_| "{}".to_string())
}

fn decode_key_fields(raw: &str) -> HashMap<String, String> {
    serde_json::from_str(raw).unwrap_or_default()
}

fn map_transaction_row(row: sqlx::sqlite::SqliteRow) -> Result<TransactionRecord, StoreError> {
    let key_fields_json = row
        .try_get::<String, _>("key_fields_json")
        .map_err(|e| StoreError::new(format!("row mapping key_fields_json failed: {e}")))?;

    Ok(TransactionRecord {
        tx_id: row
            .try_get("tx_id")
            .map_err(|e| StoreError::new(format!("row mapping tx_id failed: {e}")))?,
        pipeline: row
            .try_get("pipeline")
            .map_err(|e| StoreError::new(format!("row mapping pipeline failed: {e}")))?,
        source_channel: row
            .try_get("source_channel")
            .map_err(|e| StoreError::new(format!("row mapping source_channel failed: {e}")))?,
        message_type: row
            .try_get("message_type")
            .map_err(|e| StoreError::new(format!("row mapping message_type failed: {e}")))?,
        raw_message: row
            .try_get("raw_message")
            .map_err(|e| StoreError::new(format!("row mapping raw_message failed: {e}")))?,
        state: row
            .try_get("state")
            .map_err(|e| StoreError::new(format!("row mapping state failed: {e}")))?,
        received_at: decode_time(
            &row.try_get::<String, _>("received_at")
                .map_err(|e| StoreError::new(format!("row mapping received_at failed: {e}")))?,
        ),
        completed_at: row
            .try_get::<Option<String>, _>("completed_at")
            .map_err(|e| StoreError::new(format!("row mapping completed_at failed: {e}")))?
            .map(|v| decode_time(&v)),
        key_fields: decode_key_fields(&key_fields_json),
    })
}

#[async_trait]
impl Store for SqliteStore {
    async fn begin_transaction(&self, record: &TransactionRecord) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        sqlx::query(
            "INSERT OR REPLACE INTO transactions
             (tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .bind(&record.tx_id)
        .bind(&record.pipeline)
        .bind(&record.source_channel)
        .bind(&record.message_type)
        .bind(&record.raw_message)
        .bind(&record.state)
        .bind(encode_time(record.received_at))
        .bind(record.completed_at.map(encode_time))
        .bind(encode_key_fields(&record.key_fields))
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("begin_transaction failed: {e}")))?;

        Ok(())
    }

    async fn update_transaction(
        &self,
        tx_id: &str,
        update: TransactionUpdate,
    ) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        let mark_completed = update.error.is_some();
        let completed_at = mark_completed.then(|| encode_time(SystemTime::now()));
        let result = sqlx::query(
            "UPDATE transactions
             SET state = COALESCE(?1, state),
                 completed_at = CASE
                    WHEN ?2 THEN COALESCE(completed_at, ?3)
                    ELSE completed_at
                 END
             WHERE tx_id = ?4",
        )
        .bind(update.state)
        .bind(mark_completed)
        .bind(completed_at)
        .bind(tx_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("update_transaction failed: {e}")))?;
        if result.rows_affected() == 0 {
            return Err(StoreError::new(format!("transaction not found: {tx_id}")));
        }
        Ok(())
    }

    async fn complete_transaction(&self, tx_id: &str, outcome: Outcome) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        let state = match outcome {
            Outcome::Committed => "COMMITTED",
            Outcome::Aborted => "ABORTED",
            Outcome::Poison => "POISON",
        };

        sqlx::query("UPDATE transactions SET state = ?1, completed_at = ?2 WHERE tx_id = ?3")
            .bind(state)
            .bind(encode_time(SystemTime::now()))
            .bind(tx_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::new(format!("complete_transaction failed: {e}")))?;

        Ok(())
    }

    async fn append_context_entry(
        &self,
        tx_id: &str,
        entry: ContextEntry,
    ) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        sqlx::query(
            "INSERT INTO context_mutations (tx_id, key, writer, written_at) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(tx_id)
        .bind(entry.key)
        .bind(entry.writer)
        .bind(encode_time(entry.written_at))
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("append_context_entry failed: {e}")))?;

        Ok(())
    }

    async fn batch_append_context_entries(
        &self,
        tx_id: &str,
        entries: &[ContextEntry],
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        self.ensure_initialized().await?;
        let mut qb = QueryBuilder::<Sqlite>::new(
            "INSERT INTO context_mutations (tx_id, key, writer, written_at) ",
        );
        qb.push_values(entries, |mut b, entry| {
            b.push_bind(tx_id.to_string())
                .push_bind(entry.key.clone())
                .push_bind(entry.writer.clone())
                .push_bind(encode_time(entry.written_at));
        });
        qb.build()
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::new(format!("batch_append_context_entries failed: {e}")))?;
        Ok(())
    }

    async fn list_context_entries(&self, tx_id: &str) -> Result<Vec<ContextEntry>, StoreError> {
        self.ensure_initialized().await?;
        let rows = sqlx::query(
            "SELECT tx_id, key, writer, written_at FROM context_mutations WHERE tx_id = ?1 ORDER BY CAST(written_at AS INTEGER) ASC",
        )
        .bind(tx_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("list_context_entries failed: {e}")))?;

        let mut entries = Vec::new();
        for row in rows {
            let written_at_raw: String = row.try_get("written_at").map_err(|e| {
                StoreError::new(format!("context row mapping written_at failed: {e}"))
            })?;
            entries.push(ContextEntry {
                tx_id: row.try_get("tx_id").map_err(|e| {
                    StoreError::new(format!("context row mapping tx_id failed: {e}"))
                })?,
                key: row
                    .try_get("key")
                    .map_err(|e| StoreError::new(format!("context row mapping key failed: {e}")))?,
                writer: row.try_get("writer").map_err(|e| {
                    StoreError::new(format!("context row mapping writer failed: {e}"))
                })?,
                written_at: decode_time(&written_at_raw),
            });
        }

        Ok(entries)
    }

    async fn find_by_id(&self, tx_id: &str) -> Result<Option<TransactionRecord>, StoreError> {
        self.ensure_initialized().await?;
        let row = sqlx::query(
            "SELECT tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json
             FROM transactions WHERE tx_id = ?1",
        )
        .bind(tx_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("find_by_id failed: {e}")))?;

        row.map(map_transaction_row).transpose()
    }

    async fn find_by_message_id(&self, msg_id: &str) -> Result<Vec<TransactionRecord>, StoreError> {
        self.find_by_key_field("message_id", msg_id).await
    }

    async fn find_by_end_to_end_id(
        &self,
        e2e_id: &str,
    ) -> Result<Vec<TransactionRecord>, StoreError> {
        self.find_by_key_field("end_to_end_id", e2e_id).await
    }

    async fn find_by_uetr(&self, uetr: &str) -> Result<Vec<TransactionRecord>, StoreError> {
        self.find_by_key_field("uetr", uetr).await
    }

    async fn query(&self, filter: StoreQuery) -> Result<QueryResult, StoreError> {
        self.ensure_initialized().await?;
        let mut qb = QueryBuilder::<Sqlite>::new(
            "SELECT tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json FROM transactions",
        );
        let mut has_where = false;

        if let Some(pipeline) = &filter.pipeline {
            qb.push(if has_where { " AND " } else { " WHERE " });
            qb.push("pipeline = ");
            qb.push_bind(pipeline);
            has_where = true;
        }
        if let Some(message_type) = &filter.message_type {
            qb.push(if has_where { " AND " } else { " WHERE " });
            qb.push("message_type = ");
            qb.push_bind(message_type);
            has_where = true;
        }
        if let Some(state) = &filter.state {
            qb.push(if has_where { " AND " } else { " WHERE " });
            qb.push("state = ");
            qb.push_bind(state);
            has_where = true;
        }
        if let Some(since) = filter.since {
            qb.push(if has_where { " AND " } else { " WHERE " });
            qb.push("CAST(received_at AS INTEGER) >= ");
            qb.push_bind(encode_time_i64(since));
            has_where = true;
        }
        if let Some(until) = filter.until {
            qb.push(if has_where { " AND " } else { " WHERE " });
            qb.push("CAST(received_at AS INTEGER) <= ");
            qb.push_bind(encode_time_i64(until));
        }

        qb.push(" ORDER BY CAST(received_at AS INTEGER) DESC");
        if let Some(limit) = filter.limit {
            qb.push(" LIMIT ");
            qb.push_bind(i64::try_from(limit).unwrap_or(i64::MAX));
        }

        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::new(format!("query failed: {e}")))?;

        let mut records = Vec::new();
        for row in rows {
            records.push(map_transaction_row(row)?);
        }

        Ok(QueryResult {
            total: records.len(),
            records,
        })
    }

    async fn save_expectation(&self, exp: &Expectation) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        sqlx::query(
            "INSERT OR REPLACE INTO expectations
             (id, correlation_key, expected_message_type, timeout_at, state, matched_tx_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(&exp.id)
        .bind(&exp.correlation_key)
        .bind(&exp.expected_message_type)
        .bind(encode_time(exp.timeout_at))
        .bind("PENDING")
        .bind(Option::<String>::None)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("save_expectation failed: {e}")))?;

        Ok(())
    }

    async fn load_pending_expectations(&self) -> Result<Vec<Expectation>, StoreError> {
        self.ensure_initialized().await?;
        let rows = sqlx::query(
            "SELECT id, correlation_key, expected_message_type, timeout_at
             FROM expectations WHERE state = 'PENDING' ORDER BY timeout_at ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("load_pending_expectations failed: {e}")))?;

        let mut result = Vec::new();
        for row in rows {
            result.push(Expectation {
                id: row
                    .try_get("id")
                    .map_err(|e| StoreError::new(format!("expectation id mapping failed: {e}")))?,
                correlation_key: row.try_get("correlation_key").map_err(|e| {
                    StoreError::new(format!("expectation correlation_key mapping failed: {e}"))
                })?,
                expected_message_type: row.try_get("expected_message_type").map_err(|e| {
                    StoreError::new(format!(
                        "expectation expected_message_type mapping failed: {e}"
                    ))
                })?,
                timeout_at: decode_time(&row.try_get::<String, _>("timeout_at").map_err(|e| {
                    StoreError::new(format!("expectation timeout_at mapping failed: {e}"))
                })?),
            });
        }

        Ok(result)
    }

    async fn count_pending_expectations(&self) -> Result<usize, StoreError> {
        self.ensure_initialized().await?;
        let row = sqlx::query("SELECT COUNT(*) as total FROM expectations WHERE state = 'PENDING'")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StoreError::new(format!("count_pending_expectations failed: {e}")))?;
        let total = row.try_get::<i64, _>("total").map_err(|e| {
            StoreError::new(format!("count_pending_expectations mapping failed: {e}"))
        })?;
        Ok(usize::try_from(total.max(0)).unwrap_or(usize::MAX))
    }

    async fn update_expectation(&self, id: &str, update: ExpUpdate) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        sqlx::query(
            "UPDATE expectations SET state = COALESCE(?1, state), matched_tx_id = COALESCE(?2, matched_tx_id) WHERE id = ?3",
        )
        .bind(update.state)
        .bind(update.matched_tx_id)
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("update_expectation failed: {e}")))?;

        Ok(())
    }

    async fn save_dead_letter(&self, letter: &DeadLetter) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        sqlx::query(
            "INSERT OR REPLACE INTO dead_letters (id, tx_id, reason, failed_at, raw_message)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(&letter.id)
        .bind(&letter.tx_id)
        .bind(&letter.reason)
        .bind(encode_time(letter.failed_at))
        .bind(&letter.raw_message)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("save_dead_letter failed: {e}")))?;

        Ok(())
    }

    async fn list_dead_letters(
        &self,
        filter: DeadLetterQuery,
    ) -> Result<Vec<DeadLetter>, StoreError> {
        self.ensure_initialized().await?;
        let mut qb = QueryBuilder::<Sqlite>::new(
            "SELECT dl.id, dl.tx_id, dl.reason, dl.failed_at, dl.raw_message
             FROM dead_letters dl
             LEFT JOIN transactions tx ON tx.tx_id = dl.tx_id",
        );
        if let Some(pipeline) = &filter.pipeline {
            qb.push(" WHERE tx.pipeline = ");
            qb.push_bind(pipeline);
        }
        qb.push(" ORDER BY dl.failed_at DESC");
        if let Some(limit) = filter.limit {
            qb.push(" LIMIT ");
            qb.push_bind(i64::try_from(limit).unwrap_or(i64::MAX));
        }

        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::new(format!("list_dead_letters failed: {e}")))?;

        rows.into_iter()
            .map(|row| {
                Ok(DeadLetter {
                    id: row.try_get("id").map_err(|e| {
                        StoreError::new(format!("dead letter id mapping failed: {e}"))
                    })?,
                    tx_id: row.try_get("tx_id").map_err(|e| {
                        StoreError::new(format!("dead letter tx_id mapping failed: {e}"))
                    })?,
                    reason: row.try_get("reason").map_err(|e| {
                        StoreError::new(format!("dead letter reason mapping failed: {e}"))
                    })?,
                    failed_at: decode_time(&row.try_get::<String, _>("failed_at").map_err(
                        |e| StoreError::new(format!("dead letter failed_at mapping failed: {e}")),
                    )?),
                    raw_message: row.try_get("raw_message").map_err(|e| {
                        StoreError::new(format!("dead letter raw_message mapping failed: {e}"))
                    })?,
                })
            })
            .collect()
    }

    async fn replay_dead_letter(&self, id: &str) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        let result = sqlx::query("DELETE FROM dead_letters WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::new(format!("replay_dead_letter failed: {e}")))?;

        if result.rows_affected() == 0 {
            return Err(StoreError::new(format!("dead letter not found: {id}")));
        }

        Ok(())
    }

    async fn count_dead_letters(&self, pipeline: Option<&str>) -> Result<usize, StoreError> {
        self.ensure_initialized().await?;
        let mut qb = QueryBuilder::<Sqlite>::new(
            "SELECT COUNT(*) as total
             FROM dead_letters dl
             LEFT JOIN transactions tx ON tx.tx_id = dl.tx_id",
        );
        if let Some(pipeline) = pipeline {
            qb.push(" WHERE tx.pipeline = ");
            qb.push_bind(pipeline);
        }
        let row = qb
            .build()
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StoreError::new(format!("count_dead_letters failed: {e}")))?;
        let total = row
            .try_get::<i64, _>("total")
            .map_err(|e| StoreError::new(format!("count_dead_letters mapping failed: {e}")))?;
        Ok(usize::try_from(total.max(0)).unwrap_or(usize::MAX))
    }

    async fn count_transactions_by_states(&self, states: &[&str]) -> Result<usize, StoreError> {
        self.ensure_initialized().await?;
        if states.is_empty() {
            return Ok(0);
        }
        let mut qb = QueryBuilder::<Sqlite>::new(
            "SELECT COUNT(*) as total FROM transactions WHERE state IN (",
        );
        {
            let mut separated = qb.separated(", ");
            for state in states {
                separated.push_bind(*state);
            }
        }
        qb.push(")");
        let row =
            qb.build().fetch_one(&self.pool).await.map_err(|e| {
                StoreError::new(format!("count_transactions_by_states failed: {e}"))
            })?;
        let total = row.try_get::<i64, _>("total").map_err(|e| {
            StoreError::new(format!("count_transactions_by_states mapping failed: {e}"))
        })?;
        Ok(usize::try_from(total.max(0)).unwrap_or(usize::MAX))
    }

    async fn health(&self) -> Result<StoreHealth, StoreError> {
        self.ensure_initialized().await?;
        let _ = sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StoreError::new(format!("health check failed: {e}")))?;

        Ok(StoreHealth {
            ok: true,
            backend: "sqlite".to_string(),
            details: Some("backend=sqlite".to_string()),
        })
    }

    async fn compact(&self) -> Result<(), StoreError> {
        self.ensure_initialized().await?;
        sqlx::query("VACUUM")
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::new(format!("compact failed: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use mx20022_store::{
        ContextEntry, DeadLetter, DeadLetterQuery, ExpUpdate, Expectation, Outcome, Store,
        StoreQuery, TransactionRecord, TransactionUpdate,
    };

    use crate::SqliteStore;

    fn record(tx_id: &str) -> TransactionRecord {
        let mut key_fields = HashMap::new();
        key_fields.insert("message_id".to_string(), format!("MSG-{tx_id}"));
        key_fields.insert("end_to_end_id".to_string(), format!("E2E-{tx_id}"));
        key_fields.insert("uetr".to_string(), format!("UETR-{tx_id}"));

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

    #[tokio::test]
    async fn begin_find_update_complete_transaction() {
        let store = SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize");

        let tx = record("1");
        store
            .begin_transaction(&tx)
            .await
            .expect("begin should work");

        let found = store
            .find_by_id("1")
            .await
            .expect("query should work")
            .expect("record should exist");
        assert_eq!(found.tx_id, "1");
        assert_eq!(found.state, "RECEIVED");

        store
            .update_transaction(
                "1",
                TransactionUpdate {
                    state: Some("PREPARING".to_string()),
                    error: None,
                },
            )
            .await
            .expect("update should work");

        store
            .complete_transaction("1", Outcome::Committed)
            .await
            .expect("complete should work");

        let updated = store
            .find_by_id("1")
            .await
            .expect("query should work")
            .expect("record should exist");
        assert_eq!(updated.state, "COMMITTED");
        assert!(updated.completed_at.is_some());
    }

    #[tokio::test]
    async fn expectation_and_dead_letter_round_trip() {
        let store = SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize");
        let tx = record("2");
        store
            .begin_transaction(&tx)
            .await
            .expect("begin should work");

        store
            .append_context_entry(
                "2",
                ContextEntry {
                    tx_id: "2".to_string(),
                    key: "routing.destination".to_string(),
                    writer: "routing-engine".to_string(),
                    written_at: SystemTime::now(),
                },
            )
            .await
            .expect("append context should work");

        let entries = store
            .list_context_entries("2")
            .await
            .expect("list context should work");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].writer, "routing-engine");

        store
            .save_expectation(&Expectation {
                id: "EXP-1".to_string(),
                correlation_key: "MSG-2".to_string(),
                expected_message_type: "pacs.002".to_string(),
                timeout_at: SystemTime::now(),
            })
            .await
            .expect("save expectation should work");

        let pending = store
            .load_pending_expectations()
            .await
            .expect("load pending should work");
        assert_eq!(pending.len(), 1);

        store
            .update_expectation(
                "EXP-1",
                ExpUpdate {
                    state: Some("MATCHED".to_string()),
                    matched_tx_id: Some("2".to_string()),
                },
            )
            .await
            .expect("update expectation should work");

        store
            .save_dead_letter(&DeadLetter {
                id: "DL-1".to_string(),
                tx_id: "2".to_string(),
                reason: "test failure".to_string(),
                failed_at: SystemTime::now(),
                raw_message: "<Document/>".to_string(),
            })
            .await
            .expect("save dead letter should work");

        let dead_letters = store
            .list_dead_letters(DeadLetterQuery {
                pipeline: Some("demo".to_string()),
                limit: Some(10),
            })
            .await
            .expect("list dead letters should work");
        assert_eq!(dead_letters.len(), 1);

        store
            .replay_dead_letter("DL-1")
            .await
            .expect("replay dead letter should work");

        let dead_letters_after = store
            .list_dead_letters(DeadLetterQuery {
                pipeline: Some("demo".to_string()),
                limit: Some(10),
            })
            .await
            .expect("list dead letters should work");
        assert!(dead_letters_after.is_empty());
    }

    #[tokio::test]
    async fn query_applies_sql_filters_and_limit() {
        let store = SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize");
        let now = SystemTime::now();

        let mut a = record("q-a");
        a.pipeline = "alpha".to_string();
        a.state = "PREPARING".to_string();
        a.received_at = now;

        let mut b = record("q-b");
        b.pipeline = "beta".to_string();
        b.state = "PREPARING".to_string();
        b.received_at = now;

        let mut c = record("q-c");
        c.pipeline = "alpha".to_string();
        c.state = "COMMITTED".to_string();
        c.received_at = now;

        store.begin_transaction(&a).await.expect("insert a");
        store.begin_transaction(&b).await.expect("insert b");
        store.begin_transaction(&c).await.expect("insert c");

        let result = store
            .query(StoreQuery {
                pipeline: Some("alpha".to_string()),
                message_type: None,
                state: Some("PREPARING".to_string()),
                since: Some(now),
                until: None,
                limit: Some(1),
            })
            .await
            .expect("query should succeed");

        assert_eq!(result.records.len(), 1);
        assert_eq!(result.total, 1);
        assert_eq!(result.records[0].pipeline, "alpha");
        assert_eq!(result.records[0].state, "PREPARING");
    }
}
