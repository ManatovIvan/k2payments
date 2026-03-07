use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use mx20022_store::{
    ContextEntry, DeadLetter, DeadLetterQuery, ExpUpdate, Expectation, Outcome, QueryResult, Store,
    StoreError, StoreHealth, StoreQuery, TransactionRecord, TransactionUpdate,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, QueryBuilder, Row};

const INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS transactions (
    tx_id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL,
    source_channel TEXT NOT NULL,
    message_type TEXT NOT NULL,
    raw_message TEXT NOT NULL,
    state TEXT NOT NULL,
    received_at BIGINT NOT NULL,
    completed_at BIGINT,
    key_fields_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS context_mutations (
    id BIGSERIAL PRIMARY KEY,
    tx_id TEXT NOT NULL REFERENCES transactions(tx_id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    writer TEXT NOT NULL,
    written_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS expectations (
    id TEXT PRIMARY KEY,
    correlation_key TEXT NOT NULL,
    expected_message_type TEXT NOT NULL,
    timeout_at BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'PENDING',
    matched_tx_id TEXT
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id TEXT PRIMARY KEY,
    tx_id TEXT NOT NULL UNIQUE REFERENCES transactions(tx_id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    failed_at BIGINT NOT NULL,
    raw_message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_message_type ON transactions(message_type);
CREATE INDEX IF NOT EXISTS idx_transactions_state ON transactions(state);
CREATE INDEX IF NOT EXISTS idx_transactions_pipeline ON transactions(pipeline);
CREATE INDEX IF NOT EXISTS idx_transactions_key_fields_json_gin ON transactions USING GIN (key_fields_json);
CREATE INDEX IF NOT EXISTS idx_expectations_state_timeout ON expectations(state, timeout_at);
"#;

const ROLLBACK_SQL: &str = r#"
DROP TABLE IF EXISTS dead_letters;
DROP TABLE IF EXISTS expectations;
DROP TABLE IF EXISTS context_mutations;
DROP TABLE IF EXISTS transactions;
"#;

const DEV_SEED_SQL: &str = r#"
INSERT INTO transactions (
    tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json
) VALUES (
    'SEED-TX-1',
    'seed-pipeline',
    'seed-channel',
    'pacs.008.001.13',
    '<Document/>',
    'COMMITTED',
    0,
    0,
    '{"message_id":"SEED-MSG-1","end_to_end_id":"SEED-E2E-1","uetr":"SEED-UETR-1"}'::jsonb
)
ON CONFLICT (tx_id) DO NOTHING;
"#;

pub struct PostgresStore {
    pool: Pool<Postgres>,
}

impl PostgresStore {
    pub async fn connect(database_url: impl Into<String>) -> Result<Self, StoreError> {
        Self::connect_with_pool_size(database_url, None).await
    }

    pub async fn connect_with_pool_size(
        database_url: impl Into<String>,
        pool_size: Option<u32>,
    ) -> Result<Self, StoreError> {
        let database_url = database_url.into();
        let mut pool_builder = PgPoolOptions::new();
        if let Some(size) = pool_size {
            pool_builder = pool_builder.max_connections(size.max(1));
        }
        let pool = pool_builder
            .connect(&database_url)
            .await
            .map_err(|e| StoreError::new(format!("failed to connect postgres: {e}")))?;

        execute_batch(&pool, INIT_SQL, "postgres init").await?;

        Ok(Self { pool })
    }

    pub async fn apply_migrations(&self) -> Result<(), StoreError> {
        execute_batch(&self.pool, INIT_SQL, "postgres migrate").await
    }

    pub async fn rollback_migrations(&self) -> Result<(), StoreError> {
        execute_batch(&self.pool, ROLLBACK_SQL, "postgres rollback").await
    }

    pub async fn apply_dev_seed(&self) -> Result<(), StoreError> {
        execute_batch(&self.pool, DEV_SEED_SQL, "postgres seed").await
    }
}

async fn execute_batch(pool: &Pool<Postgres>, sql: &str, op: &str) -> Result<(), StoreError> {
    for statement in sql.split(';') {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|e| StoreError::new(format!("{op} failed: {e}")))?;
    }

    Ok(())
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

fn encode_key_fields(fields: &HashMap<String, String>) -> serde_json::Value {
    serde_json::to_value(fields).unwrap_or_else(|_| serde_json::json!({}))
}

fn decode_key_fields(value: serde_json::Value) -> HashMap<String, String> {
    serde_json::from_value(value).unwrap_or_default()
}

fn map_transaction_row(row: sqlx::postgres::PgRow) -> Result<TransactionRecord, StoreError> {
    Ok(TransactionRecord {
        tx_id: row
            .try_get("tx_id")
            .map_err(|e| StoreError::new(format!("row map tx_id failed: {e}")))?,
        pipeline: row
            .try_get("pipeline")
            .map_err(|e| StoreError::new(format!("row map pipeline failed: {e}")))?,
        source_channel: row
            .try_get("source_channel")
            .map_err(|e| StoreError::new(format!("row map source_channel failed: {e}")))?,
        message_type: row
            .try_get("message_type")
            .map_err(|e| StoreError::new(format!("row map message_type failed: {e}")))?,
        raw_message: row
            .try_get("raw_message")
            .map_err(|e| StoreError::new(format!("row map raw_message failed: {e}")))?,
        state: row
            .try_get("state")
            .map_err(|e| StoreError::new(format!("row map state failed: {e}")))?,
        received_at: decode_time(
            row.try_get("received_at")
                .map_err(|e| StoreError::new(format!("row map received_at failed: {e}")))?,
        ),
        completed_at: row
            .try_get::<Option<i64>, _>("completed_at")
            .map_err(|e| StoreError::new(format!("row map completed_at failed: {e}")))?
            .map(decode_time),
        key_fields: decode_key_fields(
            row.try_get("key_fields_json")
                .map_err(|e| StoreError::new(format!("row map key_fields_json failed: {e}")))?,
        ),
    })
}

#[async_trait]
impl Store for PostgresStore {
    async fn begin_transaction(&self, record: &TransactionRecord) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO transactions
            (tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (tx_id) DO UPDATE SET
              pipeline = EXCLUDED.pipeline,
              source_channel = EXCLUDED.source_channel,
              message_type = EXCLUDED.message_type,
              raw_message = EXCLUDED.raw_message,
              state = EXCLUDED.state,
              received_at = EXCLUDED.received_at,
              completed_at = EXCLUDED.completed_at,
              key_fields_json = EXCLUDED.key_fields_json",
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

        sqlx::query("UPDATE transactions SET state = $1, completed_at = $2 WHERE tx_id = $3")
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
        sqlx::query(
            "INSERT INTO context_mutations (tx_id, key, writer, written_at) VALUES ($1,$2,$3,$4)",
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
        let mut qb = QueryBuilder::<Postgres>::new(
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
        let rows = sqlx::query(
            "SELECT tx_id, key, writer, written_at FROM context_mutations WHERE tx_id = $1 ORDER BY written_at ASC",
        )
        .bind(tx_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("list_context_entries failed: {e}")))?;

        let mut entries = Vec::new();
        for row in rows {
            entries.push(ContextEntry {
                tx_id: row
                    .try_get("tx_id")
                    .map_err(|e| StoreError::new(format!("context tx_id map failed: {e}")))?,
                key: row
                    .try_get("key")
                    .map_err(|e| StoreError::new(format!("context key map failed: {e}")))?,
                writer: row
                    .try_get("writer")
                    .map_err(|e| StoreError::new(format!("context writer map failed: {e}")))?,
                written_at: decode_time(
                    row.try_get("written_at").map_err(|e| {
                        StoreError::new(format!("context written_at map failed: {e}"))
                    })?,
                ),
            });
        }

        Ok(entries)
    }

    async fn find_by_id(&self, tx_id: &str) -> Result<Option<TransactionRecord>, StoreError> {
        let row = sqlx::query(
            "SELECT tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json
             FROM transactions WHERE tx_id = $1",
        )
        .bind(tx_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("find_by_id failed: {e}")))?;

        row.map(map_transaction_row).transpose()
    }

    async fn find_by_message_id(&self, msg_id: &str) -> Result<Vec<TransactionRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json
             FROM transactions WHERE key_fields_json->>'message_id' = $1",
        )
        .bind(msg_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("find_by_message_id failed: {e}")))?;

        rows.into_iter()
            .map(map_transaction_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn find_by_end_to_end_id(
        &self,
        e2e_id: &str,
    ) -> Result<Vec<TransactionRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json
             FROM transactions WHERE key_fields_json->>'end_to_end_id' = $1",
        )
        .bind(e2e_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("find_by_end_to_end_id failed: {e}")))?;

        rows.into_iter()
            .map(map_transaction_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn find_by_uetr(&self, uetr: &str) -> Result<Vec<TransactionRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json
             FROM transactions WHERE key_fields_json->>'uetr' = $1",
        )
        .bind(uetr)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::new(format!("find_by_uetr failed: {e}")))?;

        rows.into_iter()
            .map(map_transaction_row)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn query(&self, filter: StoreQuery) -> Result<QueryResult, StoreError> {
        let mut qb = QueryBuilder::<Postgres>::new(
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
            qb.push("received_at >= ");
            qb.push_bind(encode_time(since));
            has_where = true;
        }
        if let Some(until) = filter.until {
            qb.push(if has_where { " AND " } else { " WHERE " });
            qb.push("received_at <= ");
            qb.push_bind(encode_time(until));
        }

        qb.push(" ORDER BY received_at DESC");
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
        sqlx::query(
            "INSERT INTO expectations
             (id, correlation_key, expected_message_type, timeout_at, state, matched_tx_id)
             VALUES ($1,$2,$3,$4,$5,$6)
             ON CONFLICT (id) DO UPDATE SET
               correlation_key = EXCLUDED.correlation_key,
               expected_message_type = EXCLUDED.expected_message_type,
               timeout_at = EXCLUDED.timeout_at,
               state = EXCLUDED.state,
               matched_tx_id = EXCLUDED.matched_tx_id",
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
                    .map_err(|e| StoreError::new(format!("expectation id map failed: {e}")))?,
                correlation_key: row.try_get("correlation_key").map_err(|e| {
                    StoreError::new(format!("expectation correlation_key map failed: {e}"))
                })?,
                expected_message_type: row.try_get("expected_message_type").map_err(|e| {
                    StoreError::new(format!("expectation expected_message_type map failed: {e}"))
                })?,
                timeout_at: decode_time(row.try_get("timeout_at").map_err(|e| {
                    StoreError::new(format!("expectation timeout_at map failed: {e}"))
                })?),
            });
        }

        Ok(result)
    }

    async fn count_pending_expectations(&self) -> Result<usize, StoreError> {
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
        sqlx::query(
            "UPDATE expectations SET state = COALESCE($1, state), matched_tx_id = COALESCE($2, matched_tx_id) WHERE id = $3",
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
        sqlx::query(
            "INSERT INTO dead_letters (id, tx_id, reason, failed_at, raw_message)
             VALUES ($1,$2,$3,$4,$5)
             ON CONFLICT (id) DO UPDATE SET
               tx_id = EXCLUDED.tx_id,
               reason = EXCLUDED.reason,
               failed_at = EXCLUDED.failed_at,
               raw_message = EXCLUDED.raw_message",
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
        let mut qb = QueryBuilder::<Postgres>::new(
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
                    id: row
                        .try_get("id")
                        .map_err(|e| StoreError::new(format!("dead letter id map failed: {e}")))?,
                    tx_id: row.try_get("tx_id").map_err(|e| {
                        StoreError::new(format!("dead letter tx_id map failed: {e}"))
                    })?,
                    reason: row.try_get("reason").map_err(|e| {
                        StoreError::new(format!("dead letter reason map failed: {e}"))
                    })?,
                    failed_at: decode_time(row.try_get("failed_at").map_err(|e| {
                        StoreError::new(format!("dead letter failed_at map failed: {e}"))
                    })?),
                    raw_message: row.try_get("raw_message").map_err(|e| {
                        StoreError::new(format!("dead letter raw_message map failed: {e}"))
                    })?,
                })
            })
            .collect()
    }

    async fn replay_dead_letter(&self, id: &str) -> Result<(), StoreError> {
        let result = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
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
        let mut qb = QueryBuilder::<Postgres>::new(
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
        if states.is_empty() {
            return Ok(0);
        }
        let mut qb = QueryBuilder::<Postgres>::new(
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
        let _ = sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StoreError::new(format!("health check failed: {e}")))?;

        Ok(StoreHealth {
            ok: true,
            backend: "postgres".to_string(),
            details: Some("backend=postgres".to_string()),
        })
    }

    async fn compact(&self) -> Result<(), StoreError> {
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
        TransactionRecord, TransactionUpdate,
    };

    use super::PostgresStore;

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

    async fn connect_or_skip() -> Option<PostgresStore> {
        let url = match std::env::var("DATABASE_URL") {
            Ok(url) if url.starts_with("postgres") => url,
            _ => return None,
        };
        PostgresStore::connect(url).await.ok()
    }

    #[tokio::test]
    async fn roundtrip_transaction() {
        let Some(store) = connect_or_skip().await else {
            eprintln!("skipping postgres test: DATABASE_URL not set");
            return;
        };

        let tx = record("pg-1");
        store
            .begin_transaction(&tx)
            .await
            .expect("begin should work");

        let found = store
            .find_by_id("pg-1")
            .await
            .expect("query should work")
            .expect("record should exist");
        assert_eq!(found.tx_id, "pg-1");
        assert_eq!(found.state, "RECEIVED");

        store
            .update_transaction(
                "pg-1",
                TransactionUpdate {
                    state: Some("PREPARING".to_string()),
                    error: None,
                },
            )
            .await
            .expect("update should work");

        store
            .complete_transaction("pg-1", Outcome::Committed)
            .await
            .expect("complete should work");

        let updated = store
            .find_by_id("pg-1")
            .await
            .expect("query should work")
            .expect("record should exist");
        assert_eq!(updated.state, "COMMITTED");
        assert!(updated.completed_at.is_some());

        // cleanup
        let _ = sqlx::query("DELETE FROM transactions WHERE tx_id = 'pg-1'")
            .execute(&store.pool)
            .await;
    }

    #[tokio::test]
    async fn expectation_and_dead_letter_roundtrip() {
        let Some(store) = connect_or_skip().await else {
            eprintln!("skipping postgres test: DATABASE_URL not set");
            return;
        };

        let tx = record("pg-2");
        store
            .begin_transaction(&tx)
            .await
            .expect("begin should work");

        store
            .append_context_entry(
                "pg-2",
                ContextEntry {
                    tx_id: "pg-2".to_string(),
                    key: "routing.destination".to_string(),
                    writer: "routing-engine".to_string(),
                    written_at: SystemTime::now(),
                },
            )
            .await
            .expect("append context should work");

        let entries = store
            .list_context_entries("pg-2")
            .await
            .expect("list context should work");
        assert!(!entries.is_empty());

        store
            .save_expectation(&Expectation {
                id: "EXP-PG-1".to_string(),
                correlation_key: "MSG-pg-2".to_string(),
                expected_message_type: "pacs.002".to_string(),
                timeout_at: SystemTime::now(),
            })
            .await
            .expect("save expectation should work");

        store
            .update_expectation(
                "EXP-PG-1",
                ExpUpdate {
                    state: Some("MATCHED".to_string()),
                    matched_tx_id: Some("pg-2".to_string()),
                },
            )
            .await
            .expect("update expectation should work");

        store
            .save_dead_letter(&DeadLetter {
                id: "DL-PG-1".to_string(),
                tx_id: "pg-2".to_string(),
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
        assert!(!dead_letters.is_empty());

        store
            .replay_dead_letter("DL-PG-1")
            .await
            .expect("replay dead letter should work");

        // cleanup
        let _ = sqlx::query("DELETE FROM expectations WHERE id = 'EXP-PG-1'")
            .execute(&store.pool)
            .await;
        let _ = sqlx::query("DELETE FROM context_mutations WHERE tx_id = 'pg-2'")
            .execute(&store.pool)
            .await;
        let _ = sqlx::query("DELETE FROM transactions WHERE tx_id = 'pg-2'")
            .execute(&store.pool)
            .await;
    }
}
