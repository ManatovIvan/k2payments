use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use mx20022_store::{ExpUpdate, Expectation, Store};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CorrelationLookupKey {
    pub correlation_key: String,
    pub expected_message_type: String,
}

#[derive(Debug, Clone)]
struct IndexedExpectation {
    expectation_id: String,
    timeout_at: SystemTime,
}

pub struct CorrelationEngine {
    store: Arc<dyn Store>,
    index: Arc<RwLock<HashMap<CorrelationLookupKey, IndexedExpectation>>>,
}

impl CorrelationEngine {
    pub async fn new(store: Arc<dyn Store>) -> Result<Self, CorrelationError> {
        let engine = Self {
            store,
            index: Arc::new(RwLock::new(HashMap::new())),
        };

        engine.reload_pending().await?;
        Ok(engine)
    }

    pub async fn reload_pending(&self) -> Result<(), CorrelationError> {
        let pending = self
            .store
            .load_pending_expectations()
            .await
            .map_err(CorrelationError::Store)?;

        let mut index = self.index.write().await;
        index.clear();

        for exp in pending {
            index.insert(
                CorrelationLookupKey {
                    correlation_key: exp.correlation_key,
                    expected_message_type: exp.expected_message_type,
                },
                IndexedExpectation {
                    expectation_id: exp.id,
                    timeout_at: exp.timeout_at,
                },
            );
        }

        Ok(())
    }

    pub async fn register(&self, expectation: Expectation) -> Result<(), CorrelationError> {
        self.store
            .save_expectation(&expectation)
            .await
            .map_err(CorrelationError::Store)?;

        let mut index = self.index.write().await;
        index.insert(
            CorrelationLookupKey {
                correlation_key: expectation.correlation_key,
                expected_message_type: expectation.expected_message_type,
            },
            IndexedExpectation {
                expectation_id: expectation.id,
                timeout_at: expectation.timeout_at,
            },
        );

        Ok(())
    }

    pub async fn match_response(
        &self,
        key: CorrelationLookupKey,
        matched_tx_id: String,
    ) -> Result<bool, CorrelationError> {
        let expectation = {
            let mut index = self.index.write().await;
            index.remove(&key)
        };

        let Some(expectation) = expectation else {
            return Ok(false);
        };

        self.store
            .update_expectation(
                &expectation.expectation_id,
                ExpUpdate {
                    state: Some("MATCHED".to_string()),
                    matched_tx_id: Some(matched_tx_id),
                },
            )
            .await
            .map_err(CorrelationError::Store)?;

        Ok(true)
    }

    pub async fn timeout_scan(&self, now: SystemTime) -> Result<Vec<String>, CorrelationError> {
        let timed_out = {
            let mut index = self.index.write().await;
            let mut timed_out = Vec::new();

            index.retain(|_, value| {
                if value.timeout_at <= now {
                    timed_out.push(value.expectation_id.clone());
                    false
                } else {
                    true
                }
            });

            timed_out
        };

        for id in &timed_out {
            self.store
                .update_expectation(
                    id,
                    ExpUpdate {
                        state: Some("TIMED_OUT".to_string()),
                        matched_tx_id: None,
                    },
                )
                .await
                .map_err(CorrelationError::Store)?;
        }

        Ok(timed_out)
    }

    pub fn spawn_timeout_worker(self: Arc<Self>, interval: Duration) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                ticker.tick().await;
                if let Err(error) = self.timeout_scan(SystemTime::now()).await {
                    tracing::error!(error = %error, "correlation timeout scan failed");
                }
            }
        });
    }

    pub async fn pending_count(&self) -> usize {
        self.index.read().await.len()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CorrelationError {
    #[error(transparent)]
    Store(mx20022_store::StoreError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use mx20022_store::Store;
    use mx20022_store_sqlite::SqliteStore;

    use crate::{CorrelationEngine, CorrelationLookupKey};

    #[tokio::test]
    async fn register_match_and_timeout() {
        let store: Arc<dyn Store> =
            Arc::new(SqliteStore::new("sqlite::memory:").expect("sqlite store should initialize"));
        let engine = CorrelationEngine::new(store)
            .await
            .expect("engine should build");

        engine
            .register(mx20022_store::Expectation {
                id: "EXP-1".to_string(),
                correlation_key: "MSG-1".to_string(),
                expected_message_type: "pacs.002".to_string(),
                timeout_at: SystemTime::now() + Duration::from_millis(5),
            })
            .await
            .expect("register should work");

        let matched = engine
            .match_response(
                CorrelationLookupKey {
                    correlation_key: "MSG-1".to_string(),
                    expected_message_type: "pacs.002".to_string(),
                },
                "TX-1".to_string(),
            )
            .await
            .expect("match should work");
        assert!(matched);

        engine
            .register(mx20022_store::Expectation {
                id: "EXP-2".to_string(),
                correlation_key: "MSG-2".to_string(),
                expected_message_type: "pacs.002".to_string(),
                timeout_at: SystemTime::now() + Duration::from_millis(1),
            })
            .await
            .expect("register should work");

        tokio::time::sleep(Duration::from_millis(10)).await;
        let timed_out = engine
            .timeout_scan(SystemTime::now())
            .await
            .expect("timeout scan should work");

        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], "EXP-2");
    }
}
