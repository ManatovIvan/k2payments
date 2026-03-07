// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use std::any::Any;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crate::state::{LifecycleState, StateError};

#[derive(Debug, Clone)]
pub struct ContextMeta {
    pub transaction_id: String,
    pub received_at: SystemTime,
    pub pipeline: String,
    pub source_channel: String,
    pub message_type: String,
    pub raw_message: String,
}

#[derive(Debug, Clone)]
pub struct ContextAuditEntry {
    pub key: String,
    pub writer: String,
    pub written_at: SystemTime,
}

#[derive(Debug)]
struct ValueMutation {
    entry: ContextAuditEntry,
    value: Box<dyn Any + Send + Sync>,
}

#[derive(Debug)]
pub struct Context {
    meta: ContextMeta,
    state: LifecycleState,
    started_at: SystemTime,
    values: HashMap<String, Vec<ValueMutation>>,
    audit_log: Vec<ContextAuditEntry>,
}

impl Context {
    pub fn new(meta: ContextMeta) -> Self {
        let now = SystemTime::now();
        Self {
            meta,
            state: LifecycleState::Received,
            started_at: now,
            values: HashMap::new(),
            audit_log: Vec::new(),
        }
    }

    pub fn transaction_id(&self) -> &str {
        &self.meta.transaction_id
    }

    pub fn pipeline(&self) -> &str {
        &self.meta.pipeline
    }

    pub fn source_channel(&self) -> &str {
        &self.meta.source_channel
    }

    pub fn message_type(&self) -> &str {
        &self.meta.message_type
    }

    pub fn raw_message(&self) -> &str {
        &self.meta.raw_message
    }

    pub fn received_at(&self) -> SystemTime {
        self.meta.received_at
    }

    pub fn state(&self) -> LifecycleState {
        self.state
    }

    pub fn transition_to(&mut self, next: LifecycleState) -> Result<(), ContextError> {
        self.state = self
            .state
            .transition(next)
            .map_err(ContextError::InvalidStateTransition)?;
        Ok(())
    }

    pub fn put<T: Any + Send + Sync>(&mut self, key: impl Into<String>, value: T) {
        self.put_with_writer(key, "runtime", value);
    }

    pub fn put_with_writer<T: Any + Send + Sync>(
        &mut self,
        key: impl Into<String>,
        writer: impl Into<String>,
        value: T,
    ) {
        let key = key.into();
        let entry = ContextAuditEntry {
            key: key.clone(),
            writer: writer.into(),
            written_at: SystemTime::now(),
        };

        self.values.entry(key).or_default().push(ValueMutation {
            entry: entry.clone(),
            value: Box::new(value),
        });

        self.audit_log.push(entry);
    }

    pub fn get<T: Any + Send + Sync>(&self, key: &str) -> Result<&T, ContextError> {
        let last = self
            .values
            .get(key)
            .and_then(|history| history.last())
            .ok_or_else(|| ContextError::MissingKey(key.to_string()))?;

        last.value
            .downcast_ref::<T>()
            .ok_or_else(|| ContextError::TypeMismatch {
                key: key.to_string(),
                requested: std::any::type_name::<T>(),
            })
    }

    pub fn get_or_none<T: Any + Send + Sync>(&self, key: &str) -> Option<&T> {
        self.values
            .get(key)
            .and_then(|history| history.last())
            .and_then(|mutation| mutation.value.downcast_ref::<T>())
    }

    pub fn audit_log(&self) -> &[ContextAuditEntry] {
        &self.audit_log
    }

    pub fn key_history(&self, key: &str) -> Option<Vec<ContextAuditEntry>> {
        self.values.get(key).map(|history| {
            history
                .iter()
                .map(|m| m.entry.clone())
                .collect::<Vec<ContextAuditEntry>>()
        })
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at
            .elapsed()
            .unwrap_or_else(|_| Duration::from_secs(0))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("context key not found: {0}")]
    MissingKey(String),
    #[error("context type mismatch for key `{key}` (requested {requested})")]
    TypeMismatch {
        key: String,
        requested: &'static str,
    },
    #[error(transparent)]
    InvalidStateTransition(StateError),
}
