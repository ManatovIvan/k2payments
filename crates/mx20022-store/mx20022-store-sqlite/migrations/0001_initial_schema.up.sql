-- Copyright (C) 2026 mx20022-runtime contributors
-- SPDX-License-Identifier: AGPL-3.0-only

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS transactions (
    tx_id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL,
    source_channel TEXT NOT NULL,
    message_type TEXT NOT NULL,
    raw_message TEXT NOT NULL,
    state TEXT NOT NULL,
    received_at TEXT NOT NULL,
    completed_at TEXT,
    key_fields_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS context_mutations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id TEXT NOT NULL,
    key TEXT NOT NULL,
    writer TEXT NOT NULL,
    written_at TEXT NOT NULL,
    FOREIGN KEY(tx_id) REFERENCES transactions(tx_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS expectations (
    id TEXT PRIMARY KEY,
    correlation_key TEXT NOT NULL,
    expected_message_type TEXT NOT NULL,
    timeout_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'PENDING',
    matched_tx_id TEXT
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id TEXT PRIMARY KEY,
    tx_id TEXT NOT NULL UNIQUE,
    reason TEXT NOT NULL,
    failed_at TEXT NOT NULL,
    raw_message TEXT NOT NULL,
    FOREIGN KEY(tx_id) REFERENCES transactions(tx_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_transactions_msg_type_received
ON transactions(message_type, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_state_received
ON transactions(state, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_context_mutations_tx_written
ON context_mutations(tx_id, written_at ASC);

CREATE INDEX IF NOT EXISTS idx_expectations_state_timeout
ON expectations(state, timeout_at ASC);
