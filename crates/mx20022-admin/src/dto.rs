// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponseDto {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyResponseDto {
    pub ready: bool,
    pub details: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponseDto {
    pub runtime: String,
    pub pipelines: Vec<String>,
    pub channels: Vec<String>,
    pub store: String,
    pub uptime_ms: String,
    pub store_ok: bool,
    pub store_details: Option<String>,
    pub in_flight_count: usize,
    pub pending_correlation_count: usize,
    pub dead_letter_count: usize,
    pub config_version: String,
    pub last_reload_result: Option<String>,
    pub last_reload_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResponseDto {
    pub tx_id: String,
    pub pipeline: String,
    pub message_type: String,
    pub state: String,
    pub received_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReloadResponseDto {
    pub reloaded: bool,
    pub details: String,
}
