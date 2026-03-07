// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

//! Admin API contract, route map, controller traits, and middleware stages.

pub mod auth;
pub mod controller;
pub mod dto;
pub mod grpc;
pub mod host;
pub mod http;
pub mod middleware;
pub mod rate_limit;
pub mod routes;
pub mod service;
pub mod tls;
