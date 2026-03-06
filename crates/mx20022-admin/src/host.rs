use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};

use crate::controller::{AdminController, AdminControllerError};

#[derive(Clone)]
struct HostState {
    controller: Arc<dyn AdminController>,
}

pub async fn serve(addr: &str, controller: Arc<dyn AdminController>) -> Result<(), HostError> {
    let state = HostState { controller };

    let router = Router::new()
        .route("/health", get(get_health))
        .route("/ready", get(get_ready))
        .route("/status", get(get_status))
        .route("/tx/:tx_id", get(get_tx))
        .route("/metrics", get(get_metrics))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| HostError::Bind(addr.to_string(), e.to_string()))?;

    axum::serve(listener, router)
        .await
        .map_err(|e| HostError::Serve(e.to_string()))
}

async fn get_metrics() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        mx20022_metrics::gather(),
    )
}

async fn get_health(
    State(state): State<HostState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    state
        .controller
        .get_health()
        .await
        .map(|dto| {
            Json(serde_json::json!({
                "ok": dto.ok,
            }))
        })
        .map_err(map_error)
}

async fn get_ready(
    State(state): State<HostState>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    require_auth(&headers)?;

    state
        .controller
        .get_ready()
        .await
        .map(|dto| {
            let status = if dto.ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (
                status,
                Json(serde_json::json!({
                    "ready": dto.ready,
                    "details": dto.details,
                })),
            )
        })
        .map_err(map_error)
}

async fn get_status(
    State(state): State<HostState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    require_auth(&headers)?;

    state
        .controller
        .get_status()
        .await
        .map(|dto| {
            Json(serde_json::json!({
                "runtime": dto.runtime,
                "pipelines": dto.pipelines,
                "channels": dto.channels,
                "store": dto.store,
            }))
        })
        .map_err(map_error)
}

async fn get_tx(
    State(state): State<HostState>,
    Path(tx_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    require_auth(&headers)?;
    require_not_readonly(&headers)?;

    state
        .controller
        .get_transaction(&tx_id)
        .await
        .map(|dto| {
            Json(serde_json::json!({
                "tx_id": dto.tx_id,
                "pipeline": dto.pipeline,
                "message_type": dto.message_type,
                "state": dto.state,
                "received_at": dto.received_at,
                "completed_at": dto.completed_at,
            }))
        })
        .map_err(map_error)
}

fn require_auth(headers: &HeaderMap) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let Some(auth) = headers.get("authorization") else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "missing bearer token"})),
        ));
    };

    let auth = auth.to_str().unwrap_or_default();
    if !auth.starts_with("Bearer ") {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "missing bearer token"})),
        ));
    }

    Ok(())
}

fn require_not_readonly(headers: &HeaderMap) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let auth = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .unwrap_or_default();

    if auth.trim() == "Bearer readonly" {
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "forbidden"})),
        ));
    }

    Ok(())
}

fn map_error(error: AdminControllerError) -> (StatusCode, Json<serde_json::Value>) {
    match error {
        AdminControllerError::NotFound => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "not found"})),
        ),
        AdminControllerError::Forbidden => (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "forbidden"})),
        ),
        AdminControllerError::Internal(message) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": message})),
        ),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HostError {
    #[error("failed to bind admin host on {0}: {1}")]
    Bind(String, String),
    #[error("failed to serve admin host: {0}")]
    Serve(String),
}
