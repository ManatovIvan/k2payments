use crate::controller::{AdminController, AdminControllerError};
use crate::middleware::{MiddlewareStage, DEFAULT_MIDDLEWARE_CHAIN};
use crate::routes::HttpMethod;

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub path: String,
    pub bearer_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

pub async fn dispatch(controller: &dyn AdminController, request: HttpRequest) -> HttpResponse {
    if let Err(response) = run_middleware(&request) {
        return response;
    }

    match (request.method, request.path.as_str()) {
        (HttpMethod::Get, "/health") => match controller.get_health().await {
            Ok(dto) => HttpResponse {
                status: 200,
                body: serde_json::to_string(&dto).unwrap_or_else(|_| "{}".to_string()),
            },
            Err(error) => map_controller_error(error),
        },
        (HttpMethod::Get, "/ready") => match controller.get_ready().await {
            Ok(dto) => HttpResponse {
                status: if dto.ready { 200 } else { 503 },
                body: serde_json::to_string(&dto).unwrap_or_else(|_| "{}".to_string()),
            },
            Err(error) => map_controller_error(error),
        },
        (HttpMethod::Get, "/status") => match controller.get_status().await {
            Ok(dto) => HttpResponse {
                status: 200,
                body: serde_json::to_string(&dto).unwrap_or_else(|_| "{}".to_string()),
            },
            Err(error) => map_controller_error(error),
        },
        (HttpMethod::Get, path) if path.starts_with("/tx/") => {
            let tx_id = path.trim_start_matches("/tx/");
            match controller.get_transaction(tx_id).await {
                Ok(dto) => HttpResponse {
                    status: 200,
                    body: serde_json::to_string(&dto).unwrap_or_else(|_| "{}".to_string()),
                },
                Err(error) => map_controller_error(error),
            }
        }
        _ => HttpResponse {
            status: 404,
            body: "{\"error\":\"not found\"}".to_string(),
        },
    }
}

fn run_middleware(request: &HttpRequest) -> Result<(), HttpResponse> {
    for stage in DEFAULT_MIDDLEWARE_CHAIN {
        match stage {
            MiddlewareStage::Authentication => {
                if request.path != "/health" && request.bearer_token.is_none() {
                    return Err(HttpResponse {
                        status: 401,
                        body: "{\"error\":\"missing bearer token\"}".to_string(),
                    });
                }
            }
            MiddlewareStage::Authorization => {
                if request.path.starts_with("/tx/")
                    && request.bearer_token.as_deref() == Some("readonly")
                {
                    return Err(HttpResponse {
                        status: 403,
                        body: "{\"error\":\"forbidden\"}".to_string(),
                    });
                }
            }
            MiddlewareStage::RateLimit => {}
            MiddlewareStage::Validation => {
                if request.path.trim().is_empty() {
                    return Err(HttpResponse {
                        status: 400,
                        body: "{\"error\":\"invalid path\"}".to_string(),
                    });
                }
            }
            MiddlewareStage::ErrorTransform => {}
            MiddlewareStage::StructuredLogging => {}
        }
    }

    Ok(())
}

fn map_controller_error(error: AdminControllerError) -> HttpResponse {
    match error {
        AdminControllerError::NotFound => HttpResponse {
            status: 404,
            body: "{\"error\":\"not found\"}".to_string(),
        },
        AdminControllerError::Forbidden => HttpResponse {
            status: 403,
            body: "{\"error\":\"forbidden\"}".to_string(),
        },
        AdminControllerError::Internal(message) => HttpResponse {
            status: 500,
            body: format!("{{\"error\":\"{}\"}}", message.replace('"', "'")),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use mx20022_store::{Store, TransactionRecord};
    use mx20022_store_sqlite::SqliteStore;

    use crate::http::{dispatch, HttpRequest};
    use crate::routes::HttpMethod;
    use crate::service::{RuntimeStatusSnapshot, StoreBackedAdminController};

    #[tokio::test]
    async fn dispatches_status_and_tx_routes() {
        let store: Arc<dyn Store> = Arc::new(SqliteStore::new("sqlite::memory:"));

        store
            .begin_transaction(&TransactionRecord {
                tx_id: "TX-E1".to_string(),
                pipeline: "demo".to_string(),
                source_channel: "http".to_string(),
                message_type: "pacs.008".to_string(),
                raw_message: "<Document/>".to_string(),
                state: "COMMITTED".to_string(),
                received_at: SystemTime::now(),
                completed_at: None,
                key_fields: HashMap::new(),
            })
            .await
            .expect("insert tx should succeed");

        let controller = StoreBackedAdminController::new(
            store,
            RuntimeStatusSnapshot {
                runtime: "test-runtime".to_string(),
                pipelines: vec!["demo".to_string()],
                channels: vec!["http".to_string()],
                store: "sqlite".to_string(),
            },
        );

        let status = dispatch(
            &controller,
            HttpRequest {
                method: HttpMethod::Get,
                path: "/status".to_string(),
                bearer_token: Some("admin".to_string()),
            },
        )
        .await;
        assert_eq!(status.status, 200);

        let tx = dispatch(
            &controller,
            HttpRequest {
                method: HttpMethod::Get,
                path: "/tx/TX-E1".to_string(),
                bearer_token: Some("admin".to_string()),
            },
        )
        .await;
        assert_eq!(tx.status, 200);
    }
}
