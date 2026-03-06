#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

#[derive(Debug, Clone, Copy)]
pub struct Route {
    pub method: HttpMethod,
    pub path: &'static str,
    pub handler: &'static str,
}

pub const ADMIN_ROUTES: &[Route] = &[
    Route {
        method: HttpMethod::Get,
        path: "/health",
        handler: "get_health",
    },
    Route {
        method: HttpMethod::Get,
        path: "/ready",
        handler: "get_ready",
    },
    Route {
        method: HttpMethod::Get,
        path: "/status",
        handler: "get_status",
    },
    Route {
        method: HttpMethod::Get,
        path: "/tx/{txId}",
        handler: "get_transaction",
    },
];
