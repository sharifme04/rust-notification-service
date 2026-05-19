use axum::{response::IntoResponse, routing::get, Router};
use prometheus::{Encoder, TextEncoder};

use crate::AppState;

pub fn router() -> Router {
    Router::new().route("/metrics", get(metrics_handler))
}

async fn metrics_handler() -> impl IntoResponse {
    let metrics = prometheus::gather();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    if encoder.encode(&metrics, &mut buf).is_err() {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, String::new());
    }
    (
        axum::http::StatusCode::OK,
        String::from_utf8(buf).unwrap_or_default(),
    )
}

// AppState is not used yet but kept so the metrics router can later be merged
// with the main router if we want to apply state-bearing layers.
#[allow(dead_code)]
fn _unused(_: AppState) {}
