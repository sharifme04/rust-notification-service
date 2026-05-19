use axum::{routing::get, Router};

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
}

async fn ready(axum::extract::State(state): axum::extract::State<AppState>) -> &'static str {
    // Cheap readiness check: ping the DB.
    match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.db)
        .await
    {
        Ok(_) => "ready",
        Err(_) => "not ready",
    }
}
