use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::middleware::auth::AuthUser;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/notifications",
            post(create_notification).get(list_notifications),
        )
        .route("/api/v1/notifications/{id}", get(get_notification))
        .route(
            "/api/v1/preferences",
            get(get_preferences).put(upsert_preferences),
        )
}

// ─── Notification models ─────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateNotification {
    pub user_id: Uuid,
    pub event_type: String,
    pub title: String,
    pub body: String,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct Notification {
    pub id: Uuid,
    pub user_id: Uuid,
    pub event_type: String,
    pub title: String,
    pub body: String,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ─── Preferences models ───────────────────────────────────────────────────────

#[derive(Deserialize, Serialize, sqlx::FromRow)]
pub struct UserPreferences {
    pub user_id: Uuid,
    pub email: Option<String>,
    pub webhook_url: Option<String>,
}

#[derive(Deserialize)]
pub struct UpsertPreferences {
    pub email: Option<String>,
    pub webhook_url: Option<String>,
    pub channels: Option<Vec<String>>,
}

// ─── Handlers ────────────────────────────────────────────────────────────────

async fn create_notification(
    State(state): State<AppState>,
    _auth: AuthUser,
    Json(req): Json<CreateNotification>,
) -> Result<Json<Notification>, (StatusCode, String)> {
    let row = sqlx::query_as::<_, Notification>(
        r#"
        INSERT INTO notifications (user_id, event_type, title, body, metadata)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, user_id, event_type, title, body, metadata, created_at, updated_at
        "#,
    )
    .bind(req.user_id)
    .bind(&req.event_type)
    .bind(&req.title)
    .bind(&req.body)
    .bind(&req.metadata)
    .fetch_one(&state.db)
    .await
    .map_err(internal)?;

    let event = serde_json::json!({
        "event_id": Uuid::new_v4(),
        "notification_id": row.id,
        "user_id": row.user_id,
        "event_type": row.event_type,
        "title": row.title,
        "body": row.body,
        "metadata": row.metadata,
        "timestamp": Utc::now(),
    });

    if let Err(e) = state
        .kafka
        .publish("notification.created", &row.id.to_string(), &event)
        .await
    {
        tracing::error!(error = %e, "kafka publish failed");
    }

    Ok(Json(row))
}

async fn get_notification(
    State(state): State<AppState>,
    _auth: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<Notification>, (StatusCode, String)> {
    let row = sqlx::query_as::<_, Notification>(
        r#"
        SELECT id, user_id, event_type, title, body, metadata, created_at, updated_at
        FROM notifications WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .map_err(internal)?
    .ok_or((StatusCode::NOT_FOUND, "not found".into()))?;
    Ok(Json(row))
}

#[derive(Deserialize)]
pub struct ListQuery {
    pub user_id: Option<Uuid>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    50
}

async fn list_notifications(
    State(state): State<AppState>,
    _auth: AuthUser,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<Notification>>, (StatusCode, String)> {
    let limit = q.limit.clamp(1, 200);
    let rows = if let Some(user_id) = q.user_id {
        sqlx::query_as::<_, Notification>(
            r#"
            SELECT id, user_id, event_type, title, body, metadata, created_at, updated_at
            FROM notifications WHERE user_id = $1
            ORDER BY created_at DESC LIMIT $2 OFFSET $3
            "#,
        )
        .bind(user_id)
        .bind(limit)
        .bind(q.offset)
        .fetch_all(&state.db)
        .await
    } else {
        sqlx::query_as::<_, Notification>(
            r#"
            SELECT id, user_id, event_type, title, body, metadata, created_at, updated_at
            FROM notifications
            ORDER BY created_at DESC LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(q.offset)
        .fetch_all(&state.db)
        .await
    }
    .map_err(internal)?;

    Ok(Json(rows))
}

async fn get_preferences(
    State(state): State<AppState>,
    auth: AuthUser,
) -> Result<Json<UserPreferences>, (StatusCode, String)> {
    let user_id: Uuid = auth
        .0
        .sub
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid user_id in token".into()))?;
    let row = sqlx::query_as::<_, UserPreferences>(
        "SELECT user_id, email, webhook_url FROM user_preferences WHERE user_id = $1",
    )
    .bind(user_id)
    .fetch_optional(&state.db)
    .await
    .map_err(internal)?
    .unwrap_or(UserPreferences {
        user_id,
        email: None,
        webhook_url: None,
    });
    Ok(Json(row))
}

async fn upsert_preferences(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<UpsertPreferences>,
) -> Result<Json<UserPreferences>, (StatusCode, String)> {
    let user_id: Uuid = auth
        .0
        .sub
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid user_id in token".into()))?;
    let default_channels = vec!["in_app".to_string()];
    let channels_pg: &[String] = body.channels.as_deref().unwrap_or(&default_channels);

    let row = sqlx::query_as::<_, UserPreferences>(
        r#"
        INSERT INTO user_preferences (user_id, email, webhook_url, channels)
        VALUES ($1, $2, $3, $4::delivery_channel[])
        ON CONFLICT (user_id) DO UPDATE
            SET email = EXCLUDED.email,
                webhook_url = EXCLUDED.webhook_url,
                channels = EXCLUDED.channels,
                updated_at = NOW()
        RETURNING user_id, email, webhook_url
        "#,
    )
    .bind(user_id)
    .bind(body.email.as_deref())
    .bind(body.webhook_url.as_deref())
    .bind(channels_pg)
    .fetch_one(&state.db)
    .await
    .map_err(internal)?;

    Ok(Json(row))
}

fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}
