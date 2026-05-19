use sqlx::PgPool;
use uuid::Uuid;

/// Creates a delivery_job row and returns its generated id.
pub async fn create_delivery_job(
    db: &PgPool,
    notification_id: Uuid,
    channel: &str,
) -> anyhow::Result<Uuid> {
    let row: (Uuid,) = sqlx::query_as(
        r#"
        INSERT INTO delivery_jobs (notification_id, channel, status, max_retries)
        VALUES ($1, $2::delivery_channel, 'queued', 3)
        RETURNING id
        "#,
    )
    .bind(notification_id)
    .bind(channel)
    .fetch_one(db)
    .await?;
    Ok(row.0)
}

/// Updates the status (and optionally delivered_at) of a delivery_job.
pub async fn update_delivery_job(
    db: &PgPool,
    job_id: Uuid,
    status: &str,
    attempt_count: i32,
    error_message: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE delivery_jobs
        SET status = $2::delivery_status,
            attempt_count = $3,
            error_message = $4,
            delivered_at = CASE WHEN $2 = 'delivered' THEN NOW() ELSE NULL END,
            updated_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(job_id)
    .bind(status)
    .bind(attempt_count)
    .bind(error_message)
    .execute(db)
    .await?;
    Ok(())
}
