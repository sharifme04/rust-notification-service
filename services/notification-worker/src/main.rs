use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

mod consumer;
mod db;
mod kafka;
mod router;

pub mod pb {
    tonic::include_proto!("notification.v1");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let group_id = std::env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "notification-worker".into());
    let delivery_url =
        std::env::var("DELIVERY_SERVICE_URL").unwrap_or_else(|_| "http://localhost:50051".into());

    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .context("connect Postgres")?;

    let delivery =
        pb::delivery_service_client::DeliveryServiceClient::connect(delivery_url.clone())
            .await
            .with_context(|| format!("connect delivery service at {delivery_url}"))?;

    let producer = kafka::producer::KafkaProducer::new(&brokers)?;

    tracing::info!(%brokers, %group_id, "notification-worker starting");

    consumer::run(&brokers, &group_id, db, delivery, producer).await
}
