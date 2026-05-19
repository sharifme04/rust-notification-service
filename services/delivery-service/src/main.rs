use std::net::SocketAddr;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

mod channels;
mod grpc;
mod kafka;
mod retry;

pub mod pb {
    tonic::include_proto!("notification.v1");
}

use grpc::delivery_impl::DeliveryServiceImpl;
use pb::delivery_service_server::DeliveryServiceServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "50051".into())
        .parse()
        .context("invalid GRPC_PORT")?;
    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    let kafka_brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());

    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("connect Postgres")?;

    let service = DeliveryServiceImpl::from_env(db, &kafka_brokers)?;

    tracing::info!(%addr, "delivery-service starting");

    Server::builder()
        .add_service(DeliveryServiceServer::new(service))
        .serve(addr)
        .await
        .context("gRPC server failed")?;

    Ok(())
}
