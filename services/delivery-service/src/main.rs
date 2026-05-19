use std::net::SocketAddr;

use anyhow::Context;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

mod channels;
mod grpc;
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

    let service = DeliveryServiceImpl::from_env()?;

    tracing::info!(%addr, "delivery-service starting");

    Server::builder()
        .add_service(DeliveryServiceServer::new(service))
        .serve(addr)
        .await
        .context("gRPC server failed")?;

    Ok(())
}
