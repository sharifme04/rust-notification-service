use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::Router;
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

mod kafka;
mod metrics;
mod middleware;
mod routes;
mod websocket;

use kafka::producer::KafkaProducer;
use middleware::rate_limit::{rate_limit_middleware, RateLimiter};
use websocket::hub::WsHub;

#[derive(Clone)]
pub struct AppState {
    pub db: sqlx::PgPool,
    pub kafka: Arc<KafkaProducer>,
    pub ws_hub: Arc<WsHub>,
    pub jwt_secret: String,
    pub rate_limiter: RateLimiter,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()?;
    let metrics_port: u16 = std::env::var("METRICS_PORT")
        .unwrap_or_else(|_| "9090".into())
        .parse()?;
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    let kafka_brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let jwt_secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| "local-dev-secret".into());
    let rate_limit_rps: u32 = std::env::var("RATE_LIMIT_RPS")
        .unwrap_or_else(|_| "200".into())
        .parse()
        .unwrap_or(200);

    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .context("connect Postgres")?;

    let kafka = Arc::new(KafkaProducer::new(&kafka_brokers)?);
    let ws_hub = Arc::new(WsHub::default());

    // Background: consume delivery.completed/failed → broadcast to WS clients
    {
        let hub = ws_hub.clone();
        let brokers = kafka_brokers.clone();
        tokio::spawn(async move {
            kafka::consumer::run(&brokers, hub).await;
        });
    }

    let state = AppState {
        db,
        kafka,
        ws_hub,
        jwt_secret,
        rate_limiter: RateLimiter::new(rate_limit_rps),
    };

    let app = Router::new()
        .merge(routes::health::router())
        .merge(routes::notifications::router())
        .merge(websocket::handler::router())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware,
        ))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
        .into_make_service_with_connect_info::<SocketAddr>();

    let metrics_router = metrics::router();

    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;
    let metrics_addr: SocketAddr = format!("0.0.0.0:{metrics_port}").parse()?;

    tracing::info!(%addr, %metrics_addr, rate_limit_rps, "api-gateway starting");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let metrics_listener = tokio::net::TcpListener::bind(metrics_addr).await?;

    tokio::try_join!(
        async {
            axum::serve(listener, app)
                .await
                .map_err(anyhow::Error::from)
        },
        async {
            axum::serve(metrics_listener, metrics_router.into_make_service())
                .await
                .map_err(anyhow::Error::from)
        },
    )?;

    Ok(())
}
