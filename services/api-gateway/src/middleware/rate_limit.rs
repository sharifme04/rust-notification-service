use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{ConnectInfo, Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use dashmap::DashMap;

use crate::AppState;

struct Window {
    count: u32,
    reset_at: Instant,
}

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<DashMap<IpAddr, Window>>,
    max_per_second: u32,
}

impl RateLimiter {
    pub fn new(max_per_second: u32) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            max_per_second,
        }
    }
}

pub async fn rate_limit_middleware(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let limiter = &state.rate_limiter;
    let ip = addr.ip();
    let now = Instant::now();

    let allow = {
        let mut entry = limiter.inner.entry(ip).or_insert(Window {
            count: 0,
            reset_at: now + Duration::from_secs(1),
        });
        if now >= entry.reset_at {
            entry.count = 1;
            entry.reset_at = now + Duration::from_secs(1);
            true
        } else if entry.count < limiter.max_per_second {
            entry.count += 1;
            true
        } else {
            false
        }
    };

    if allow {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::TOO_MANY_REQUESTS)
    }
}
