use std::sync::Arc;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::Deserialize;
use uuid::Uuid;

use crate::websocket::hub::WsHub;

#[derive(Debug, Deserialize)]
struct DeliveryEvent {
    notification_id: String,
    user_id: String,
    channel: Option<String>,
    status: Option<String>,
    attempt_count: Option<u32>,
}

/// Runs forever, consuming delivery.completed and delivery.failed and
/// broadcasting each event to the relevant WebSocket client.
pub async fn run(brokers: &str, ws_hub: Arc<WsHub>) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "api-gateway-ws-bridge")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "create ws-bridge consumer failed");
            return;
        }
    };

    if let Err(e) = consumer.subscribe(&["delivery.completed", "delivery.failed"]) {
        tracing::error!(error = %e, "subscribe ws-bridge failed");
        return;
    }

    tracing::info!("ws-bridge subscribed to delivery.completed / delivery.failed");

    loop {
        match consumer.recv().await {
            Err(e) => tracing::error!(error = %e, "ws-bridge kafka recv"),
            Ok(msg) => {
                let Some(payload) = msg.payload() else {
                    continue;
                };
                let Ok(event) = serde_json::from_slice::<DeliveryEvent>(payload) else {
                    continue;
                };
                let Ok(user_id) = Uuid::parse_str(&event.user_id) else {
                    continue;
                };
                let broadcast_msg = serde_json::json!({
                    "notification_id": event.notification_id,
                    "channel": event.channel,
                    "status": event.status,
                    "attempt_count": event.attempt_count,
                });
                ws_hub.broadcast(user_id, broadcast_msg.to_string());
            }
        }
    }
}
