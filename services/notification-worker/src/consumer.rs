use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::pb::delivery_service_client::DeliveryServiceClient;
use crate::pb::{Channel, DeliverRequest, Payload};
use crate::router::route_channels;

#[derive(Debug, Deserialize)]
struct NotificationCreated {
    notification_id: Uuid,
    user_id: Uuid,
    title: String,
    body: String,
    #[serde(default)]
    metadata: std::collections::HashMap<String, serde_json::Value>,
}

pub async fn run(
    brokers: &str,
    group_id: &str,
    db: PgPool,
    mut delivery: DeliveryServiceClient<tonic::transport::Channel>,
) -> anyhow::Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&["notification.created"])?;

    tracing::info!("subscribed to notification.created");

    loop {
        match consumer.recv().await {
            Err(e) => tracing::error!(error = %e, "kafka recv"),
            Ok(msg) => {
                let payload = match msg.payload() {
                    Some(b) => b,
                    None => continue,
                };
                let event: NotificationCreated = match serde_json::from_slice(payload) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(error = %e, "skip malformed event");
                        continue;
                    }
                };

                if let Err(e) = handle(&db, &mut delivery, event).await {
                    tracing::error!(error = %e, "handle failed");
                }
            }
        }
    }
}

async fn handle(
    db: &PgPool,
    delivery: &mut DeliveryServiceClient<tonic::transport::Channel>,
    event: NotificationCreated,
) -> anyhow::Result<()> {
    let channels = route_channels(db, event.user_id).await?;
    tracing::info!(
        notification_id = %event.notification_id,
        user_id = %event.user_id,
        channels = ?channels,
        "routing"
    );

    let metadata: std::collections::HashMap<String, String> = event
        .metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect();

    for channel in channels {
        let req = DeliverRequest {
            notification_id: event.notification_id.to_string(),
            user_id: event.user_id.to_string(),
            channel: channel as i32,
            payload: Some(Payload {
                title: event.title.clone(),
                body: event.body.clone(),
                metadata: metadata.clone(),
            }),
            options: None,
        };

        match delivery.deliver(req).await {
            Ok(resp) => tracing::info!(?resp, "delivery ok"),
            Err(e) => tracing::error!(error = %e, ?channel, "delivery rpc failed"),
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn channel_label(c: Channel) -> &'static str {
    match c {
        Channel::Email => "email",
        Channel::Webhook => "webhook",
        Channel::InApp => "in_app",
        Channel::Unspecified => "unspecified",
    }
}
