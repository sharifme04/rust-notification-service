use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::jobs;
use crate::kafka::producer::KafkaProducer;
use crate::pb::delivery_service_client::DeliveryServiceClient;
use crate::pb::{Channel, DeliverRequest, DeliveryOptions, Payload};
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
    producer: KafkaProducer,
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
                if let Err(e) = handle(&db, &mut delivery, &producer, event).await {
                    tracing::error!(error = %e, "handle failed");
                }
            }
        }
    }
}

async fn handle(
    db: &PgPool,
    delivery: &mut DeliveryServiceClient<tonic::transport::Channel>,
    producer: &KafkaProducer,
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

    let channel_names: Vec<&str> = channels
        .iter()
        .map(|c| match c {
            Channel::Email => "email",
            Channel::Webhook => "webhook",
            Channel::InApp => "in_app",
            Channel::Unspecified => "unspecified",
        })
        .collect();

    // Create delivery_job rows and collect (channel, job_id) pairs
    let mut jobs: Vec<(Channel, Uuid)> = Vec::new();
    for (ch, name) in channels.iter().zip(channel_names.iter()) {
        match jobs::create_delivery_job(db, event.notification_id, name).await {
            Ok(job_id) => jobs.push((*ch, job_id)),
            Err(e) => tracing::error!(error = %e, channel = name, "create delivery_job failed"),
        }
    }

    // Publish notification.routed
    let routed_event = serde_json::json!({
        "event_id": Uuid::new_v4(),
        "notification_id": event.notification_id,
        "user_id": event.user_id,
        "channels": channel_names,
        "timestamp": Utc::now(),
    });
    if let Err(e) = producer
        .publish(
            "notification.routed",
            &event.notification_id.to_string(),
            &routed_event,
        )
        .await
    {
        tracing::warn!(error = %e, "publish notification.routed failed");
    }

    // Call delivery-service gRPC for each job
    for (channel, job_id) in jobs {
        let req = DeliverRequest {
            notification_id: event.notification_id.to_string(),
            user_id: event.user_id.to_string(),
            channel: channel as i32,
            payload: Some(Payload {
                title: event.title.clone(),
                body: event.body.clone(),
                metadata: metadata.clone(),
            }),
            options: Some(DeliveryOptions {
                max_retries: 3,
                retry_delay_ms: 200,
                timeout_ms: 10_000,
            }),
            delivery_job_id: job_id.to_string(),
        };

        match delivery.deliver(req).await {
            Ok(resp) => {
                let r = resp.into_inner();
                let status_str = if r.status == 2 { "delivered" } else { "failed" };
                let error_opt = if r.error_message.is_empty() {
                    None
                } else {
                    Some(r.error_message.as_str())
                };
                if let Err(e) = jobs::update_delivery_job(
                    db,
                    job_id,
                    status_str,
                    r.attempt_count as i32,
                    error_opt,
                )
                .await
                {
                    tracing::error!(error = %e, "update delivery_job failed");
                }
                tracing::info!(
                    job_id = %job_id,
                    status = status_str,
                    attempts = r.attempt_count,
                    "delivery done"
                );
            }
            Err(e) => {
                tracing::error!(error = %e, job_id = %job_id, "delivery rpc failed");
                let _ =
                    jobs::update_delivery_job(db, job_id, "failed", 1, Some(&e.to_string())).await;
            }
        }
    }

    Ok(())
}
