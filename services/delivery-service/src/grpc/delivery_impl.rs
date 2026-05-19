use std::time::Instant;

use chrono::Utc;
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::channels::{email::EmailChannel, webhook::WebhookChannel, ChannelOutcome};
use crate::kafka::producer::KafkaProducer;
use crate::pb::{
    delivery_service_server::DeliveryService, Channel, DeliverBatchRequest, DeliverBatchResponse,
    DeliverRequest, DeliverResponse, DeliveryStatus, DeliveryStatusRequest, DeliveryStatusResponse,
};
use crate::retry::backoff_delay;

pub struct DeliveryServiceImpl {
    email: EmailChannel,
    webhook: WebhookChannel,
    db: PgPool,
    kafka: KafkaProducer,
}

impl DeliveryServiceImpl {
    pub fn from_env(db: PgPool, brokers: &str) -> anyhow::Result<Self> {
        Ok(Self {
            email: EmailChannel::from_env()?,
            webhook: WebhookChannel::new(),
            db,
            kafka: KafkaProducer::new(brokers)?,
        })
    }

    async fn attempt_delivery(
        &self,
        channel: Channel,
        user_id: &str,
        payload: &crate::pb::Payload,
    ) -> ChannelOutcome {
        match channel {
            Channel::Email => self.email.send(user_id, payload).await,
            Channel::Webhook => self.webhook.send(payload).await,
            Channel::InApp => ChannelOutcome::Ok,
            Channel::Unspecified => ChannelOutcome::Err("channel unspecified".into()),
        }
    }

    async fn write_log(
        &self,
        delivery_job_id: Uuid,
        attempt_number: i32,
        outcome: &ChannelOutcome,
        duration_ms: i32,
    ) {
        let (status_str, error_detail) = match outcome {
            ChannelOutcome::Ok => ("delivered", None),
            ChannelOutcome::Err(e) => ("failed", Some(e.as_str())),
        };
        let _ = sqlx::query(
            r#"
            INSERT INTO delivery_logs
                (delivery_job_id, attempt_number, status, error_detail, duration_ms)
            VALUES ($1, $2, $3::delivery_status, $4, $5)
            "#,
        )
        .bind(delivery_job_id)
        .bind(attempt_number)
        .bind(status_str)
        .bind(error_detail)
        .bind(duration_ms)
        .execute(&self.db)
        .await
        .map_err(|e| tracing::error!(error = %e, "write delivery_log failed"));
    }

    async fn publish_completed(
        &self,
        req: &DeliverRequest,
        delivery_job_id: Option<Uuid>,
        status: DeliveryStatus,
        attempt_count: u32,
        error: &str,
    ) {
        let topic = if status == DeliveryStatus::StatusDelivered {
            "delivery.completed"
        } else {
            "delivery.failed"
        };
        let event = serde_json::json!({
            "event_id": Uuid::new_v4(),
            "delivery_job_id": delivery_job_id,
            "notification_id": req.notification_id,
            "user_id": req.user_id,
            "channel": req.channel,
            "status": format!("{:?}", status),
            "attempt_count": attempt_count,
            "error": error,
            "timestamp": Utc::now(),
        });
        if let Err(e) = self
            .kafka
            .publish(topic, &req.notification_id, &event)
            .await
        {
            tracing::warn!(error = %e, topic, "kafka publish failed");
        }
    }
}

#[tonic::async_trait]
impl DeliveryService for DeliveryServiceImpl {
    async fn deliver(
        &self,
        request: Request<DeliverRequest>,
    ) -> Result<Response<DeliverResponse>, Status> {
        let req = request.into_inner();
        let delivery_id = Uuid::new_v4().to_string();
        let channel = Channel::try_from(req.channel).unwrap_or(Channel::Unspecified);
        let delivery_job_id = Uuid::parse_str(&req.delivery_job_id).ok();

        tracing::info!(
            notification_id = %req.notification_id,
            user_id = %req.user_id,
            channel = ?channel,
            "Deliver RPC"
        );

        let options = req.options.as_ref();
        let max_retries = options.map(|o| o.max_retries).unwrap_or(3);
        let base_delay_ms = options.map(|o| o.retry_delay_ms as u64).unwrap_or(200);

        let payload = req.payload.clone().unwrap_or_default();
        let mut last_error = String::new();
        let mut attempt_count = 0u32;
        let mut final_status = DeliveryStatus::StatusFailed;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                tokio::time::sleep(backoff_delay(attempt - 1, base_delay_ms)).await;
            }
            attempt_count = attempt + 1;

            let t0 = Instant::now();
            let outcome = self.attempt_delivery(channel, &req.user_id, &payload).await;
            let duration_ms = t0.elapsed().as_millis() as i32;

            if let Some(jid) = delivery_job_id {
                self.write_log(jid, attempt_count as i32, &outcome, duration_ms)
                    .await;
            }

            match outcome {
                ChannelOutcome::Ok => {
                    final_status = DeliveryStatus::StatusDelivered;
                    last_error = String::new();
                    break;
                }
                ChannelOutcome::Err(e) => {
                    last_error = e;
                    tracing::warn!(
                        attempt,
                        error = %last_error,
                        channel = ?channel,
                        "delivery attempt failed"
                    );
                }
            }
        }

        self.publish_completed(
            &req,
            delivery_job_id,
            final_status,
            attempt_count,
            &last_error,
        )
        .await;

        Ok(Response::new(DeliverResponse {
            delivery_id,
            status: final_status as i32,
            error_message: last_error,
            attempt_count,
        }))
    }

    type DeliverBatchStream =
        tokio_stream::wrappers::ReceiverStream<Result<DeliverBatchResponse, Status>>;

    async fn deliver_batch(
        &self,
        request: Request<DeliverBatchRequest>,
    ) -> Result<Response<Self::DeliverBatchStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            for r in req.requests {
                let _ = tx
                    .send(Ok(DeliverBatchResponse {
                        notification_id: r.notification_id,
                        status: DeliveryStatus::StatusQueued as i32,
                    }))
                    .await;
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn get_delivery_status(
        &self,
        request: Request<DeliveryStatusRequest>,
    ) -> Result<Response<DeliveryStatusResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(DeliveryStatusResponse {
            delivery_id: req.delivery_id,
            status: DeliveryStatus::StatusUnspecified as i32,
            attempt_count: 0,
            last_error: String::new(),
        }))
    }
}
