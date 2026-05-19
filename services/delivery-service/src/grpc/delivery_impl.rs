use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::channels::{email::EmailChannel, webhook::WebhookChannel, ChannelOutcome};
use crate::pb::{
    delivery_service_server::DeliveryService, Channel, DeliverBatchRequest, DeliverBatchResponse,
    DeliverRequest, DeliverResponse, DeliveryStatus, DeliveryStatusRequest, DeliveryStatusResponse,
};

pub struct DeliveryServiceImpl {
    email: EmailChannel,
    webhook: WebhookChannel,
}

impl DeliveryServiceImpl {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            email: EmailChannel::from_env()?,
            webhook: WebhookChannel::new(),
        })
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

        tracing::info!(
            notification_id = %req.notification_id,
            user_id = %req.user_id,
            channel = ?channel,
            "received Deliver RPC"
        );

        let payload = req.payload.unwrap_or_default();
        let outcome = match channel {
            Channel::Email => self.email.send(&req.user_id, &payload).await,
            Channel::Webhook => self.webhook.send(&payload).await,
            Channel::InApp => ChannelOutcome::Ok,
            Channel::Unspecified => ChannelOutcome::Err("channel unspecified".into()),
        };

        let (status, error_message) = match outcome {
            ChannelOutcome::Ok => (DeliveryStatus::StatusDelivered, String::new()),
            ChannelOutcome::Err(e) => (DeliveryStatus::StatusFailed, e),
        };

        Ok(Response::new(DeliverResponse {
            delivery_id,
            status: status as i32,
            error_message,
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
