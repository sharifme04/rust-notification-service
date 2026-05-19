use std::time::Duration;

use super::ChannelOutcome;
use crate::pb::Payload;

pub struct WebhookChannel {
    client: reqwest::Client,
}

impl WebhookChannel {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client");
        Self { client }
    }

    pub async fn send(&self, payload: &Payload) -> ChannelOutcome {
        let url = match payload.metadata.get("webhook_url") {
            Some(u) => u.clone(),
            None => return ChannelOutcome::Err("missing metadata.webhook_url".into()),
        };

        let body = serde_json::json!({
            "title": payload.title,
            "body": payload.body,
            "metadata": payload.metadata,
        });

        match self.client.post(&url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => ChannelOutcome::Ok,
            Ok(resp) => ChannelOutcome::Err(format!("webhook returned {}", resp.status())),
            Err(e) => ChannelOutcome::Err(format!("webhook: {e}")),
        }
    }
}

impl Default for WebhookChannel {
    fn default() -> Self {
        Self::new()
    }
}
