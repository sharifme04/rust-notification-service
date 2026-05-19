use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

pub struct KafkaProducer {
    inner: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> anyhow::Result<Self> {
        let inner: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;
        Ok(Self { inner })
    }

    pub async fn publish(
        &self,
        topic: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(value)?;
        let record = FutureRecord::to(topic).key(key).payload(&payload);

        self.inner
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| anyhow::anyhow!(e))?;
        Ok(())
    }
}
