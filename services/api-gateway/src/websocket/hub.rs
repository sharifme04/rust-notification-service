use dashmap::DashMap;
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Default)]
pub struct WsHub {
    channels: DashMap<Uuid, broadcast::Sender<String>>,
}

impl WsHub {
    pub fn subscribe(&self, user_id: Uuid) -> broadcast::Receiver<String> {
        let tx = self
            .channels
            .entry(user_id)
            .or_insert_with(|| broadcast::channel::<String>(64).0)
            .clone();
        tx.subscribe()
    }

    pub fn broadcast(&self, user_id: Uuid, msg: String) {
        if let Some(tx) = self.channels.get(&user_id) {
            let _ = tx.send(msg);
        }
    }
}
