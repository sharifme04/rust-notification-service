pub mod email;
pub mod webhook;

pub enum ChannelOutcome {
    Ok,
    Err(String),
}
