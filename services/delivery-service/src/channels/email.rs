use lettre::message::Mailbox;
use lettre::transport::smtp::client::Tls;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use super::ChannelOutcome;
use crate::pb::Payload;

pub struct EmailChannel {
    mailer: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
}

impl EmailChannel {
    pub fn from_env() -> anyhow::Result<Self> {
        let host = std::env::var("SMTP_HOST").unwrap_or_else(|_| "localhost".into());
        let port: u16 = std::env::var("SMTP_PORT")
            .unwrap_or_else(|_| "1025".into())
            .parse()?;
        let from_addr =
            std::env::var("SMTP_FROM").unwrap_or_else(|_| "notifications@example.com".into());

        let mailer = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&host)
            .port(port)
            .tls(Tls::None)
            .build();

        Ok(Self {
            mailer,
            from: from_addr.parse()?,
        })
    }

    pub async fn send(&self, to: &str, payload: &Payload) -> ChannelOutcome {
        // `to` is a user_id; a real impl would look up the user's email in user_preferences.
        // For local-dev demo we synthesise an address so MailHog can capture it.
        let to_addr: Mailbox = match to.parse() {
            Ok(a) => a,
            Err(_) => match format!("user-{to}@example.com").parse() {
                Ok(a) => a,
                Err(e) => return ChannelOutcome::Err(format!("bad address: {e}")),
            },
        };

        let email = match Message::builder()
            .from(self.from.clone())
            .to(to_addr)
            .subject(payload.title.clone())
            .body(payload.body.clone())
        {
            Ok(m) => m,
            Err(e) => return ChannelOutcome::Err(format!("build email: {e}")),
        };

        match self.mailer.send(email).await {
            Ok(_) => ChannelOutcome::Ok,
            Err(e) => ChannelOutcome::Err(format!("smtp: {e}")),
        }
    }
}
