use sqlx::PgPool;
use uuid::Uuid;

use crate::pb::Channel;

pub async fn route_channels(db: &PgPool, user_id: Uuid) -> anyhow::Result<Vec<Channel>> {
    let row: Option<(Vec<String>,)> =
        sqlx::query_as(r#"SELECT channels::text[] FROM user_preferences WHERE user_id = $1"#)
            .bind(user_id)
            .fetch_optional(db)
            .await?;

    let channels = row
        .map(|(c,)| c)
        .unwrap_or_else(|| vec!["in_app".to_string()]);

    Ok(channels.into_iter().filter_map(parse).collect())
}

fn parse(s: String) -> Option<Channel> {
    match s.as_str() {
        "email" => Some(Channel::Email),
        "webhook" => Some(Channel::Webhook),
        "in_app" => Some(Channel::InApp),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_channels() {
        assert!(matches!(parse("email".into()), Some(Channel::Email)));
        assert!(matches!(parse("webhook".into()), Some(Channel::Webhook)));
        assert!(matches!(parse("in_app".into()), Some(Channel::InApp)));
    }

    #[test]
    fn unknown_channel_is_dropped() {
        assert!(parse("sms".into()).is_none());
    }
}
