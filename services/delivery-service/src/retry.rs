use std::time::Duration;

/// Returns exponential backoff delay: base_ms * 2^attempt, capped at attempt=10.
pub fn backoff_delay(attempt: u32, base_ms: u64) -> Duration {
    let factor = 2u64.saturating_pow(attempt.min(10));
    Duration::from_millis(base_ms.saturating_mul(factor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_each_attempt() {
        assert_eq!(backoff_delay(0, 100), Duration::from_millis(100));
        assert_eq!(backoff_delay(1, 100), Duration::from_millis(200));
        assert_eq!(backoff_delay(2, 100), Duration::from_millis(400));
        assert_eq!(backoff_delay(3, 100), Duration::from_millis(800));
    }

    #[test]
    fn backoff_saturates_instead_of_overflowing() {
        let d = backoff_delay(u32::MAX, 1000);
        assert!(d.as_secs() > 0);
    }
}
