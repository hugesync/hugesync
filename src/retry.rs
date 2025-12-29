//! Retry logic with exponential backoff and jitter

use crate::config::Config;
use crate::error::{Error, Result};
use std::time::Duration;
use tokio::time::sleep;

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retries
    pub max_retries: u32,
    /// Base delay between retries in milliseconds
    pub base_delay_ms: u64,
    /// Maximum delay between retries in milliseconds
    pub max_delay_ms: u64,
    /// Jitter factor (0.0 to 1.0)
    pub jitter: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 1000,
            max_delay_ms: 30000,
            jitter: 0.25,
        }
    }
}

impl From<&Config> for RetryConfig {
    fn from(config: &Config) -> Self {
        Self {
            max_retries: config.max_retries,
            base_delay_ms: config.retry_delay_ms,
            ..Default::default()
        }
    }
}

/// Execute an async operation with retry logic
pub async fn with_retry<F, Fut, T>(config: &RetryConfig, operation: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                if !is_retryable(&e) {
                    return Err(e);
                }

                last_error = Some(e);

                if attempt < config.max_retries {
                    let delay = calculate_delay(config, attempt);
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_retries = config.max_retries,
                        delay_ms = delay.as_millis(),
                        "Operation failed, retrying"
                    );
                    sleep(delay).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| Error::storage("max retries exceeded")))
}

/// Execute a sync operation with retry logic
pub fn with_retry_sync<F, T>(config: &RetryConfig, mut operation: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        match operation() {
            Ok(result) => return Ok(result),
            Err(e) => {
                if !is_retryable(&e) {
                    return Err(e);
                }

                last_error = Some(e);

                if attempt < config.max_retries {
                    let delay = calculate_delay(config, attempt);
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_retries = config.max_retries,
                        delay_ms = delay.as_millis(),
                        "Operation failed, retrying"
                    );
                    std::thread::sleep(delay);
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| Error::storage("max retries exceeded")))
}

/// Check if an error is retryable
pub fn is_retryable(error: &Error) -> bool {
    match error {
        // Network errors are generally retryable
        Error::Network { .. } => true,
        
        // AWS errors - check for throttling, timeout, etc.
        Error::Aws { message } => {
            let msg = message.to_lowercase();
            msg.contains("throttl")
                || msg.contains("timeout")
                || msg.contains("connection")
                || msg.contains("503")
                || msg.contains("500")
                || msg.contains("429")
        }
        
        // I/O errors - some are retryable
        Error::Io { source, .. } => {
            use std::io::ErrorKind;
            matches!(
                source.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::TimedOut
                    | ErrorKind::Interrupted
            )
        }
        
        // Multipart upload errors - usually retryable
        Error::MultipartUpload { .. } => true,
        
        // Not retryable by default
        _ => false,
    }
}

/// Calculate delay with exponential backoff and jitter
fn calculate_delay(config: &RetryConfig, attempt: u32) -> Duration {
    // Exponential backoff: base * 2^attempt
    let exponential = config.base_delay_ms.saturating_mul(1 << attempt);
    let capped = std::cmp::min(exponential, config.max_delay_ms);

    // Add jitter
    let jitter_range = (capped as f64 * config.jitter) as u64;
    let jitter = if jitter_range > 0 {
        rand_jitter(jitter_range)
    } else {
        0
    };

    Duration::from_millis(capped.saturating_add(jitter))
}

/// Generate random jitter (simple LCG)
fn rand_jitter(max: u64) -> u64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos % max
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delay_calculation() {
        let config = RetryConfig {
            base_delay_ms: 1000,
            max_delay_ms: 30000,
            jitter: 0.0, // No jitter for testing
            ..Default::default()
        };

        assert_eq!(calculate_delay(&config, 0), Duration::from_millis(1000));
        assert_eq!(calculate_delay(&config, 1), Duration::from_millis(2000));
        assert_eq!(calculate_delay(&config, 2), Duration::from_millis(4000));
        assert_eq!(calculate_delay(&config, 5), Duration::from_millis(30000)); // Capped
    }

    #[test]
    fn test_is_retryable() {
        let io_err = Error::io("test", std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
        assert!(is_retryable(&io_err));

        let aws_err = Error::Aws {
            message: "ThrottlingException".to_string(),
        };
        assert!(is_retryable(&aws_err));

        let config_err = Error::config("bad config");
        assert!(!is_retryable(&config_err));
    }
}
