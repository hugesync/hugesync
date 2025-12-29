//! Bandwidth limiting for rsync-compatible rate control
//!
//! Implements a token bucket algorithm for limiting I/O bandwidth.
//! Supports:
//! - --bwlimit=RATE: Limit bandwidth in KiB/s

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Bandwidth limiter using token bucket algorithm
#[derive(Clone)]
pub struct BandwidthLimiter {
    inner: Arc<BandwidthLimiterInner>,
}

struct BandwidthLimiterInner {
    /// Rate limit in bytes per second (0 = unlimited)
    rate_bps: u64,
    /// Available tokens (bytes we can send)
    tokens: Mutex<f64>,
    /// Last time tokens were added
    last_update: Mutex<Instant>,
    /// Total bytes transferred
    bytes_transferred: AtomicU64,
}

impl BandwidthLimiter {
    /// Create a new bandwidth limiter
    ///
    /// # Arguments
    /// * `rate_kbps` - Rate limit in KiB/s (0 = unlimited)
    pub fn new(rate_kbps: u64) -> Self {
        let rate_bps = rate_kbps * 1024; // Convert KiB/s to bytes/s
        let initial_tokens = if rate_bps > 0 {
            rate_bps as f64 // Start with 1 second worth of tokens
        } else {
            f64::MAX
        };

        Self {
            inner: Arc::new(BandwidthLimiterInner {
                rate_bps,
                tokens: Mutex::new(initial_tokens),
                last_update: Mutex::new(Instant::now()),
                bytes_transferred: AtomicU64::new(0),
            }),
        }
    }

    /// Create an unlimited bandwidth limiter
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// Check if this limiter has a rate limit
    pub fn is_limited(&self) -> bool {
        self.inner.rate_bps > 0
    }

    /// Get the rate limit in bytes per second
    pub fn rate_bps(&self) -> u64 {
        self.inner.rate_bps
    }

    /// Wait until we have permission to transfer `bytes` bytes
    ///
    /// Returns immediately if unlimited or enough tokens available.
    pub async fn acquire(&self, bytes: usize) {
        if !self.is_limited() {
            self.inner.bytes_transferred.fetch_add(bytes as u64, Ordering::Relaxed);
            return;
        }

        let bytes_f64 = bytes as f64;

        loop {
            // Add tokens based on elapsed time
            {
                let mut last_update = self.inner.last_update.lock().await;
                let mut tokens = self.inner.tokens.lock().await;

                let elapsed = last_update.elapsed().as_secs_f64();
                let new_tokens = elapsed * self.inner.rate_bps as f64;
                *tokens = (*tokens + new_tokens).min(self.inner.rate_bps as f64 * 2.0); // Max 2 seconds burst
                *last_update = Instant::now();

                if *tokens >= bytes_f64 {
                    *tokens -= bytes_f64;
                    self.inner.bytes_transferred.fetch_add(bytes as u64, Ordering::Relaxed);
                    return;
                }
            }

            // Not enough tokens, wait for some to accumulate
            let wait_time = bytes_f64 / self.inner.rate_bps as f64;
            tokio::time::sleep(Duration::from_secs_f64(wait_time.min(0.1))).await;
        }
    }

    /// Get the total bytes transferred through this limiter
    pub fn bytes_transferred(&self) -> u64 {
        self.inner.bytes_transferred.load(Ordering::Relaxed)
    }
}

/// Wrapper stream that applies bandwidth limiting
pub struct LimitedStream<S> {
    inner: S,
    limiter: BandwidthLimiter,
}

impl<S> LimitedStream<S> {
    pub fn new(inner: S, limiter: BandwidthLimiter) -> Self {
        Self { inner, limiter }
    }
}

impl<S> futures::Stream for LimitedStream<S>
where
    S: futures::Stream<Item = Result<bytes::Bytes, crate::error::Error>> + Unpin,
{
    type Item = Result<bytes::Bytes, crate::error::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::pin::Pin;
        use std::task::Poll;

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                let len = bytes.len();
                let limiter = self.limiter.clone();

                // For simplicity, we don't actually wait here in poll_next
                // The acquire() should be called by the consumer
                // This is a simplified implementation
                limiter.inner.bytes_transferred.fetch_add(len as u64, Ordering::Relaxed);

                Poll::Ready(Some(Ok(bytes)))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_unlimited() {
        let limiter = BandwidthLimiter::unlimited();
        assert!(!limiter.is_limited());

        limiter.acquire(1024 * 1024).await; // Should return immediately
        assert_eq!(limiter.bytes_transferred(), 1024 * 1024);
    }

    #[tokio::test]
    async fn test_limited() {
        let limiter = BandwidthLimiter::new(100); // 100 KiB/s
        assert!(limiter.is_limited());
        assert_eq!(limiter.rate_bps(), 100 * 1024);
    }

    #[tokio::test]
    async fn test_acquire_small() {
        let limiter = BandwidthLimiter::new(1024); // 1 MiB/s

        // Small transfers should be nearly instant
        let start = Instant::now();
        limiter.acquire(1024).await; // 1 KiB
        assert!(start.elapsed() < Duration::from_millis(100));
    }
}
