#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

//! Bounded asynchronous rate limiting for Tokio tasks.
//!
//! [`TokenBucket`] gates the start of work at a configured rate. It contains
//! no work queue and spawns no background task: callers await one permit
//! immediately before starting one operation.

use std::num::NonZeroU32;
use std::time::{Duration, Instant};

/// A bounded token bucket for asynchronous rate limiting.
///
/// The bucket starts full, allowing an initial burst up to its configured
/// capacity. After that, one token becomes available at each rate interval,
/// up to the capacity. The bucket stores only its counters and timestamps; it
/// never stores waiting operations or payloads.
pub struct TokenBucket {
    capacity: u32,
    refill_interval: Duration,
    tokens: u32,
    last_refill: Instant,
}

impl TokenBucket {
    /// Creates a bucket whose burst capacity equals its per-second rate.
    ///
    /// The bucket starts full. For example, a rate of four permits per second
    /// allows four immediate acquisitions, then spaces later acquisitions by
    /// approximately 250 milliseconds.
    #[must_use]
    pub fn new(permits_per_second: NonZeroU32) -> Self {
        Self::with_burst(permits_per_second, permits_per_second)
    }

    /// Creates a bucket with an independent sustained rate and burst capacity.
    ///
    /// `permits_per_second` controls how quickly tokens return. `burst_capacity`
    /// controls the maximum number of immediately available permits.
    #[must_use]
    pub fn with_burst(permits_per_second: NonZeroU32, burst_capacity: NonZeroU32) -> Self {
        let rate = u64::from(permits_per_second.get());
        let interval_nanos = (1_000_000_000 / rate).max(1);
        Self {
            capacity: burst_capacity.get(),
            refill_interval: Duration::from_nanos(interval_nanos),
            tokens: burst_capacity.get(),
            last_refill: Instant::now(),
        }
    }

    /// Waits for and consumes one permit.
    ///
    /// The future performs no allocation and does not enqueue work. If it is
    /// cancelled while waiting, no permit is consumed.
    pub async fn acquire(&mut self) {
        loop {
            let now = Instant::now();
            self.refill(now);
            if self.tokens > 0 {
                self.tokens -= 1;
                return;
            }

            tokio::time::sleep(self.time_until_next_token(now)).await;
        }
    }

    fn refill(&mut self, now: Instant) {
        if self.tokens >= self.capacity {
            return;
        }

        let elapsed = now.duration_since(self.last_refill);
        let interval_nanos = self.refill_interval.as_nanos();
        let elapsed_intervals = elapsed.as_nanos() / interval_nanos;
        if elapsed_intervals == 0 {
            return;
        }

        let missing = u128::from(self.capacity - self.tokens);
        let added = elapsed_intervals.min(missing);
        self.tokens += u32::try_from(added).expect("refill cannot exceed bucket capacity");

        if added == missing {
            self.last_refill = now;
        } else {
            let elapsed_nanos = interval_nanos.saturating_mul(added);
            let elapsed_duration = Duration::from_nanos(
                u64::try_from(elapsed_nanos).expect("refill duration must fit in a Duration"),
            );
            self.last_refill += elapsed_duration;
        }
    }

    fn time_until_next_token(&self, now: Instant) -> Duration {
        let elapsed = now.duration_since(self.last_refill);
        let remainder_nanos = elapsed.as_nanos() % self.refill_interval.as_nanos();
        let wait_nanos = self.refill_interval.as_nanos() - remainder_nanos;
        Duration::from_nanos(
            u64::try_from(wait_nanos).expect("refill interval must fit in a Duration"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::TokenBucket;
    use std::num::NonZeroU32;
    use std::time::Duration;

    fn non_zero(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).expect("test rate must be non-zero")
    }

    #[tokio::test(start_paused = true)]
    async fn starts_with_burst_capacity() {
        let mut bucket = TokenBucket::with_burst(non_zero(4), non_zero(2));

        bucket.acquire().await;
        bucket.acquire().await;

        let mut pending = Box::pin(bucket.acquire());
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut pending)
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refills_at_the_configured_rate() {
        let mut bucket = TokenBucket::with_burst(non_zero(4), non_zero(1));
        bucket.acquire().await;

        let mut pending = Box::pin(bucket.acquire());
        tokio::time::advance(Duration::from_millis(249)).await;
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut pending)
                .await
                .is_err()
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        pending.await;
    }

    #[tokio::test(start_paused = true)]
    async fn excess_elapsed_time_does_not_exceed_burst_capacity() {
        let mut bucket = TokenBucket::with_burst(non_zero(2), non_zero(2));
        bucket.acquire().await;
        bucket.acquire().await;
        tokio::time::advance(Duration::from_secs(10)).await;

        bucket.acquire().await;
        bucket.acquire().await;

        let mut pending = Box::pin(bucket.acquire());
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut pending)
                .await
                .is_err()
        );
    }
}
